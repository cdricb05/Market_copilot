r"""alpha_agent.alpha_recovery.merger_arb_events - the deal calendar for
``MERGER_ARBITRAGE_CASH_DEAL_TARGET_SPREAD_V1``: episodes, point-in-time terms and the
point-in-time target identity. It reads NO price return.

EPISODE
    Every merger-form document of one target CIK (a target merger proxy or information
    statement, the target's SC 14D9, or a third-party SC TO-T whose SEC header names the
    target as SUBJECT COMPANY), in filing order. A document more than ``EPISODE_GAP_DAYS``
    after the previous one starts a new episode.

THE ENTRY IS WHEN THE FILINGS THEMSELVES ESTABLISH THE DEAL - never later knowledge
    Walking an episode's documents in filing-date order, the ESTABLISHING date is the first
    filing date by which the documents filed so far, together:
      * state a cash price per share for ALL outstanding shares;
      * state NO stock, election, exchange-offer or contingent-value-right consideration and
        no partial offer (any such statement filed on or before that date excludes the
        episode permanently: a later document cannot rescue it);
      * state the target's trading symbol (tagged to the company itself, never to the
        counterparty) that resolves to EXACTLY ONE Norgate US equity quoted on that date.
    Nothing filed after the establishing date is read for inclusion, terms or identity.

IDENTITY
    Symbol statement from the target's own filing -> the Norgate security with that base
    ticker whose quoted range covers the establishing date. No CUSIP guess and no current
    ticker map. A REFUSAL-ONLY name guard then keeps a stated symbol only when its Norgate
    security name shares a distinctive token with the target's SEC conformed name: it can
    remove a candidate (an acquirer's symbol printed in a cash deal) and can never create a
    match. Zero candidates leave the episode UNRESOLVED; several leave it AMBIGUOUS.

RESEARCH ONLY. No purchase, subscription, registration, promotion, capital, order or fill.
"""
from __future__ import annotations

import collections
import csv
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from . import merger_arb_data as MD

CALCULATION_OWNER = "alpha_agent.alpha_recovery.merger_arb_events"

EPISODE_GAP_DAYS = 365
ESTABLISH_WINDOW_DAYS = 120
UNIVERSE_DATABASES = (("US Equities", "ACTIVE"), ("US Equities Delisted", "DELISTED"))
_DELIST_SUFFIX = re.compile(r"-(\d{6})$")

ST_INCLUDED = "INCLUDED_ALL_CASH"
ST_NOT_CASH = "EXCLUDED_NOT_AN_ALL_CASH_ALL_SHARES_DEAL"
ST_NOT_ESTABLISHED = "EXCLUDED_TERMS_NEVER_ESTABLISHED"
ST_UNRESOLVED = "UNRESOLVED_IDENTITY"
ST_AMBIGUOUS = "AMBIGUOUS_IDENTITY"
#: The filing says the target is quoted over the counter and no stated symbol is a listed Norgate
#: US equity: outside the investable (listed) universe, and NOT counted as an identity failure.
ST_OTC = "EXCLUDED_TARGET_QUOTED_OVER_THE_COUNTER"


# --------------------------------------------------------------------------- #
# 1. The Norgate US equity universe (metadata only)
# --------------------------------------------------------------------------- #
def universe_path() -> Path:
    return MD.data_dir() / "norgate_us_equity_universe.csv"


def base_ticker(symbol: str) -> str:
    return _DELIST_SUFFIX.sub("", str(symbol or "")).upper()


def build_universe(*, verbose: bool = True) -> dict:
    """Every active and delisted Norgate US equity: base ticker and quoted range."""
    import norgatedata as ng
    rows = []
    for db, status in UNIVERSE_DATABASES:
        syms = ng.database_symbols(db)
        for k, s in enumerate(syms):
            try:
                fq, lq = ng.first_quoted_date(s), ng.last_quoted_date(s)
                name = ng.security_name(s)
                exch = ng.exchange_name(s)
                sub = ng.subtype1(s)
            except Exception:                            # noqa: BLE001
                continue
            rows.append({"symbol": s, "base": base_ticker(s), "status": status,
                         "first_quoted": str(fq)[:10] if fq else "",
                         "last_quoted": str(lq)[:10] if lq else "",
                         "name": name or "", "exchange": exch or "", "subtype": sub or ""})
            if verbose and k % 5000 == 0:
                print("  universe %s %d/%d" % (db, k, len(syms)), flush=True)
    p = universe_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    tmp.replace(p)
    return {"securities": len(rows),
            "by_status": dict(collections.Counter(r["status"] for r in rows))}


def load_universe(path: Optional[Path] = None) -> dict:
    """``{base ticker: [security rows]}``."""
    out = collections.defaultdict(list)
    with Path(path or universe_path()).open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            out[r["base"]].append(r)
    return dict(out)


def securities_quoted_on(universe: dict, ticker: str, day: str) -> list:
    t = str(ticker or "").upper().replace("-", ".")
    return [r for r in universe.get(t, [])
            if r["first_quoted"] and r["first_quoted"] <= day <= (r["last_quoted"] or "9999")]


# --------------------------------------------------------------------------- #
# 2. Episodes
# --------------------------------------------------------------------------- #
def documents_by_target(index_rows: list, parsed: dict) -> dict:
    """``{target CIK: [document]}`` in filing-date order. Pure."""
    by = collections.defaultdict(dict)
    for r in index_rows:
        rec = parsed.get(r["accession"])
        if rec is None:
            continue
        if r["form"] in MD.FORMS_TENDER:
            target = ((rec.get("subject") or {}).get("cik") or "").lstrip("0")
            if not target or target != r["cik"]:
                continue                               # the bidder's copy of the index row
        else:
            target = r["cik"]
        filed = rec.get("filed_as_of") or r["date"]
        by[target][r["accession"]] = {"accession": r["accession"], "form": r["form"],
                                      "filed": filed, "company": r["company"],
                                      "acceptance_local": rec.get("acceptance_local"),
                                      "terms": rec.get("terms") or {}}
    return {cik: sorted(d.values(), key=lambda x: (x["filed"], x["accession"]))
            for cik, d in by.items()}


def episodes(docs_by_target: dict) -> list:
    out = []
    for cik, docs in docs_by_target.items():
        cur, last = [], None
        for d in docs:
            if last is not None and (date.fromisoformat(d["filed"]) - date.fromisoformat(last)).days \
                    > EPISODE_GAP_DAYS:
                out.append({"target_cik": cik, "documents": cur})
                cur = []
            cur.append(d)
            last = d["filed"]
        if cur:
            out.append({"target_cik": cik, "documents": cur})
    out.sort(key=lambda e: (e["documents"][0]["filed"], e["target_cik"]))
    return out


# --------------------------------------------------------------------------- #
# 3. Establishing the deal from the filings, in filing order
# --------------------------------------------------------------------------- #
def _name_tokens(s: str) -> set:
    stop = {"INC", "CORP", "CORPORATION", "CO", "COMPANY", "LTD", "LIMITED", "HOLDINGS", "GROUP",
            "THE", "PLC", "LP", "LLC", "COMMON", "STOCK", "CLASS", "SHARES", "ORDINARY", "NEW", "DE",
            "DEL", "AND", "OF", "TRUST", "INTERNATIONAL", "INTL"}
    return {t for t in re.findall(r"[A-Z0-9]+", str(s or "").upper()) if t not in stop and len(t) > 1}


def establish(episode: dict, universe: dict) -> dict:
    """The establishing date, terms and identity of ONE episode - or why there is none."""
    docs = episode["documents"]
    first = docs[0]["filed"]
    seen_price = seen_all = False
    flags = {"stock_consideration": False, "election": False, "partial_offer": False,
             "contingent_value_right": False}
    self_tickers, untagged_tickers = [], []
    otc_tickers, listed_tickers = set(), set()
    price, price_doc = None, None
    by_date = collections.OrderedDict()
    for d in docs:
        by_date.setdefault(d["filed"], []).append(d)
    base = {"target_cik": episode["target_cik"], "first_filed": first,
            "n_documents": len(docs), "forms": [d["form"] for d in docs],
            "company": docs[0]["company"]}
    for day, group in by_date.items():
        if (date.fromisoformat(day) - date.fromisoformat(first)).days > ESTABLISH_WINDOW_DAYS:
            break
        for d in group:
            t = d["terms"]
            for k in flags:
                flags[k] = flags[k] or bool(t.get(k))
            if t.get("cash_prices") and price is None:
                price, price_doc = t["cash_prices"][0]["price"], d["accession"]
            seen_price = seen_price or bool(t.get("cash_prices"))
            seen_all = seen_all or bool(t.get("all_shares"))
            for tk in t.get("tickers") or []:
                if tk["context"] == "COUNTERPARTY":
                    continue
                (self_tickers if tk["context"] == "SELF" else untagged_tickers).append(tk["ticker"])
                (otc_tickers if tk.get("venue") == "OTC" else listed_tickers).add(tk["ticker"])
        if any(flags.values()):
            return {**base, "state": ST_NOT_CASH, "decided_on": day, "flags": dict(flags)}
        if not (seen_price and seen_all):
            continue
        cands = sorted(set(self_tickers)) or sorted(set(untagged_tickers))
        quoted = {}
        for tk in cands:
            for s in securities_quoted_on(universe, tk, day):
                quoted[s["symbol"]] = s
        # REFUSAL-ONLY GUARD. A stated symbol is kept only when its Norgate security's name
        # shares a distinctive token with the target's own SEC conformed name. It can remove a
        # candidate (the acquirer's symbol printed in a cash deal) and can never add one.
        target_tokens = _name_tokens(base["company"])
        secs = {k: s for k, s in quoted.items() if target_tokens & _name_tokens(s["name"])}
        if len(secs) == 1:
            sec = next(iter(secs.values()))
            overlap = target_tokens & _name_tokens(sec["name"])
            return {**base, "state": ST_INCLUDED, "established_on": day,
                    "days_after_first_filing": (date.fromisoformat(day) - date.fromisoformat(first)).days,
                    "cash_price": price, "cash_price_accession": price_doc,
                    "symbol": sec["symbol"], "ticker": sec["base"], "norgate_name": sec["name"],
                    "norgate_status": sec["status"], "exchange": sec["exchange"],
                    "subtype": sec["subtype"], "ticker_source": "SELF" if self_tickers else "UNTAGGED",
                    "name_guard_tokens": sorted(overlap), "candidates_refused_by_name_guard":
                        sorted(set(quoted) - set(secs)), "flags": dict(flags)}
        if len(secs) > 1:
            return {**base, "state": ST_AMBIGUOUS, "decided_on": day, "candidates": sorted(secs)}
        if quoted:
            return {**base, "state": ST_UNRESOLVED, "decided_on": day,
                    "reason": "SYMBOL_REFUSED_BY_NAME_GUARD", "refused": sorted(quoted)}
        if cands and set(cands) <= otc_tickers - listed_tickers:
            # The filing itself says the target is quoted over the counter, and no stated symbol is
            # a listed Norgate US equity: outside the investable universe, not an identity failure.
            return {**base, "state": ST_OTC, "decided_on": day, "tickers_seen": sorted(set(cands))}
    if seen_price and seen_all:
        return {**base, "state": ST_UNRESOLVED, "tickers_seen": sorted(set(self_tickers + untagged_tickers))}
    return {**base, "state": ST_NOT_ESTABLISHED, "seen_price": seen_price, "seen_all_shares": seen_all}


def build_calendar(index_rows: list, parsed: dict, universe: dict) -> list:
    return [establish(e, universe) for e in episodes(documents_by_target(index_rows, parsed))]


def calendar_hash(calendar: list) -> str:
    """The ONE identity of the included deal list: (target CIK, establishing date, symbol), sorted."""
    from . import stable_hash
    rows = sorted([c["target_cik"], c["established_on"], c["symbol"]]
                  for c in calendar if c.get("state") == ST_INCLUDED)
    return stable_hash(rows)


__all__ = ["CALCULATION_OWNER", "EPISODE_GAP_DAYS", "ESTABLISH_WINDOW_DAYS", "build_universe",
           "load_universe", "base_ticker", "securities_quoted_on", "documents_by_target", "episodes",
           "establish", "build_calendar", "ST_INCLUDED", "ST_NOT_CASH", "ST_NOT_ESTABLISHED",
           "ST_UNRESOLVED", "ST_AMBIGUOUS"]
