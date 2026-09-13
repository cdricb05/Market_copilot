"""alpha_agent.alpha_recovery.event_8k_data - the SEC Form 8-K ITEM-CODE stream
and its census, for the 8-K event-type axis.

One source, published by the SEC itself, free, no key, no account, no licence:
the per-issuer EDGAR submissions histories ALREADY collected by the canonical
owner :mod:`alpha_agent.r63.acquire`. Nothing here spends money.

THIS MODULE COMPUTES NO RETURN AND READS NO PRICE. It turns filings into an
Item-coded stream and measures how much of the eligible universe that stream
can see. Keeping prices out of it is what makes "the cells were chosen before
any return existed" a property of the code rather than a claim - a test pins it.

WHY NO DOCUMENT IS NEEDED

EDGAR's submissions API publishes an ``items`` column: the Item codes the FILER
declared in the EDGAR header at submission. It is stamped at acceptance, so it
is point-in-time by construction, and it is structured, so an Item code is READ
and never inferred from filing text. Measured on the stored histories: populated
for 220,226 of 220,226 modern-era 8-K and 8-K/A filings (100%) across 1,080
issuer histories; 2 filings after the regime change still carry a legacy token.

FACTS THIS MODULE ENCODES, each of which silently corrupts the census if assumed

1. **Two Item vocabularies.** Before 2004-08-23 (SEC Release 33-8400) a current
   report's items were integers 1-12 with DIFFERENT meanings: legacy "5" was
   Other Events, while modern 5.xx is corporate governance. A token is admitted
   only in the modern ``d.dd`` form and only from the effective date; a legacy
   integer is counted and never mapped to a modern Item.

2. **Forms are matched EXACTLY.** The canonical keep-list admits anything that
   starts with ``8-K``, which includes the successor-registration forms
   ``8-K12B``, ``8-K12G3`` and ``8-K15D5``. Those are not current reports of an
   event. They are counted and excluded (measured: 97 in the stored histories).

3. **``acceptanceDateTime`` is UTC** (measured on the 13D/G axis against EDGAR's
   06:00-22:00 Eastern acceptance window). The one converter,
   :func:`control_block_data.acceptance_et`, is reused rather than re-derived.

4. **An accession can sit under several CIKs.** A parent and its registrant
   subsidiaries file ONE combined 8-K; EDGAR lists it in each history (measured:
   118 accessions sit under more than one stored CIK). The
   stream deduplicates on (issuer CIK, accession) - never on accession alone,
   which would hand the event to whichever history was read first.

RESEARCH ONLY. No purchase, no subscription, no promotion, no capital, no
proposal, no order, no fill, no backfill, no live write.
"""
from __future__ import annotations

import gzip
import json
import re
from collections import Counter, defaultdict

import numpy as np

import threading
import time
from concurrent import futures

from alpha_agent.r63 import EQUITY_DISCOVERY_START
from alpha_agent.r63 import acquire as ACQ
from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import panels as P
from alpha_agent.r63 import pit

from . import MIN_EFFECTIVE_PERIODS, now_iso, research_root, write_artifact
from . import control_block_data as CBD
from . import control_block_events as CBE

CALCULATION_OWNER = "alpha_agent.alpha_recovery.event_8k_data"
CENSUS_ARTIFACT = "event_8k_census.json"

#: The two current-report forms, matched exactly.
FORMS = ("8-K", "8-K/A")
ORIGINAL_FORM = "8-K"

#: SEC Release 33-8400: the modern Item numbering takes effect.
ITEM_REGIME_START = "2004-08-23"
_MODERN_ITEM = re.compile(r"^[1-9]\.[0-9]{2}$")

#: A US regular session, Eastern wall clock.
SESSION_OPEN_ET_HOUR = 9.5
SESSION_CLOSE_ET_HOUR = CBE.SESSION_CLOSE_ET_HOUR

#: The census grid: the canonical R63 equity grid at cadence 21.
CENSUS_CADENCE = 21

ITEM_TITLES = {
    "1.01": "Entry into a Material Definitive Agreement",
    "1.02": "Termination of a Material Definitive Agreement",
    "1.03": "Bankruptcy or Receivership",
    "2.01": "Completion of Acquisition or Disposition of Assets",
    "2.02": "Results of Operations and Financial Condition",
    "2.03": "Creation of a Direct Financial Obligation",
    "2.04": "Triggering Events That Accelerate a Direct Financial Obligation",
    "2.05": "Costs Associated with Exit or Disposal Activities",
    "2.06": "Material Impairments",
    "3.01": "Notice of Delisting or Failure to Satisfy a Continued Listing Rule; "
            "Transfer of Listing",
    "3.03": "Material Modification to Rights of Security Holders",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure/Election of Directors or Officers; Compensatory Arrangements",
    "5.03": "Amendments to Articles or Bylaws; Change in Fiscal Year",
    "5.07": "Submission of Matters to a Vote of Security Holders",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
    "9.01": "Financial Statements and Exhibits",
}

#: The Items the census must report individually.
CENSUS_ITEMS = ("1.01", "1.02", "2.01", "2.02", "2.05", "2.06", "3.01", "4.01",
                "4.02", "5.02", "7.01", "8.01")

#: Candidate cleaning rules, each defined ONLY by which other Item codes are
#: co-filed in the same report. (name, require any of, exclude any of). They are
#: measured so a cell's definition can be chosen on its sample and its
#: co-filing structure - never on a return.
COMPOSITES = (
    ("1.02 without 1.01", ("1.02",), ("1.01",)),
    ("1.02 without 1.01/2.01", ("1.02",), ("1.01", "2.01")),
    ("2.05 or 2.06", ("2.05", "2.06"), ()),
    ("2.05 or 2.06 without 2.02", ("2.05", "2.06"), ("2.02",)),
    ("2.05 without 2.02", ("2.05",), ("2.02",)),
    ("2.06 without 2.02", ("2.06",), ("2.02",)),
    ("3.01 without 2.01/3.03/5.01", ("3.01",), ("2.01", "3.03", "5.01")),
    ("4.01 without 2.01/5.01", ("4.01",), ("2.01", "5.01")),
    ("4.02 without 2.02", ("4.02",), ("2.02",)),
)


def data_dir():
    return research_root() / "_data_sec_8k"


def stream_path():
    return data_dir() / "filings_8k.jsonl"


# --------------------------------------------------------------------------- #
# Item codes
# --------------------------------------------------------------------------- #
def parse_items(raw) -> tuple:
    """(modern Items, legacy tokens), each sorted and de-duplicated.

    Only a ``d.dd`` token is a modern Item. Anything else - a legacy integer,
    an empty token - is returned separately and never coerced.
    """
    modern, legacy = set(), set()
    for tok in str(raw or "").split(","):
        s = tok.strip()
        if not s:
            continue
        (modern if _MODERN_ITEM.match(s) else legacy).add(s)
    return tuple(sorted(modern)), tuple(sorted(legacy))


def matches(items, require_any, exclude_any) -> bool:
    s = set(items)
    return bool(s & set(require_any)) and not (s & set(exclude_any))


# --------------------------------------------------------------------------- #
# The stream, from the already-acquired submissions histories
# --------------------------------------------------------------------------- #
def enumerate_8k(*, verbose: bool = True) -> dict:
    """Every 8-K and 8-K/A in the acquired per-issuer histories.

    Returns ``{"rows": [...], "history_ciks": set, "excluded_variant_forms":
    Counter}``. Deduplicated on (issuer CIK, accession): an issuer's ``recent``
    block and its older shard files overlap, and counting a filing twice would
    invent an event.
    """
    raw_dir = P.submissions_dir() / "raw"
    rows, seen = [], set()
    history_ciks: set = set()
    variants: Counter = Counter()
    files = sorted(raw_dir.glob("*.json.gz"))
    for k, p in enumerate(files):
        try:
            obj = json.loads(gzip.open(p, "rb").read().decode("utf-8"))
        except Exception:                                    # noqa: BLE001
            continue
        cik = str(obj.get("cik") or "").lstrip("0")
        if not cik:
            continue
        history_ciks.add(cik)
        blocks = [(obj.get("filings") or {}).get("recent") or {}]
        blocks += list(obj.get("_r63_older_blocks") or [])
        for b in blocks:
            fs = b.get("form") or []
            fd = b.get("filingDate") or []
            ad = b.get("acceptanceDateTime") or []
            an = b.get("accessionNumber") or []
            it = b.get("items") or []
            dc = b.get("primaryDocument") or []
            for i, f in enumerate(fs):
                form = str(f)
                if form not in FORMS:
                    if form.startswith("8-K"):
                        variants[form] += 1
                    continue
                acc = CBD._norm_accession(an[i] if i < len(an) else "")
                if not acc or (cik, acc) in seen:
                    continue
                seen.add((cik, acc))
                modern, legacy = parse_items(it[i] if i < len(it) else "")
                rows.append({
                    "issuer_cik": cik, "form": form,
                    "filing_date": str(fd[i])[:10] if i < len(fd) else "",
                    "acceptance_utc": str(ad[i]) if i < len(ad) and ad[i] else "",
                    "accession": acc, "items": list(modern),
                    "legacy_items": list(legacy),
                    "items_raw_present": bool(i < len(it) and it[i]),
                    "primary_document": str(dc[i]) if i < len(dc) else "",
                })
        if verbose and k % 200 == 0:
            print("  enumerate %d/%d 8-K=%d" % (k, len(files), len(rows)), flush=True)
    rows.sort(key=lambda r: (r["acceptance_utc"], r["issuer_cik"], r["accession"]))
    return {"rows": rows, "history_ciks": history_ciks, "excluded_variant_forms": variants}


def write_stream(rows: list) -> None:
    data_dir().mkdir(parents=True, exist_ok=True)
    stream_path().write_text("\n".join(json.dumps(r, sort_keys=True) for r in rows),
                             encoding="utf-8")


def load_stream() -> list:
    p = stream_path()
    if not p.exists():
        return []
    return [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]


# --------------------------------------------------------------------------- #
# The acceptance instant against the session calendar
# --------------------------------------------------------------------------- #
def session_class(dates64: np.ndarray, acceptance_utc: str) -> str:
    """Where the acceptance instant falls relative to a regular US session."""
    et = CBD.acceptance_et(acceptance_utc)
    if et is None:
        return "UNREADABLE"
    day = np.datetime64(et.date(), "D").astype("datetime64[ns]")
    pos = int(np.searchsorted(dates64, day, side="left"))
    if pos >= len(dates64) or dates64[pos] != day:
        return "NON_SESSION_DAY"
    hour = et.hour + et.minute / 60.0
    if hour < SESSION_OPEN_ET_HOUR:
        return "PRE_OPEN"
    if hour < SESSION_CLOSE_ET_HOUR:
        return "INTRADAY"
    return "AFTER_CLOSE"


def decision_session(dates64: np.ndarray, acceptance_utc: str) -> int | None:
    """:func:`control_block_events.decision_index`, refusing an instant BEFORE
    the calendar's first session.

    ``searchsorted`` maps any earlier instant to position 0, which would stack
    every pre-panel filing onto the first session. The 13D/G stream began after
    the panel did and never met this; the 8-K stream begins in 2004 and would.
    """
    et = CBD.acceptance_et(acceptance_utc)
    if et is None or np.datetime64(et.date(), "D").astype("datetime64[ns]") < dates64[0]:
        return None
    return CBE.decision_index(dates64, acceptance_utc)


# --------------------------------------------------------------------------- #
# Census
# --------------------------------------------------------------------------- #
def _new_acc() -> dict:
    return {"filings": 0, "originals": 0, "amendments": 0, "issuers": set(),
            "first": None, "last": None, "by_year": Counter(), "cofiled": Counter(),
            "in_panel": 0, "identified": 0, "eligible_filings": 0,
            "session": Counter(), "row_filings": 0, "events": set(),
            "events_all_forms": set()}


def _years_span(first: str | None, last_full_year: int) -> list:
    if not first:
        return []
    return list(range(max(int(first[:4]), int(ITEM_REGIME_START[:4]) + 1), last_full_year + 1))


def census(E: dict, elig: np.ndarray, stream: dict, *, verbose: bool = True) -> dict:
    """The Phase-1 census. Reads the eligibility mask and the calendar only."""
    dates64 = np.asarray(E["dates"], dtype="datetime64[ns]")
    n_t = len(dates64)
    cik2rows_all, id_meta = CBE.identity_map(E)
    hist = stream["history_ciks"]
    cik2rows = {c: r for c, r in cik2rows_all.items() if c in hist}
    disc_t = int(np.searchsorted(dates64, np.datetime64(EQUITY_DISCOVERY_START, "D")
                                 .astype("datetime64[ns]")))
    dec = pit.decision_indices(np.asarray(E["dates"]), EQUITY_DISCOVERY_START,
                               CENSUS_CADENCE, CENSUS_CADENCE)
    last_elig = np.full(elig.shape[0], -1, dtype=int)
    for i in range(elig.shape[0]):
        w = np.where(elig[i])[0]
        if len(w):
            last_elig[i] = int(w[-1])

    keys = list(CENSUS_ITEMS) + [c[0] for c in COMPOSITES]
    acc = {k: _new_acc() for k in keys}
    all_codes: Counter = Counter()
    tot = {"filings": 0, "originals": 0, "amendments": 0, "legacy_token_filings": 0,
           "pre_regime_filings": 0, "no_modern_item": 0, "only_9_01": 0,
           "accessions_under_multiple_ciks": 0}
    by_acc: dict = defaultdict(set)
    rows = stream["rows"]
    for r in rows:
        by_acc[r["accession"]].add(r["issuer_cik"])
        if r["filing_date"] < ITEM_REGIME_START:
            tot["pre_regime_filings"] += 1
            continue
        items = tuple(r["items"])
        tot["filings"] += 1
        tot["originals" if r["form"] == ORIGINAL_FORM else "amendments"] += 1
        tot["legacy_token_filings"] += int(bool(r["legacy_items"]))
        tot["no_modern_item"] += int(not items)
        tot["only_9_01"] += int(items == ("9.01",))
        for c in items:
            all_codes[c] += 1
        hit = [k for k in CENSUS_ITEMS if k in items]
        hit += [name for name, req, exc in COMPOSITES if matches(items, req, exc)]
        if not hit:
            continue
        t = decision_session(dates64, r["acceptance_utc"])
        klass = session_class(dates64, r["acceptance_utc"]) if t is not None else None
        cik = r["issuer_cik"]
        prow = cik2rows.get(cik) or []
        erows = [i for i in prow if t is not None and bool(elig[i, t])]
        year = r["filing_date"][:4]
        for k in hit:
            a = acc[k]
            a["filings"] += 1
            a["originals" if r["form"] == ORIGINAL_FORM else "amendments"] += 1
            a["issuers"].add(cik)
            a["first"] = r["filing_date"] if a["first"] is None else min(a["first"], r["filing_date"])
            a["last"] = r["filing_date"] if a["last"] is None else max(a["last"], r["filing_date"])
            a["by_year"][year] += 1
            for c in items:
                if c != k:
                    a["cofiled"][c] += 1
            if t is None:
                continue
            a["in_panel"] += 1
            a["session"][klass] += 1
            a["identified"] += int(bool(prow))
            a["eligible_filings"] += int(bool(erows))
            for i in erows:
                a["events_all_forms"].add((i, t))
                if r["form"] == ORIGINAL_FORM:
                    a["row_filings"] += 1
                    a["events"].add((i, t))
    tot["accessions_under_multiple_ciks"] = sum(1 for v in by_acc.values() if len(v) > 1)

    last_full_year = int(str(dates64[-1])[:4]) - 1
    out_items = {}
    for k in keys:
        a = acc[k]
        ev = sorted(a["events"], key=lambda e: (e[1], e[0]))
        ev_disc = [e for e in ev if e[1] >= disc_t]
        sessions_disc = sorted({e[1] for e in ev_disc})
        ev_by_year = Counter(str(dates64[t])[:4] for _i, t in ev)
        span = _years_span(a["first"], last_full_year)
        per_year = [a["by_year"].get(str(y), 0) for y in span]
        # events per canonical 21-session grid period, discovery window onward
        per_period = []
        ts = np.asarray([t for _i, t in ev_disc], dtype=int)
        for t in np.asarray(dec, dtype=int):
            per_period.append(int(((ts > t - CENSUS_CADENCE) & (ts <= t)).sum()))
        pp = np.asarray(per_period) if per_period else np.zeros(1)
        exits = [int(last_elig[i] - t <= 63 and last_elig[i] < n_t - 6) for i, t in ev]
        klass_n = sum(a["session"].values()) or 1
        out_items[k] = {
            "title": ITEM_TITLES.get(k, k),
            "filings": a["filings"], "originals": a["originals"],
            "amendments": a["amendments"],
            "amendment_rate": a["amendments"] / max(a["filings"], 1),
            "unique_issuers": len(a["issuers"]),
            "history_start": a["first"], "history_end": a["last"],
            "filings_per_year": {str(y): a["by_year"].get(str(y), 0) for y in span},
            "continuity": {
                "years": len(span),
                "years_with_zero": int(sum(1 for v in per_year if v == 0)),
                "min_per_year": int(min(per_year)) if per_year else 0,
                "max_per_year": int(max(per_year)) if per_year else 0,
            },
            "filings_in_panel_calendar": a["in_panel"],
            "identity_match_rate": a["identified"] / max(a["in_panel"], 1),
            "eligible_universe_match_rate": a["eligible_filings"] / max(a["in_panel"], 1),
            "session_share": {c: a["session"].get(c, 0) / klass_n
                              for c in ("PRE_OPEN", "INTRADAY", "AFTER_CLOSE",
                                        "NON_SESSION_DAY", "UNREADABLE")},
            "outside_regular_session_share":
                (a["session"].get("PRE_OPEN", 0) + a["session"].get("AFTER_CLOSE", 0)
                 + a["session"].get("NON_SESSION_DAY", 0)) / klass_n,
            "eligible_events_original_only": len(ev),
            "duplicate_rate": 1.0 - len(ev) / max(a["row_filings"], 1),
            "amendment_only_events": len(a["events_all_forms"] - a["events"]),
            "eligible_events_by_year": dict(sorted(ev_by_year.items())),
            "eligible_events_from_discovery_start": len(ev_disc),
            "unique_decision_sessions_from_discovery_start": len(sessions_disc),
            "effective_observations_h21": len(sessions_disc) / 21.0,
            "effective_observations_h63": len(sessions_disc) / 63.0,
            "events_per_grid_period": {"mean": float(pp.mean()), "median": float(np.median(pp)),
                                       "share_periods_with_event": float((pp > 0).mean()),
                                       "periods": int(len(per_period))},
            "leaves_eligible_universe_within_63_sessions":
                float(np.mean(exits)) if exits else None,
            "top_cofiled_items": [(c, n, n / max(a["filings"], 1))
                                  for c, n in a["cofiled"].most_common(8)],
        }
        if verbose:
            o = out_items[k]
            print("  %-28s filings=%6d issuers=%4d events=%5d sess=%5d effh21=%.1f "
                  "effh63=%.1f outside=%.2f amend=%.3f elig=%.3f"
                  % (k, o["filings"], o["unique_issuers"], o["eligible_events_original_only"],
                     o["unique_decision_sessions_from_discovery_start"],
                     o["effective_observations_h21"], o["effective_observations_h63"],
                     o["outside_regular_session_share"], o["amendment_rate"],
                     o["eligible_universe_match_rate"]), flush=True)

    coverage = CBE.coverage_report(E, elig, cik2rows, dec)
    coverage.pop("per_session", None)
    identified_rows_without_history = sum(
        len(r) for c, r in cik2rows_all.items() if c not in hist)
    return {
        "schema": "alpha_recovery_event_8k_census/1",
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": now_iso(),
        "returns_computed": False, "prices_read": False,
        "source": "EDGAR submissions histories (items column), acquired by alpha_agent.r63.acquire",
        "paid_dollars": 0,
        "item_regime_start": ITEM_REGIME_START,
        "panel": {"first_session": str(dates64[0])[:10], "last_session": str(dates64[-1])[:10],
                  "discovery_start": EQUITY_DISCOVERY_START,
                  "grid_cadence": CENSUS_CADENCE, "grid_periods": int(len(dec))},
        "stream": {**tot, "issuer_histories": len(hist),
                   "excluded_variant_forms": dict(stream["excluded_variant_forms"]),
                   "items_field_populated": float(np.mean([r["items_raw_present"] for r in rows
                                                           if r["filing_date"] >= ITEM_REGIME_START]))
                   if rows else None},
        "all_item_codes_modern_era": dict(all_codes.most_common()),
        "identity": {**id_meta,
                     "identified_rows_without_submissions_history": identified_rows_without_history,
                     "ciks_with_history_and_panel_row": len(cik2rows)},
        "coverage": coverage,
        "items": out_items,
    }


# --------------------------------------------------------------------------- #
# Sample sufficiency, in the canonical scorer's own units
# --------------------------------------------------------------------------- #
def oos_sufficiency(dates, event_ts, *, horizons=(5, 21, 63)) -> dict:
    """How many OUT-OF-SAMPLE periods can carry an event at all.

    The canonical scorer counts effective periods as ``periods x min(1,
    cadence / h)`` over every grid period it tests, whether or not that
    period's cross-section contains a single event. For a sparse event signal
    that overstates the evidence: a period with no event has an all-zero
    signal and says nothing about the event. This counts ONLY the tested
    periods (the canonical walk-forward's test positions) whose cadence window
    holds at least one eligible event, in the scorer's own formula, against the
    estate's existing floor. It reads dates and event sessions; no return.
    """
    ts = np.asarray(sorted({int(t) for t in event_ts}), dtype=int)
    out = {}
    for h in horizons:
        cad = X.cadence_for(int(h))
        dec = pit.decision_indices(dates, EQUITY_DISCOVERY_START, cad, int(h))
        folds = pit.walk_forward(dates, dec, horizon=int(h))
        test_pos = sorted({int(p) for f in folds for p in f["test"]})
        n_inf = 0
        for p in test_pos:
            t = int(dec[p])
            n_inf += int(np.searchsorted(ts, t, side="right")
                         - np.searchsorted(ts, t - cad, side="right") > 0)
        eff = int(n_inf * min(1.0, cad / float(h)))
        out[str(int(h))] = {"cadence": cad, "oos_periods": len(test_pos),
                            "oos_periods_with_event": n_inf,
                            "effective_informative_periods": eff,
                            "floor": MIN_EFFECTIVE_PERIODS,
                            "meets_floor": eff >= MIN_EFFECTIVE_PERIODS}
    return out


def eligible_originals(E: dict, elig: np.ndarray, rows: list, cik2rows: dict, *,
                       require_any, exclude_any=(), since: str = EQUITY_DISCOVERY_START) -> list:
    """Original 8-Ks carrying any of ``require_any`` and none of
    ``exclude_any``, accepted from ``since``, whose issuer maps to a panel row
    eligible at the decision session. Each carries its (row, t) pairs."""
    dates64 = np.asarray(E["dates"], dtype="datetime64[ns]")
    out = []
    for r in rows:
        if r["form"] != ORIGINAL_FORM or r["filing_date"] < since:
            continue
        if not matches(r["items"], require_any, exclude_any):
            continue
        t = decision_session(dates64, r["acceptance_utc"])
        if t is None:
            continue
        erows = [i for i in (cik2rows.get(r["issuer_cik"]) or []) if bool(elig[i, t])]
        if erows:
            out.append({**r, "t": int(t), "rows": erows})
    return out


# --------------------------------------------------------------------------- #
# Documents - read to check what an Item tag MEANS, never to pick a cell by return
# --------------------------------------------------------------------------- #
def docs_dir():
    return data_dir() / "docs"


def doc_path(cik: str, accession: str):
    acc = CBD._norm_accession(accession)
    return docs_dir() / str(cik)[-3:].zfill(3) / str(cik) / ("%s.gz" % acc)


def document_url(row: dict) -> str:
    return CBD.ARCHIVE_URL % (row["issuer_cik"], CBD._norm_accession(row["accession"]),
                              CBD._root_leaf(row["primary_document"]))


def read_document_text(row: dict) -> str | None:
    p = doc_path(row["issuer_cik"], row["accession"])
    if not p.exists():
        return None
    try:
        return CBD._flatten(gzip.open(p, "rb").read())
    except Exception:                                        # noqa: BLE001
        return None


#: An Item heading in a current report's body: "Item 1.02", "ITEM 5.02.",
#: "Item 5.02 -". Filing agents vary the spacing and punctuation, never the
#: digits.
_ITEM_HEAD = re.compile(r"\bITEM\s*([1-9])\s*\.\s*([0-9]{2})\b", re.I)


def item_section(text: str | None, code: str) -> str:
    """The narrative filed under ``Item <code>``, up to the next Item heading.

    A body can name the same Item more than once - a cross-reference ("the
    information set forth under Item 1.01 is incorporated by reference") is
    itself an Item heading match. The LONGEST segment is taken, because the
    operative narrative is the one that says something; a cross-reference is a
    sentence. Deterministic, and empty rather than guessed when absent.
    """
    if not text:
        return ""
    heads = [(m.start(), "%s.%s" % (m.group(1), m.group(2))) for m in _ITEM_HEAD.finditer(text)]
    best = ""
    for k, (pos, c) in enumerate(heads):
        if c != code:
            continue
        end = heads[k + 1][0] if k + 1 < len(heads) else len(text)
        seg = text[pos:end]
        if len(seg) > len(best):
            best = seg
    return best


def fetch_documents(rows: list, *, workers: int = CBD.FETCH_WORKERS,
                    verbose: bool = True) -> dict:
    """Fetch each filing's primary document once, gzipped, restartable.

    The SAME fair-access contract as the 13D/G acquisition: one global rate
    gate built from the canonical interval, shared by every worker, so the
    worker count can never raise the request rate.
    """
    headers = CBD._headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT"}
    todo, seen, cached = [], set(), 0
    for r in rows:
        key = (r["issuer_cik"], CBD._norm_accession(r["accession"]))
        if key in seen or not r.get("primary_document"):
            continue
        seen.add(key)
        p = doc_path(*key)
        if p.exists() and p.stat().st_size > 64:
            cached += 1
        else:
            todo.append(r)
    gate = CBD._RateGate(ACQ.MIN_INTERVAL_S)
    counters = {"ok": 0, "failed": 0, "bytes": 0}
    lock = threading.Lock()
    t0 = time.time()
    manifest = data_dir() / "documents_manifest.jsonl"
    data_dir().mkdir(parents=True, exist_ok=True)

    def _one(r: dict) -> None:
        gate.wait()
        status, body = ACQ._get(document_url(r), headers)
        if status != 200 or not body:
            with lock:
                counters["failed"] += 1
                with open(manifest, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps({"at": now_iso(), "accession": r["accession"],
                                         "issuer_cik": r["issuer_cik"], "http": status,
                                         "state": "FAILED"}, sort_keys=True) + "\n")
            return
        p = doc_path(r["issuer_cik"], r["accession"])
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".%d.tmp" % threading.get_ident())
        with gzip.open(tmp, "wb") as fh:
            fh.write(body)
        tmp.replace(p)
        with lock:
            counters["ok"] += 1
            counters["bytes"] += len(body)
            n = counters["ok"]
        if verbose and n % 1000 == 0:
            print("  documents %d/%d failed=%d %.2fGB %.1f req/s"
                  % (n, len(todo), counters["failed"], counters["bytes"] / 1e9,
                     n / max(time.time() - t0, 1e-9)), flush=True)

    if todo:
        with futures.ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            list(pool.map(_one, todo))
    return {"requested": len(seen), "fetched": counters["ok"], "cached": cached,
            "failed": counters["failed"], "bytes": counters["bytes"],
            "workers": int(workers), "min_interval_s": ACQ.MIN_INTERVAL_S,
            "dir": str(docs_dir())}


def run_census(*, verbose: bool = True, write: bool = True) -> dict:
    from . import incumbent as INC
    E, elig = INC.equity_substrate()
    stream = enumerate_8k(verbose=verbose)
    write_stream(stream["rows"])
    body = census(E, elig, stream, verbose=verbose)
    if write:
        write_artifact(CENSUS_ARTIFACT, body)
    return body
