"""alpha_agent.alpha_recovery.event_8k_events - point-in-time SEC Form 8-K
Item-code events on the owned survivorship-safe equity substrate.

Executes the event definitions of
``research/preregistration/EVENT_8K_ITEM_PREREGISTRATION.md`` and changes
nothing in them.

THIS MODULE COMPUTES NO RETURN. It turns the Item-coded filing stream into
dated, identified, de-duplicated events and reports what it dropped and why.
Keeping returns out of it is what makes "the events were defined before the
returns were looked at" a property of the code rather than a claim.

RESEARCH ONLY. No purchase, no promotion, no capital, no proposal, no order, no
fill, no backfill, no live write.
"""
from __future__ import annotations

import re

import numpy as np

from alpha_agent.r63 import EQUITY_DISCOVERY_START

from . import now_iso
from . import control_block_events as CBE
from . import event_8k_data as D

CALCULATION_OWNER = "alpha_agent.alpha_recovery.event_8k_events"


# --------------------------------------------------------------------------- #
# The Item 5.02 candidate - EVALUATED, AND NOT FROZEN
# --------------------------------------------------------------------------- #
# Item 5.02 is heterogeneous - routine director elections, compensation plans,
# planned retirements and abrupt departures share one code. The ONE subset a
# deterministic rule could plausibly isolate is the appointment of an interim or
# acting CEO: a term of art a board uses when the seat is vacant without a
# successor. Every clause below exists because a measured document would
# otherwise have been misread.
#
# IT IS NOT A CELL. On the 2011-2016 development documents (5,328 of 6,147
# eligible 5.02 filings) the rule accepted 37 filings, of which about 24 report
# a new appointment: ~65% precision. The rest are follow-up compensation
# reports, "will continue to serve as interim" updates, a tenure ending phrased
# as "Retirement of X as Interim CEO", an interim made permanent, biographies of
# past interim service and an affiliate's seat. Closing those gaps means
# patching the rule on the very documents that measured it, plus an arbitrary
# episode window, for ~4.4 episodes a year - about 40 informative periods at
# h=21 and 13 at h=63. That is not a clean deterministic event, so under the
# brief's own rule 5.02 was dropped BEFORE any return existed. The code stays so
# the audit is reproducible and the misreadings stay pinned by tests.

#: Abbreviations whose period is not a sentence end ("Mr. Ahearn", "William H.").
_ABBREV = re.compile(r"\b(Mr|Ms|Mrs|Dr|Jr|Sr|Inc|Corp|Co|Ltd|No|St|L\.P|N\.A|U\.S|[A-Z])\.")
_SENT = re.compile(r"(?<=[.;])\s+(?=[A-Z(\"“])")
_CEO = r"(?:chief\s+executive\s+officer|\bC\.?E\.?O\b)"
_ROLES = (r"(?:(?:president|chairman(?:\s+of\s+the\s+board)?|chair(?:woman|person)?|"
          r"executive\s+chair(?:man)?)\s*(?:and|&|,)\s*){0,2}")
#: The qualifier must attach to the CEO TITLE itself. "the President and CEO
#: will serve as President of PayPal on an interim basis" is a CEO covering a
#: division - measured, and not a vacancy.
INTERIM_CEO = re.compile(
    r"\b(?:interim|acting)\s+" + _ROLES + _CEO
    + r"|" + _CEO + r"\s+(?:of\s+the\s+company\s+)?(?:on\s+an\s+interim\s+basis|"
    r"in\s+an\s+interim\s+capacity|ad\s+interim)\b", re.I)
#: A present, future or just-completed appointment.
APPOINT = re.compile(
    r"\b(?:appoint\w*|named|elect(?:ed|s)?|designat\w*|select(?:ed|s)?|promot\w*|"
    r"will\s+(?:serve|assume|become|act)|to\s+(?:serve|act|assume)|"
    r"(?:has|have)\s+(?:agreed\s+to\s+)?(?:serve|assume)|assum(?:e|ed|es|ing))\b", re.I)
#: A sentence about PAST interim service (a biography, a payment for service
#: already rendered, a restatement of an earlier announcement) is not the event.
PAST_OR_RESTATED = re.compile(
    r"\bserv(?:ed|ing)\s+as\s+(?:the\s+)?(?:\w+\W?s\s+)?(?:interim|acting)\b"
    r"|\b(?:previously|formerly)\s+(?:the\s+)?(?:\w+\W?s\s+)?(?:interim|acting)\b"
    r"|\b(?:interim|acting)\b[^.;]{0,120}\bfrom\s+(?:\w+\s+)?(?:19|20)\d\d\s+(?:to|until|through)\b"
    r"|\bin\s+connection\s+with\s+(?:his|her|their|such)\s+(?:service|appointment|role)\s+as\b"
    r"|\bas\s+previously\s+(?:announced|disclosed|reported)\b", re.I)
#: The interim seat of a SUBSIDIARY, segment or division.
SUBSIDIARY = re.compile(
    r"\b(?:interim|acting)\s+" + _ROLES + _CEO + r"\s+of\s+(?:the\s+company\W?s\s+|our\s+|its\s+)?"
    r"[^.;]{0,60}?\b(?:subsidiary|segment|division|unit|business|group|operations)\b", re.I)
#: A filing that ENDS an interim tenure - the interim leaves, or is made
#: permanent - reports the resolution of a vacancy, not a new one.
ENDING = re.compile(
    r"\b(?:no\s+longer|ceas\w*\s+to|will\s+cease\s+to)\s+(?:serve\s+as\s+)?(?:the\s+)?"
    r"(?:\w+\W?s\s+)?(?:interim|acting)\b"
    r"|\bremov\w*\s+(?:the\s+)?\W?interim\W?"
    r"|\b(?:has|had|have)\s+(?:been\s+)?serv(?:ed|ing)\s+as\s+(?:the\s+)?(?:\w+\W?s\s+)?"
    r"(?:interim|acting)\s+" + _ROLES + _CEO, re.I)


def sentences(text: str) -> list:
    return [s for s in _SENT.split(_ABBREV.sub(r"\1", text or "")) if s.strip()]


def interim_ceo_sentences(section: str) -> list:
    """The sentences that report an interim or acting CEO being appointed now."""
    out = []
    for s in sentences(section):
        if not INTERIM_CEO.search(s) or not APPOINT.search(s):
            continue
        if PAST_OR_RESTATED.search(s) or SUBSIDIARY.search(s):
            continue
        out.append(s)
    return out


def interim_ceo_text(text: str | None) -> bool | None:
    """True / False on a document's text; None when there is no document."""
    if text is None:
        return None
    section = D.item_section(text, "5.02") or text
    if ENDING.search(section):
        return False
    return bool(interim_ceo_sentences(section))


def interim_ceo_rule(row: dict) -> bool | None:
    return interim_ceo_text(D.read_document_text(row))

SIGN_NEGATIVE = -1

CELL_RESTRUCTURING_IMPAIRMENT = "RESTRUCTURING_OR_IMPAIRMENT"

#: The frozen cells. Each is defined ONLY by structured Item codes the filer
#: declared at acceptance, plus which Items may not be co-filed.
CELL_SPECS = {
    CELL_RESTRUCTURING_IMPAIRMENT: {
        "items_any": ("2.05", "2.06"),
        "exclude_cofiled": ("2.01", "2.02"),
        "sign": SIGN_NEGATIVE,
        "text_rule": None,
    },
}
CELLS = tuple(CELL_SPECS)

#: Events are counted from the canonical equity discovery start, the SAME
#: window both measurement arms see.
EVENT_WINDOW_START = EQUITY_DISCOVERY_START


def build_events(E: dict, elig: np.ndarray, *, rows: list | None = None,
                 cik2rows: dict | None = None, verbose: bool = True) -> dict:
    """Every frozen cell, point-in-time, with the drop accounting the
    preregistration requires. No forward return is touched.

    ``rows`` and ``cik2rows`` are injectable so the construction can be
    exercised on a known stream in a test without a network call.
    """
    rows = D.load_stream() if rows is None else list(rows)
    if cik2rows is None:
        cik2rows, id_meta = CBE.identity_map(E)
    else:
        id_meta = {"injected": True}
    dates64 = np.asarray(E["dates"], dtype="datetime64[ns]")
    events, stats = {}, {}
    for cell, spec in CELL_SPECS.items():
        sel = D.eligible_originals(E, elig, rows, cik2rows,
                                   require_any=spec["items_any"],
                                   exclude_any=spec["exclude_cofiled"],
                                   since=EVENT_WINDOW_START)
        st = {"filings_selected": len(sel), "text_rule_dropped": 0, "no_document": 0}
        by_key: dict = {}
        for r in sel:
            rule = spec.get("text_rule")
            if rule is not None:
                verdict = rule(r)
                if verdict is None:
                    st["no_document"] += 1
                    continue
                if not verdict:
                    st["text_rule_dropped"] += 1
                    continue
            for i in r["rows"]:
                e = by_key.setdefault((i, r["t"]), {
                    "row": int(i), "t": int(r["t"]), "cell": cell,
                    "date": str(dates64[r["t"]])[:10], "issuer_cik": r["issuer_cik"],
                    "accessions": [], "items": sorted(set(r["items"])),
                    "acceptance_utc": r["acceptance_utc"], "sign": spec["sign"],
                    "is_13d": False, "n_collapsed": 0})
                e["accessions"].append(r["accession"])
                e["n_collapsed"] += 1
        ev = sorted(by_key.values(), key=lambda e: (e["t"], e["row"]))
        st["events"] = len(ev)
        st["collapsed_same_row_session"] = sum(e["n_collapsed"] - 1 for e in ev)
        events[cell] = ev
        stats[cell] = st
        if verbose:
            print("  %s: %d events, %s -> %s %s" % (
                cell, len(ev), ev[0]["date"] if ev else None,
                ev[-1]["date"] if ev else None, st), flush=True)
    return {"events": events, "stats": stats, "identity": id_meta,
            "cik2rows": cik2rows, "generated_at": now_iso(),
            "calculation_owner": CALCULATION_OWNER}
