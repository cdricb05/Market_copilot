"""alpha_agent.r58.fundamentals - the owned SEC point-in-time fact reader.

This module is the whole reason R58 can ask its question. It turns 1.6M raw
XBRL facts into an as-of-knowledge-date view that an investor could actually
have held, and it refuses to do anything else.

Three rules, all pre-registered in R58_RESEARCH_PROTOCOL.json:

AVAILABILITY  a fact exists at decision session t iff its SEC ``filed`` date is
              <= the calendar date of t. Entry is NEXT_CLOSE, so every position
              is taken at least one session after the information was public.

RESTATEMENT   for each (cik, concept, period_start, period_end) the observation
              used at t is the one with the LATEST filed <= t. A restatement
              filed after t is invisible at t. 431,086 fact groups in the owned
              store carry more than one filed date, so this is not academic.

TTM           cash-flow concepts are reported YTD-cumulative in 10-Q (89 / 180 /
              272 / 364 day durations), income-statement concepts additionally
              carry genuine 3-month facts. A single uniform construction covers
              both: anchor on the latest ANNUAL fact A, and if a fresher
              year-to-date fact Y exists for the current fiscal year together
              with the prior year's same-length YTD fact P, return A + Y - P.
              Otherwise return A. Stage 24 discarded 575,008 non-annual flow
              facts and could not do this; the YTD_DIFF path is what makes both
              freshness and CHANGE signals possible.

Pure stdlib + numpy. The sqlite store is opened IMMUTABLE and never written.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import date, timedelta

from . import IDENTITY_DB, SEC_FACTS_DB

# --------------------------------------------------------------------------- #
# Concept ladders - fixed BEFORE any experiment ran. The first synonym that
# yields a value at the knowledge date wins; the order never changes.
# --------------------------------------------------------------------------- #
INSTANT_CONCEPTS = {
    "assets": ["Assets"],
    "equity": ["StockholdersEquity"],
    "inventory": ["InventoryNet"],
    "receivables": ["AccountsReceivableNetCurrent"],
}
FLOW_CONCEPTS = {
    "cfo": ["NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"],
    "capex": ["PaymentsToAcquirePropertyPlantAndEquipment",
              "PaymentsToAcquireProductiveAssets"],
    "ni": ["NetIncomeLoss"],
    "opinc": ["OperatingIncomeLoss"],
    "revenue": ["Revenues",
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                "SalesRevenueNet"],
    "rnd": ["ResearchAndDevelopmentExpense"],
}
ALL_TAGS = sorted({t for v in INSTANT_CONCEPTS.values() for t in v} |
                  {t for v in FLOW_CONCEPTS.values() for t in v})

ANNUAL_MIN, ANNUAL_MAX = 350, 380
FLOW_MIN, FLOW_MAX = 80, 380
# Year-on-year YTD periods of a 52/53-week filer differ by up to a week (Apple's
# Q3 YTD is 272 days one year and 279 the next), so a 5-day tolerance silently
# refuses the YTD_DIFF path for every such filer and falls back to a stale
# annual figure. 10 days admits the real match and cannot confuse a 90-day
# quarter with a 181-day half. Corrected before any R58 experiment ran; see the
# amendment note in R58_RESEARCH_PROTOCOL.json.
YTD_LEN_TOL = 10
PERIOD_FLOOR = "2008-01-01"          # facts older than this cannot matter here


def _ro(path) -> sqlite3.Connection:
    return sqlite3.connect("file:%s?immutable=1" % str(path).replace("\\", "/"),
                           uri=True)


def _norm_cik(c) -> str:
    s = str(c).strip().upper().replace("CIK", "").lstrip("0")
    return s or "0"


# --------------------------------------------------------------------------- #
# Identity bridge
# --------------------------------------------------------------------------- #
def cik_bridge() -> dict:
    """{norgate_symbol: normalised_cik} for RESOLVED, active mappings only.

    AMBIGUOUS and CONFLICT mappings are deliberately excluded: a wrong CIK is a
    wrong company, and there is no honest way to average two of them.
    """
    con = _ro(IDENTITY_DB)
    cur = con.cursor()
    sid2sym = {r[0]: r[1] for r in
               cur.execute("SELECT security_id, norgate_symbol FROM securities")}
    out = {}
    for sid, cik in cur.execute(
            "SELECT security_id, cik FROM cik_map "
            "WHERE active=1 AND status='RESOLVED' AND cik IS NOT NULL"):
        sym = sid2sym.get(sid)
        if sym and sym not in out:
            out[sym] = _norm_cik(cik)
    con.close()
    return out


def security_status() -> dict:
    """{norgate_symbol: {'is_current': int, 'delisting_date': str|None}}."""
    con = _ro(IDENTITY_DB)
    out = {}
    for sym, cur_, dd in con.execute(
            "SELECT norgate_symbol, is_current, delisting_date FROM securities"):
        out[sym] = {"is_current": int(cur_ or 0), "delisting_date": dd}
    con.close()
    return out


# --------------------------------------------------------------------------- #
# Fact loading
# --------------------------------------------------------------------------- #
def _dur(ps, pe):
    try:
        return (date.fromisoformat(pe) - date.fromisoformat(ps)).days
    except Exception:                                   # noqa: BLE001
        return None


def load_facts(ciks) -> dict:
    """{cik: [(filed, tag, period_start, period_end, duration, value)]} sorted by filed.

    Only the pre-registered concept ladders are read, and only facts with a
    usable period and a finite value.
    """
    want = set(ciks)
    con = _ro(SEC_FACTS_DB)
    cur = con.cursor()
    marks = ",".join("?" * len(ALL_TAGS))
    out = defaultdict(list)
    q = ("SELECT cik, concept_tag, period_start, period_end, filed, value "
         "FROM cf_fact WHERE concept_tag IN (%s) AND period_end >= ?" % marks)
    for cik, tag, ps, pe, filed, val in cur.execute(q, ALL_TAGS + [PERIOD_FLOOR]):
        c = _norm_cik(cik)
        if c not in want or val is None or pe is None or filed is None:
            continue
        if ps is None or ps == "":
            d = None                                     # INSTANT
        else:
            d = _dur(ps, pe)
            if d is None or not (FLOW_MIN <= d <= FLOW_MAX):
                continue
        out[c].append((filed, tag, ps, pe, d, float(val)))
    con.close()
    for c in out:
        out[c].sort(key=lambda r: (r[0], r[3]))
    return dict(out)


# --------------------------------------------------------------------------- #
# As-of state
# --------------------------------------------------------------------------- #
class CompanyState:
    """Incremental as-of-knowledge-date view of one company's facts.

    Facts are absorbed in ``filed`` order; ``snapshot()`` returns the derived
    fundamentals visible at that moment. Nothing filed later can influence it,
    which is the entire point.
    """

    __slots__ = ("instant", "annual", "ytd", "last_filed", "last_filed_by_tag")

    def __init__(self):
        # tag -> {period_end: value}   (latest filed wins by absorption order)
        self.instant = defaultdict(dict)
        # tag -> {period_end: (period_start, value)}   duration 350..380
        self.annual = defaultdict(dict)
        # tag -> {period_start: {period_end: (duration, value)}}
        self.ytd = defaultdict(lambda: defaultdict(dict))
        self.last_filed = None
        self.last_filed_by_tag = {}

    def absorb(self, filed, tag, ps, pe, d, val):
        if d is None:
            self.instant[tag][pe] = val
        else:
            if ANNUAL_MIN <= d <= ANNUAL_MAX:
                self.annual[tag][pe] = (ps, val)
            self.ytd[tag][ps][pe] = (d, val)
        self.last_filed = filed if self.last_filed is None else max(self.last_filed, filed)
        self.last_filed_by_tag[tag] = filed

    # -- instant -----------------------------------------------------------
    def instant_value(self, names):
        """Freshest synonym wins, ties broken by ladder order.

        Picking the FIRST synonym that has any value at all is a trap: a tag a
        company abandoned in 2018 (``Revenues`` after ASC 606, say) keeps
        returning its last pre-abandonment number forever, and the signal
        silently freezes. Freshness is chosen on the observation's own period
        end, never on the ladder position.
        """
        best = None
        for rank, tag in enumerate(names):
            m = self.instant.get(tag)
            if not m:
                continue
            pe = max(m)
            if best is None or pe > best[0] or (pe == best[0] and rank < best[2]):
                best = (pe, m[pe], rank, tag)
        return (best[1], best[0]) if best else (None, None)

    def instant_prior(self, names, ref_pe, lag_days=365, tol=120):
        """The instant value roughly one year before ``ref_pe``."""
        if ref_pe is None:
            return None, None
        try:
            target = date.fromisoformat(ref_pe) - timedelta(days=lag_days)
        except Exception:                               # noqa: BLE001
            return None, None
        for tag in names:
            m = self.instant.get(tag)
            if not m:
                continue
            best, bestd = None, None
            for pe, v in m.items():
                try:
                    delta = abs((date.fromisoformat(pe) - target).days)
                except Exception:                       # noqa: BLE001
                    continue
                if delta <= tol and (bestd is None or delta < bestd):
                    best, bestd = (v, pe), delta
            if best:
                return best
        return None, None

    # -- flow --------------------------------------------------------------
    @staticmethod
    def _year_back(pes, a_pe, lag_days=365, tol=120):
        """The annual period end roughly one YEAR before ``a_pe``.

        Not simply the next entry in the sorted list: many filers publish a
        genuine "twelve months ended <quarter date>" fact, which lands in the
        annual bucket, so the adjacent entry can be one QUARTER back. Taking it
        as the prior year would make a year-on-year change measure a quarter and
        understate it by roughly a factor of four.
        """
        try:
            target = date.fromisoformat(a_pe) - timedelta(days=lag_days)
        except Exception:                               # noqa: BLE001
            return None
        best, bestd = None, None
        for pe in pes:
            if pe >= a_pe:
                continue
            try:
                delta = abs((date.fromisoformat(pe) - target).days)
            except Exception:                           # noqa: BLE001
                continue
            if delta <= tol and (bestd is None or delta < bestd):
                best, bestd = pe, delta
        return best

    def _ttm_for_tag(self, tag, anchor_back=0):
        """TTM anchored on the latest annual fact, or on the one a YEAR before it.

        Returns (value, path, period_end) or (None, None, None).
        """
        ann = self.annual.get(tag)
        if not ann:
            return None, None, None
        pes = sorted(ann, reverse=True)
        if anchor_back != 0:
            # The YEAR-AGO term of a change signal must be the plain annual
            # figure at that anchor. Freshening it with the current year's YTD
            # would drag it forward to one QUARTER behind the current TTM, and
            # the "year-on-year" change would silently become quarter-on-quarter.
            a_pe = self._year_back(pes, pes[0])
            if a_pe is None:
                return None, None, None
            return ann[a_pe][1], "ANNUAL", a_pe
        a_pe = pes[0]
        a_ps, a_val = ann[a_pe]
        # fiscal-year start immediately after the anchor
        try:
            fy_start = (date.fromisoformat(a_pe) + timedelta(days=1)).isoformat()
        except Exception:                               # noqa: BLE001
            return a_val, "ANNUAL", a_pe
        cur_ytd = self.ytd.get(tag, {}).get(fy_start) or {}
        # A year-length "YTD" fact IS the next annual anchor and is already
        # stored as one. Admitting it here would telescope an older anchor
        # forward (A_prior + FY - FY_prior == FY) and make a year-on-year
        # change identically zero, so full-year durations are excluded.
        cands = [(pe, d, v) for pe, (d, v) in cur_ytd.items()
                 if pe > a_pe and d < ANNUAL_MIN]
        if cands:
            pe_y, d_y, v_y = max(cands, key=lambda r: r[0])
            prior_ytd = self.ytd.get(tag, {}).get(a_ps) or {}
            best = None
            for pe_p, (d_p, v_p) in prior_ytd.items():
                if d_p < ANNUAL_MIN and abs(d_p - d_y) <= YTD_LEN_TOL:
                    if best is None or abs(d_p - d_y) < abs(best[0] - d_y):
                        best = (d_p, v_p)
            if best is not None:
                return a_val + v_y - best[1], "YTD_DIFF", pe_y
        return a_val, "ANNUAL", a_pe

    def ttm(self, names, anchor_back=0):
        """Freshest synonym wins, ties broken by ladder order (see instant_value)."""
        best = None
        for rank, tag in enumerate(names):
            v, path, pe = self._ttm_for_tag(tag, anchor_back=anchor_back)
            if v is None:
                continue
            if best is None or pe > best[2] or (pe == best[2] and rank < best[4]):
                best = (v, path, pe, tag, rank)
        return (best[0], best[1], best[2], best[3]) if best else (None, None, None, None)

    def ttm_prior(self, names, tag=None):
        """Year-earlier TTM on the SAME tag and the SAME construction path.

        ANNUAL path  -> the previous annual anchor.
        YTD_DIFF path-> A_prior + P - P_prior with the identical YTD length, so
                        both terms of a change signal are measured the same way
                        and a growth number is never an artefact of one leg
                        being a year longer than the other.

        The tag is passed in from ``ttm`` rather than re-chosen: a change signal
        that silently switches XBRL tag between its two terms measures the tag
        change, not the company.
        """
        if tag is None:
            _v, _p, _pe, tag = self.ttm(names)
            if tag is None:
                return None, None, None, None
        v_now, path, _pe_now = self._ttm_for_tag(tag, anchor_back=0)
        if v_now is None:
            return None, None, None, None
        if path == "ANNUAL":
            v_p, _p_path, pe_p = self._ttm_for_tag(tag, anchor_back=1)
            return (v_p, "ANNUAL", pe_p, tag) if v_p is not None else (None, None, None, None)
        # YTD_DIFF: rebuild the same construction one fiscal year back
        ann = self.annual.get(tag) or {}
        pes = sorted(ann, reverse=True)
        if len(pes) < 2:
            return None, None, None, None
        a_pe = pes[0]
        a_pe_prior = self._year_back(pes, a_pe)
        if a_pe_prior is None:
            return None, None, None, None
        a_ps, _a_val = ann[a_pe]
        ap_ps, ap_val = ann[a_pe_prior]
        try:
            fy_start = (date.fromisoformat(a_pe) + timedelta(days=1)).isoformat()
        except Exception:                               # noqa: BLE001
            return None, None, None, None
        cur_ytd = self.ytd.get(tag, {}).get(fy_start) or {}
        cands = [(pe, d, vv) for pe, (d, vv) in cur_ytd.items()
                 if pe > a_pe and d < ANNUAL_MIN]
        if not cands:
            return None, None, None, None
        _pe_y, d_y, _v_y = max(cands, key=lambda r: r[0])

        def _closest(m):
            best = None
            for _pe, (d_, v_) in (m or {}).items():
                if d_ < ANNUAL_MIN and abs(d_ - d_y) <= YTD_LEN_TOL:
                    if best is None or abs(d_ - d_y) < abs(best[0] - d_y):
                        best = (d_, v_)
            return best

        p = _closest(self.ytd.get(tag, {}).get(a_ps))
        pp = _closest(self.ytd.get(tag, {}).get(ap_ps))
        if p is None or pp is None:
            return None, None, None, None
        return ap_val + p[1] - pp[1], "YTD_DIFF", a_pe, tag

    # -- derived snapshot ---------------------------------------------------
    def snapshot(self):
        """Everything R58's families need, as of the absorbed knowledge date."""
        assets, assets_pe = self.instant_value(INSTANT_CONCEPTS["assets"])
        inv, inv_pe = self.instant_value(INSTANT_CONCEPTS["inventory"])
        rec, rec_pe = self.instant_value(INSTANT_CONCEPTS["receivables"])
        assets_p, _ = self.instant_prior(INSTANT_CONCEPTS["assets"], assets_pe)
        inv_p, _ = self.instant_prior(INSTANT_CONCEPTS["inventory"], inv_pe)
        rec_p, _ = self.instant_prior(INSTANT_CONCEPTS["receivables"], rec_pe)

        cfo, cfo_path, cfo_pe, cfo_tag = self.ttm(FLOW_CONCEPTS["cfo"])
        capex, _cp, _cpe, capex_tag = self.ttm(FLOW_CONCEPTS["capex"])
        ni, _np, _npe, ni_tag = self.ttm(FLOW_CONCEPTS["ni"])
        opinc, _op, _ope, opinc_tag = self.ttm(FLOW_CONCEPTS["opinc"])
        rev, _rp, _rpe, rev_tag = self.ttm(FLOW_CONCEPTS["revenue"])
        rnd, _rd, _rde, _ = self.ttm(FLOW_CONCEPTS["rnd"])

        cfo_pr, _, _, _ = self.ttm_prior(FLOW_CONCEPTS["cfo"], cfo_tag)
        capex_pr, _, _, _ = self.ttm_prior(FLOW_CONCEPTS["capex"], capex_tag)
        ni_pr, _, _, _ = self.ttm_prior(FLOW_CONCEPTS["ni"], ni_tag)
        opinc_pr, _, _, _ = self.ttm_prior(FLOW_CONCEPTS["opinc"], opinc_tag)
        rev_pr, _, _, _ = self.ttm_prior(FLOW_CONCEPTS["revenue"], rev_tag)

        return {
            "assets": assets, "assets_prior": assets_p, "assets_pe": assets_pe,
            "inventory": inv, "inventory_prior": inv_p,
            "receivables": rec, "receivables_prior": rec_p,
            "cfo": cfo, "cfo_prior": cfo_pr, "cfo_path": cfo_path,
            "capex": capex, "capex_prior": capex_pr,
            "ni": ni, "ni_prior": ni_pr,
            "opinc": opinc, "opinc_prior": opinc_pr,
            "revenue": rev, "revenue_prior": rev_pr,
            "rnd": rnd,
            "obs_period_end": cfo_pe or assets_pe,
            "last_filed": self.last_filed,
        }
