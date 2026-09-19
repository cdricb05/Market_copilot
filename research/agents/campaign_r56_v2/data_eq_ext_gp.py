r"""campaign_r56_v2.data_eq_ext_gp - point-in-time gross-profitability inputs for the extension panel (DF3).

Dataset id : ``eq_ext_pit_fundamental_gp_v1``  (PAPER_TRADER_MULTI_AGENT_ALPHA_CAMPAIGN_R56_V2)
Owner      : data-foundation-agent.  RESEARCH ONLY.  ON-DISK DATA ONLY - this module never opens a socket.

Sources (all already owned, all read-only)
------------------------------------------
* identity evidence : SEC Financial Statement Data Sets ``sub.txt`` (68 quarters, 2009q2..2026q1) under
  ``alpha_agent\identity\sec_bulk\financial_statement_data_sets`` - per filing: cik, conformed name,
  former name, SIC, form, period, FILED date, ACCEPTED timestamp and the XBRL instance file name, whose
  prefix is the registrant's ticker AT THAT FILING (``acm-20141231.xml``). That is a DATED ticker->CIK bridge.
* facts             : the SEC bulk ``companyfacts.zip`` (20,144 registrants) - every fact instance is kept
  per filing accession with that filing's own ``filed`` date, so the value AS FIRST FILED is recoverable.
* matcher           : ``alpha_agent.historical_identity.match_security_to_cik`` - the estate's ONE
  deterministic identity contract, called as a pure function (no store is written). Ticker text is a
  candidate generator only; a mapping resolves only with date-overlap corroboration; two surviving CIKs
  stay unresolved. This module is STRICTER, never looser: a ticker-tier mapping whose SEC and vendor names
  share no token is refused (``NAME_MISMATCH``).

Rules (fixed here, before any return is looked at - none ever is in this module)
-------------------------------------------------------------------------------
AVAILABILITY  a filing is visible at decision session t iff its EDGAR filed date <= the date of session t-1
              (filed date + 1 session; EDGAR stamps a filing accepted after 17:30 ET with the NEXT day).
AS FILED      one row per (cik, fiscal_period_end): the EARLIEST-filed 10-K family accession that reports
              that period as its own fiscal year. A later 10-K/A or a later year's comparative column never
              rewrites it. A 10-K/A is used only when it is the first report of that fiscal year.
FISCAL YEAR   inside one accession the fiscal-year end is the latest ``Assets`` instant; flows must end on
              it and last 350..380 days (52/53-week years pass, transition periods do not).
STALE         a filing older than 380 calendar days at t is unscorable.
LADDERS       first tag that yields a value wins; the order never changes (see REVENUE_TAGS / COST_TAGS).

STATUS 2026-09-19 - THE COVERAGE GATE FAILED, NO DATASET WAS WRITTEN, NOTHING WAS CERTIFIED
------------------------------------------------------------------------------------------
Eligible non-financial name-decisions with a computable GP/A: D 58.33 %, V 63.45 %, L 62.79 % (gate 60 % in
EACH). D fails. P9 is DATA_HOLD. The resolver below is left EXACTLY as it ran so that this code still
reproduces ``df3_coverage_measurement.json`` and ``eq_ext_identity_map_v1.csv``.

KNOWN DEFECT, measured after the run (``df3_identity_contamination_bound.json``): 76 of 4,760 resolved
securities (1.6 %) are suspect wrong-company mappings - SUCCESSOR_TOOK_THE_TICKER_AND_THE_NAME. The vendor
spells a live security by its CURRENT ticker and name; when an acquirer adopts its target's ticker and name,
ticker AND name evidence both point at the TARGET's CIK, and the XBRL instance prefix is a filer-chosen
string (II-VI, now Coherent Corp, CIK 820318, still files as ``iivi-``), so ``COHR`` resolves to the acquired
Coherent Inc (CIK 21510). Every fix removes mappings, so the coverages above are UPPER bounds (lower bounds
D 56.83 %, V 62.17 %, L 62.03 %); the verdict is the same under both. Before ANY reuse add:
  R1 TIER AGREEMENT  refuse when a life-overlapping exact-normalised-name candidate is a DIFFERENT CIK;
  R2 FILER LIFE      refuse when the CIK's last FSDS filing is > 400 days before the security's LAST
                     Russell 2000 member session.

Usage (PowerShell):
    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\research\agents\campaign_r56_v2\data_eq_ext_gp.py measure
    ... data_eq_ext_gp.py build        # writes the dataset ONLY if the director's coverage gate is met
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sys
import zipfile
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
for _p in (str(_HERE), str(_REPO)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import data_eq_ext as DX                                                   # noqa: E402

DATASET = "eq_ext_pit_fundamental_gp_v1"
SEC_BULK = Path(r"D:\Stock_Prediction_app_data\alpha_agent\identity\sec_bulk")
FSDS_ROOT = SEC_BULK / "financial_statement_data_sets"
COMPANYFACTS_ZIP = SEC_BULK / "companyfacts.zip"

REVENUE_TAGS = ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet",
                "RevenueFromContractWithCustomerIncludingAssessedTax", "SalesRevenueGoodsNet")
COST_TOTAL_TAGS = ("CostOfRevenue", "CostOfGoodsAndServicesSold")
COST_GOODS_TAG, COST_SERVICES_TAG = "CostOfGoodsSold", "CostOfServices"
COST_EX_DA_TAG = "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization"
GP_TAG, ASSETS_TAG = "GrossProfit", "Assets"
ALL_TAGS = REVENUE_TAGS + COST_TOTAL_TAGS + (COST_GOODS_TAG, COST_SERVICES_TAG, COST_EX_DA_TAG, GP_TAG, ASSETS_TAG)

ANNUAL_MIN_DAYS, ANNUAL_MAX_DAYS = 350, 380
STALE_DAYS = 380
TICKER_USE_SLACK_DAYS = 400            # a ticker prefix is evidence for +-400 days around its dated use

# director's samples (agenda G5) and universe rule U5
DISCOVERY_START, VALIDATION_START, LOCKBOX_START = "2011-07-01", "2018-01-01", "2023-01-01"
CADENCE, HORIZON = 21, 21
U5_MIN_PRICE, U5_MIN_ADV, U5_MIN_HISTORY = 5.0, 5.0e6, 260
COVERAGE_GATE = 0.60
FINANCIAL_GICS = ("Financials", "Real Estate")          # denominator fallback when no SIC is resolved

_INSTANCE_RE = re.compile(r"^([a-z]{1,5})[-_]\d{6,8}")
_CLASS_RE = re.compile(r"\s+(Class\s+[A-Z]\s+)?(Common|Ordinary)(\s+(Stock|Shares?))?\s*$", re.IGNORECASE)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso8(s: str):
    s = (s or "").strip()
    return "%s-%s-%s" % (s[:4], s[4:6], s[6:8]) if len(s) == 8 and s.isdigit() else None


def _days(a: str, b: str) -> int:
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


def _shift(d: str, days: int) -> str:
    return date.fromordinal(date.fromisoformat(d).toordinal() + days).isoformat()


# --------------------------------------------------------------------------- #
# A. identity evidence from the owned FSDS sub.txt files
# --------------------------------------------------------------------------- #
def fsds_index() -> dict:
    """{cik: evidence}, {TICKER: {cik: [first_use, last_use]}}, {accn: (sic, accepted)}, file inventory."""
    from alpha_agent.historical_identity import norm_cik
    by_cik, ticker_use, accn_info, files = {}, defaultdict(dict), {}, []
    for qdir in sorted(p for p in FSDS_ROOT.iterdir() if p.is_dir()):
        f = qdir / "sub.txt"
        if not f.exists():
            continue
        n = 0
        with open(f, "r", encoding="utf-8", errors="replace", newline="") as fh:
            rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
            head = next(rd)
            ix = {c: i for i, c in enumerate(head)}
            for row in rd:
                if len(row) < len(head):
                    continue
                cik = norm_cik(row[ix["cik"]])
                filed = _iso8(row[ix["filed"]])
                if not cik or not filed:
                    continue
                n += 1
                e = by_cik.get(cik)
                if e is None:
                    e = by_cik[cik] = {"cik": cik, "name": None, "former_names": {}, "first_filing": filed,
                                       "last_filing": filed, "sic": None, "sic_filed": ""}
                e["first_filing"] = min(e["first_filing"], filed)
                if filed >= e["last_filing"]:
                    e["last_filing"] = filed
                    e["name"] = row[ix["name"]].strip() or e["name"]
                nm = row[ix["name"]].strip()
                if nm:
                    e["former_names"].setdefault(nm, None)
                fm = row[ix["former"]].strip()
                if fm:
                    e["former_names"].setdefault(fm, None)
                sic = row[ix["sic"]].strip()
                if sic and filed >= e["sic_filed"]:
                    e["sic"], e["sic_filed"] = sic, filed
                accn_info[row[ix["adsh"]]] = (sic or None, row[ix["accepted"]].strip() or None)
                m = _INSTANCE_RE.match(row[ix["instance"]].strip().lower())
                if m:
                    use = ticker_use[m.group(1).upper()].setdefault(cik, [filed, filed])
                    use[0], use[1] = min(use[0], filed), max(use[1], filed)
        files.append({"quarter": qdir.name, "rows": n, "bytes": f.stat().st_size})
    for e in by_cik.values():
        e["former_names"] = [{"name": k, "from": None, "to": None} for k in e["former_names"] if k != e["name"]]
    return {"by_cik": by_cik, "ticker_use": dict(ticker_use), "accn_info": accn_info, "files": files}


def _clean_vendor_name(name: str) -> str:
    return _CLASS_RE.sub("", name or "").strip()


def resolve_identities(identity_rows: list, idx: dict) -> list:
    """One mapping per panel security, through the estate's owned matcher. Pure - writes nothing."""
    from alpha_agent.historical_identity import _norm_name, match_security_to_cik
    by_cik, ticker_use = idx["by_cik"], idx["ticker_use"]
    name_ix = defaultdict(set)
    for cik, e in by_cik.items():
        for nm in [e["name"]] + [f["name"] for f in e["former_names"]]:
            if nm:
                name_ix[_norm_name(nm)].add(cik)
    out = []
    for r in identity_rows:
        life0 = r["first_quoted_date"][:10] if r["first_quoted_date"] else None
        life1 = r["last_quoted_date"][:10] if r["last_quoted_date"] else None
        tick = r["base_ticker"].upper()
        vendor_name = _clean_vendor_name(r["security_name"] or "")
        cand_t = []
        for cik, (u0, u1) in (ticker_use.get(tick) or {}).items():
            if (life0 or "0000-00-00") <= _shift(u1, TICKER_USE_SLACK_DAYS) and \
                    _shift(u0, -TICKER_USE_SLACK_DAYS) <= (life1 or "9999-12-31"):
                cand_t.append(cik)
        cand_n = sorted(name_ix.get(_norm_name(vendor_name), ())) if vendor_name else []
        subs = {c: by_cik[c] for c in set(cand_t) | set(cand_n)}
        sec = {"security_id": r["assetid"] or r["symbol"], "norgate_assetid": r["assetid"], "ticker": tick,
               "security_start_date": life0, "security_end_date": life1, "issuer_name": vendor_name,
               "is_current": False}     # never take the current-listing shortcut: every mapping needs dated evidence
        res = match_security_to_cik(sec, ticker_cik_index={tick: cand_t} if cand_t else None,
                                    submissions_by_cik=subs or None)
        status, cik, note = res.status, res.cik, None
        if status == "RESOLVED" and res.tier == 3:
            sec_names = [by_cik[cik]["name"]] + [f["name"] for f in by_cik[cik]["former_names"]]
            a = set(_norm_name(vendor_name).split())
            b = set(t for nm in sec_names if nm for t in _norm_name(nm).split())
            if a and b and not (a & b):
                status, note, cik = "NAME_MISMATCH", "ticker tier refused: no shared name token", None
        out.append({"symbol": r["symbol"], "assetid": r["assetid"], "base_ticker": tick,
                    "vendor_name": vendor_name, "delisted": r["last_quoted_date"] is not None,
                    "status": status, "cik": cik, "tier": res.tier, "method": res.method,
                    "sec_name": by_cik[cik]["name"] if cik else None,
                    "sic": by_cik[cik]["sic"] if cik else None, "note": note,
                    "n_ticker_candidates": len(cand_t), "n_name_candidates": len(cand_n)})
    return out


# --------------------------------------------------------------------------- #
# B. as-first-filed 10-K rows from the owned bulk companyfacts.zip
# --------------------------------------------------------------------------- #
def _annual(rows, accn, fy_end):
    for r in rows:
        if r.get("accn") == accn and r.get("end") == fy_end and r.get("start") and r.get("val") is not None:
            if ANNUAL_MIN_DAYS <= _days(r["start"], r["end"]) <= ANNUAL_MAX_DAYS:
                return float(r["val"])
    return None


def filings_for_cik(doc: dict, accn_info: dict) -> list:
    g = (doc.get("facts") or {}).get("us-gaap") or {}
    usd = {t: [r for r in ((g.get(t) or {}).get("units") or {}).get("USD", [])
               if str(r.get("form", "")).startswith("10-K")] for t in ALL_TAGS}
    per_accn = {}
    for r in usd[ASSETS_TAG]:
        if r.get("val") is None or not r.get("end") or not r.get("filed"):
            continue
        cur = per_accn.get(r["accn"])
        if cur is None or r["end"] > cur["fiscal_period_end"]:
            per_accn[r["accn"]] = {"accn": r["accn"], "form": r["form"], "filed_date": r["filed"],
                                   "fiscal_period_end": r["end"], "assets": float(r["val"])}
    first = {}
    for f in per_accn.values():                                 # AS FILED: earliest accession per fiscal year
        k = f["fiscal_period_end"]
        if k not in first or (f["filed_date"], f["accn"]) < (first[k]["filed_date"], first[k]["accn"]):
            first[k] = f
    out = []
    for f in sorted(first.values(), key=lambda x: (x["filed_date"], x["fiscal_period_end"])):
        a, e = f["accn"], f["fiscal_period_end"]
        rev = rev_tag = None
        for t in REVENUE_TAGS:
            v = _annual(usd[t], a, e)
            if v is not None:
                rev, rev_tag = v, t
                break
        cost = cost_tag = None
        for t in COST_TOTAL_TAGS:
            v = _annual(usd[t], a, e)
            if v is not None:
                cost, cost_tag = v, t
                break
        if cost is None:
            cg, cs = _annual(usd[COST_GOODS_TAG], a, e), _annual(usd[COST_SERVICES_TAG], a, e)
            if cg is not None and cs is not None:
                cost, cost_tag = cg + cs, COST_GOODS_TAG + "+" + COST_SERVICES_TAG
            elif cg is not None:
                cost, cost_tag = cg, COST_GOODS_TAG
            elif cs is not None:
                cost, cost_tag = cs, COST_SERVICES_TAG
        if cost is None:
            v = _annual(usd[COST_EX_DA_TAG], a, e)
            if v is not None:
                cost, cost_tag = v, COST_EX_DA_TAG
        gp = _annual(usd[GP_TAG], a, e)
        sic, accepted = accn_info.get(a, (None, None))
        f.update({"revenues": rev, "revenues_tag": rev_tag, "cost_of_revenue": cost, "cost_tag": cost_tag,
                  "gross_profit": gp, "sic_at_filing": sic, "accepted": accepted,
                  "gp_computable": bool(f["assets"] > 0 and (gp is not None or (rev is not None and cost is not None)))})
        out.append(f)
    return out


def extract_filings(ciks, accn_info: dict, progress_every: int = 500) -> dict:
    zf = zipfile.ZipFile(COMPANYFACTS_ZIP)
    members = set(zf.namelist())
    out, absent = {}, []
    for k, cik in enumerate(sorted(ciks)):
        if progress_every and k % progress_every == 0:
            print("companyfacts %d/%d cik=%s" % (k, len(ciks), cik), flush=True)
        name = "CIK%s.json" % str(cik).zfill(10)
        if name not in members:
            absent.append(cik)
            continue
        try:
            out[cik] = filings_for_cik(json.loads(zf.read(name)), accn_info)
        except Exception as exc:                               # noqa: BLE001
            absent.append("%s (%s)" % (cik, type(exc).__name__))
    return {"by_cik": out, "absent": absent}


# --------------------------------------------------------------------------- #
# C. coverage  (counts and shares only - no return is read, no signal is ranked)
# --------------------------------------------------------------------------- #
def decision_indices(dates: np.ndarray) -> np.ndarray:
    start = int(np.searchsorted(dates, DISCOVERY_START))
    return np.arange(start, len(dates) - HORIZON - 2 + 1, CADENCE)


def u5_eligible(panel: dict, t: int) -> np.ndarray:
    """Director rule U5 = alpha_agent.r57.engine.eligibility with EQ_MIN_ADV 5e6 and the S&P 500 exclusion."""
    tr, un, vol = panel["tr"], panel["un"], panel["vol"]
    ok = (panel["mem"][:, t] > 0) & ~(panel["sp500_mem"][:, t] > 0)
    ok &= np.isfinite(un[:, t]) & (un[:, t] >= U5_MIN_PRICE)
    lo = max(0, t - 62)
    dvol = un[:, lo:t + 1] * vol[:, lo:t + 1]
    with np.errstate(all="ignore"):
        med = np.nanmedian(np.where(np.isfinite(dvol), dvol, np.nan), axis=1)
    ok &= np.isfinite(med) & (med >= U5_MIN_ADV)
    lo2 = max(0, t - U5_MIN_HISTORY + 1)
    ok &= np.isfinite(tr[:, lo2:t + 1]).sum(axis=1) >= U5_MIN_HISTORY * 0.9
    ok &= np.isfinite(tr[:, t])
    return ok


def _is_financial_sic(sic) -> bool:
    try:
        return 6000 <= int(sic) <= 6999
    except (TypeError, ValueError):
        return False


def measure(panel: dict, mappings: list, filings: dict) -> dict:
    dates, syms = panel["dates"], list(panel["symbols"])
    m_by_sym = {m["symbol"]: m for m in mappings}
    resolved = np.array([m_by_sym[s]["status"] == "RESOLVED" for s in syms])
    delisted = np.asarray(panel["delisted"], dtype=bool)
    fin = np.zeros(len(syms), dtype=bool)
    fin_known_by_sic = np.zeros(len(syms), dtype=bool)
    for i, s in enumerate(syms):
        m = m_by_sym[s]
        if m["status"] == "RESOLVED" and m["sic"]:
            fin[i], fin_known_by_sic[i] = _is_financial_sic(m["sic"]), True
        else:
            fin[i] = str(panel["sectors"][i]) in FINANCIAL_GICS
    # per symbol: filed dates of the computable / all as-filed 10-K rows
    filed_all, filed_ok = {}, {}
    for i, s in enumerate(syms):
        rows = filings.get(m_by_sym[s]["cik"]) if resolved[i] else None
        if rows:
            filed_all[i] = sorted((r["filed_date"], r["gp_computable"]) for r in rows)
    dec = decision_indices(dates)
    layers = {"D": [], "V": [], "L": []}
    acc = {k: defaultdict(int) for k in layers}
    by_year = defaultdict(lambda: defaultdict(int))
    for t in dec:
        d = str(dates[t])
        lay = "D" if d < VALIDATION_START else ("V" if d < LOCKBOX_START else "L")
        prev = str(dates[t - 1])
        el = u5_eligible(panel, t)
        member = (panel["mem"][:, t] > 0) & ~(panel["sp500_mem"][:, t] > 0)
        for basis, mask in (("eligible", el), ("member", member)):
            nf = mask & ~fin
            idxs = np.flatnonzero(nf)
            a = acc[lay]
            a[basis + "_nonfin"] += len(idxs)
            a[basis + "_all"] += int(mask.sum())
            for i in idxs:
                dl = "delisted" if delisted[i] else "surviving"
                a["%s_nonfin_%s" % (basis, dl)] += 1
                if resolved[i]:
                    a[basis + "_nonfin_resolved"] += 1
                    a["%s_nonfin_resolved_%s" % (basis, dl)] += 1
                rows = filed_all.get(i)
                vis = None
                if rows:
                    for fd, okc in rows:                           # latest filing visible at t (filed <= t-1)
                        if fd <= prev:
                            vis = (fd, okc)
                        else:
                            break
                if vis is not None:
                    a[basis + "_nonfin_any_10k_visible"] += 1
                    fresh = _days(vis[0], d) <= STALE_DAYS
                    if fresh:
                        a[basis + "_nonfin_fresh_10k"] += 1
                    if fresh and vis[1]:
                        a[basis + "_nonfin_gp_computable"] += 1
                        a["%s_nonfin_gp_computable_%s" % (basis, dl)] += 1
                        if basis == "eligible":
                            by_year[d[:4]]["gp_computable"] += 1
                if basis == "eligible":
                    by_year[d[:4]]["eligible_nonfin"] += 1
        layers[lay].append(d)

    def rate(a, num, den):
        return float(a[num] / a[den]) if a[den] else None

    out = {"decision_dates": {k: {"n": len(v), "first": v[0] if v else None, "last": v[-1] if v else None}
                              for k, v in layers.items()}, "layers": {}}
    for k, a in acc.items():
        row = {"counts": dict(a)}
        for basis in ("eligible", "member"):
            row[basis] = {
                "identity_resolution_rate": rate(a, basis + "_nonfin_resolved", basis + "_nonfin"),
                "any_10k_visible_rate": rate(a, basis + "_nonfin_any_10k_visible", basis + "_nonfin"),
                "fresh_10k_rate": rate(a, basis + "_nonfin_fresh_10k", basis + "_nonfin"),
                "gp_computable_rate": rate(a, basis + "_nonfin_gp_computable", basis + "_nonfin"),
                "gp_computable_rate_surviving": rate(a, basis + "_nonfin_gp_computable_surviving",
                                                     basis + "_nonfin_surviving"),
                "gp_computable_rate_delisted": rate(a, basis + "_nonfin_gp_computable_delisted",
                                                    basis + "_nonfin_delisted"),
                "identity_resolution_rate_surviving": rate(a, basis + "_nonfin_resolved_surviving",
                                                           basis + "_nonfin_surviving"),
                "identity_resolution_rate_delisted": rate(a, basis + "_nonfin_resolved_delisted",
                                                          basis + "_nonfin_delisted"),
            }
        out["layers"][k] = row
    out["eligible_gp_computable_rate_by_year"] = {
        y: (float(v["gp_computable"] / v["eligible_nonfin"]) if v["eligible_nonfin"] else None)
        for y, v in sorted(by_year.items())}
    out["eligible_nonfin_name_decisions_by_year"] = {y: int(v["eligible_nonfin"]) for y, v in sorted(by_year.items())}
    st = defaultdict(int)
    for m in mappings:
        st[m["status"] + ("|delisted" if m["delisted"] else "|surviving")] += 1
    out["symbol_mapping_status"] = dict(st)
    out["non_financial_basis"] = {"by_sic_resolved": int(fin_known_by_sic.sum()),
                                  "by_gics_fallback": int((~fin_known_by_sic).sum()),
                                  "financial_symbols": int(fin.sum())}
    e = {k: out["layers"][k]["eligible"] for k in ("D", "V", "L")}
    gate_cov = all((e[k]["gp_computable_rate"] or 0.0) >= COVERAGE_GATE for k in e)
    gate_del = all((e[k]["gp_computable_rate_delisted"] or 0.0) >= 0.5 * (e[k]["gp_computable_rate_surviving"] or 0.0)
                   for k in e)
    out["gate"] = {"rule": ">= 0.60 of eligible NON-FINANCIAL name-decisions with a computable GP/A in EACH of D, V, L; "
                           "delisted-name rate >= half the surviving-name rate in each",
                   "coverage_each_layer_ge_0_60": bool(gate_cov), "delisted_ge_half_surviving": bool(gate_del),
                   "passed": bool(gate_cov and gate_del)}
    return out


# --------------------------------------------------------------------------- #
# D. build / load
# --------------------------------------------------------------------------- #
FILING_COLUMNS = ("symbol", "assetid", "cik", "accn", "form", "fiscal_period_end", "filed_date", "accepted",
                  "revenues", "revenues_tag", "cost_of_revenue", "cost_tag", "gross_profit", "assets",
                  "sic", "gp_computable", "mapping_tier", "mapping_method")


def _sha(path: Path) -> str:
    return DX.file_sha256(path)


def run(write_dataset: bool) -> dict:
    out_dir = DX.data_dir()
    panel = DX.load_eq_ext_panel(verify_hash=True)
    idx = fsds_index()
    mappings = resolve_identities(panel["meta"]["identity"], idx)
    ciks = sorted({m["cik"] for m in mappings if m["status"] == "RESOLVED"})
    ext = extract_filings(ciks, idx["accn_info"])
    cov = measure(panel, mappings, ext["by_cik"])
    report = {"dataset": DATASET, "measured_at": now_iso(),
              "price_panel": panel["meta"]["panel"], "price_panel_npz_sha256": panel["meta"]["npz_sha256"],
              "sources": {"fsds_sub_quarters": len(idx["files"]), "fsds_first": idx["files"][0]["quarter"],
                          "fsds_last": idx["files"][-1]["quarter"], "fsds_rows": int(sum(f["rows"] for f in idx["files"])),
                          "fsds_ciks": len(idx["by_cik"]), "fsds_ticker_prefixes": len(idx["ticker_use"]),
                          "companyfacts_zip": str(COMPANYFACTS_ZIP), "companyfacts_bytes": COMPANYFACTS_ZIP.stat().st_size},
              "resolved_ciks": len(ciks), "resolved_ciks_absent_from_companyfacts": len(ext["absent"]),
              "as_filed_10k_rows": int(sum(len(v) for v in ext["by_cik"].values())),
              "coverage": cov}
    map_path = out_dir / "eq_ext_identity_map_v1.csv"
    with open(map_path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(mappings[0].keys()))
        w.writeheader()
        w.writerows(mappings)
    report["identity_map_path"], report["identity_map_sha256"] = str(map_path), _sha(map_path)
    if write_dataset and cov["gate"]["passed"]:
        csv_path, meta_path = out_dir / (DATASET + ".csv"), out_dir / (DATASET + ".meta.json")
        n = 0
        with open(csv_path, "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(FILING_COLUMNS)
            for m in sorted(mappings, key=lambda x: x["symbol"]):
                if m["status"] != "RESOLVED":
                    continue
                for r in ext["by_cik"].get(m["cik"], []):
                    w.writerow([m["symbol"], m["assetid"], m["cik"], r["accn"], r["form"], r["fiscal_period_end"],
                                r["filed_date"], r["accepted"], r["revenues"], r["revenues_tag"], r["cost_of_revenue"],
                                r["cost_tag"], r["gross_profit"], r["assets"], r["sic_at_filing"] or m["sic"],
                                int(r["gp_computable"]), m["tier"], m["method"]])
                    n += 1
        meta = dict(report)
        meta.update({"built_at": now_iso(), "builder": "research/agents/campaign_r56_v2/data_eq_ext_gp.py",
                     "csv_path": str(csv_path), "csv_rows": n, "csv_sha256": _sha(csv_path),
                     "columns": list(FILING_COLUMNS),
                     "availability_rule": "visible at decision session t iff EDGAR filed_date <= date of session t-1",
                     "restatement_rule": "as first filed: earliest 10-K family accession per (cik, fiscal_period_end)",
                     "ladders": {"revenues": list(REVENUE_TAGS), "cost_total": list(COST_TOTAL_TAGS),
                                 "cost_components": [COST_GOODS_TAG, COST_SERVICES_TAG], "cost_ex_da": COST_EX_DA_TAG}})
        meta_path.write_text(json.dumps(meta, indent=1), encoding="utf-8")
        report["dataset_written"] = str(csv_path)
    else:
        report["dataset_written"] = None
    return report


def load_eq_ext_gp(directory: Path | None = None, verify_hash: bool = True) -> dict:
    """{'meta': ..., 'rows': [dict per as-filed 10-K], 'by_symbol': {symbol: [rows sorted by filed_date]}}."""
    d = Path(directory) if directory else DX.data_dir()
    meta = json.loads((d / (DATASET + ".meta.json")).read_text(encoding="utf-8"))
    p = d / (DATASET + ".csv")
    if verify_hash and _sha(p) != meta["csv_sha256"]:
        raise ValueError("gp dataset hash mismatch")
    rows, by = [], defaultdict(list)
    with open(p, "r", encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            for k in ("revenues", "cost_of_revenue", "gross_profit", "assets"):
                r[k] = float(r[k]) if r[k] not in ("", None) else None
            r["gp_computable"] = r["gp_computable"] == "1"
            rows.append(r)
            by[r["symbol"]].append(r)
    for v in by.values():
        v.sort(key=lambda r: r["filed_date"])
    return {"meta": meta, "rows": rows, "by_symbol": dict(by)}


def main(argv) -> int:
    cmd = argv[1] if len(argv) > 1 else "measure"
    rep = run(write_dataset=(cmd == "build"))
    out = DX.data_dir().parent / "df3_coverage_measurement.json"
    out.write_text(json.dumps(rep, indent=1), encoding="utf-8")
    print(json.dumps({k: rep[k] for k in ("resolved_ciks", "resolved_ciks_absent_from_companyfacts",
                                          "as_filed_10k_rows", "dataset_written")}, indent=1))
    print(json.dumps(rep["coverage"]["gate"], indent=1))
    for k in ("D", "V", "L"):
        print(k, json.dumps(rep["coverage"]["layers"][k]["eligible"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
