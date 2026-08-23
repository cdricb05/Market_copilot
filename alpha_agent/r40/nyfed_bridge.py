"""alpha_agent.r40.nyfed_bridge - NYFED_LEGACY_MAPPING + NYFED_INCREMENTAL_RESULT
(Track F).

Release 39 found a genuine $0 unfinished information branch: the
current-format primary-dealer positioning mnemonics (``PDPOS...``) begin in
April 2013, AFTER every in-zone fit window, so a fitted point-in-time model
of dealer positioning was structurally impossible. The already-owned CSV
(``native_market_data_gate_r37/acquired/nyfed/primary_dealer_timeseries.csv``)
carries the legacy mnemonics back to 1998-01-28 in two further survey
formats. This module builds a DOCUMENTED bridge from those legacy codes into
stable economic concepts - and refuses to invent one where the semantics
cannot be proven.

Evidence, in order of authority:

1. The New York Fed's own reference menu for the Primary Dealer Statistics
   tool (``markets.newyorkfed.org/read?productCode=refdata&targetProductCode
   =40&group=menu``), which labels every positions code per official SERIES
   BREAK (SBP2001 = Jan 1998-Jun 2001, SBP2013 = Jul 2001-Mar 2013, SBN2013,
   SBN2015, SBN2022, SBN2024). The menu is fetched read-only, saved under
   the campaign directory and hashed; the mapping below quotes its labels.
2. Within-era ARITHMETIC identities measured on the owned data (the official
   total must equal the sum of its declared components).
3. SEAM CONTINUITY at each series break (level jump in pooled weekly-change
   standard deviations).

Concepts bridged (net outright positions, USD millions, positive = dealers
net long): Treasury bills; Treasury coupons ex-TIPS (total); TIPS; Treasury
ex-TIPS total; and three coupon duration buckets (<=6y, 6-11y, >11y) that
bridge EXACTLY from July 2001 because the 2001 and 2013 bucket boundaries
share the 6-year and 11-year cuts. The 1998-2001 coupon split (<=5y / >5y)
has no later equivalent and is recorded as BLOCKED_IDENTITY_SEMANTICS for
duration buckets (the coupon TOTAL still bridges). Financing / repo is
recorded as BLOCKED_IDENTITY_SEMANTICS: the financing taxonomy changed in
2001, 2013, 2015, 2022 and 2024 (collateral class x term x venue x
specified/general), and no per-era code is documented as equivalent.

Publication timing: the survey's as-of date is a Wednesday and the release
is the Thursday of the following week (documented on the statistics page;
the R39 owner declared 9 calendar days). The same lag is used here, so no
feature is observable before its release.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r39 import info_expansion as IE
from ..r39.continuation_director import new_cand
from ..r39.estate import NYFED_CSV
from ..r39.representation_factory import CLASSICAL_FUT
from ..r39.wide_prosecution import _paired_increment
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import director as D

CALCULATION_OWNER = "alpha_agent.r40.nyfed_bridge"
MAPPING_NAME = "nyfed_legacy_mapping.json"
RESULT_NAME = "nyfed_incremental_result.json"
MENU_NAME = "nyfed_reference_menu.json"
HISTORY_NAME = "nyfed_bridged_history.csv"
STAGE = "R40_NYFED"

MENU_URL = ("https://markets.newyorkfed.org/read?productCode=refdata"
            "&targetProductCode=40&group=menu")
LAG_DAYS = IE.NYFED_LAG_DAYS
UNIT = "USD millions"
SIGN = "positive = dealers net long (long minus short)"

#: Official series breaks (from the menu; verified against the API's
#: /api/pd/list/seriesbreaks).
SERIES_BREAKS = (
    ("SBP2001", "1998-01-28", "2001-06-30"),
    ("SBP2013", "2001-07-01", "2013-03-31"),
    ("SBN2013", "2013-04-01", "2014-12-31"),
    ("SBN2015", "2015-01-01", "2022-01-04"),
    ("SBN2022", "2022-01-05", "2024-07-02"),
    ("SBN2024", "2024-07-03", "2099-12-31"),
)

#: Per-era component codes per concept. A concept absent from an era is
#: NOT_BRIDGEABLE in that era (NaN, never filled).
CONCEPTS = {
    "UST_BILLS_NET": {
        "economic_concept": "net outright position, Treasury bills",
        "SBP2001": ["PDPUSGTBNOP"], "SBP2013": ["PDPUSGTBNOP"],
        "SBN2013": ["PDPOSGS-B"], "SBN2015": ["PDPOSGS-B"],
        "SBN2022": ["PDPOSGS-B"], "SBN2024": ["PDPOSGS-B"]},
    "UST_COUPONS_NET_TOTAL": {
        "economic_concept": "net outright position, Treasury coupons "
                            "excluding TIPS (all maturities)",
        "SBP2001": ["PDPUSGCS5LNOP", "PDPUSGCS5MNOP"],
        "SBP2013": ["PDPUSGCS3LNOP", "PDPUSGCS36NOP", "PDPUSGCS611NOP",
                    "PDPUSGCSM11NOP"],
        "SBN2013": ["PDPOSGSC-L2", "PDPOSGSC-G2L3", "PDPOSGSC-G3L6",
                    "PDPOSGSC-G6L7", "PDPOSGSC-G7L11", "PDPOSGSC-G11"],
        "SBN2015": ["PDPOSGSC-L2", "PDPOSGSC-G2L3", "PDPOSGSC-G3L6",
                    "PDPOSGSC-G6L7", "PDPOSGSC-G7L11", "PDPOSGSC-G11"],
        "SBN2022": ["PDPOSGSC-L2", "PDPOSGSC-G2L3", "PDPOSGSC-G3L6",
                    "PDPOSGSC-G6L7", "PDPOSGSC-G7L11", "PDPOSGSC-G11L21",
                    "PDPOSGSC-G21"],
        "SBN2024": ["PDPOSGSC-L2", "PDPOSGSC-G2L3", "PDPOSGSC-G3L6",
                    "PDPOSGSC-G6L7", "PDPOSGSC-G7L11", "PDPOSGSC-G11L21",
                    "PDPOSGSC-G21"]},
    "UST_TIPS_NET": {
        "economic_concept": "net outright position, Treasury "
                            "inflation-indexed securities (TIIS/TIPS)",
        "SBP2001": ["PDPUSGTIISNOP"], "SBP2013": ["PDPUSGTIISNOP"],
        "SBN2013": ["PDPOSTIPS-L2", "PDPOSTIPS-G2", "PDPOSTIPS-G6L11",
                    "PDPOSTIPS-G11"],
        "SBN2015": ["PDPOSTIPS-L2", "PDPOSTIPS-G2", "PDPOSTIPS-G6L11",
                    "PDPOSTIPS-G11"],
        "SBN2022": ["PDPOSTIPS-L2", "PDPOSTIPS-G2", "PDPOSTIPS-G6L11",
                    "PDPOSTIPS-G11"],
        "SBN2024": ["PDPOSTIPS-L2", "PDPOSTIPS-G2", "PDPOSTIPS-G6L11",
                    "PDPOSTIPS-G11"]},
    "UST_TOTAL_NET": {
        "economic_concept": "net outright position, ALL Treasury "
                            "securities INCLUDING TIPS (bills + coupons + "
                            "FRNs once issued + TIPS) - the official total "
                            "in every era; the current code's description "
                            "says 'excluding TIPS' but the arithmetic "
                            "identity (residual 0 over 600+ weeks) proves "
                            "inclusion, and the arithmetic wins",
        "SBP2001": ["PDPUSGTBNOP", "PDPUSGCS5LNOP", "PDPUSGCS5MNOP",
                    "PDPUSGTIISNOP"],
        "SBP2013": ["PDPUSGTNOP"],
        "SBN2013": ["PDPOSGST-TOT"], "SBN2015": ["PDPOSGST-TOT"],
        "SBN2022": ["PDPOSGST-TOT"], "SBN2024": ["PDPOSGST-TOT"]},
    "UST_COUPONS_LE_6Y_NET": {
        "economic_concept": "net outright position, coupons due in <= 6 "
                            "years (ex-TIPS)",
        "SBP2013": ["PDPUSGCS3LNOP", "PDPUSGCS36NOP"],
        "SBN2013": ["PDPOSGSC-L2", "PDPOSGSC-G2L3", "PDPOSGSC-G3L6"],
        "SBN2015": ["PDPOSGSC-L2", "PDPOSGSC-G2L3", "PDPOSGSC-G3L6"],
        "SBN2022": ["PDPOSGSC-L2", "PDPOSGSC-G2L3", "PDPOSGSC-G3L6"],
        "SBN2024": ["PDPOSGSC-L2", "PDPOSGSC-G2L3", "PDPOSGSC-G3L6"]},
    "UST_COUPONS_6_11Y_NET": {
        "economic_concept": "net outright position, coupons due in > 6 "
                            "and <= 11 years (ex-TIPS)",
        "SBP2013": ["PDPUSGCS611NOP"],
        "SBN2013": ["PDPOSGSC-G6L7", "PDPOSGSC-G7L11"],
        "SBN2015": ["PDPOSGSC-G6L7", "PDPOSGSC-G7L11"],
        "SBN2022": ["PDPOSGSC-G6L7", "PDPOSGSC-G7L11"],
        "SBN2024": ["PDPOSGSC-G6L7", "PDPOSGSC-G7L11"]},
    "UST_COUPONS_GT_11Y_NET": {
        "economic_concept": "net outright position, coupons due in > 11 "
                            "years (ex-TIPS)",
        "SBP2013": ["PDPUSGCSM11NOP"],
        "SBN2013": ["PDPOSGSC-G11"], "SBN2015": ["PDPOSGSC-G11"],
        "SBN2022": ["PDPOSGSC-G11L21", "PDPOSGSC-G21"],
        "SBN2024": ["PDPOSGSC-G11L21", "PDPOSGSC-G21"]},
}

BLOCKED = {
    "UST_COUPON_DURATION_BUCKETS_1998_2001": {
        "state": "BLOCKED_IDENTITY_SEMANTICS",
        "why": "the 1998-2001 format splits coupons at 5 years (<=5y / >5y; "
               "codes PDPUSGCS5LNOP / PDPUSGCS5MNOP); every later format "
               "cuts at 6 and 11 years. No arithmetic identity exists "
               "between a 5-year and a 6-year boundary, so duration "
               "buckets begin 2001-07-04 and the coupon TOTAL alone "
               "bridges from 1998-01-28",
    },
    "DEALER_FINANCING_REPO": {
        "state": "BLOCKED_IDENTITY_SEMANTICS",
        "why": "financing codes were re-taxonomised at every series break "
               "(2001: PDFSI*/PDFSO* securities in/out by collateral; 2013: "
               "PDSIRRA-*/PDSORA-* by collateral x term; 2015: MBS "
               "settlement split; 2022/2024: uncleared/cleared bilateral, "
               "GCF, tri-party, sponsored x specified/general). The "
               "reference menu documents no cross-era equivalence and "
               "the owned data cannot prove one, so no financing concept "
               "is bridged - rather than an invented backfill",
    },
    "UST_FRN_NET_2014": {
        "state": "NOT_REPORTED_SEPARATELY",
        "why": "floating-rate notes were first issued 2014-01; the SBN2013 "
               "format carries no FRN positions line, and PDPOSGS-BFRN "
               "begins 2015-01. FRNs enter the ex-TIPS TOTAL through the "
               "official total code; no separate FRN concept is bridged "
               "before 2015",
    },
}

#: Rule for merging eras when normalising: a seam is treated as continuous
#: only if the code set is identical on both sides OR the level jump at the
#: seam is below this many pooled weekly-change standard deviations.
SEAM_CONTINUITY_Z = 2.0

#: Market-invariant (timing) features - a common series cannot move a
#: cross-sectional rank, so these are tested through TS expressions.
FEATURES = ("nyfed2_total_z", "nyfed2_total_chg13_z", "nyfed2_coupons_z",
            "nyfed2_bills_z", "nyfed2_tips_z", "nyfed2_dur_tilt_z",
            "nyfed2_gt11_z")
#: Market-SPECIFIC features: dealers' inventory in the maturity bucket the
#: futures contract's deliverable basket sits in ("inventory in YOUR
#: bucket"), which CAN move a curve relative-value rank.
OWN_BUCKET_FEATURES = ("nyfed2_own_bucket_z", "nyfed2_own_bucket_chg13_z")
MARKET_BUCKET = {"ZT": "UST_COUPONS_LE_6Y_NET", "ZF": "UST_COUPONS_LE_6Y_NET",
                 "ZN": "UST_COUPONS_6_11Y_NET", "TN": "UST_COUPONS_6_11Y_NET",
                 "ZB": "UST_COUPONS_GT_11Y_NET", "UB": "UST_COUPONS_GT_11Y_NET"}
MARKET_BUCKET_BASIS = ("CME deliverable baskets: ZT 1.75-2y, ZF 4y2m-5y3m "
                       "(<=6y bucket); ZN 6.5-10y, TN 9y5m-10y (6-11y "
                       "bucket); ZB 15-25y, UB >=25y (>11y bucket)")


# --------------------------------------------------------------------------- #
# Reference menu (provenance)
# --------------------------------------------------------------------------- #
def fetch_menu(campaign_id: str = CAMPAIGN_ID,
               cached: Path = None) -> dict:
    """Read-only GET of the NY Fed reference menu; saved + hashed under the
    campaign directory. A previously saved copy is reused."""
    dst = campaign_dir(campaign_id) / MENU_NAME
    if dst.exists():
        return {"state": "CACHED", "path": str(dst),
                "sha256": _r39.sha_file(dst), "url": MENU_URL}
    dst.parent.mkdir(parents=True, exist_ok=True)
    if cached is not None and Path(cached).exists():
        dst.write_bytes(Path(cached).read_bytes())
        return {"state": "COPIED_FROM_SESSION_DOWNLOAD", "path": str(dst),
                "sha256": _r39.sha_file(dst), "url": MENU_URL}
    try:
        import urllib.request
        req = urllib.request.Request(
            MENU_URL, headers={"User-Agent": "Mozilla/5.0 (research; "
                                             "paper_trader R40)",
                               "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            dst.write_bytes(r.read())
        return {"state": "FETCHED", "path": str(dst),
                "sha256": _r39.sha_file(dst), "url": MENU_URL}
    except Exception as e:  # pragma: no cover - network
        return {"state": "UNAVAILABLE", "error": str(e)[:200],
                "url": MENU_URL}


def menu_labels(menu_path: Path) -> dict:
    """{(series_break, code): label} for every positions code in the menu."""
    try:
        j = json.loads(Path(menu_path).read_text(encoding="utf-8"))
        menu = j["data"][0]["referenceData"][0]["pd"]["menu"]
    except Exception:
        return {}
    out = {}

    def walk(node, sb):
        for k, v in node.items():
            if isinstance(v, dict):
                if "label" in v:
                    out[(sb, k)] = str(v["label"])
                if "submenu" in v:
                    walk(v["submenu"], sb)
    for sb, era in menu.items():
        walk(era.get("jsonMenu", {}), sb)
    return out


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def load_raw() -> pd.DataFrame:
    ny = pd.read_csv(NYFED_CSV)
    ny.columns = ["as_of", "series", "value"]
    ny["as_of"] = pd.to_datetime(ny["as_of"])
    ny["value"] = pd.to_numeric(ny["value"], errors="coerce")
    return ny


def _era_of(dates: pd.Series) -> pd.Series:
    out = pd.Series("", index=dates.index, dtype=object)
    for sb, a, b in SERIES_BREAKS:
        m = (dates >= pd.Timestamp(a)) & (dates <= pd.Timestamp(b))
        out[m] = sb
    return out


def bridge(ny: pd.DataFrame) -> tuple:
    """Weekly frame of bridged concepts + the proof table."""
    wide = ny.pivot_table(index="as_of", columns="series", values="value",
                          aggfunc="last").sort_index()
    era = _era_of(pd.Series(wide.index, index=wide.index))
    out = pd.DataFrame(index=wide.index)
    proofs = {}
    for concept, spec in CONCEPTS.items():
        col = pd.Series(np.nan, index=wide.index)
        per_era = {}
        for sb, a, b in SERIES_BREAKS:
            codes = spec.get(sb)
            if not codes:
                per_era[sb] = {"state": "NOT_BRIDGEABLE_IN_ERA"}
                continue
            present = [c for c in codes if c in wide.columns]
            m = (era == sb).to_numpy()
            if len(present) != len(codes) or not m.any():
                per_era[sb] = {"state": "CODES_MISSING",
                               "missing": [c for c in codes
                                           if c not in present]}
                continue
            block = wide.loc[m, present]
            s = block.sum(axis=1, min_count=len(present))
            col.loc[m] = s.to_numpy()
            per_era[sb] = {"state": "OK", "codes": present,
                           "weeks": int(s.notna().sum()),
                           "first": str(s.dropna().index.min().date())
                           if s.notna().any() else None,
                           "last": str(s.dropna().index.max().date())
                           if s.notna().any() else None}
        out[concept] = col
        proofs[concept] = per_era
    # DERIVED (arithmetic over bridged concepts, never a code guess)
    out["UST_EX_TIPS_TOTAL_NET"] = out["UST_TOTAL_NET"] - out["UST_TIPS_NET"]
    proofs["UST_EX_TIPS_TOTAL_NET"] = {
        "DERIVED": {"state": "OK",
                    "definition": "UST_TOTAL_NET - UST_TIPS_NET in every "
                                  "era (captures FRNs through the official "
                                  "total wherever they are not a separate "
                                  "line)"}}
    return out, proofs, wide, era


def arithmetic_identities(wide: pd.DataFrame, era: pd.Series) -> dict:
    """Official totals versus declared components, within era."""
    checks = {}

    def resid(total_code, parts, sb):
        m = (era == sb).to_numpy()
        if total_code not in wide.columns or \
                any(p not in wide.columns for p in parts):
            return {"state": "CODES_MISSING"}
        t = wide.loc[m, total_code]
        s = wide.loc[m, parts].sum(axis=1, min_count=len(parts))
        j = pd.concat([t, s], axis=1).dropna()
        if j.empty:
            return {"state": "NO_OVERLAP"}
        r = (j.iloc[:, 0] - j.iloc[:, 1]).abs()
        scale = j.iloc[:, 0].abs().median() or 1.0
        return {"state": "OK", "weeks": int(len(j)),
                "max_abs_residual": float(r.max()),
                "median_abs_residual": float(r.median()),
                "max_abs_residual_over_median_level":
                    float(r.max() / scale),
                "identity_holds": bool(r.max() / scale < 0.02)}
    tips_cur = ["PDPOSTIPS-L2", "PDPOSTIPS-G2", "PDPOSTIPS-G6L11",
                "PDPOSTIPS-G11"]
    checks["SBP2013_total_equals_bills_plus_coupons_plus_tiis"] = resid(
        "PDPUSGTNOP", ["PDPUSGTBNOP", "PDPUSGCS3LNOP", "PDPUSGCS36NOP",
                       "PDPUSGCS611NOP", "PDPUSGCSM11NOP", "PDPUSGTIISNOP"],
        "SBP2013")
    checks["SBP2013_total_equals_bills_plus_coupons_EX_tiis_(expected_to_"
           "fail)"] = resid(
        "PDPUSGTNOP", ["PDPUSGTBNOP", "PDPUSGCS3LNOP", "PDPUSGCS36NOP",
                       "PDPUSGCS611NOP", "PDPUSGCSM11NOP"], "SBP2013")
    checks["SBN2013_total_equals_bills_coupons_tips"] = resid(
        "PDPOSGST-TOT", ["PDPOSGS-B", "PDPOSGSC-L2", "PDPOSGSC-G2L3",
                         "PDPOSGSC-G3L6", "PDPOSGSC-G6L7", "PDPOSGSC-G7L11",
                         "PDPOSGSC-G11"] + tips_cur, "SBN2013")
    checks["SBN2015_total_equals_bills_frn_coupons_tips"] = resid(
        "PDPOSGST-TOT", ["PDPOSGS-B", "PDPOSGS-BFRN", "PDPOSGSC-L2",
                         "PDPOSGSC-G2L3", "PDPOSGSC-G3L6", "PDPOSGSC-G6L7",
                         "PDPOSGSC-G7L11", "PDPOSGSC-G11"] + tips_cur,
        "SBN2015")
    checks["SBN2015_total_equals_bills_frn_coupons_EX_tips_(expected_to_"
           "fail)"] = resid(
        "PDPOSGST-TOT", ["PDPOSGS-B", "PDPOSGS-BFRN", "PDPOSGSC-L2",
                         "PDPOSGSC-G2L3", "PDPOSGSC-G3L6", "PDPOSGSC-G6L7",
                         "PDPOSGSC-G7L11", "PDPOSGSC-G11"], "SBN2015")
    for sb in ("SBN2022", "SBN2024"):
        checks["%s_total_equals_bills_frn_coupons_tips" % sb] = resid(
            "PDPOSGST-TOT", ["PDPOSGS-B", "PDPOSGS-BFRN", "PDPOSGSC-L2",
                             "PDPOSGSC-G2L3", "PDPOSGSC-G3L6",
                             "PDPOSGSC-G6L7", "PDPOSGSC-G7L11",
                             "PDPOSGSC-G11L21", "PDPOSGSC-G21"] + tips_cur,
            sb)
    checks["label_vs_arithmetic"] = {
        "api_description_of_PDPOSGST-TOT": "Total - U.S. TREASURY SECURITIES "
                                           "(EXCLUDING TIPS)",
        "arithmetic": "TOT == bills + FRN + coupons + TIPS with zero "
                      "residual; the legacy PDPUSGTNOP likewise equals "
                      "bills + coupons + TIIS with zero residual",
        "resolution": "the ARITHMETIC defines the concept (UST_TOTAL_NET "
                      "includes TIPS in every era); ex-TIPS is DERIVED as "
                      "TOTAL - TIPS"}
    checks["SBN2022_G11_split_equals_G11L21_plus_G21"] = {
        "state": "NOT_OVERLAPPING",
        "note": "PDPOSGSC-G11 ends with SBN2015 and the split begins with "
                "SBN2022; no week carries both, so the identity is "
                "documentary (menu labels '> 11 years' vs '> 11 <= 21' + "
                "'> 21'), not numerical"}
    return checks


def seam_checks(bridged: pd.DataFrame) -> dict:
    """Level jump at every series break, in pooled weekly-change SDs."""
    out = {}
    for concept in bridged.columns:
        s = bridged[concept].dropna()
        rows = {}
        for i in range(1, len(SERIES_BREAKS)):
            sb_prev, _, b_prev = SERIES_BREAKS[i - 1]
            sb_next, a_next, _ = SERIES_BREAKS[i]
            before = s[s.index <= pd.Timestamp(b_prev)].tail(8)
            after = s[s.index >= pd.Timestamp(a_next)].head(8)
            if len(before) < 4 or len(after) < 4:
                rows["%s->%s" % (sb_prev, sb_next)] = {
                    "state": "NO_DATA_ON_ONE_SIDE"}
                continue
            pooled = pd.concat([before, after]).diff().dropna().std()
            jump = float(after.mean() - before.mean())
            if pooled and np.isfinite(pooled) and pooled > 0:
                z = float(jump / pooled)
            else:
                z = 0.0 if abs(jump) < 1e-9 else float("inf")
            spec = CONCEPTS.get(concept) or {}
            codes_same = spec.get(sb_prev) == spec.get(sb_next) \
                and spec.get(sb_prev) is not None
            rows["%s->%s" % (sb_prev, sb_next)] = {
                "state": "OK", "before_mean": float(before.mean()),
                "after_mean": float(after.mean()), "jump": jump,
                "pooled_weekly_change_sd": float(pooled),
                "seam_z": z, "codes_identical": bool(codes_same),
                "continuous_under_rule": bool(
                    codes_same or (np.isfinite(z)
                                   and abs(z) < SEAM_CONTINUITY_Z))}
        out[concept] = rows
    return out


def _roll_z(s: pd.Series, window: int = 156, min_periods: int = 52):
    return IE._roll_z(s, window, min_periods)


def features(bridged: pd.DataFrame, seams: dict) -> pd.DataFrame:
    """PIT feature frame indexed by AVAILABLE date (as-of + lag).

    Normalisation restarts at any seam the continuity rule rejects (the
    first 52 weeks after such a seam are NaN - masked, never filled)."""
    def segmented_z(col: str, s: pd.Series) -> pd.Series:
        s = s.copy()
        cuts = []
        for seam, row in (seams.get(col) or {}).items():
            if row.get("state") == "OK" and not row["continuous_under_rule"]:
                nxt = seam.split("->")[1]
                start = [a for sb, a, _ in SERIES_BREAKS if sb == nxt][0]
                cuts.append(pd.Timestamp(start))
        cuts = sorted(cuts)
        parts, lo = [], s.index.min()
        for c in cuts + [None]:
            seg = s[(s.index >= lo) & ((s.index < c) if c is not None
                                       else True)]
            parts.append(_roll_z(seg))
            if c is not None:
                lo = c
        return pd.concat(parts).reindex(s.index)

    total = bridged["UST_EX_TIPS_TOTAL_NET"]
    coup = bridged["UST_COUPONS_NET_TOTAL"]
    bills = bridged["UST_BILLS_NET"]
    tips = bridged["UST_TIPS_NET"]
    le6 = bridged["UST_COUPONS_LE_6Y_NET"]
    mid = bridged["UST_COUPONS_6_11Y_NET"]
    gt11 = bridged["UST_COUPONS_GT_11Y_NET"]
    gross = le6.abs() + mid.abs() + gt11.abs()
    tilt = (gt11 - le6) / gross.replace(0.0, np.nan)
    f = pd.DataFrame({
        "nyfed2_total_z": segmented_z("UST_TOTAL_NET", total),
        "nyfed2_total_chg13_z": segmented_z("UST_TOTAL_NET",
                                            total.diff(13)),
        "nyfed2_coupons_z": segmented_z("UST_COUPONS_NET_TOTAL", coup),
        "nyfed2_bills_z": segmented_z("UST_BILLS_NET", bills),
        "nyfed2_tips_z": segmented_z("UST_TIPS_NET", tips),
        "nyfed2_dur_tilt_z": segmented_z("UST_COUPONS_GT_11Y_NET", tilt),
        "nyfed2_gt11_z": segmented_z("UST_COUPONS_GT_11Y_NET", gt11),
        # per-bucket z (joined per market through MARKET_BUCKET)
        "_bucket_le6_z": segmented_z("UST_COUPONS_LE_6Y_NET", le6),
        "_bucket_mid_z": segmented_z("UST_COUPONS_6_11Y_NET", mid),
        "_bucket_gt11_z": segmented_z("UST_COUPONS_GT_11Y_NET", gt11),
        "_bucket_le6_chg13_z": segmented_z("UST_COUPONS_LE_6Y_NET",
                                           le6.diff(13)),
        "_bucket_mid_chg13_z": segmented_z("UST_COUPONS_6_11Y_NET",
                                           mid.diff(13)),
        "_bucket_gt11_chg13_z": segmented_z("UST_COUPONS_GT_11Y_NET",
                                            gt11.diff(13)),
    })
    f.index = f.index + pd.Timedelta(days=LAG_DAYS)
    return f


_BUCKET_COL = {"UST_COUPONS_LE_6Y_NET": "le6", "UST_COUPONS_6_11Y_NET": "mid",
               "UST_COUPONS_GT_11Y_NET": "gt11"}


def add_own_bucket(fut: pd.DataFrame) -> pd.DataFrame:
    """Market-specific own-bucket features from the joined per-bucket
    columns; markets outside MARKET_BUCKET stay NaN (mask, never fill)."""
    fut = fut.copy()
    own = np.full(len(fut), np.nan)
    own_chg = np.full(len(fut), np.nan)
    mk = fut["market_id"].astype(str).to_numpy()
    for m, concept in MARKET_BUCKET.items():
        tag = _BUCKET_COL[concept]
        sel = mk == m
        own[sel] = fut.loc[sel, "_bucket_%s_z" % tag].to_numpy(dtype=float)
        own_chg[sel] = fut.loc[sel, "_bucket_%s_chg13_z" % tag] \
            .to_numpy(dtype=float)
    fut["nyfed2_own_bucket_z"] = own
    fut["nyfed2_own_bucket_chg13_z"] = own_chg
    return fut


# --------------------------------------------------------------------------- #
# Artifacts
# --------------------------------------------------------------------------- #
def build_mapping(campaign_id: str = CAMPAIGN_ID,
                  cached_menu: Path = None) -> dict:
    menu = fetch_menu(campaign_id, cached=cached_menu)
    labels = menu_labels(Path(menu["path"])) if menu.get("path") else {}
    ny = load_raw()
    bridged, proofs, wide, era = bridge(ny)
    ident = arithmetic_identities(wide, era)
    seams = seam_checks(bridged)
    hist_path = campaign_dir(campaign_id) / HISTORY_NAME
    bridged.to_csv(hist_path)
    rows = []
    for concept, spec in CONCEPTS.items():
        for sb, a, b in SERIES_BREAKS:
            codes = spec.get(sb)
            if not codes:
                rows.append({"concept": concept, "series_break": sb,
                             "effective_period": [a, b],
                             "state": "NOT_BRIDGEABLE_IN_ERA"})
                continue
            rows.append({
                "concept": concept, "series_break": sb,
                "effective_period": [a, b],
                "old_mnemonics": codes,
                "new_mnemonic_or_concept": concept,
                "menu_labels": {c: labels.get((sb, c)) for c in codes},
                "unit": UNIT, "sign_convention": SIGN,
                "method_of_equivalence_proof":
                    "official reference-menu labels per series break "
                    "(boundary identity) + within-era arithmetic identity "
                    "against the official total where one exists + seam "
                    "continuity in pooled weekly-change SDs",
                "source": MENU_URL,
                "data_state": proofs[concept].get(sb),
                "confidence": "HIGH" if all(
                    labels.get((sb, c)) for c in codes) else "MEDIUM",
            })
    all_ok = all(v.get("state") == "OK" for p in proofs.values()
                 for v in p.values() if v.get("state") !=
                 "NOT_BRIDGEABLE_IN_ERA")
    body = artifact_body("r40_nyfed_legacy_mapping/1", {
        "calculation_owner": CALCULATION_OWNER,
        "source_csv": {"path": str(NYFED_CSV),
                       **_r39.file_fingerprint(NYFED_CSV)},
        "reference_menu": menu,
        "series_breaks": [{"series_break": sb, "start": a, "end": b}
                          for sb, a, b in SERIES_BREAKS],
        "publication_lag_days": LAG_DAYS,
        "publication_rule": "as-of Wednesday, released the Thursday of "
                            "the following week (NY Fed statistics page); "
                            "features are stamped at as-of + %d days"
                            % LAG_DAYS,
        "concepts": {k: v["economic_concept"] for k, v in CONCEPTS.items()},
        "mapping_rows": rows,
        "arithmetic_identities": ident,
        "seam_continuity": seams,
        "seam_continuity_rule_z": SEAM_CONTINUITY_Z,
        "blocked": BLOCKED,
        "bridged_history": {"path": str(hist_path),
                            "sha256": _r39.sha_file(hist_path),
                            "weeks": int(len(bridged)),
                            "first": str(bridged.index.min().date()),
                            "last": str(bridged.index.max().date()),
                            "coverage": {c: int(bridged[c].notna().sum())
                                         for c in bridged.columns}},
        "r39_feature_note": "the R39 owner summed every PDPOSGS* level "
                            "code including the official total "
                            "(PDPOSGST-TOT), i.e. 2x the total; a z-score "
                            "is scale-invariant so the R39 feature was "
                            "unaffected - recorded, not repaired (R39 is "
                            "immutable)",
        "state": "BRIDGE_VALID" if all_ok else "BRIDGE_PARTIAL",
        "no_invented_backfill": True,
    })
    body["nyfed_mapping_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / MAPPING_NAME, body,
                    immutable=False)
    return body


def attach(d2, campaign_id: str = CAMPAIGN_ID) -> dict:
    """Join the bridged PIT features to the futures panel + register the
    bundles. Returns coverage facts."""
    ny = load_raw()
    bridged, _, _, _ = bridge(ny)
    seams = seam_checks(bridged)
    f = features(bridged, seams)
    fut = d2.state["fut"]
    all_cols = list(f.columns)
    fut = fut.drop(columns=[c for c in all_cols + list(OWN_BUCKET_FEATURES)
                            + ["neg_nyfed2_total_z", "neg_nyfed2_dur_tilt_z",
                               "neg_nyfed2_own_bucket_z"]
                            if c in fut.columns])
    fut = IE.join_weekly_to_fut(fut, f, tuple(all_cols))
    fut = add_own_bucket(fut)
    fut["neg_nyfed2_total_z"] = -fut["nyfed2_total_z"]
    fut["neg_nyfed2_dur_tilt_z"] = -fut["nyfed2_dur_tilt_z"]
    fut["neg_nyfed2_own_bucket_z"] = -fut["nyfed2_own_bucket_z"]
    d2.state["fut"] = fut
    d2.bundles["CLS"] = list(CLASSICAL_FUT)
    d2.bundles["CLS_NYFED2"] = list(CLASSICAL_FUT) + list(FEATURES)
    d2.bundles["CLS_NYFED2_DUR"] = list(CLASSICAL_FUT) + [
        "nyfed2_dur_tilt_z", "nyfed2_gt11_z", "nyfed2_total_z"]
    d2.bundles["CLS_NYFED2_OWN"] = list(CLASSICAL_FUT) + \
        list(OWN_BUCKET_FEATURES)
    rates = fut[fut["asset_class"] == "RATES"]
    cov = {z: int(rates[(rates["zone"] == z)
                        & rates["nyfed2_total_z"].notna()].shape[0])
           for z in ("ZONE_A", "ZONE_B", "ZONE_C")}
    cov_own = {z: int(rates[(rates["zone"] == z)
                            & rates["nyfed2_own_bucket_z"].notna()].shape[0])
               for z in ("ZONE_A", "ZONE_B", "ZONE_C")}
    return {"rates_rows_with_feature_by_zone": cov,
            "rates_rows_with_own_bucket_by_zone": cov_own,
            "feature_first_available": str(f.dropna(how="all")
                                           .index.min().date()),
            "own_bucket_first_available": str(
                f["_bucket_mid_z"].dropna().index.min().date())
            if f["_bucket_mid_z"].notna().any() else None,
            "market_bucket_map": MARKET_BUCKET,
            "market_bucket_basis": MARKET_BUCKET_BASIS,
            "rates_markets": sorted(rates["market_id"].unique().tolist())}


def run_increment(d2=None, campaign_id: str = CAMPAIGN_ID) -> dict:
    """The paired incremental experiment: BASE(CLASSICAL) vs
    BASE+NYFED2 on the six Treasury futures, fit Zone A, judged Zone B,
    every evaluation counted. Protocol ZONE_B is possible for the first time
    because the bridged history begins 1998."""
    d2 = d2 or D.session()
    cov = attach(d2, campaign_id)
    zone_a_rows = cov["rates_rows_with_feature_by_zone"]["ZONE_A"]
    protocol = "ZONE_B" if zone_a_rows >= 200 else "SUBSPLIT"
    pairs, reps = {}, {}

    def pair(key, base_bundle, var_bundle, model, expr, scope="RATES"):
        b = new_cand("FUT", scope, base_bundle, "FUT:INFO_NYFED", model,
                     expr)
        v = new_cand("FUT", scope, var_bundle, "FUT:INFO_NYFED2", model,
                     expr)
        if protocol == "ZONE_B":
            rb = d2.eval_zone_b(b, stage=STAGE)
            rv = d2.eval_zone_b(v, stage=STAGE)
        else:
            rb = d2.eval_subsplit(b, stage=STAGE)
            rv = d2.eval_subsplit(v, stage=STAGE)
        reps[v["candidate_id"]] = rv
        pairs[key] = {"base": {"candidate_id": b["candidate_id"],
                               **D.summarise(rb)},
                      "variant": {"candidate_id": v["candidate_id"],
                                  **D.summarise(rv)},
                      "paired_increment": _paired_increment(rb, rv)
                      if rb.get("state") == "OK" and rv.get("state") == "OK"
                      else {"state": "NOT_COMPARABLE"}}

    # TIMING expressions for the market-invariant features (a common series
    # cannot move a cross-sectional rank - XS/RV with common features is a
    # degenerate test and is recorded as such, not run again)
    pair("ts_ridge_all", "CLS", "CLS_NYFED2", "ridge", "TS_OUTRIGHT")
    pair("ts_lgbm_all", "CLS", "CLS_NYFED2", "lightgbm", "TS_OUTRIGHT")
    pair("ts_ridge_duration", "CLS", "CLS_NYFED2_DUR", "ridge",
         "TS_OUTRIGHT")
    # CURVE RELATIVE VALUE with the market-specific own-bucket inventory
    pair("rv_ridge_own_bucket", "CLS", "CLS_NYFED2_OWN", "ridge",
         "GROUP_RV")
    pair("rv_lgbm_own_bucket", "CLS", "CLS_NYFED2_OWN", "lightgbm",
         "GROUP_RV")
    pair("ts_ridge_own_bucket", "CLS", "CLS_NYFED2_OWN", "ridge",
         "TS_OUTRIGHT")
    rules = {}
    for feat, expr, note in (
            ("neg_nyfed2_total_z", "TS_OUTRIGHT",
             "dealers net long -> bearish (inventory absorption)"),
            ("nyfed2_total_z", "TS_OUTRIGHT",
             "dealers net long -> bullish (demand signal)"),
            ("neg_nyfed2_dur_tilt_z", "TS_OUTRIGHT",
             "long-duration tilt -> bearish long end"),
            ("neg_nyfed2_own_bucket_z", "GROUP_RV",
             "heavy dealer inventory in a contract's own maturity bucket "
             "-> that contract cheapens vs the curve (absorption)"),
            ("nyfed2_own_bucket_chg13_z", "GROUP_RV",
             "rising own-bucket inventory -> demand in that sector")):
        c = new_cand("FUT", "RATES", "CLS_NYFED2_OWN", "FUT:INFO_NYFED2",
                     "rule:" + feat, expr)
        r = d2.eval_zone_b(c, stage=STAGE) if protocol == "ZONE_B" \
            else d2.eval_subsplit(c, stage=STAGE)
        rules[feat + "|" + expr] = {"candidate_id": c["candidate_id"],
                                    "note": note, **D.summarise(r)}

    def _inc_ok(p):
        inc = p["paired_increment"]
        return (isinstance(inc, dict) and inc.get("incremental_t")
                is not None and (inc.get("correlation_with_base") or 0)
                < 0.999)
    incs = [(k, p["paired_increment"]["incremental_t"],
             p["paired_increment"].get("incremental_excess_annualised"))
            for k, p in pairs.items() if _inc_ok(p)]
    best = max(incs, key=lambda x: x[1], default=None)
    best_var_t = max(((p["variant"].get("after_cost_excess_t_stat")
                       or -9.9) for p in pairs.values()), default=None)
    robust = bool(best is not None and best[1] >= 2.0
                  and (best[2] or 0) > 0)
    headline = {"protocol": protocol,
                "zone_a_rows_with_feature": zone_a_rows,
                "best_paired_increment": None if best is None else {
                    "pair": best[0], "incremental_t": best[1],
                    "incremental_excess_annualised": best[2]},
                "best_variant_zone_b_t": best_var_t,
                "robust_increment": robust,
                "degenerate_xs_note": "XS_LONG_SHORT on the six staggered "
                                      "Treasury futures is degenerate (<2 "
                                      "names per leg on most dates) and a "
                                      "common feature cannot move a rank; "
                                      "the first-pass XS pairs are counted "
                                      "in the ledger and recorded as "
                                      "NOT_APPLICABLE",
                "result": ("NYFED_INCREMENT_ROBUST" if robust else
                           "NYFED_NO_ROBUST_INCREMENT")}
    body = artifact_body("r40_nyfed_incremental_result/1", {
        "calculation_owner": CALCULATION_OWNER,
        "coverage": cov,
        "protocol": protocol,
        "protocol_note": "ZONE_B = fit Zone A (1998-2007 covered) / judge "
                         "Zone B; this is the fitted point-in-time test "
                         "R39 recorded as structurally impossible with "
                         "current-format codes only",
        "only_the_increment_counts": True,
        "standalone_significance_does_not_count": True,
        "pairs": pairs,
        "standalone_rule_diagnostics": rules,
        "headline": headline,
        "ledger_counted": True,
        "zone_c_untouched": True,
    })
    body["nyfed_increment_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / RESULT_NAME, body,
                    immutable=False)
    body["_reps"] = reps
    return body
