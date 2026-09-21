r"""campaign_r60_information_frontier.panels - the certified substrates.

RESEARCH ONLY. PAPER ONLY. NO ORDERS. This module loads owned data and builds
point-in-time universe masks. It owns no book, no gate, no registry and no
forward clock.

Two substrates:

    us_equity_extension_pit_panel_v1   NEW. Point-in-time S&P MidCap 400 +
                                       SmallCap 600 membership and total
                                       returns, delisted names retained.
                                       Certified PIT_SAFE by the
                                       data-foundation-agent in Phase 2. This
                                       is the first substrate in this estate
                                       BELOW the S&P 500: all 1,858 prior US
                                       equity hypotheses were large-cap.
    r38_native_contract_layer          OWNED, certified. Loaded through the
                                       canonical ``alpha_agent.r59.native``
                                       loader - never re-read from CSV here.

TIMING, stated once and obeyed everywhere below. Index s is session s; nothing
is forward filled. A decision at t may read:

    equity tr / op_tr / un / vol / dv   through t-1   (SIGNAL_LAG_SESSIONS = 1)
    equity membership                   at t          (the flag IS the
                                                       point-in-time fact)
    futures ret / ret2 / slope          through t-1
    futures open interest / volume      through t-2   (provisional newest row)

A name that did not trade on a session is NaN, never a forward fill, so every
window counts a name's OWN sessions rather than grid slots.

THE OPEN-INTEREST ZERO RULE, carried from CERTIFICATION_DF3B_DEFERRED_LEG:
``open_interest == 0`` is MISSING, everywhere, not only on the newest row.
6,727 zero cells exist; 63 are the provisional newest row of 63 of the 68
markets and 6,664 are in-history across 23 markets. A zero used as a level, or
as the denominator of a growth rate, manufactures signal. ``futures_layer()``
applies the rule at load so no executor can forget it.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional

import numpy as np

from alpha_agent import r59
from alpha_agent.r59 import native

CAMPAIGN_ID = "R60_INFORMATION_FRONTIER"

#: Dataset ids, named as the pre-registrations name them.
DS_EXTENSION = "us_equity_extension_pit_panel_v1"
DS_R38 = "r38_native_contract_layer_with_deferred_leg_v1"

#: Lags, from the certifications.
PRICE_LAG = 1
OI_LAG = 2

#: The R38 manifest spells an asset class its own way; the estate spells it
#: another. One translation, here, so no universe rule compares the two
#: vocabularies and silently matches nothing.
AC_MAP = {"COMMODITY": r59.AC_COMMODITY,
          "FX": r59.AC_FX,
          "RATES": r59.AC_RATES,
          "INTERNATIONAL_EQUITY": r59.AC_EQUITY_INDEX,
          "VOLATILITY": r59.AC_VOLATILITY}

#: The two Norgate watchlists the extension universe is drawn from, and the
#: index names whose point-in-time membership defines eligibility.
WATCHLISTS = ("S&P MidCap 400 Current & Past",
              "S&P SmallCap 600 Current & Past")
INDEX_NAMES = ("S&P MidCap 400", "S&P SmallCap 600")

#: Where the materialised panel lives. A build output, not repo source.
PANEL_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\r60_extension")
PANEL_NPZ = PANEL_ROOT / "us_equity_extension_pit_panel_v1.npz"

#: The panel starts well before DISCOVERY_START so the first decision has a
#: full trailing window. 12-1 momentum needs 252 sessions; the fundamental
#: windows need more. 2004 gives seven years of run-up.
PANEL_START = "2004-01-01"

#: Pre-registered tradability filter, chosen from data alone with NO return
#: scored, and identical to the one the certification measured its
#: cross-section under.
MIN_UNADJ_PRICE = 1.0
MIN_MEDIAN_DV = 100_000.0     # USD/day, median over the trailing window
DV_WINDOW = 63

_CACHE: dict = {}


# --------------------------------------------------------------------------- #
# Substrate 1: the NEW extension panel
# --------------------------------------------------------------------------- #
def build_extension_panel(out: Optional[Path] = None, *,
                          limit: int = 0, verbose: bool = True) -> Path:
    """Materialise the extension panel from Norgate. Run ONCE; then load.

    Arrays are ``[n_names, n_sessions]`` on the union of sessions actually
    observed. A name that did not trade is NaN. Delisted names are retained -
    that is the whole point of the 'Current & Past' watchlists, and the
    certification verified four delisted symbols return full history.
    """
    import norgatedata as nd

    out = Path(out or PANEL_NPZ)
    out.parent.mkdir(parents=True, exist_ok=True)

    symbols: list = []
    seen = set()
    for wl in WATCHLISTS:
        for s in nd.watchlist_symbols(wl):
            if s not in seen:
                seen.add(s)
                symbols.append(s)
    symbols.sort()
    if limit:
        symbols = symbols[:int(limit)]
    if verbose:
        print("symbols: %d from %d watchlists" % (len(symbols),
                                                  len(WATCHLISTS)))

    adj = nd.StockPriceAdjustmentType.TOTALRETURN
    pad = nd.PaddingType.NONE

    frames = {}
    for i, s in enumerate(symbols):
        try:
            px = nd.price_timeseries(s, stock_price_adjustment_setting=adj,
                                     padding_setting=pad,
                                     start_date=PANEL_START,
                                     timeseriesformat="numpy-recarray")
        except Exception:                                      # noqa: BLE001
            continue
        if px is None or not len(px):
            continue
        frames[s] = px
        if verbose and (i + 1) % 250 == 0:
            print("  priced %d/%d" % (i + 1, len(symbols)), flush=True)

    if not frames:
        raise RuntimeError("no price history returned for any symbol")

    all_dates = sorted({str(d)[:10] for px in frames.values()
                        for d in px["Date"]})
    dix = {d: i for i, d in enumerate(all_dates)}
    syms = sorted(frames)
    n_m, n_d = len(syms), len(all_dates)
    if verbose:
        print("grid: %d names x %d sessions (%s .. %s)"
              % (n_m, n_d, all_dates[0], all_dates[-1]))

    tr = np.full((n_m, n_d), np.nan, dtype=np.float64)
    op_tr = np.full((n_m, n_d), np.nan, dtype=np.float64)
    un = np.full((n_m, n_d), np.nan, dtype=np.float32)
    vol = np.full((n_m, n_d), np.nan, dtype=np.float32)
    dv = np.full((n_m, n_d), np.nan, dtype=np.float32)
    mem = np.zeros((n_m, n_d), dtype=np.int8)

    for i, s in enumerate(syms):
        px = frames[s]
        ii = np.array([dix[str(d)[:10]] for d in px["Date"]])
        tr[i, ii] = px["Close"]
        op_tr[i, ii] = px["Open"]
        un[i, ii] = px["Unadjusted Close"]
        vol[i, ii] = px["Volume"]
        dv[i, ii] = px["Turnover"]
        # Point-in-time membership: the union of the two indices, taken from
        # the vendor's own daily constituent series. NOT forward filled past
        # the series' own end - a name that stops reporting membership is out.
        for index_name in INDEX_NAMES:
            try:
                ic = nd.index_constituent_timeseries(
                    s, index_name, padding_setting=nd.PaddingType.NONE,
                    start_date=PANEL_START, timeseriesformat="numpy-recarray")
            except Exception:                                  # noqa: BLE001
                continue
            if ic is None or not len(ic):
                continue
            jj = np.array([dix[str(d)[:10]] for d in ic["Date"]
                           if str(d)[:10] in dix])
            if not len(jj):
                continue
            flag = np.asarray(ic["Index Constituent"])[
                [k for k, d in enumerate(ic["Date"]) if str(d)[:10] in dix]]
            mem[i, jj] = np.maximum(mem[i, jj],
                                    (np.asarray(flag) > 0).astype(np.int8))
        if verbose and (i + 1) % 250 == 0:
            print("  membership %d/%d" % (i + 1, len(syms)), flush=True)

    np.savez_compressed(
        out, symbols=np.array(syms), dates=np.array(all_dates),
        tr=tr, op_tr=op_tr, un=un, vol=vol, dv=dv, mem=mem,
        dataset_id=DS_EXTENSION, panel_start=PANEL_START,
        watchlists=np.array(WATCHLISTS), indices=np.array(INDEX_NAMES))
    if verbose:
        print("wrote %s (%.1f MB)" % (out, out.stat().st_size / 1e6))
    return out


def equity_panel(path: Optional[Path] = None) -> dict:
    """The materialised extension panel, as the books expect it."""
    if "equity" in _CACHE:
        return _CACHE["equity"]
    p = Path(path or PANEL_NPZ)
    if not p.exists():
        raise FileNotFoundError(
            "extension panel not built: %s - run build_extension_panel()" % p)
    z = np.load(p, allow_pickle=False)
    panel = {"symbols": list(z["symbols"]), "dates": z["dates"],
             "tr": z["tr"], "op_tr": z["op_tr"],
             "un": z["un"].astype(np.float64),
             "vol": z["vol"].astype(np.float64),
             "dv": z["dv"].astype(np.float64),
             "mem": z["mem"].astype(np.float64),
             "dataset_id": DS_EXTENSION}
    _CACHE["equity"] = panel
    return panel


def extension_eligible(panel: dict, t: int) -> np.ndarray:
    """A point-in-time index member at t that is TRADABLE on the pre-registered
    filter, with a finite total-return price at t and at t+1 (the entry).

    The liquidity window closes at t-1: a decision never reads its own
    session's turnover.
    """
    tr = panel["tr"]
    ok = (panel["mem"][:, t] > 0)
    ok &= np.isfinite(tr[:, t]) & (tr[:, t] > 0)
    if t + 1 < tr.shape[1]:
        ok &= np.isfinite(tr[:, t + 1])
    un = panel["un"][:, max(0, t - PRICE_LAG)]
    ok &= np.isfinite(un) & (un >= MIN_UNADJ_PRICE)
    lo, hi = max(0, t - PRICE_LAG - DV_WINDOW), max(0, t - PRICE_LAG)
    w = panel["dv"][:, lo:hi]
    med = np.full(w.shape[0], np.nan)
    if w.shape[1]:
        fin = np.isfinite(w)
        enough = fin.sum(axis=1) >= int(0.5 * DV_WINDOW)
        for i in np.where(enough)[0]:
            med[i] = float(np.median(w[i][fin[i]]))
    return ok & np.isfinite(med) & (med >= MIN_MEDIAN_DV)


# --------------------------------------------------------------------------- #
# Substrate 1b: PIT fundamentals for the extension universe
# --------------------------------------------------------------------------- #
#: The SEC bulk stores. ``cf_fact`` indexes only the 858 CIKs R58 materialised
#: for the S&P 500, so the extension universe is extracted from the archive
#: itself; ``ticker_current`` supplies the ticker -> CIK bridge.
SEC_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\identity")
COMPANYFACTS_ZIP = SEC_ROOT / "sec_bulk" / "companyfacts.zip"
ISSUER_DB = SEC_ROOT / "sec_issuer_history.sqlite"
FUND_NPZ = PANEL_ROOT / "extension_pit_fundamental_panel_v1.npz"

#: Concepts the two fundamental cells need, and nothing else. A concept not
#: named here cannot enter a feature, which is what makes the extraction a
#: pre-registered act rather than a fishing licence.
FUND_CONCEPTS = (
    ("us-gaap", "CommonStockSharesOutstanding"),
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "GrossProfit"),
    ("us-gaap", "Assets"),
    ("us-gaap", "Revenues"),
    ("us-gaap", "CostOfRevenue"),
)
FUND_NAMES = tuple(c for _t, c in FUND_CONCEPTS)


def base_ticker(sym: str) -> str:
    """Norgate spells a delisted security ``TICKER-YYYYMM``. The SEC knows it
    by its ticker, so the suffix is stripped for the bridge only - never for
    the panel, where the suffixed symbol remains the identity."""
    return str(sym).split("-", 1)[0].strip().upper()


_SUFFIX_RE = re.compile(
    r"\b(COMMON|CLASS [A-Z]|INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|"
    r"LTD|LIMITED|LLC|LP|PLC|HOLDINGS|HOLDING|GROUP|THE|NEW|SA|NV|AG|"
    r"TRUST|REIT)\b", re.I)


def norm_issuer(name: str) -> str:
    """The estate's normalisation, so a Norgate security name and an SEC
    issuer name meet in one spelling."""
    s = str(name or "").upper()
    s = s.replace("&", " AND ").replace(".", " ").replace(",", " ")
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = _SUFFIX_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def ticker_cik_bridge(symbols, *, verbose: bool = False) -> dict:
    """symbol -> zero-padded CIK, resolved by IDENTITY, not by ticker alone.

    THE SURVIVORSHIP TRAP THIS EXISTS TO AVOID. ``ticker_current`` holds only
    CURRENT tickers: it bridges 55% of this panel overall and just 181 of the
    1,484 delisted symbols. Running a fundamental book off that set would be a
    survivor-only book on a panel that was built specifically to RETAIN
    delisted names - manufacturing exactly the bias the substrate exists to
    remove.

    Worse, a delisted company's ticker is frequently REISSUED to a different
    company. Looking up a dead name's ticker in a current table does not fail
    loudly; it silently returns somebody else's financials. So a delisted
    symbol (Norgate spells it ``TICKER-YYYYMM``) is NEVER resolved by ticker.
    It is resolved by issuer NAME against ``name_lookup``, which carries
    72,209 FORMER names - that is how AAI-201105 reaches AirTran Holdings
    (CIK 0000835768) through a name the company no longer uses.
    """
    import sqlite3

    import norgatedata as nd

    con = sqlite3.connect("file:%s?mode=ro"
                          % str(ISSUER_DB).replace("\\", "/"), uri=True)
    try:
        cur = {str(k).strip().upper(): v for k, v in
               con.execute("SELECT ticker, cik FROM ticker_current")}
        names: dict = {}
        for nm, cik, _kind in con.execute(
                "SELECT norm_name, cik, kind FROM name_lookup"):
            names.setdefault(norm_issuer(nm), cik)
    finally:
        con.close()

    out, by_ticker, by_name = {}, 0, 0
    for s in symbols:
        sym = str(s)
        delisted = "-" in sym
        cik = None
        if not delisted:
            cik = cur.get(base_ticker(sym))
            if cik:
                by_ticker += 1
        if cik is None:
            try:
                nm = nd.security_name(sym)
            except Exception:                                  # noqa: BLE001
                nm = None
            if nm:
                cik = names.get(norm_issuer(nm))
                if cik:
                    by_name += 1
        if cik:
            out[sym] = str(cik).zfill(10)
    if verbose:
        print("bridge: %d of %d (%d by ticker, %d by issuer name)"
              % (len(out), len(list(symbols)), by_ticker, by_name))
    return out


def extract_fundamental_facts(symbols, *, verbose: bool = True) -> dict:
    """symbol -> concept -> list of (filed, period_end, value), filed-sorted.

    EVERY observation keeps its own ``filed`` date. Nothing is collapsed here,
    because the restatement rule (latest-filed-as-of-t) can only be applied
    against the full filing history - collapsing to one value per period would
    silently choose the restated number and reintroduce look-ahead.
    """
    import json
    import zipfile

    bridge = ticker_cik_bridge(symbols, verbose=verbose)
    want = {}
    for taxo, concept in FUND_CONCEPTS:
        want.setdefault(taxo, set()).add(concept)

    zf = zipfile.ZipFile(COMPANYFACTS_ZIP)
    have = set(zf.namelist())
    out: dict = {}
    miss = 0
    for n, (sym, cik) in enumerate(sorted(bridge.items())):
        member = "CIK%s.json" % cik
        if member not in have:
            miss += 1
            continue
        try:
            obj = json.loads(zf.read(member).decode("utf-8", "replace"))
        except Exception:                                      # noqa: BLE001
            miss += 1
            continue
        facts = obj.get("facts") or {}
        rec: dict = {}
        for taxo, concepts in want.items():
            node = facts.get(taxo) or {}
            for concept in concepts:
                c = node.get(concept)
                if not c:
                    continue
                obs = []
                for _unit, rows in (c.get("units") or {}).items():
                    for r in rows:
                        f, v = r.get("filed"), r.get("val")
                        if f and v is not None:
                            obs.append((str(f), str(r.get("end") or ""),
                                        float(v)))
                if obs:
                    obs.sort()
                    rec[concept] = obs
        if rec:
            out[sym] = rec
        if verbose and (n + 1) % 400 == 0:
            print("  facts %d/%d" % (n + 1, len(bridge)), flush=True)
    zf.close()
    if verbose:
        print("issuers with facts: %d (archive miss %d)" % (len(out), miss))
    return out


def _as_of(obs, cutoff: str):
    """The value from the LATEST filing STRICTLY BEFORE ``cutoff``.

    This is the restatement rule. A later filing that restates the same period
    is invisible until it is filed, which is the entire point.
    """
    best = None
    for filed, _end, val in obs:
        if filed < cutoff:
            best = val
        else:
            break
    return best


def build_fundamental_panel(out: Optional[Path] = None, *,
                            verbose: bool = True) -> Path:
    """Project the filing history onto the equity book's own decision grid.

    Cube is ``[n_names, n_decisions, n_concepts]``, aligned to the SAME symbol
    order and decision indices the price panel uses - verified, not assumed.
    """
    from alpha_agent.r57 import engine as K

    out = Path(out or FUND_NPZ)
    out.parent.mkdir(parents=True, exist_ok=True)
    panel = equity_panel()
    syms = list(panel["symbols"])
    dates = np.asarray(panel["dates"])
    dec = K.decision_indices(dates, r59.CADENCE, r59.DISCOVERY_START,
                             r59.HORIZON)
    if verbose:
        print("decision grid: %d (%s .. %s)"
              % (len(dec), dates[dec[0]], dates[dec[-1]]))

    facts = extract_fundamental_facts(syms, verbose=verbose)
    n_m, n_j, n_f = len(syms), len(dec), len(FUND_NAMES)
    cube = np.full((n_m, n_j, n_f), np.nan, dtype=np.float64)
    # The value that WAS KNOWN 252 sessions earlier - r60_02 needs the share
    # count as it stood a year ago, not today's opinion about a year ago.
    lag_cube = np.full((n_m, n_j), np.nan, dtype=np.float64)
    shares_ix = [FUND_NAMES.index("CommonStockSharesOutstanding"),
                 FUND_NAMES.index("EntityCommonStockSharesOutstanding")]

    for i, sym in enumerate(syms):
        rec = facts.get(sym)
        if not rec:
            continue
        for j, t in enumerate(dec):
            cutoff = str(dates[t])
            for k, concept in enumerate(FUND_NAMES):
                obs = rec.get(concept)
                if obs:
                    v = _as_of(obs, cutoff)
                    if v is not None:
                        cube[i, j, k] = v
            t0 = max(0, int(t) - 252)
            cut0 = str(dates[t0])
            for k in shares_ix:
                obs = rec.get(FUND_NAMES[k])
                if obs:
                    v0 = _as_of(obs, cut0)
                    if v0 is not None:
                        lag_cube[i, j] = v0
                        break
        if verbose and (i + 1) % 400 == 0:
            print("  projected %d/%d" % (i + 1, n_m), flush=True)

    np.savez_compressed(out, symbols=np.array(syms), dec=np.asarray(dec),
                        cube=cube, shares_252=lag_cube,
                        concepts=np.array(FUND_NAMES),
                        dataset_id="extension_pit_fundamental_panel_v1")
    if verbose:
        fin = np.isfinite(cube).any(axis=(1, 2)).sum()
        print("names with any fact: %d of %d" % (int(fin), n_m))
        print("wrote %s (%.1f MB)" % (out, out.stat().st_size / 1e6))
    return out


def fundamental_panel(path: Optional[Path] = None) -> dict:
    if "fund" in _CACHE:
        return _CACHE["fund"]
    p = Path(path or FUND_NPZ)
    if not p.exists():
        raise FileNotFoundError(
            "fundamental panel not built: %s - run build_fundamental_panel()"
            % p)
    z = np.load(p, allow_pickle=False)
    eq = equity_panel()
    if list(z["symbols"]) != list(eq["symbols"]):
        raise RuntimeError(
            "fundamental panel is not aligned to the price panel")
    f = {"symbols": list(z["symbols"]), "dec": z["dec"], "cube": z["cube"],
         "shares_252": z["shares_252"],
         "concepts": list(z["concepts"]),
         "slot": {int(t): j for j, t in enumerate(z["dec"])}}
    f["f_ix"] = {c: i for i, c in enumerate(f["concepts"])}
    _CACHE["fund"] = f
    return f


# --------------------------------------------------------------------------- #
# Substrate 1c: declared dividend events
# --------------------------------------------------------------------------- #
DIV_JSON = PANEL_ROOT / "us_equity_dividend_announcement_panel_v1.json"

#: The pre-registered BLOCKING coverage pre-check, frozen before measurement.
DIV_MIN_NONNULL = 0.80
DIV_CHECK_YEARS = tuple(str(y) for y in range(2011, 2024))


def build_dividend_panel(out: Optional[Path] = None, *,
                         limit: int = 0, verbose: bool = True) -> Path:
    """Pull /div history under the EXISTING EODHD entitlement.

    Every record keeps its ``declarationDate``. A record without one is kept
    but flagged: the pre-registration forbids dating it by its ex-date, and
    the coverage pre-check needs to COUNT the nulls rather than silently drop
    them.
    """
    import json
    import os
    import urllib.parse
    import urllib.request

    key = os.environ.get("EODHD_API_KEY")
    if not key:
        raise RuntimeError("EODHD_API_KEY is not set")
    out = Path(out or DIV_JSON)
    out.parent.mkdir(parents=True, exist_ok=True)

    panel = equity_panel()
    syms = [str(s) for s in panel["symbols"]]
    ever = (panel["mem"] > 0).any(axis=1)
    syms = [s for s, e in zip(syms, ever) if e]
    if limit:
        syms = syms[:int(limit)]
    if verbose:
        print("dividend pull: %d ever-member symbols" % len(syms))

    store: dict = {}
    errs = 0
    for i, sym in enumerate(syms):
        tic = base_ticker(sym)
        url = ("https://eodhd.com/api/div/%s.US?%s"
               % (urllib.parse.quote(tic),
                  urllib.parse.urlencode({"api_token": key, "fmt": "json",
                                          "from": "2009-01-01"})))
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                rows = json.loads(r.read().decode("utf-8", "replace"))
            if isinstance(rows, list):
                store[sym] = [
                    {"date": x.get("date"),
                     "declarationDate": x.get("declarationDate"),
                     "value": x.get("value"),
                     "period": x.get("period"),
                     "unadjustedValue": x.get("unadjustedValue")}
                    for x in rows]
        except Exception:                                      # noqa: BLE001
            errs += 1
        if verbose and (i + 1) % 250 == 0:
            print("  pulled %d/%d (errors %d)" % (i + 1, len(syms), errs),
                  flush=True)

    out.write_text(json.dumps(store), encoding="utf-8")
    if verbose:
        print("symbols with records: %d, errors %d" % (len(store), errs))
        print("wrote %s (%.1f MB)" % (out, out.stat().st_size / 1e6))
    return out


def dividend_coverage(store: dict) -> dict:
    """Non-null declarationDate fraction BY YEAR - the pre-registered check."""
    tot: dict = {}
    good: dict = {}
    for rows in store.values():
        for r in rows or ():
            d = str(r.get("date") or "")
            if len(d) < 4:
                continue
            y = d[:4]
            tot[y] = tot.get(y, 0) + 1
            if r.get("declarationDate"):
                good[y] = good.get(y, 0) + 1
    return {y: (good.get(y, 0) / tot[y]) for y in sorted(tot) if tot[y]}


def dividend_precheck(store: dict) -> dict:
    """PASS only if EVERY checked year clears the frozen floor.

    Frozen before measurement: years 2011-2023, floor 0.80. A year with no
    records at all FAILS - absence of dividend records is not evidence of
    full declaration-date coverage.
    """
    cov = dividend_coverage(store)
    failed = {y: round(cov.get(y, 0.0), 4) for y in DIV_CHECK_YEARS
              if cov.get(y, 0.0) < DIV_MIN_NONNULL}
    live = [s for s in store if "-" not in s]
    dead = [s for s in store if "-" in s]

    def _with(rows):
        return sum(1 for s in rows if store.get(s))

    return {"coverage_by_year": {k: round(v, 4) for k, v in cov.items()},
            "floor": DIV_MIN_NONNULL, "years_checked": list(DIV_CHECK_YEARS),
            "failed_years": failed, "passed": not failed,
            # REPORTED, NOT BLOCKING. The pre-registered gate is the
            # declaration-date null rate and nothing else; adding a second
            # blocking condition after the fact would be moving the frozen
            # goalposts. But the null rate is silent about SURVIVORSHIP, and
            # the smoke test showed a delisted symbol returning zero records -
            # so an event book could pass this gate while being live-only on a
            # panel that is half delisted. The skeptic is owed the number.
            "survivorship_diagnostic": {
                "live_symbols_with_records": _with(live),
                "live_symbols": len(live),
                "delisted_symbols_with_records": _with(dead),
                "delisted_symbols": len(dead),
                "meaning": ("if delisted coverage is near zero the event book "
                            "is survivor-only and its drift is not evidence "
                            "about the certified extension universe")}}


def dividend_store(path: Optional[Path] = None) -> dict:
    import json
    p = Path(path or DIV_JSON)
    if not p.exists():
        raise FileNotFoundError("dividend panel not built: %s" % p)
    return json.loads(p.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# Substrate 2: the certified futures layer, with the zero rule applied
# --------------------------------------------------------------------------- #
def futures_layer() -> dict:
    """The certified R38 layer, loaded through its canonical owner.

    THE ZERO RULE IS APPLIED HERE, once, at load: ``open_interest == 0``
    becomes NaN. Doing it at load rather than in each executor is deliberate -
    three hypotheses in this campaign read open interest, and a rule that has
    to be remembered three times is a rule that will be forgotten once.
    """
    if "layer" in _CACHE:
        return _CACHE["layer"]
    layer = dict(native.load_layer(verify_hashes=True))
    oi = np.array(layer["open_interest"], dtype=np.float64, copy=True)
    zeros = np.isfinite(oi) & (oi == 0.0)
    oi[zeros] = np.nan
    layer["open_interest"] = oi
    layer["oi_zeros_masked"] = int(zeros.sum())
    layer["own"] = np.isfinite(layer["ret"])

    # The layer carries arrays; the MANIFEST carries what each market IS.
    # Attached here so a universe rule reads one object, and translated into
    # the r59 asset-class vocabulary because the manifest's own spelling
    # ("COMMODITY", "INTERNATIONAL_EQUITY") is not the estate's
    # ("COMMODITY_FUTURES", "EQUITY_INDEX_FUTURES"). An untranslated compare
    # silently matches nothing and a universe quietly empties.
    meta = native.load_meta()
    layer["asset_class"] = [AC_MAP.get((meta.get(s) or {}).get("asset_class"),
                                       (meta.get(s) or {}).get("asset_class"))
                            for s in layer["symbols"]]
    layer["economic_group"] = [(meta.get(s) or {}).get("economic_group")
                               for s in layer["symbols"]]
    bps = np.array([(meta.get(s) or {}).get("cost_bps_per_side", np.nan)
                    for s in layer["symbols"]], dtype=np.float64)
    layer["cost_bps_per_side"] = bps
    # The frozen futures book charges ``cost_per_side`` as a RATE on traded
    # notional; the manifest states it in BASIS POINTS. Converting here, once,
    # keeps the single place where the two units meet - passing bps straight
    # through would have charged 10,000x the real cost and every cell would
    # have "failed" on a cost that was never real.
    layer["cost_per_side"] = bps / 1e4
    layer["cost_units"] = "rate on traded notional (manifest bps / 1e4)"
    # load_layer admits ONLY manifest-certified markets, so everything here is
    # certified by construction; the flag exists for the universe rules.
    layer["certified"] = np.ones(len(layer["symbols"]), dtype=bool)
    _CACHE["layer"] = layer
    return layer


def commodity_rows(layer: dict) -> np.ndarray:
    return np.array([a == r59.AC_COMMODITY for a in layer["asset_class"]])


def intl_index_rows(layer: dict) -> np.ndarray:
    groups = ("INTL_INDEX_FUTURES", "INTL_INDEX_FUTURES_EMERGING")
    return np.array([g in groups for g in layer["economic_group"]])


if __name__ == "__main__":
    lim = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    build_extension_panel(limit=lim)
