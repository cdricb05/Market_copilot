r"""alpha_agent.r61.identity_audit - the extension bridge audit (Workstream C).

RESEARCH DATA-QUALITY ONLY. THIS IS NOT AN ALPHA EXPERIMENT. It scores no
candidate, registers no hypothesis, charges no burden and measures no return
as evidence. It asks ONE question: can the issuer-name identity bridge under
the R60 extension universe be trusted?

WHY IT BLOCKS
-------------
The R60 substrate is the first the estate has ever had below the S&P 500 -
2,947 names, ~1,000 tradable per decision, delisted RETAINED - and the
director froze further candidate scoring on it until this audit passes. The
mechanism of harm is specific: identity for a DELISTED name is resolved by
ISSUER NAME, because its ticker has usually been reissued to somebody else. A
wrong name match splices one firm's share count onto another's across a
one-year ratio, which produces an enormous spurious log ratio and lands that
name at an EXTREME of the ranking. A book that takes the bottom 100 of about
1,000 is therefore a selection mechanism biased TOWARD mis-bridged names: the
rank construction concentrates identity error by design.

THE SPECIFIC DEFECT THIS AUDIT WAS BUILT AROUND
-----------------------------------------------
``panels.norm_issuer`` strips INC, CORP, CO, HOLDINGS, GROUP, TRUST, THE, NEW
and more before matching, and the bridge then does ``names.setdefault(key,
cik)`` - FIRST CIK WINS, silently. Over the SEC's 1,049,941 name rows that
normalisation produces 1,012,400 keys of which 23,631 map to two or more
distinct CIKs, including the EMPTY STRING (25 CIKs), ``CAPITAL`` (14),
``ENERGY`` (14) and ``VENTURE`` (9). Any panel symbol whose normalised name
lands on such a key was matched to whichever issuer the database happened to
yield first. That is not noise; it is an arbitrary assignment, and it is
measured here rather than assumed either way.

THE THRESHOLDS ARE PRE-REGISTERED
---------------------------------
:func:`pre_registration` is written to disk and content-hashed BEFORE any
measurement runs. An audit graded after the fact is not an audit.

THE VERDICT IS ONE OF THREE
---------------------------
    IDENTITY_BRIDGE_PASS          every pre-registered threshold measured and
                                  met
    IDENTITY_BRIDGE_FAIL          a threshold was measured and missed
    IDENTITY_BRIDGE_INCONCLUSIVE  a threshold could not be measured at all

INCONCLUSIVE IS NOT A SOFT PASS. A check with no independent evidence behind
it cannot be reported as passed, and the substrate stays frozen either way
unless the verdict is PASS.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

import numpy as np

from . import stable_hash

AUDIT_OWNER = "alpha_agent.r61.identity_audit"
AUDIT_VERSION = "R61_EXTENSION_IDENTITY_BRIDGE_AUDIT_V1"

PASS = "IDENTITY_BRIDGE_PASS"
FAIL = "IDENTITY_BRIDGE_FAIL"
INCONCLUSIVE = "IDENTITY_BRIDGE_INCONCLUSIVE"

# --------------------------------------------------------------------------- #
# PRE-REGISTERED THRESHOLDS. Frozen before measurement.
# --------------------------------------------------------------------------- #
THRESHOLDS = {
    # The name channel, cross-checked against the INDEPENDENT ticker channel
    # on the live population where both exist. This is the only place a true
    # false-match RATE is directly observable.
    "max_name_channel_false_match_rate_live": 0.02,
    # A matched CIK whose SEC filing window does not overlap the security's
    # own quoted window is a PROVEN wrong match, whatever its name said.
    "max_temporal_implausibility_rate": 0.05,
    # Symbols resolved through a normalised name key that maps to two or more
    # CIKs, where the bridge silently kept the first.
    "max_ambiguous_key_share": 0.10,
    # The rank book concentrates identity error, so bounding the AVERAGE is
    # not enough: low-confidence names must not be over-represented in the
    # extremes by more than this multiple of their own base rate.
    "max_extreme_decile_concentration_multiple": 2.0,
    # Survivorship must be CURED, not addressed: a delisted name that simply
    # stops appearing is survivorship bias with extra steps.
    "min_delisted_terminal_return_coverage": 0.95,
    # Errors that survive a non-positive filter: a sign flip, or a units spike
    # that jumps and reverts.
    "max_share_count_sign_error_rate": 0.005,
    "max_share_count_reverting_spike_rate": 0.01,
    # Attrition must not be correlated with the outcome. Measured as the
    # absolute difference in mean forward return between resolved and
    # unresolved delisted names, in annualised return units.
    "max_attrition_forward_return_gap": 0.05,
}

#: A share-count log ratio beyond this is not a corporate action anyone
#: performs in one year; it is an identity or units error.
EXTREME_LOG_RATIO = np.log(10.0)

#: A jump beyond this that REVERTS within the next two observations is a
#: units/entity spike rather than a real corporate action.
SPIKE_LOG_RATIO = np.log(5.0)


def pre_registration() -> dict:
    body = {
        "owner": AUDIT_OWNER, "version": AUDIT_VERSION,
        "is_alpha_experiment": False,
        "registers_hypothesis": False,
        "charges_search_burden": False,
        "substrate": "us_equity_extension_pit_panel_v1",
        "thresholds": dict(THRESHOLDS),
        "thresholds_frozen_before_measurement": True,
        "verdicts": [PASS, FAIL, INCONCLUSIVE],
        "verdict_rule": ("PASS only when every threshold was MEASURED and "
                         "met. A threshold that could not be measured yields "
                         "INCONCLUSIVE, which is not a soft pass: the "
                         "substrate stays frozen unless the verdict is PASS."),
        "independent_evidence_channels": [
            "ticker_current (ticker -> CIK), independent of the issuer-name "
            "channel, available on the live population",
            "issuer.first_filing / last_filing versus the security's own "
            "Norgate quoted window - a temporal falsification that needs no "
            "third-party identifier",
            "ticker_observation (ticker seen in a filing at a date)",
            "name_lookup key multiplicity - the bridge's own silent "
            "first-wins collision, counted",
        ],
    }
    body["pre_registration_hash"] = stable_hash(body)
    return body


class AuditRefusal(RuntimeError):
    """The audit could not be run as specified."""


_CACHE: dict = {}


def _r60_panels():
    if "pn" in _CACHE:
        return _CACHE["pn"]
    root = Path(__file__).resolve().parents[2] / "research" / "agents"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from campaign_r60_information_frontier import panels as PN  # noqa: PLC0415
    _CACHE["pn"] = PN
    return PN


def _ro(path) -> sqlite3.Connection:
    return sqlite3.connect(
        "file:" + str(path).replace("\\", "/") + "?mode=ro", uri=True)


# --------------------------------------------------------------------------- #
# The instrumented bridge
# --------------------------------------------------------------------------- #
def build_identity_map(symbols, *, verbose: bool = False) -> dict:
    """``panels.ticker_cik_bridge`` with its working shown.

    Same resolution ORDER, same normalisation, same first-wins rule - and for
    every symbol it also records HOW the match was made, which name key it
    went through, how many distinct CIKs that key covers, and what the
    independent channels say. The bridge is not changed; it is instrumented.
    """
    import norgatedata as nd                                  # noqa: PLC0415

    PN = _r60_panels()
    con = _ro(PN.ISSUER_DB)
    try:
        ticker_cik: dict = defaultdict(set)
        for tk, cik in con.execute("SELECT ticker, cik FROM ticker_current"):
            ticker_cik[str(tk).strip().upper()].add(str(cik))
        # The bridge's own map: first CIK wins per normalised key.
        first_cik: dict = {}
        key_ciks: dict = defaultdict(set)
        for nm, cik, _kind in con.execute(
                "SELECT norm_name, cik, kind FROM name_lookup"):
            k = PN.norm_issuer(nm)
            first_cik.setdefault(k, cik)
            key_ciks[k].add(str(cik))
        issuer: dict = {}
        for cik, first_f, last_f, still in con.execute(
                "SELECT cik, first_filing, last_filing, still_active"
                " FROM issuer"):
            issuer[str(cik)] = (first_f, last_f, int(still or 0))
        tick_obs: dict = defaultdict(set)
        for cik, tk in con.execute(
                "SELECT cik, ticker FROM ticker_observation"):
            tick_obs[str(tk).strip().upper()].add(str(cik))
    finally:
        con.close()

    rows = []
    for i, s in enumerate(symbols):
        sym = str(s)
        delisted = "-" in sym
        base = PN.base_ticker(sym)
        try:
            name = nd.security_name(sym)
        except Exception:                                      # noqa: BLE001
            name = None
        key = PN.norm_issuer(name) if name else None
        try:
            fq = str(nd.first_quoted_date(sym) or "")[:10] or None
            lq = str(nd.last_quoted_date(sym) or "")[:10] or None
        except Exception:                                      # noqa: BLE001
            fq = lq = None

        by_ticker = None
        if not delisted:
            cands = ticker_cik.get(base) or set()
            by_ticker = (sorted(cands)[0] if len(cands) == 1
                         else (sorted(cands)[0] if cands else None))
        by_name = first_cik.get(key) if key else None

        cik = None
        channel = "UNRESOLVED"
        if by_ticker:
            cik, channel = by_ticker, "TICKER"
        elif by_name:
            cik, channel = by_name, "NAME"

        n_ciks = len(key_ciks.get(key) or ()) if key else 0
        first_f, last_f, still = issuer.get(str(cik), (None, None, 0)) \
            if cik else (None, None, 0)
        rows.append({
            "symbol": sym, "base_ticker": base, "delisted": bool(delisted),
            "security_name": name, "norm_key": key,
            "first_quoted": fq, "last_quoted": lq,
            "cik": (str(cik).zfill(10) if cik else None),
            "channel": channel,
            "name_key_cik_count": int(n_ciks),
            "ambiguous_key": bool(n_ciks > 1),
            "empty_key": bool(key is not None and key == ""),
            "cik_by_ticker": (str(by_ticker).zfill(10) if by_ticker else None),
            "cik_by_name": (str(by_name).zfill(10) if by_name else None),
            "cik_first_filing": first_f, "cik_last_filing": last_f,
            "cik_still_active": int(still),
            "ticker_observation_ciks": sorted(tick_obs.get(base) or ()),
        })
        if verbose and i % 500 == 0:
            print("   bridge %d/%d" % (i, len(list(symbols))), flush=True)

    body = {
        "owner": AUDIT_OWNER, "version": AUDIT_VERSION,
        "n_symbols": len(rows),
        "rows": rows,
    }
    body["identity_map_hash"] = stable_hash(
        [[r["symbol"], r["cik"], r["channel"]] for r in rows])
    return body


# --------------------------------------------------------------------------- #
# Measurement 1 - the name channel, cross-checked on the live population
# --------------------------------------------------------------------------- #
def measure_name_channel_false_match(rows: list) -> dict:
    """Where BOTH channels resolve a live name, do they name the same issuer?

    The bridge resolves a LIVE symbol by ticker and only falls back to the
    name. So the live population is where an independent identifier exists for
    the channel that carries every delisted name - and disagreement there is a
    directly observed false match of the NAME channel.
    """
    both = [r for r in rows
            if not r["delisted"] and r["cik_by_ticker"] and r["cik_by_name"]]
    if not both:
        return {"measurable": False, "n": 0,
                "reason": "no live symbol resolved by both channels"}
    bad = [r for r in both if r["cik_by_ticker"] != r["cik_by_name"]]
    amb = [r for r in bad if r["ambiguous_key"]]
    return {
        "measurable": True, "n": len(both), "mismatches": len(bad),
        "rate": len(bad) / float(len(both)),
        "mismatches_on_ambiguous_key": len(amb),
        "share_of_mismatches_from_ambiguous_keys":
            (len(amb) / float(len(bad)) if bad else None),
        "examples": [{"symbol": r["symbol"], "name": r["security_name"],
                      "key": r["norm_key"], "by_ticker": r["cik_by_ticker"],
                      "by_name": r["cik_by_name"],
                      "key_cik_count": r["name_key_cik_count"]}
                     for r in bad[:15]],
    }


# --------------------------------------------------------------------------- #
# Measurement 2 - temporal falsification
# --------------------------------------------------------------------------- #
def measure_temporal_plausibility(rows: list) -> dict:
    """Does the matched issuer's filing life overlap the security's quoted life?

    A CIK that stopped filing before the security began trading, or began
    filing after it stopped, is not that security's issuer. This needs no
    third-party identifier and it falsifies a match outright rather than
    casting doubt on it.
    """
    checked, bad = [], []
    for r in rows:
        if not r["cik"] or not r["first_quoted"]:
            continue
        ff, lf = r["cik_first_filing"], r["cik_last_filing"]
        if not ff:
            continue
        checked.append(r)
        # Half-open overlap with a one-year grace on each side: a company can
        # list shortly before its first filing and be delisted shortly after
        # its last, and neither is evidence of a wrong match.
        #
        # AN UNKNOWN END DATE FALSIFIES NOTHING. ``last_filing`` is NULL for a
        # large share of issuers and ``still_active`` does not reliably
        # distinguish "still filing" from "end unrecorded", so a NULL is
        # treated as OPEN-ENDED. Reading it as "stopped at first_filing"
        # flagged every live ticker match as implausible - a 100% rate, which
        # is the signature of a broken test rather than a broken bridge.
        starts_too_late = ff > _shift_year(r["last_quoted"] or OPEN_ENDED, +1)
        ends_too_early = bool(lf) and lf < _shift_year(r["first_quoted"], -1)
        if starts_too_late or ends_too_early:
            bad.append(r)
    if not checked:
        return {"measurable": False, "n": 0,
                "reason": "no matched symbol carried both a filing window and "
                          "a quoted window"}
    by_ch: dict = defaultdict(lambda: [0, 0])
    for r in checked:
        by_ch[r["channel"]][0] += 1
    for r in bad:
        by_ch[r["channel"]][1] += 1
    return {
        "measurable": True, "n": len(checked), "implausible": len(bad),
        "rate": len(bad) / float(len(checked)),
        "by_channel": {k: {"checked": v[0], "implausible": v[1],
                           "rate": (v[1] / v[0]) if v[0] else None}
                       for k, v in by_ch.items()},
        "delisted_rate": _rate(bad, checked, lambda r: r["delisted"]),
        "live_rate": _rate(bad, checked, lambda r: not r["delisted"]),
        "examples": [{"symbol": r["symbol"], "name": r["security_name"],
                      "quoted": [r["first_quoted"], r["last_quoted"]],
                      "cik": r["cik"],
                      "filings": [r["cik_first_filing"],
                                  r["cik_last_filing"]],
                      "channel": r["channel"]} for r in bad[:15]],
    }


#: An open-ended date. It must stay FOUR digits: these comparisons are string
#: comparisons, and a year that overflows to 10000 sorts BEFORE 2016 because
#: "1" < "2". Shifting the sentinel by a year did exactly that and flagged
#: every live ticker match as implausible.
OPEN_ENDED = "9999-12-31"


def _shift_year(date: str, years: int) -> str:
    try:
        y = int(str(date)[:4]) + int(years)
    except (TypeError, ValueError):
        return str(date)
    if y > 9999:
        return OPEN_ENDED
    if y < 1:
        return "0001-01-01"
    return "%04d%s" % (y, str(date)[4:10])


def _rate(bad: list, checked: list, pred) -> Optional[float]:
    den = [r for r in checked if pred(r)]
    if not den:
        return None
    return len([r for r in bad if pred(r)]) / float(len(den))


# --------------------------------------------------------------------------- #
# Measurement 3 - the bridge's own silent collisions
# --------------------------------------------------------------------------- #
def measure_ambiguous_keys(rows: list) -> dict:
    resolved = [r for r in rows if r["cik"]]
    by_name = [r for r in resolved if r["channel"] == "NAME"]
    if not resolved:
        return {"measurable": False, "n": 0, "reason": "nothing resolved"}
    amb = [r for r in resolved if r["ambiguous_key"]]
    amb_name = [r for r in by_name if r["ambiguous_key"]]
    empty = [r for r in resolved if r["empty_key"]]
    return {
        "measurable": True,
        "n_resolved": len(resolved), "n_resolved_by_name": len(by_name),
        "ambiguous": len(amb), "share": len(amb) / float(len(resolved)),
        "ambiguous_among_name_resolved": len(amb_name),
        "share_among_name_resolved":
            (len(amb_name) / float(len(by_name)) if by_name else None),
        "empty_normalised_key": len(empty),
        "worst": sorted(
            [{"symbol": r["symbol"], "name": r["security_name"],
              "key": r["norm_key"], "key_cik_count": r["name_key_cik_count"],
              "delisted": r["delisted"], "channel": r["channel"]}
             for r in amb], key=lambda d: -d["key_cik_count"])[:15],
    }


# --------------------------------------------------------------------------- #
# Measurement 4 - the unresolved tail, classified by EVIDENCE
# --------------------------------------------------------------------------- #
def classify_unresolved(rows: list, panel: dict) -> dict:
    """Why did a symbol not bridge, and what did its price path do at the end?

    The legal reason a company disappeared is NOT observable from anything the
    estate owns for a name that never bridged - by definition it has no CIK to
    look one up with. So the classification reports what IS observable: the
    reason the BRIDGE failed, and the terminal price path as a proxy for the
    kind of termination (a collapse to near zero looks like a wind-up, a final
    premium looks like a cash acquisition, a flat end looks like a share
    exchange). These are named for the evidence, not for a legal reason the
    audit cannot see.
    """
    sym_ix = {str(s): i for i, s in enumerate(panel["symbols"])}
    tr = panel["tr"]
    out: dict = defaultdict(int)
    paths: dict = defaultdict(int)
    detail = []
    unres = [r for r in rows if not r["cik"]]
    for r in unres:
        if not r["security_name"]:
            why = "NO_VENDOR_SECURITY_NAME"
        elif r["norm_key"] == "":
            why = "NORMALISATION_PRODUCED_AN_EMPTY_KEY"
        elif r["ambiguous_key"]:
            why = "NAME_KEY_AMBIGUOUS_BUT_UNMATCHED"
        else:
            why = "NAME_NOT_IN_SEC_INDEX"
        out[why] += 1
        path = "NO_PRICE_PATH"
        i = sym_ix.get(r["symbol"])
        if i is not None:
            v = tr[i]
            fin = np.where(np.isfinite(v))[0]
            if fin.size > 25:
                last, ref = v[fin[-1]], v[fin[max(0, fin.size - 22)]]
                if ref > 0:
                    ret = last / ref - 1.0
                    path = ("TERMINAL_COLLAPSE" if ret <= -0.50
                            else "TERMINAL_PREMIUM" if ret >= 0.10
                            else "TERMINAL_NEUTRAL")
        paths[path] += 1
        if len(detail) < 20:
            detail.append({"symbol": r["symbol"], "name": r["security_name"],
                           "why": why, "terminal_path": path})
    delisted = [r for r in rows if r["delisted"]]
    live = [r for r in rows if not r["delisted"]]
    return {
        "n_unresolved": len(unres),
        "unresolved_share_overall": len(unres) / float(len(rows) or 1),
        "unresolved_share_delisted":
            (len([r for r in unres if r["delisted"]]) / float(len(delisted))
             if delisted else None),
        "unresolved_share_live":
            (len([r for r in unres if not r["delisted"]]) / float(len(live))
             if live else None),
        "by_bridge_failure_reason": dict(out),
        "by_terminal_price_path": dict(paths),
        "examples": detail,
        "note": ("The LEGAL termination reason (merger, acquisition, "
                 "bankruptcy, exchange move, deregistration) is not "
                 "observable for a name that never bridged - it has no CIK to "
                 "look one up with. What is reported is the measurable "
                 "reason the BRIDGE failed and the terminal price path as a "
                 "proxy for the kind of termination."),
    }


# --------------------------------------------------------------------------- #
# Measurement 5 - is attrition correlated with the outcome?
# --------------------------------------------------------------------------- #
def measure_attrition_correlation(rows: list, panel: dict) -> dict:
    """Do unresolved delisted names differ from resolved ones on what matters?

    The hazard is that the missing tail is not random - that it is
    concentrated in names whose forward return differs systematically. That is
    measurable from the price panel alone, with no fundamentals and therefore
    no circularity.
    """
    sym_ix = {str(s): i for i, s in enumerate(panel["symbols"])}
    tr = panel["tr"]

    def terminal_stats(rs):
        rets, sizes = [], []
        for r in rs:
            i = sym_ix.get(r["symbol"])
            if i is None:
                continue
            v = tr[i]
            fin = np.where(np.isfinite(v))[0]
            if fin.size < 260:
                continue
            last = v[fin[-1]]
            ref = v[fin[max(0, fin.size - 22)]]
            yr = v[fin[max(0, fin.size - 253)]]
            if ref > 0:
                rets.append(last / ref - 1.0)
            if yr > 0:
                sizes.append(last / yr - 1.0)
        return rets, sizes

    delisted = [r for r in rows if r["delisted"]]
    res = [r for r in delisted if r["cik"]]
    unres = [r for r in delisted if not r["cik"]]
    r_ret, r_yr = terminal_stats(res)
    u_ret, u_yr = terminal_stats(unres)
    if not r_ret or not u_ret:
        return {"measurable": False,
                "reason": "not enough price history on one of the two groups"}
    gap = abs(float(np.mean(r_ret)) - float(np.mean(u_ret)))
    from scipy.stats import ttest_ind                          # noqa: PLC0415
    tt = ttest_ind(r_ret, u_ret, equal_var=False)
    return {
        "measurable": True,
        "n_resolved_delisted": len(r_ret), "n_unresolved_delisted": len(u_ret),
        "mean_terminal_21s_return_resolved": float(np.mean(r_ret)),
        "mean_terminal_21s_return_unresolved": float(np.mean(u_ret)),
        "gap": gap,
        "welch_t": float(tt.statistic), "welch_p": float(tt.pvalue),
        "mean_terminal_252s_return_resolved":
            (float(np.mean(r_yr)) if r_yr else None),
        "mean_terminal_252s_return_unresolved":
            (float(np.mean(u_yr)) if u_yr else None),
        "note": ("The skeptic's sharpest hazard - that the missing tail is "
                 "disproportionately stock-financed M&A at firms that had "
                 "been repurchasing - is NOT answerable here: measuring "
                 "issuance for an unresolved name requires the bridge that "
                 "failed on it. It is reported as an untestable hazard, "
                 "never as a passed check."),
    }


# --------------------------------------------------------------------------- #
# Measurement 6 - does the rank book concentrate identity error?
# --------------------------------------------------------------------------- #
def measure_extreme_rank_contamination(rows: list, fund: dict) -> dict:
    """What share of the feature's EXTREME deciles is low-confidence identity?

    The feature is R60's own: the log ratio of shares outstanding to its value
    252 sessions earlier, which is what r60_02 ranked. Bounding the average
    error rate is not enough - a rank book selects the extremes, so the
    question is whether low-confidence names are over-represented THERE.
    """
    conf = {r["symbol"]: (not r["ambiguous_key"]) and bool(r["cik"])
            for r in rows}
    syms = [str(s) for s in fund["symbols"]]
    low = np.array([not conf.get(s, False) for s in syms])
    shares = fund["shares_252"]
    cube = fund["cube"]
    ix = fund["f_ix"]
    col = ix.get("CommonStockSharesOutstanding")
    if col is None:
        return {"measurable": False, "reason": "share concept not in panel"}
    now = cube[:, :, col]
    with np.errstate(invalid="ignore", divide="ignore"):
        ratio = np.log(np.where((now > 0) & (shares > 0), now / shares,
                                np.nan))
    base_scored, ext_low, ext_n = [], 0, 0
    for j in range(ratio.shape[1]):
        col_v = ratio[:, j]
        m = np.isfinite(col_v)
        n = int(m.sum())
        if n < 50:
            continue
        base_scored.append(float(low[m].mean()))
        k = max(1, n // 10)
        order = np.argsort(np.where(m, col_v, np.nan))
        valid = order[:n]
        top, bot = valid[-k:], valid[:k]
        ext = np.concatenate([top, bot])
        ext_low += int(low[ext].sum())
        ext_n += int(ext.size)
    if not base_scored or not ext_n:
        return {"measurable": False, "reason": "no decision had 50+ scored"}
    base = float(np.mean(base_scored))
    ext_rate = ext_low / float(ext_n)
    return {
        "measurable": True,
        "feature": "log(shares_outstanding_t / shares_outstanding_t-252) "
                   "- the r60_02 feature",
        "base_low_confidence_rate": base,
        "extreme_decile_low_confidence_rate": ext_rate,
        "concentration_multiple": (ext_rate / base) if base > 0 else None,
        "extreme_observations": int(ext_n),
        "decisions_scored": len(base_scored),
    }


# --------------------------------------------------------------------------- #
# Measurement 7 - errors that survive a non-positive filter
# --------------------------------------------------------------------------- #
def measure_data_quality(fund: dict) -> dict:
    """Sign flips, and units spikes that jump and revert.

    A -25,878,050 share count later corrected to +25,878,050 is a sign error,
    and the errors that SURVIVE a non-positive filter are the positive ones: a
    units error of 1000x stays positive, passes the filter, and lands in the
    same extreme tail the book selects from.
    """
    ix = fund["f_ix"]
    col = ix.get("CommonStockSharesOutstanding")
    if col is None:
        return {"measurable": False, "reason": "share concept not in panel"}
    v = fund["cube"][:, :, col]
    finite = np.isfinite(v)
    n_obs = int(finite.sum())
    if not n_obs:
        return {"measurable": False, "reason": "no share observations"}
    negatives = int((finite & (v < 0)).sum())
    zeros = int((finite & (v == 0)).sum())
    spikes = 0
    extremes = 0
    comparisons = 0
    for i in range(v.shape[0]):
        row = v[i]
        f = np.where(np.isfinite(row) & (row > 0))[0]
        if f.size < 3:
            continue
        series = row[f]
        lr = np.log(series[1:] / series[:-1])
        comparisons += int(lr.size)
        extremes += int((np.abs(lr) > EXTREME_LOG_RATIO).sum())
        for j in range(lr.size - 1):
            if abs(lr[j]) > SPIKE_LOG_RATIO and \
                    np.sign(lr[j + 1]) == -np.sign(lr[j]) and \
                    abs(lr[j + 1]) > SPIKE_LOG_RATIO:
                spikes += 1
    return {
        "measurable": True, "observations": n_obs,
        "negative_values": negatives,
        "sign_error_rate": negatives / float(n_obs),
        "zero_values": zeros,
        "consecutive_comparisons": comparisons,
        "extreme_year_on_year_moves_gt_10x": extremes,
        "reverting_spikes": spikes,
        "reverting_spike_rate": (spikes / float(comparisons)
                                 if comparisons else None),
    }


# --------------------------------------------------------------------------- #
# Measurement 8 - is survivorship actually cured?
# --------------------------------------------------------------------------- #
def measure_delisting_returns(rows: list, panel: dict) -> dict:
    """Does every delisted name carry a terminal return, or does it vanish?

    A retained-but-truncated delisted name is survivorship bias with extra
    steps. The test is that the price series runs to the vendor's own last
    quoted date rather than stopping early and leaving the book holding
    nothing.
    """
    dates = np.asarray(panel["dates"])
    sym_ix = {str(s): i for i, s in enumerate(panel["symbols"])}
    tr = panel["tr"]
    delisted = [r for r in rows if r["delisted"]]
    have, checked, gaps = 0, 0, []
    for r in delisted:
        i = sym_ix.get(r["symbol"])
        if i is None or not r["last_quoted"]:
            continue
        fin = np.where(np.isfinite(tr[i]))[0]
        if fin.size == 0:
            checked += 1
            continue
        checked += 1
        last_panel = str(dates[fin[-1]])
        lq = str(r["last_quoted"])
        if last_panel >= lq or (lq > str(dates[-1])):
            have += 1
        else:
            gaps.append({"symbol": r["symbol"], "panel_last": last_panel,
                         "vendor_last_quoted": lq})
    return {
        "measurable": bool(checked),
        "delisted_symbols": len(delisted), "checked": checked,
        "carrying_terminal_return": have,
        "coverage": (have / float(checked) if checked else None),
        "truncated_examples": gaps[:15],
    }


# --------------------------------------------------------------------------- #
# The verdict
# --------------------------------------------------------------------------- #
def adjudicate(measurements: dict,
               thresholds: Optional[dict] = None) -> dict:
    """Grade every pre-registered threshold. PASS needs all of them MEASURED."""
    th = dict(thresholds or THRESHOLDS)
    checks = []

    def add(name, measured, limit, direction, measurable, note=""):
        ok = (None if not measurable
              else (measured <= limit if direction == "max"
                    else measured >= limit))
        checks.append({"check": name, "measured": measured,
                       "frozen_threshold": limit, "direction": direction,
                       "measurable": bool(measurable), "passed": ok,
                       "note": note})

    m = measurements
    nm = m.get("name_channel_false_match") or {}
    add("name_channel_false_match_rate_live", nm.get("rate"),
        th["max_name_channel_false_match_rate_live"], "max",
        nm.get("measurable"), nm.get("reason", ""))

    tp = m.get("temporal_plausibility") or {}
    add("temporal_implausibility_rate", tp.get("rate"),
        th["max_temporal_implausibility_rate"], "max", tp.get("measurable"),
        tp.get("reason", ""))

    ak = m.get("ambiguous_keys") or {}
    add("ambiguous_key_share", ak.get("share"),
        th["max_ambiguous_key_share"], "max", ak.get("measurable"),
        ak.get("reason", ""))

    ec = m.get("extreme_rank_contamination") or {}
    add("extreme_decile_concentration_multiple",
        ec.get("concentration_multiple"),
        th["max_extreme_decile_concentration_multiple"], "max",
        ec.get("measurable") and ec.get("concentration_multiple") is not None,
        ec.get("reason", ""))

    dr = m.get("delisting_returns") or {}
    add("delisted_terminal_return_coverage", dr.get("coverage"),
        th["min_delisted_terminal_return_coverage"], "min",
        dr.get("measurable") and dr.get("coverage") is not None)

    dq = m.get("data_quality") or {}
    add("share_count_sign_error_rate", dq.get("sign_error_rate"),
        th["max_share_count_sign_error_rate"], "max", dq.get("measurable"),
        dq.get("reason", ""))
    add("share_count_reverting_spike_rate", dq.get("reverting_spike_rate"),
        th["max_share_count_reverting_spike_rate"], "max",
        dq.get("measurable") and dq.get("reverting_spike_rate") is not None,
        dq.get("reason", ""))

    at = m.get("attrition_correlation") or {}
    add("attrition_forward_return_gap", at.get("gap"),
        th["max_attrition_forward_return_gap"], "max", at.get("measurable"),
        at.get("reason", ""))

    unmeasurable = [c["check"] for c in checks if not c["measurable"]]
    failed = [c["check"] for c in checks if c["passed"] is False]
    if failed:
        verdict = FAIL
    elif unmeasurable:
        verdict = INCONCLUSIVE
    else:
        verdict = PASS
    return {
        "verdict": verdict, "checks": checks,
        "failed_checks": failed, "unmeasurable_checks": unmeasurable,
        "substrate_reuse_allowed": verdict == PASS,
        "rule": ("PASS only when every pre-registered threshold was MEASURED "
                 "and met. INCONCLUSIVE is not a soft pass; the substrate "
                 "stays frozen unless the verdict is PASS."),
    }


def run(*, verbose: bool = True) -> dict:
    """The whole audit. Read-only over owned stores."""
    PN = _r60_panels()
    panel = PN.equity_panel()
    fund = PN.fundamental_panel()
    symbols = [str(s) for s in panel["symbols"]]
    if verbose:
        print("bridging %d symbols ..." % len(symbols), flush=True)
    imap = build_identity_map(symbols, verbose=verbose)
    rows = imap["rows"]
    if verbose:
        print("measuring ...", flush=True)
    measurements = {
        "name_channel_false_match": measure_name_channel_false_match(rows),
        "temporal_plausibility": measure_temporal_plausibility(rows),
        "ambiguous_keys": measure_ambiguous_keys(rows),
        "unresolved_tail": classify_unresolved(rows, panel),
        "attrition_correlation": measure_attrition_correlation(rows, panel),
        "extreme_rank_contamination":
            measure_extreme_rank_contamination(rows, fund),
        "data_quality": measure_data_quality(fund),
        "delisting_returns": measure_delisting_returns(rows, panel),
    }
    verdict = adjudicate(measurements)
    resolved = [r for r in rows if r["cik"]]
    delisted = [r for r in rows if r["delisted"]]
    return {
        "owner": AUDIT_OWNER, "version": AUDIT_VERSION,
        "substrate": "us_equity_extension_pit_panel_v1",
        "identity_map_hash": imap["identity_map_hash"],
        "coverage": {
            "symbols": len(rows),
            "resolved": len(resolved),
            "resolved_share": len(resolved) / float(len(rows) or 1),
            "delisted": len(delisted),
            "delisted_resolved_share":
                (len([r for r in delisted if r["cik"]]) / float(len(delisted))
                 if delisted else None),
            "live_resolved_share":
                (len([r for r in rows if not r["delisted"] and r["cik"]])
                 / float(len([r for r in rows if not r["delisted"]]) or 1)),
            "by_channel": {c: sum(1 for r in rows if r["channel"] == c)
                           for c in ("TICKER", "NAME", "UNRESOLVED")},
        },
        "measurements": measurements,
        **verdict,
    }
