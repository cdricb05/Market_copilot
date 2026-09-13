r"""alpha_agent.alpha_recovery.treasury_month_end_duration_extension - the preregistered
bond-index month-end duration-extension executor (``TREASURY_INDEX_MONTH_END_DURATION_EXTENSION_V1``).

Executes ``research/preregistration/TREASURY_INDEX_MONTH_END_DURATION_EXTENSION_PREREGISTRATION.md``
and changes nothing in it.

    mechanism    bond index providers add newly settled Treasuries only at month end, so
                 benchmarked funds buy duration at the month-end close; months whose settled
                 issuance adds more duration should see a larger lift into that close
    measure      DA_M = sum, over nominal fixed-coupon notes and bonds whose issueDate is in M
                 and whose auctionDate is STRICTLY BEFORE M's entry session, of
                 offeringAmount x approximate modified duration (par bond, yield = coupon)
    ranking      high_M = DA_M > median(DA_{M-12} .. DA_{M-1}); prior months only
    position     LONG one unit of ZN notional from the close of L[-3] to the close of L[-1]
                 (L = M's R38 sessions) in high months; flat otherwise
    increment    z_M = (1[high_M] - pi_W) * u_M; u_M the unconditional month-end long's net
                 return at the gate cost, pi_W the window's share of high months (a count)
    data         the TreasuryDirect TA_WS cache OWNED by treasury_auction_concession (never
                 fetched here) and the owned R38 native contract layer (ZN)
    statistics   alpha_agent.r63.sensitivity.nw_tstat, bh_fdr and _max_dd - reused

The measure, the ranking and the census read no return. Returns are read only by
:func:`window_rows`, and the CONFIRMATION window only after every qualification gate passed.

RESEARCH ONLY. No purchase, no subscription, no promotion, no registration, no capital
allocation, no portfolio mutation, no proposal, no order, no fill, no live write. Every write
lands under the campaign research root.
"""
from __future__ import annotations

import math
from datetime import date

import numpy as np
import pandas as pd

from alpha_agent.r59 import native as N
from alpha_agent.r63 import sensitivity as S

from . import (BH_Q, MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, research_root, stable_hash,
               write_artifact)
from . import treasury_auction_concession as TA

CALCULATION_OWNER = "alpha_agent.alpha_recovery.treasury_month_end_duration_extension"
MECHANISM_ID = "TREASURY_INDEX_MONTH_END_DURATION_EXTENSION_V1"
ARTIFACT_NAME = "treasury_month_end_duration_extension.json"
CENSUS_ARTIFACT_NAME = "treasury_month_end_duration_census.json"
PREREGISTRATION = ("research/preregistration/"
                   "TREASURY_INDEX_MONTH_END_DURATION_EXTENSION_PREREGISTRATION.md")
SCORER = ("alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd + "
          "alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily")

#: The catalog's frozen kill rule, verbatim. A different rule is refused, never judged.
KILL_RULE_FROZEN = (
    "Kill if the extension-ranked month-end long book has NW t < 2.0 or net below 1.5 %/yr at "
    "the R38 per-market cost (2 bp per side); or its increment over an unconditional month-end "
    "long of the same futures has NW t < 2.0; or it fails BH q=0.10 over m=3 with "
    "R32_EVENT_DRIVEN_CALENDAR and TREASURY_AUCTION_SUPPLY_CONCESSION_V1 inherited at p=1.")

# --------------------------------------------------------------------------- #
# Frozen design (section numbers refer to the preregistration)
# --------------------------------------------------------------------------- #
MARKET = "ZN"                                   # s5: the one instrument
#: s3: classifier exclusions (alpha_agent.alpha_recovery.treasury_auction_concession.classify)
#: that are NOT index supply. Every other nominal coupon (2y..30y, reopenings) counts.
NOT_INDEX_SUPPLY = ("TIPS", "FRN", "NOT_A_NOMINAL_COUPON_SECURITY", "UNPARSEABLE_TERM")
#: census only: the futures bucket whose deliverable range holds each remaining term
TENOR_BUCKET = {2: "ZT", 3: "ZT", 5: "ZF", 6: "ZF", 7: "ZN", 9: "ZN", 10: "ZN", 20: "ZB", 30: "ZB"}
MEASURE_START = "2000-01"                       # first month of the TA_WS cache
LOOKBACK_MONTHS = 12                            # s4
LOOKBACK_CENSUS = (12, 24, 36)                  # alternatives measured, NOT tested
ENTRY_FROM_END = 3                              # s6: entry close L[-3]
HOLD_SESSIONS = 2                               # return sessions L[-2], L[-1]
MIN_MONTH_SESSIONS = 10
QUALIFICATION = ("2000-01", "2016-12")
HALVES = {"H1_2000_2008": ("2000-01", "2008-12"), "H2_2009_2016": ("2009-01", "2016-12")}
CONFIRMATION = ("2017-01", "2026-08")
WINDOWS = {"QUALIFICATION": QUALIFICATION, "CONFIRMATION": CONFIRMATION}
KILL_RULE_COST_BPS = 2.0
STRESS_COST_BPS = 5.0
COST_LADDER_BPS = (0.0, 2.0, 5.0)
PERIODS_PER_YEAR = 12.0
T_FLOOR = 2.0
MIN_USABLE_MONTH_SHARE = 0.95
MAX_FIELD_MISSING_SHARE = 0.02
INHERITED_NULLS = ("R32_EVENT_DRIVEN_CALENDAR", "TREASURY_AUCTION_SUPPLY_CONCESSION_V1")

# Executor verdict vocabulary (alpha_agent.r59.mechanisms.EXECUTOR_VERDICTS).
V_QUALIFIED = "QUALIFIED"
V_NO_EDGE = "NO_EDGE"
V_NO_INC_INFO = "NO_INCREMENTAL_INFORMATION_EDGE"
V_SAMPLE = "KILLED_INSUFFICIENT_SAMPLE"
V_MATERIALITY = "KILLED_BELOW_MATERIALITY"
V_NONINCREMENTAL = "KILLED_NONINCREMENTAL"
V_UNSTABLE = "KILLED_UNSTABLE"
V_WRONG_SIGN = "KILLED_WRONG_SIGN"
V_MULTIPLICITY = "KILLED_MULTIPLICITY"
V_DATA_HOLD = "DATA_HOLD"
V_NEED_MORE = "NEED_MORE_EVIDENCE"
VERDICTS_USED = (V_QUALIFIED, V_NO_EDGE, V_NO_INC_INFO, V_SAMPLE, V_MATERIALITY, V_NONINCREMENTAL,
                 V_UNSTABLE, V_WRONG_SIGN, V_MULTIPLICITY, V_DATA_HOLD, V_NEED_MORE)


# --------------------------------------------------------------------------- #
# Substrate (existing owners; this module never fetches)
# --------------------------------------------------------------------------- #
def load_auctions() -> dict:
    """The TA_WS cache owned by treasury_auction_concession, manifest-checked. A missing cache is
    a DATA_HOLD here: acquisition belongs to the owner, never to this executor."""
    return TA.load_auctions(allow_fetch=False)


def load_bars() -> dict:
    return TA.load_bars((MARKET,))


def load_costs() -> dict:
    return TA.load_costs((MARKET,))


def load_calendar(market: str = MARKET) -> dict:
    """Census reader: session dates, held contracts and a finiteness flag. No return value
    leaves this function."""
    df = pd.read_csv(N.NATIVE_LAYER_DIR / ("%s.csv" % market), usecols=["Date", "ret", "held"],
                     dtype={"Date": str, "held": str}).sort_values("Date")
    return {"dates": df["Date"].str[:10].to_numpy().astype("<U10"), "held": df["held"].to_numpy(),
            "finite": np.isfinite(df["ret"].to_numpy(dtype=float))}


# --------------------------------------------------------------------------- #
# Issuance and the duration-added measure (no price, no return)
# --------------------------------------------------------------------------- #
def _num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def approx_modified_duration(coupon_pct, issue_date: str, maturity_date: str):
    """Modified duration of a par bond with semi-annual coupons at yield = coupon, from the
    coupon, issue date and maturity only: (1 - (1 + y/2)^(-2T)) / y; T at a zero coupon."""
    years = (date.fromisoformat(maturity_date) - date.fromisoformat(issue_date)).days / 365.25
    if years <= 0:
        return None
    y = float(coupon_pct) / 100.0
    if y <= 0:
        return years
    return (1.0 - (1.0 + y / 2.0) ** (-2.0 * years)) / y


def issue_table(records: list) -> list:
    """One row per (CUSIP, auction date): index-supply flag, fields, duration and DA."""
    seen, out = set(), []
    for r in records:
        c = TA.classify(r)
        key = (c["cusip"], c["auction_date"])
        if key in seen or not c["auction_date"]:
            continue
        seen.add(key)
        a = {"cusip": c["cusip"], "auction_date": c["auction_date"],
             "issue_date": TA._day(r.get("issueDate")),                    # noqa: SLF001
             "maturity_date": TA._day(r.get("maturityDate")),              # noqa: SLF001
             "coupon_pct": _num(r.get("interestRate")), "offering": _num(r.get("offeringAmount")),
             "tenor": TA.tenor_of(r.get("securityTerm")), "reopening": c["reopening"],
             "exclusion": c["exclusion"] if c["exclusion"] in NOT_INDEX_SUPPLY else None}
        a["month"] = (a["issue_date"] or a["auction_date"])[:7]
        dur = None
        if (a["exclusion"] is None and a["issue_date"] and a["maturity_date"]
                and a["coupon_pct"] is not None and a["offering"] and a["offering"] > 0):
            dur = approx_modified_duration(a["coupon_pct"], a["issue_date"], a["maturity_date"])
        a["duration"] = dur
        a["field_missing"] = a["exclusion"] is None and dur is None
        a["duration_added"] = None if dur is None else a["offering"] / 1e9 * dur
        out.append(a)
    return sorted(out, key=lambda x: (x["auction_date"], str(x["cusip"])))


def month_table(dates, held=None, finite=None) -> dict:
    """ym -> the month-end window on the market's own sessions. No return is read."""
    dates = np.asarray(dates)
    by: dict = {}
    for i, d in enumerate(dates):
        by.setdefault(str(d)[:7], []).append(i)
    last = len(dates) - 1
    out = {}
    for ym, ii in by.items():
        if len(ii) < ENTRY_FROM_END:
            continue
        e, x = ii[-ENTRY_FROM_END], ii[-1]
        roll_at = []
        if held is not None:
            h = np.asarray(held)[e:x + 1]
            roll_at = [int(k) + 1 for k in np.nonzero(h[1:] != h[:-1])[0]]
        out[ym] = {"sessions": len(ii), "E": e, "X": x, "entry_date": str(dates[e]),
                   "exit_date": str(dates[x]), "complete": x < last, "rolls": len(roll_at),
                   "roll_sessions_after_entry": roll_at,
                   "window_dates": [str(dates[k]) for k in range(e + 1, x + 1)],
                   "returns_finite": (None if finite is None
                                      else bool(np.asarray(finite)[e + 1:x + 1].all()))}
    return out


def measured(m: dict) -> bool:
    return bool(m["complete"]) and m["sessions"] >= MIN_MONTH_SESSIONS


def pit_counted(a: dict, months: dict) -> bool:
    """s3 PIT: the auction is STRICTLY BEFORE the entry session of the issue month."""
    return a["auction_date"] < months[a["month"]]["entry_date"]


def monthly_measure(issues: list, months: dict) -> dict:
    out = {ym: {"duration_added": 0.0, "duration_added_full": 0.0, "issues": 0, "pit_excluded": 0,
                "pit_excluded_tenors": [], "by_bucket": {}}
           for ym, m in months.items() if ym >= MEASURE_START and measured(m)}
    for a in issues:
        if a["duration_added"] is None or a["month"] not in out:
            continue
        o = out[a["month"]]
        o["issues"] += 1
        o["duration_added_full"] += a["duration_added"]
        if pit_counted(a, months):
            o["duration_added"] += a["duration_added"]
            b = TENOR_BUCKET.get(a["tenor"], "OTHER")
            o["by_bucket"][b] = o["by_bucket"].get(b, 0.0) + a["duration_added"]
        else:
            o["pit_excluded"] += 1
            o["pit_excluded_tenors"].append(a["tenor"])
    return out


def rank_months(measure: dict, lookback: int = LOOKBACK_MONTHS, key: str = "duration_added") -> dict:
    """s4: high iff DA_M exceeds the median of the ``lookback`` calendar months BEFORE M. A month
    with any unmeasured prior month is unranked; no later month is ever read."""
    out = {}
    for ym in sorted(measure):
        p = pd.Period(ym, "M")
        prior = [str(p - j) for j in range(lookback, 0, -1)]
        if any(q not in measure for q in prior):
            continue
        med = float(np.median([measure[q][key] for q in prior]))
        out[ym] = {"high": bool(measure[ym][key] > med), "trailing_median": med}
    return out


def _in(ym: str, win) -> bool:
    return win[0] <= ym <= win[1]


def expected_months(win, lookback: int = LOOKBACK_MONTHS) -> list:
    first = str(pd.Period(MEASURE_START, "M") + lookback)
    lo = max(win[0], first)
    return [str(p) for p in pd.period_range(lo, win[1], freq="M")] if lo <= win[1] else []


# --------------------------------------------------------------------------- #
# Census - issuance and calendar only, never a return
# --------------------------------------------------------------------------- #
def _longest_run(flags: list) -> int:
    best = cur = 0
    for f in flags:
        cur = cur + 1 if f else 0
        best = max(best, cur)
    return best


def _drag(ms: list, ranks: dict, months: dict, bps: float, conditional: bool) -> float | None:
    if not ms:
        return None
    tot = sum((2 + 2 * months[ym]["rolls"]) * bps / 1e4 for ym in ms
              if ranks[ym]["high"] or not conditional)
    return PERIODS_PER_YEAR * tot / len(ms)


def census(issues: list, months: dict, measure: dict, lookbacks=LOOKBACK_CENSUS) -> dict:
    ranks = {L: rank_months(measure, L) for L in lookbacks}
    full = {L: rank_months(measure, L, key="duration_added_full") for L in lookbacks}
    wins = {"QUALIFICATION": QUALIFICATION, **HALVES, "CONFIRMATION": CONFIRMATION}
    per = {}
    for name, win in wins.items():
        ms = [ym for ym in sorted(measure) if _in(ym, win)]
        iss = [a for a in issues if a["exclusion"] is None and a["month"] in measure and _in(a["month"], win)]
        da = np.array([measure[ym]["duration_added"] for ym in ms], dtype=float)
        buckets = {}
        for b in ("ZT", "ZF", "ZN", "ZB"):
            s = np.array([measure[ym]["by_bucket"].get(b, 0.0) for ym in ms], dtype=float)
            buckets[b] = {"level_share": float(s.sum() / da.sum()) if da.sum() > 0 else None,
                          "variance_contribution": (float(np.cov(s, da)[0, 1] / np.var(da, ddof=1))
                                                    if len(da) > 2 and np.var(da) > 0 else None)}
        tenors: dict = {}
        for a in iss:
            t = tenors.setdefault("%sY" % a["tenor"], {"issues": 0, "pit_excluded": 0, "reopenings": 0})
            t["issues"] += 1
            t["reopenings"] += int(a["reopening"])
            t["pit_excluded"] += int(a["duration_added"] is not None and not pit_counted(a, months))
        counted = [a for a in iss if a["duration_added"] is not None and pit_counted(a, months)]
        full_da = sum(measure[ym]["duration_added_full"] for ym in ms)
        lb = {}
        for L in lookbacks:
            rk = [ym for ym in ms if ym in ranks[L]]
            hi = [ym for ym in rk if ranks[L][ym]["high"]]
            lo = [ym for ym in rk if not ranks[L][ym]["high"]]
            moy: dict = {}
            for ym in rk:
                c = moy.setdefault(ym[5:], [0, 0])
                c[0] += int(ranks[L][ym]["high"])
                c[1] += 1
            lb[str(L)] = {
                "first_ranked_month": min(ranks[L]) if ranks[L] else None, "ranked_months": len(rk),
                "high_months": len(hi), "high_share": (len(hi) / len(rk)) if rk else None,
                "longest_high_run": _longest_run([ranks[L][ym]["high"] for ym in rk]),
                "agreement_with_full_information_flag": (float(np.mean(
                    [ranks[L][ym]["high"] == full[L][ym]["high"] for ym in rk])) if rk else None),
                "roll_in_window_share_high": (float(np.mean([months[ym]["rolls"] > 0 for ym in hi]))
                                              if hi else None),
                "roll_in_window_share_low": (float(np.mean([months[ym]["rolls"] > 0 for ym in lo]))
                                             if lo else None),
                "high_by_month_of_year": {k: "%d/%d" % tuple(v) for k, v in sorted(moy.items())}}
        rk = [ym for ym in ms if ym in ranks[LOOKBACK_MONTHS]]
        hurdle = {}
        for c in (KILL_RULE_COST_BPS, STRESS_COST_BPS):
            d_book = _drag(rk, ranks[LOOKBACK_MONTHS], months, c, True)
            hurdle["%gbp" % c] = {
                "book_cost_drag_ann": d_book,
                "book_gross_needed_ann": None if d_book is None else MATERIALITY_ANN_NET + d_book,
                "unconditional_cost_drag_ann": _drag(rk, ranks[LOOKBACK_MONTHS], months, c, False)}
        per[name] = {
            "months_measured": len(ms), "issues_counted": len(iss),
            "issues_field_missing": sum(a["field_missing"] for a in iss),
            "issues_pit_excluded": sum(t["pit_excluded"] for t in tenors.values()),
            "months_with_a_pit_exclusion": sum(measure[ym]["pit_excluded"] > 0 for ym in ms),
            "duration_added_pit_excluded_share": (1.0 - float(da.sum()) / full_da) if full_da > 0 else None,
            "tenors": dict(sorted(tenors.items(), key=lambda kv: int(kv[0][:-1]) if kv[0][:-1].isdigit() else 99)),
            "duration_added_bn_years_per_month": TA._dist(da),                     # noqa: SLF001
            "amount_weighted_duration_of_counted_issues": (
                float(sum(a["offering"] * a["duration"] for a in counted) / sum(a["offering"] for a in counted))
                if counted else None),
            "futures_bucket_of_duration_added": buckets,
            "roll_in_window_share_all_months": (float(np.mean([months[ym]["rolls"] > 0 for ym in ms]))
                                                if ms else None),
            "lookbacks": lb, "cost_hurdle_per_unit_zn_notional": hurdle}
    exit_not_last_weekday = [
        [ym, m["exit_date"], (pd.Timestamp(ym + "-01") + pd.offsets.BMonthEnd(0)).strftime("%Y-%m-%d")]
        for ym, m in sorted(months.items()) if ym >= MEASURE_START and m["exit_date"] !=
        (pd.Timestamp(ym + "-01") + pd.offsets.BMonthEnd(0)).strftime("%Y-%m-%d")]
    return {"reads_returns": False, "frozen_lookback_months": LOOKBACK_MONTHS, "windows": per,
            "exit_session_not_last_weekday": exit_not_last_weekday,
            "months_not_measured": sorted(ym for ym, m in months.items()
                                          if ym >= MEASURE_START and not measured(m))}


def roll_census(months: dict) -> dict:
    by_year: dict = {}
    for ym, m in sorted(months.items()):
        if ym < MEASURE_START or not measured(m):
            continue
        y = by_year.setdefault(ym[:4], {"months": 0, "roll_in_window": 0, "after_entry_session": 0,
                                        "before_exit_session": 0, "missing_window_return": 0})
        y["months"] += 1
        y["roll_in_window"] += int(m["rolls"] > 0)
        y["after_entry_session"] += int(1 in m["roll_sessions_after_entry"])
        y["before_exit_session"] += int(2 in m["roll_sessions_after_entry"])
        y["missing_window_return"] += int(m["returns_finite"] is False)
    return by_year


def run_census(*, write: bool = True, markets=("ZF", "ZN", "ZB")) -> dict:
    """The pre-registration census: issuance, PIT exclusion, ranking share, roll timing and cost
    hurdle. Reads no return (the layer's ret column is reduced to a finiteness flag)."""
    body = {"schema": "alpha_recovery_treasury_month_end_duration_census/1",
            "calculation_owner": CALCULATION_OWNER, "mechanism_id": MECHANISM_ID,
            "reads_returns": False, "capital_eligible": False}
    auc = load_auctions()
    if auc["state"] != "OK":
        body.update({"state": V_DATA_HOLD, "why": auc["why"]})
    else:
        issues = issue_table(auc["records"])
        cals = {m: load_calendar(m) for m in markets}
        tables = {m: month_table(c["dates"], c["held"], c["finite"]) for m, c in cals.items()}
        measure = monthly_measure(issues, tables[MARKET])
        body.update({"state": "OK", "census": census(issues, tables[MARKET], measure),
                     "roll_census_by_market_and_year": {m: roll_census(t) for m, t in tables.items()},
                     "calendars_identical_from_2000": all(
                         np.array_equal(cals[MARKET]["dates"][cals[MARKET]["dates"] >= "2000-01-01"],
                                        c["dates"][c["dates"] >= "2000-01-01"]) for c in cals.values()),
                     "issue_month_differs_from_auction_month_by_tenor": TA._count(      # noqa: SLF001
                         "%sY" % a["tenor"] for a in issues
                         if a["exclusion"] is None and a["month"] != a["auction_date"][:7]),
                     "auction_manifest": {k: auc["manifest"].get(k) for k in ("rows", "sha256", "fetched_at_utc")},
                     "r38_cost_bps_per_side": {m: (N.load_meta().get(m) or {}).get("cost_bps_per_side")
                                               for m in ("ZT", "ZF", "ZN", "ZB", "UB", "TN")}})
    if write:
        body["artifact_path"] = str(write_artifact(CENSUS_ARTIFACT_NAME, body))
    return body


# --------------------------------------------------------------------------- #
# Returns (read ONLY here) and the books
# --------------------------------------------------------------------------- #
def window_rows(window: str, months: dict, ranks: dict, ret) -> list:
    """Usable ranked months of ONE window with the month-end two-session gross return."""
    rows = []
    for ym in expected_months(WINDOWS[window]):
        m, r = months.get(ym), ranks.get(ym)
        if m is None or r is None or not measured(m):
            continue
        w = np.asarray(ret[m["E"] + 1:m["X"] + 1], dtype=float)
        if len(w) != HOLD_SESSIONS or not np.isfinite(w).all():
            continue
        rows.append({"ym": ym, "high": bool(r["high"]), "gross": float(np.prod(1.0 + w) - 1.0),
                     "rolls": int(m["rolls"]), "window_dates": list(m["window_dates"])})
    return rows


def cost_of(row: dict, bps: float) -> float:
    """s7: one round trip, plus one round trip per held-contract change in [E, X]."""
    return (2 + 2 * int(row["rolls"])) * bps / 1e4


def book(rows: list, bps: float) -> np.ndarray:
    return np.array([(r["gross"] - cost_of(r, bps)) if r["high"] else 0.0 for r in rows], dtype=float)


def unconditional(rows: list, bps: float) -> np.ndarray:
    return np.array([r["gross"] - cost_of(r, bps) for r in rows], dtype=float)


def increment(rows: list, bps: float) -> tuple:
    h = np.array([1.0 if r["high"] else 0.0 for r in rows], dtype=float)
    pi = float(h.mean()) if len(h) else 0.0
    return (h - pi) * unconditional(rows, bps), pi


def stats(x) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    st = S.nw_tstat(x, 0)
    if st["mean"] is None:
        return {"months": int(len(x)), "mean": None, "ann": None, "t": None, "p_one_sided": None,
                "nw_lag": 0}
    sd = float(np.std(x, ddof=1))
    return {"months": int(len(x)), "mean": st["mean"], "ann": st["mean"] * PERIODS_PER_YEAR,
            "t": st["t"], "p_one_sided": st["p_one_sided"], "nw_lag": 0,
            "sharpe": (st["mean"] / sd * math.sqrt(PERIODS_PER_YEAR)) if sd > 0 else None,
            "max_dd": S._max_dd(x)}                                                # noqa: SLF001


def summarize(rows: list, gate_bps: float) -> dict:
    act = [r for r in rows if r["high"]]
    low = [r for r in rows if not r["high"]]
    inc, pi = increment(rows, gate_bps)
    net_act = [r["gross"] - cost_of(r, gate_bps) for r in act]
    return {"months": len(rows), "high_months": len(act), "low_months": len(low), "high_share": pi,
            "roll_share_high": float(np.mean([r["rolls"] > 0 for r in act])) if act else None,
            "roll_share_low": float(np.mean([r["rolls"] > 0 for r in low])) if low else None,
            "gross_mean_active": float(np.mean([r["gross"] for r in act])) if act else None,
            "hit_rate_net_active": float(np.mean([v > 0 for v in net_act])) if act else None,
            "book_net": stats(book(rows, gate_bps)), "book_gross": stats(book(rows, 0.0)),
            "book_net_stress": stats(book(rows, STRESS_COST_BPS)),
            "book_ann_net_ladder": {"%gbp" % c: stats(book(rows, c))["ann"] for c in COST_LADDER_BPS},
            "unconditional_net": stats(unconditional(rows, gate_bps)),
            "unconditional_gross": stats(unconditional(rows, 0.0)),
            "increment": dict(stats(inc), pi=pi,
                              definition="z_M = (1[high_M] - pi_W) * u_M, u_M = unconditional "
                                         "two-session long net at the gate cost; nw_tstat(z, 0)"),
            "increment_gross_descriptive": stats(increment(rows, 0.0)[0])}


def incumbent_increment(rows: list, gate_bps: float, incumbent_daily) -> dict:
    if incumbent_daily is None or not len(incumbent_daily):
        return {"state": V_DATA_HOLD, "why": "the incumbent daily path is unavailable"}
    from .intraday_alpha import equal_risk_daily
    parts = []
    for r in rows:
        if r["high"]:
            per = (1.0 + r["gross"] - cost_of(r, gate_bps)) ** (1.0 / HOLD_SESSIONS) - 1.0
            parts.append(pd.Series(per, index=pd.to_datetime(r["window_dates"])))
    if not parts:
        return {"state": V_DATA_HOLD, "why": "no active month"}
    sleeve = pd.concat(parts).groupby(level=0).sum()
    sleeve = sleeve.reindex(pd.bdate_range(sleeve.index.min(), sleeve.index.max())).fillna(0.0)
    return equal_risk_daily(sleeve, incumbent_daily, label=MECHANISM_ID)


def data_check(window: str, rows: list, issues: list) -> dict:
    exp = expected_months(WINDOWS[window])
    share = (len(rows) / len(exp)) if exp else 0.0
    scope = [a for a in issues if a["exclusion"] is None and _in(a["month"], WINDOWS[window])]
    missing = sum(a["field_missing"] for a in scope) / max(1, len(scope))
    problems = []
    if share < MIN_USABLE_MONTH_SHARE:
        problems.append("%s: %d of %d ranked months usable (share %.3f < %.2f)"
                        % (window, len(rows), len(exp), share, MIN_USABLE_MONTH_SHARE))
    if missing > MAX_FIELD_MISSING_SHARE:
        problems.append("%s: %.1f%% of index-supply issues lack issueDate/maturityDate/interestRate/"
                        "offeringAmount (limit %.0f%%)" % (window, 100 * missing, 100 * MAX_FIELD_MISSING_SHARE))
    return {"expected_months": len(exp), "usable_months": len(rows), "usable_share": share,
            "field_missing_share": missing, "hold": "; ".join(problems) or None}


# --------------------------------------------------------------------------- #
# Verdict - the preregistered gates, in the preregistered order (section 10)
# --------------------------------------------------------------------------- #
def _v(verdict: str, gate, why: str, kill=None) -> dict:
    return {"verdict": verdict, "gate": gate, "why": why, "kill_rule_fired": kill}


def _below(x, floor: float) -> bool:
    return x is None or not np.isfinite(float(x)) or float(x) < floor


def _r(x, n=3):
    return None if x is None else round(float(x), n)


def qualification_gate(g: dict):
    """Gates 1-9 on the qualification window. None means every gate passed."""
    if g.get("data_hold"):
        return _v(V_DATA_HOLD, "DATA", g["data_hold"])
    if min(g["high_months"], g["low_months"]) < MIN_EFFECTIVE_PERIODS:
        return _v(V_SAMPLE, "SAMPLE", "%d high / %d low usable months (floor %d each)"
                  % (g["high_months"], g["low_months"], MIN_EFFECTIVE_PERIODS), "INSUFFICIENT_SAMPLE")
    if g["gross_mean_active"] is None or g["gross_mean_active"] <= 0:
        return _v(V_WRONG_SIGN, "FROZEN_SIGN", "the month-end long loses before costs in high months "
                                               "(mean %s); the sign is never reversed"
                  % _r(g["gross_mean_active"], 6), "LONG_DOES_NOT_EARN")
    if _below(g["net_t"], T_FLOOR):
        return _v(V_NO_EDGE, "STANDALONE_T", "book NW t %s below %.1f at the gate cost"
                  % (_r(g["net_t"]), T_FLOOR), "NW_T_BELOW_2")
    if g["ann_net"] is None or g["ann_net"] < MATERIALITY_ANN_NET:
        return _v(V_MATERIALITY, "MATERIALITY", "book net %s/yr below %.3f at the gate cost"
                  % (_r(g["ann_net"], 4), MATERIALITY_ANN_NET), "NET_BELOW_1.5PCT_PER_YEAR")
    if _below(g["increment_t"], T_FLOOR):
        return _v(V_NONINCREMENTAL, "UNCONDITIONAL_INCREMENT",
                  "increment over the unconditional month-end long has NW t %s below %.1f"
                  % (_r(g["increment_t"]), T_FLOOR), "NO_INCREMENT_OVER_UNCONDITIONAL_MONTH_END")
    bad = sorted(k for k, v in g["halves_ann_net"].items() if v is None or v <= 0)
    if bad:
        return _v(V_UNSTABLE, "HALVES", "non-positive annualised book net in %s" % ", ".join(bad),
                  "HALF_NOT_POSITIVE")
    if not g["fdr_pass"]:
        return _v(V_MULTIPLICITY, "BH_INHERITED", "fails BH q=%.2f at m=%d" % (BH_Q, 1 + len(INHERITED_NULLS)),
                  "BH_Q010_FAILS_M3")
    if g["stress_ann_net"] is None or g["stress_ann_net"] < MATERIALITY_ANN_NET:
        return _v(V_MATERIALITY, "STRESS_COST", "book net %s/yr at %g bp per side below %.3f"
                  % (_r(g["stress_ann_net"], 4), STRESS_COST_BPS, MATERIALITY_ANN_NET),
                  "NET_BELOW_1.5PCT_PER_YEAR_AT_STRESS_COST")
    inc = g.get("incumbent") or {}
    if inc.get("state") == "OK" and not inc.get("positive_incremental_utility_after_costs"):
        return _v(V_NO_INC_INFO, "INCUMBENT", "equal-risk increment %s t %s"
                  % (inc.get("incremental_ann_net_return"), inc.get("t_incremental")),
                  "NO_EQUAL_RISK_INCREMENT_OVER_INCUMBENT")
    return None


def confirmation_gate(c: dict) -> dict:
    """Gate 10 on the untouched window, reached only after gates 1-9."""
    if c.get("data_hold"):
        return _v(V_DATA_HOLD, "CONFIRMATION_DATA", c["data_hold"])
    if c["high_months"] < MIN_EFFECTIVE_PERIODS:
        return _v(V_NEED_MORE, "CONFIRMATION_SAMPLE", "%d confirmation high months, floor %d"
                  % (c["high_months"], MIN_EFFECTIVE_PERIODS))
    fails = []
    if c["net_mean"] is None or c["net_mean"] <= 0:
        fails.append("book net mean %s not positive" % _r(c["net_mean"], 6))
    if c["ann_net"] is None or c["ann_net"] < MATERIALITY_ANN_NET:
        fails.append("book net %s/yr below %.3f" % (_r(c["ann_net"], 4), MATERIALITY_ANN_NET))
    if _below(c["t"], T_FLOOR):
        fails.append("book NW t %s below %.1f" % (_r(c["t"]), T_FLOOR))
    if c["p"] is None or float(c["p"]) > BH_Q:
        fails.append("one-sided p %s above %.2f" % (_r(c["p"], 4), BH_Q))
    if c["increment_mean"] is None or c["increment_mean"] <= 0:
        fails.append("increment mean %s not positive" % _r(c["increment_mean"], 6))
    if fails:
        return _v(V_UNSTABLE, "UNTOUCHED_CONFIRMATION", "; ".join(fails), "CONFIRMATION_DID_NOT_REPRODUCE")
    return _v(V_QUALIFIED, None, "every preregistered gate passed, including the untouched confirmation; "
                                 "a HUMAN gate governs prospective registration and capital eligibility")


def verdict_for(g: dict, confirmation: dict | None = None) -> dict:
    v = qualification_gate(g)
    if v is not None:
        return v
    if confirmation is None:
        return _v(V_NEED_MORE, "CONFIRMATION_NOT_EVALUATED",
                  "qualification passed but the confirmation window was not evaluated")
    return confirmation_gate(confirmation)


def multiplicity(p) -> dict:
    pv = {MECHANISM_ID: p}
    pv.update({n: 1.0 for n in INHERITED_NULLS})
    bh = S.bh_fdr(pv, q=BH_Q)
    return {"m_declared": 1, "m_inherited": 1 + len(INHERITED_NULLS), "q": BH_Q, "p_value": p,
            "p_value_definition": "one-sided NW p (lag 0) of the qualification book net at the gate cost",
            "inherited_nulls": list(INHERITED_NULLS), "inherited_p_value_policy": "p = 1 each",
            "benjamini_hochberg": bh, "passes": MECHANISM_ID in bh["survivors"], "denominator_reset": False}


# --------------------------------------------------------------------------- #
# Evaluation and the executor contract
# --------------------------------------------------------------------------- #
def artifact_path() -> str:
    return str(research_root() / "results" / ARTIFACT_NAME)


def _result(v: dict, statistic: dict, economics: dict, mult: dict, identity: dict) -> dict:
    return {"verdict": v["verdict"], "why": v["why"], "kill_rule_fired": v.get("kill_rule_fired"),
            "statistic": statistic, "economics": economics, "multiplicity": mult,
            "artifact": artifact_path(), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": stable_hash(identity or {})}


def hold_result(why: str, identity: dict | None = None) -> dict:
    mult = {"m_declared": 1, "m_inherited": 1 + len(INHERITED_NULLS), "q": BH_Q,
            "inherited_nulls": list(INHERITED_NULLS), "state": "NOT_REACHED"}
    return _result(_v(V_DATA_HOLD, "DATA", why), {"lockbox_t": None}, {}, mult, identity or {})


def evaluate(records: list, bars: dict, costs: dict, *, incumbent_daily=None,
             identity: dict | None = None) -> dict:
    identity = dict(identity or {})
    b = bars[MARKET]
    r38 = float(costs["r38_bps_per_side"][MARKET])
    gate_bps = max(KILL_RULE_COST_BPS, r38)
    identity["cost_bps_per_side_r38"] = r38
    issues = issue_table(records)
    months = month_table(b["dates"], b["held"])
    measure = monthly_measure(issues, months)
    ranks = rank_months(measure)
    cen = census(issues, months, measure, lookbacks=(LOOKBACK_MONTHS,))
    q_rows = window_rows("QUALIFICATION", months, ranks, b["ret"])
    dq = data_check("QUALIFICATION", q_rows, issues)
    if dq["hold"]:
        return {"census": cen, "qualification": {"data": dq}, "confirmation": "UNTOUCHED",
                "executor_result": hold_result(dq["hold"], identity)}
    q = summarize(q_rows, gate_bps)
    q["data"] = dq
    q["halves_ann_net"] = {k: stats(book([r for r in q_rows if _in(r["ym"], w)], gate_bps))["ann"]
                           for k, w in HALVES.items()}
    mult = multiplicity(q["book_net"]["p_one_sided"])
    inc = incumbent_increment(q_rows, gate_bps, incumbent_daily)
    g = {"data_hold": None, "high_months": q["high_months"], "low_months": q["low_months"],
         "gross_mean_active": q["gross_mean_active"], "net_t": q["book_net"]["t"],
         "ann_net": q["book_net"]["ann"], "increment_t": q["increment"]["t"],
         "halves_ann_net": q["halves_ann_net"], "fdr_pass": mult["passes"],
         "stress_ann_net": q["book_net_stress"]["ann"], "incumbent": inc}
    v = qualification_gate(g)
    conf, cg = None, None
    if v is None:
        c_rows = window_rows("CONFIRMATION", months, ranks, b["ret"])
        dc = data_check("CONFIRMATION", c_rows, issues)
        if dc["hold"]:
            cg = {"data_hold": dc["hold"]}
        else:
            conf = summarize(c_rows, gate_bps)
            conf["data"] = dc
            cg = {"high_months": conf["high_months"], "net_mean": conf["book_net"]["mean"],
                  "ann_net": conf["book_net"]["ann"], "t": conf["book_net"]["t"],
                  "p": conf["book_net"]["p_one_sided"], "increment_mean": conf["increment"]["mean"]}
        v = confirmation_gate(cg)
    statistic = {
        "lockbox_t": conf["book_net"]["t"] if conf else None,
        "lockbox_window": "CONFIRMATION %s..%s" % CONFIRMATION, "nw_lag": 0,
        "qualification_net_t": q["book_net"]["t"],
        "qualification_net_p_one_sided": q["book_net"]["p_one_sided"],
        "qualification_gross_mean_per_active_month": q["gross_mean_active"],
        "increment_t": q["increment"]["t"], "increment_pi": q["increment"]["pi"],
        "unconditional_net_t": q["unconditional_net"]["t"],
        "qualification_months": q["months"], "qualification_high_months": q["high_months"],
        "confirmation_months": conf["months"] if conf else None,
        "confirmation_high_months": conf["high_months"] if conf else None,
        "confirmation_increment_mean": conf["increment"]["mean"] if conf else None}
    economics = {
        "units": "fraction of one unit of ZN notional per month, x12 per year",
        "cost_bps_per_side_r38": r38, "cost_bps_per_side_gate": gate_bps,
        "stress_cost_bps_per_side": STRESS_COST_BPS,
        "qualification_ann_net_gate": q["book_net"]["ann"], "qualification_ann_gross": q["book_gross"]["ann"],
        "qualification_ann_net_stress": q["book_net_stress"]["ann"],
        "qualification_ann_net_ladder": q["book_ann_net_ladder"],
        "qualification_max_dd_net_gate": q["book_net"].get("max_dd"),
        "qualification_halves_ann_net": q["halves_ann_net"],
        "qualification_unconditional_ann_net_gate": q["unconditional_net"]["ann"],
        "qualification_increment_ann": q["increment"]["ann"],
        "incumbent_equal_risk": inc,
        "confirmation_ann_net_gate": conf["book_net"]["ann"] if conf else None,
        "confirmation_ann_net_ladder": conf["book_ann_net_ladder"] if conf else None}
    return {"census": cen, "gate_inputs": g, "confirmation_gate_inputs": cg, "qualification": q,
            "confirmation": conf if conf else "UNTOUCHED",
            "executor_result": _result(v, statistic, economics, mult, identity)}


def design() -> dict:
    return {"instrument": MARKET, "index_supply": "nominal fixed-coupon notes and bonds (2y..30y, new "
                                                  "issues and reopenings); TIPS, FRNs, bills and CMBs excluded",
            "amount": "offeringAmount (public offering; excludes SOMA add-ons; fixed at announcement)",
            "duration": "par-bond modified duration at yield = interestRate from issueDate to maturityDate",
            "month": "issueDate in M", "pit": "auctionDate strictly before M's entry session L[-3]",
            "ranking": "high iff DA_M > median(DA_{M-%d..M-1})" % LOOKBACK_MONTHS,
            "entry_exit": "close of L[-3] to close of L[-1] on the R38 %s sessions" % MARKET,
            "roll_rule": "one extra round trip per held-contract change in [L[-3], L[-1]]",
            "increment": "(1[high] - pi_W) * u_M, nw_tstat lag 0",
            "windows": {"QUALIFICATION": list(QUALIFICATION), "CONFIRMATION": list(CONFIRMATION),
                        "HALVES": {k: list(w) for k, w in HALVES.items()}},
            "gate_cost_bps_per_side": "max(%g, R38 %s cost)" % (KILL_RULE_COST_BPS, MARKET),
            "stress_cost_bps_per_side": STRESS_COST_BPS, "inherited_nulls": list(INHERITED_NULLS)}


def run(*, verbose: bool = True, write: bool = True, incumbent_daily="LOAD") -> dict:
    body = {"schema": "alpha_recovery_treasury_month_end_duration_extension/1",
            "calculation_owner": CALCULATION_OWNER, "mechanism_id": MECHANISM_ID,
            "preregistration": PREREGISTRATION, "preregistration_altered_after_results": False,
            "kill_rule_frozen": KILL_RULE_FROZEN, "design": design(), "scorer": SCORER,
            "capital_eligible": False}
    identity: dict = {}
    auc = load_auctions()
    identity["auction_cache_sha256"] = (auc.get("manifest") or {}).get("sha256")
    if auc["state"] != "OK":
        detail = {"executor_result": hold_result(auc["why"], identity)}
    else:
        bl = load_bars()
        identity["r38_layer"] = bl.get("identity")
        if bl["state"] != "OK":
            detail = {"executor_result": hold_result(bl["why"], identity)}
        else:
            try:
                costs = load_costs()
            except (OSError, ValueError, KeyError) as e:
                detail = {"executor_result": hold_result("R38 cost panel unreadable: %r" % (e,), identity)}
            else:
                if isinstance(incumbent_daily, str):
                    from .intraday_alpha import incumbent_daily_path
                    incumbent_daily = incumbent_daily_path()
                detail = evaluate(auc["records"], bl["bars"], costs, incumbent_daily=incumbent_daily,
                                  identity=identity)
                body["costs"] = costs
        body["auction_manifest"] = {k: (auc.get("manifest") or {}).get(k)
                                    for k in ("source", "rows", "sha256", "fetched_at_utc")}
    body.update(detail)
    res = body["executor_result"]
    body["verdict"], body["why"] = res["verdict"], res["why"]
    if write:
        write_artifact(ARTIFACT_NAME, body)
    if verbose:
        print("%s %s: %s" % (MECHANISM_ID, res["verdict"], res["why"]), flush=True)
    return body


def run_mechanism(*, mechanism: dict) -> dict:
    """The R59 mechanism-executor contract. Refuses a different mechanism or a kill rule that no
    longer matches the preregistered one, byte for byte."""
    mid = (mechanism or {}).get("mechanism_id")
    if mid != MECHANISM_ID:
        raise ValueError("executor for %s was handed %r" % (MECHANISM_ID, mid))
    rule = ((mechanism or {}).get("pnl_gate") or {}).get("KILL_RULE")
    if rule != KILL_RULE_FROZEN:
        raise ValueError("the catalog KILL_RULE differs from the preregistered rule; an executor "
                         "does not judge a changed contract")
    return dict(run(verbose=False, write=True)["executor_result"])
