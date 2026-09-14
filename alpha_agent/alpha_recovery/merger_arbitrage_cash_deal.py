r"""alpha_agent.alpha_recovery.merger_arbitrage_cash_deal - executor for the Alpha Agent mechanism
``MERGER_ARBITRAGE_CASH_DEAL_TARGET_SPREAD_V1``.

Contract: ``research/preregistration/MERGER_ARBITRAGE_CASH_DEAL_TARGET_SPREAD_PREREGISTRATION.md``.
Executes it and changes nothing in it.

    mechanism   an all-cash acquisition target trades below the cash consideration until the deal
                closes because holders shed completion, time, financing and regulatory risk to
                capital-constrained arbitrageurs (Mitchell and Pulvino 2001; Baker and Savasoglu 2002)
    universe    EVERY EDGAR filer's merger-form documents (not an index-selected store)
    entry       the close of the first session STRICTLY AFTER the filing date on which the target's
                own filings first establish an all-cash, all-shares deal and its trading symbol
    position    a fixed slot of NAV per deal, buy-and-hold on Norgate total-return closes, to the
                target's last quoted session (delisting) or the frozen 126-session cap
    P&L         fixed-notional slot accounting, net of 12.5 bp per side on entry and exit notional
                and of the 3-month Treasury bill on the capital each position ties up

Existing owners only: ``merger_arb_data`` / ``merger_arb_events`` (the calendar, which reads no
price), ``r63.panels.load_norgate_total_return`` and ``load_fred_daily`` (prices, the bill),
``r63.sensitivity`` (``nw_tstat``, ``bh_fdr``, ``_max_dd``).

Target returns are read only after the data gate, only through :class:`PriceReader`, and the
confirmation window only after every qualification gate passes.

RESEARCH ONLY. No purchase, subscription, promotion, registration, capital, proposal, order,
fill or live write. ``capital_eligible`` is always False; QUALIFIED only opens a human gate.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import STANDALONE_T_FLOOR
from alpha_agent.r63 import sensitivity as S

from . import (BH_Q, EQ_COST_RATE_PER_SIDE, MATERIALITY_ANN_NET, research_root, stable_hash,
               write_artifact)
from . import merger_arb_data as MD
from . import merger_arb_events as ME

CALCULATION_OWNER = "alpha_agent.alpha_recovery.merger_arbitrage_cash_deal"
MECHANISM_ID = "MERGER_ARBITRAGE_CASH_DEAL_TARGET_SPREAD_V1"
ARTIFACT_NAME = "merger_arbitrage_cash_deal_target_spread.json"
PREREGISTRATION = "research/preregistration/MERGER_ARBITRAGE_CASH_DEAL_TARGET_SPREAD_PREREGISTRATION.md"
SCORER = ("alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd on a calendar-time fixed-slot event book "
          "(run_cell's walk-forward ridge scorer has no event entry or exit)")

#: The catalog's frozen kill rule, verbatim.
KILL_RULE_FROZEN = (
    "Kill if the calendar-time all-cash target book has NW t < 2.0 or net below 1.5 %/yr of NAV at "
    "12.5 bp; or its increment over a beta-matched SPY exposure has NW t < 2.0; or annualised net is "
    "non-positive in either qualification half; or it fails BH q=0.10 at m=1 with EVENT_8K_ITEM_CODES "
    "and SP500_INDEX_ADDITION_DELETION inherited at p=1; or deal identification or consideration type "
    "requires information dated after the entry session.")

# --------------------------------------------------------------------------- #
# Frozen design (section numbers refer to the preregistration)
# --------------------------------------------------------------------------- #
QUALIFICATION = ("2003-01-01", "2016-12-31")                                   # s8
HALVES = {"H1_2003_2009": ("2003-01-01", "2009-12-31"), "H2_2010_2016": ("2010-01-01", "2016-12-31")}
CONFIRMATION = ("2017-01-01", "2026-08-31")
HOLD_CAP = 126                                                                  # s4
#: s5 - the smallest of 40/50/60/80/100 at which the metadata census refuses no deal for capacity in either window
#: (qualification: 9 refusals at 40, none at 50; maximum concurrency 45). No return entered the choice.
SLOTS = 50
SLOT_WEIGHT = 1.0 / SLOTS
COST_BPS = EQ_COST_RATE_PER_SIDE * 1e4                                          # 12.5 bp per side
COST_LADDER_BPS = (0.0, COST_BPS, 25.0, 50.0)
BOOK_NW_LAG = 10                                                                # s6
T_FLOOR = STANDALONE_T_FLOOR                                                    # 2.0
PPY = 252.0
BENCHMARK = "SPY"
RF_COLUMN = "CMT_3M"
MIN_HEAD_COVERAGE = 0.98                                                        # s10 gate 1
MIN_IDENTITY_COVERAGE = 0.85
MIN_AUDIT_PRECISION = 0.90
MIN_QUALIFICATION_DEALS = 150
MIN_CONFIRMATION_DEALS = 60
AUDIT_FILE = "classification_audit.json"
#: sha256 of the frozen classification audit, recorded in the preregistration before any return.
AUDIT_SHA256 = "d58ce030c21fc0277517a60967bab499ab4611c92dc3dc1fc3b6478d0936744b"
#: ``merger_arb_events.calendar_hash`` of the included deal list, recorded in the preregistration before any
#: return: the deals are frozen, so no parser, identity or data change after freezing can alter the sample.
CALENDAR_HASH = "7ce9ec4e479dfb7fbcb8e3756f1d2966b8e09a14180c8ea4fed5273f5d309f92"
#: s9 - the kill rule's inherited families, one entry per recorded cell, each at p = 1.
INHERITED_NULLS = (
    "SP500_INDEX_ADDITION_DELETION|ADDITION_REVERSAL|h63",
    "SP500_INDEX_ADDITION_DELETION|ADDITION_ECHO|h5",
    "SP500_INDEX_ADDITION_DELETION|DELETION_REBOUND",
    "EVENT_8K_ITEM_CODES|RESTRUCTURING_OR_IMPAIRMENT|h5",
    "EVENT_8K_ITEM_CODES|RESTRUCTURING_OR_IMPAIRMENT|h21",
    "EVENT_8K_ITEM_CODES|RESTRUCTURING_OR_IMPAIRMENT|h63",
)

V_QUALIFIED = "QUALIFIED"
V_NO_EDGE = "NO_EDGE"
V_MATERIALITY = "KILLED_BELOW_MATERIALITY"
V_NONINCREMENTAL = "KILLED_NONINCREMENTAL"
V_UNSTABLE = "KILLED_UNSTABLE"
V_WRONG_SIGN = "KILLED_WRONG_SIGN"
V_MULTIPLICITY = "KILLED_MULTIPLICITY"
V_PIT = "KILLED_PIT_UNESTABLISHED"
V_DATA_HOLD = "DATA_HOLD"
V_NEED_MORE = "NEED_MORE_EVIDENCE"
VERDICTS_USED = (V_QUALIFIED, V_NO_EDGE, V_MATERIALITY, V_NONINCREMENTAL, V_UNSTABLE, V_WRONG_SIGN,
                 V_MULTIPLICITY, V_PIT, V_DATA_HOLD, V_NEED_MORE)


def _v(verdict: str, gate: Optional[str], why: str, fired: Optional[str] = None) -> dict:
    return {"verdict": verdict, "gate": gate, "why": why, "kill_rule_fired": fired}


def _r(x, n: int = 3):
    return None if x is None else round(float(x), n)


# --------------------------------------------------------------------------- #
# Prices, read through ONE audited reader
# --------------------------------------------------------------------------- #
class PriceReader:
    """Every target price read goes through here; the latest session read is recorded."""

    def __init__(self, loader):
        self._loader = loader
        self._cache: dict = {}
        self.max_date_read: Optional[str] = None
        self.symbols_read = 0

    def series(self, symbol: str, *, through: str) -> Optional[pd.Series]:
        if symbol not in self._cache:
            got = self._loader((symbol,)) or {}
            self._cache[symbol] = got.get(symbol)
            self.symbols_read += 1
        s = self._cache[symbol]
        if s is None or not len(s):
            return None
        s = s[s.index <= pd.Timestamp(through)]
        if len(s):
            last = str(s.index[-1].date())
            if self.max_date_read is None or last > self.max_date_read:
                self.max_date_read = last
        return s


# --------------------------------------------------------------------------- #
# The book
# --------------------------------------------------------------------------- #
def entry_index(cal: pd.DatetimeIndex, established_on: str) -> Optional[int]:
    """The first session STRICTLY AFTER the establishing filing date."""
    i = int(cal.searchsorted(pd.Timestamp(established_on), side="right"))
    return i if i < len(cal) else None


def book(deals: list, reader: PriceReader, cal: pd.DatetimeIndex, rf_daily: np.ndarray,
         start: str, end: str, *, slots: int = SLOTS, cost_bps: float = COST_BPS,
         read_through: Optional[str] = None) -> dict:
    """The fixed-slot calendar-time book of the deals ENTERED in [start, end]."""
    w = 1.0 / float(slots)
    c = float(cost_bps) / 1e4
    n = len(cal)
    pnl = np.zeros(n)
    fin = np.zeros(n)
    cost = np.zeros(n)
    expo = np.zeros(n)
    taken, records = [], []
    skipped = {"NO_ENTRY_SESSION": 0, "NO_ENTRY_PRICE": 0, "CAPACITY": 0}
    for d in sorted(deals, key=lambda x: (x["established_on"], x["target_cik"])):
        e = entry_index(cal, d["established_on"])
        if e is None:
            skipped["NO_ENTRY_SESSION"] += 1
            continue
        edate = str(cal[e].date())
        if not (start <= edate <= end):
            continue
        cap_i = min(e + HOLD_CAP, n - 1)
        through = str(cal[cap_i].date())
        if read_through is not None and through > read_through:
            through = read_through
            cap_i = int(cal.searchsorted(pd.Timestamp(through), side="right")) - 1
        s = reader.series(d["symbol"], through=through)
        if s is None or pd.Timestamp(edate) not in s.index or not np.isfinite(s.loc[pd.Timestamp(edate)]):
            skipped["NO_ENTRY_PRICE"] += 1
            continue
        if sum(1 for (e2, x2) in taken if e2 <= e < x2) >= slots:
            skipped["CAPACITY"] += 1
            continue
        last_bar = s.index[-1]
        x = min(cap_i, int(cal.searchsorted(last_bar, side="right")) - 1)
        x = max(x, e)
        p0 = float(s.loc[pd.Timestamp(edate)])
        vals = s.reindex(cal[e:x + 1]).ffill().to_numpy(dtype=float) / p0
        vals = np.where(np.isfinite(vals), vals, 1.0)
        pnl[e + 1:x + 1] += w * np.diff(vals)
        fin[e + 1:x + 1] += w * vals[:-1] * rf_daily[e + 1:x + 1]
        expo[e:x] += w * vals[:-1] if x > e else 0.0
        cost[e] += w * c
        cost[x] += w * vals[-1] * c
        taken.append((e, x))
        delisted = bool(last_bar <= cal[cap_i]) and x < cap_i
        records.append({"target_cik": d["target_cik"], "symbol": d["symbol"], "entry": edate,
                        "exit": str(cal[x].date()), "held_sessions": int(x - e),
                        "exit_reason": "LAST_QUOTED_SESSION" if delisted else "HOLD_CAP_OR_PANEL_END",
                        "gross_return": float(vals[-1] - 1.0)})
    if not taken:
        return {"deals": 0, "skipped": skipped, "records": [], "daily": pd.Series(dtype=float)}
    lo = min(e for e, _ in taken)
    hi = max(x for _, x in taken)
    net = (pnl - fin - cost)[lo:hi + 1]
    idx = cal[lo:hi + 1]
    daily = pd.Series(net, index=idx)
    gross_excess = pd.Series((pnl - fin)[lo:hi + 1], index=idx)
    st = S.nw_tstat(daily.to_numpy(), BOOK_NW_LAG)
    sd = float(np.std(daily.to_numpy(), ddof=1)) if len(daily) > 2 else float("nan")
    rets = np.array([r["gross_return"] for r in records])
    return {
        "deals": len(taken), "skipped": skipped, "records": records, "daily": daily,
        "gross_excess_daily": gross_excess,
        "sessions": int(len(daily)), "first_session": str(idx[0].date()), "last_session": str(idx[-1].date()),
        "ann_net": float(daily.mean() * PPY), "nw_t": st["t"], "p_one_sided": st["p_one_sided"],
        "sharpe": float(daily.mean() / sd * np.sqrt(PPY)) if sd and sd > 0 else None,
        "ann_vol": float(sd * np.sqrt(PPY)) if sd == sd else None,
        "max_drawdown": float(S._max_dd(daily.to_numpy())),
        "ann_cost_drag": float(cost[lo:hi + 1].mean() * PPY),
        "ann_financing": float(fin[lo:hi + 1].mean() * PPY),
        "mean_exposure": float(expo[lo:hi + 1].mean()), "max_exposure": float(expo[lo:hi + 1].max()),
        "months": int(len(pd.period_range(idx[0], idx[-1], freq="M"))),
        "deal_gross_return_mean": float(rets.mean()), "deal_gross_return_median": float(np.median(rets)),
        "deal_loss_share": float((rets < 0).mean()),
        "deal_loss_below_minus_20pct_share": float((rets < -0.20).mean()),
        "exit_by_last_quoted_share": float(np.mean([r["exit_reason"] == "LAST_QUOTED_SESSION" for r in records])),
    }


def beta_matched_increment(daily: pd.Series, spy_excess: pd.Series) -> dict:
    """The book minus a beta-matched SPY exposure; beta estimated over the same sessions."""
    j = pd.concat([daily.rename("b"), spy_excess.rename("m")], axis=1, sort=True).dropna()
    if len(j) < 60:
        return {"state": "INSUFFICIENT_OVERLAP", "t": None}
    var = float(np.var(j["m"], ddof=1))
    beta = float(np.cov(j["b"], j["m"], ddof=1)[0, 1] / var) if var > 0 else 0.0
    inc = j["b"] - beta * j["m"]
    st = S.nw_tstat(inc.to_numpy(), BOOK_NW_LAG)
    return {"state": "OK", "beta": beta, "ann_increment": float(inc.mean() * PPY), "t": st["t"],
            "p_one_sided": st["p_one_sided"], "sessions": int(len(j))}


def multiplicity(p) -> dict:
    m = 1 + len(INHERITED_NULLS)
    out = {"m": m, "q": BH_Q, "p_value": p, "inherited_nulls": list(INHERITED_NULLS),
           "inherited_p_value_policy": "p = 1 each", "single_survivor_threshold": BH_Q / m,
           "p_value_definition": "one-sided Newey-West p (lag %d) of the qualification daily net book" % BOOK_NW_LAG,
           "denominator_reset": False}
    if p is None:
        return dict(out, passes=False, benjamini_hochberg=None)
    pv = {MECHANISM_ID: float(p)}
    pv.update({k: 1.0 for k in INHERITED_NULLS})
    bh = S.bh_fdr(pv, BH_Q)
    return dict(out, passes=MECHANISM_ID in bh["survivors"], benjamini_hochberg=bh)


# --------------------------------------------------------------------------- #
# Gates
# --------------------------------------------------------------------------- #
def coverage(calendar: list, start: str, end: str) -> dict:
    rows = [c for c in calendar if start <= (c.get("established_on") or c.get("decided_on")
                                              or c.get("first_filed") or "") <= end]
    by = {}
    for c in rows:
        by[c["state"]] = by.get(c["state"], 0) + 1
    inc = by.get(ME.ST_INCLUDED, 0)
    cash_like = inc + by.get(ME.ST_UNRESOLVED, 0) + by.get(ME.ST_AMBIGUOUS, 0)
    return {"episodes": len(rows), "by_state": by, "included": inc,
            "identity_coverage": (inc / cash_like) if cash_like else None}


def pit_violations(calendar: list, cal: pd.DatetimeIndex) -> list:
    bad = []
    for c in calendar:
        if c.get("state") != ME.ST_INCLUDED:
            continue
        e = entry_index(cal, c["established_on"])
        if c["established_on"] < c["first_filed"] or (e is not None and str(cal[e].date()) <= c["established_on"]):
            bad.append(c["target_cik"])
    return bad


def data_hold_reason(inputs: dict, cov: dict, *, min_deals: int, label: str) -> Optional[str]:
    if (inputs.get("head_coverage") or 0.0) < MIN_HEAD_COVERAGE:
        return "filing-head coverage %.3f below %.2f" % (inputs.get("head_coverage") or 0.0, MIN_HEAD_COVERAGE)
    audit = inputs.get("audit") or {}
    if not audit.get("verified"):
        return "the frozen classification audit is missing or does not match its preregistered hash"
    if (audit.get("precision") or 0.0) < MIN_AUDIT_PRECISION:
        return "classification audit precision %.3f below %.2f" % (audit.get("precision") or 0.0,
                                                                   MIN_AUDIT_PRECISION)
    if CALENDAR_HASH is None or inputs.get("calendar_hash") != CALENDAR_HASH:
        return ("the deal calendar hash %s is not the preregistered %s: the calendar moved after freezing"
                % (inputs.get("calendar_hash"), CALENDAR_HASH))
    if cov.get("identity_coverage") is None or cov["identity_coverage"] < MIN_IDENTITY_COVERAGE:
        return "%s identity coverage %s below %.2f" % (label, _r(cov.get("identity_coverage")),
                                                       MIN_IDENTITY_COVERAGE)
    if cov.get("included", 0) < min_deals:
        return "%s included all-cash deals %d below %d" % (label, cov.get("included", 0), min_deals)
    if inputs.get("spy") is None or inputs.get("rf") is None:
        return "the SPY total-return or Treasury bill series is unavailable"
    return None


def qualification_gate(q: dict, inc: dict, halves: dict, mult: dict) -> Optional[dict]:
    if q.get("ann_net") is None or q["ann_net"] <= 0:
        return _v(V_WRONG_SIGN, "SIGN", "qualification net book %s/yr is not positive" % _r(q.get("ann_net"), 4),
                  "BOOK_NET_NOT_POSITIVE")
    if q.get("nw_t") is None or q["nw_t"] < T_FLOOR:
        return _v(V_NO_EDGE, "BOOK_T", "qualification book NW t %s below %.1f" % (_r(q.get("nw_t")), T_FLOOR),
                  "BOOK_NW_T_BELOW_2")
    if q["ann_net"] < MATERIALITY_ANN_NET:
        return _v(V_MATERIALITY, "MATERIALITY", "qualification net %s/yr below %.3f" % (_r(q["ann_net"], 4),
                                                                                         MATERIALITY_ANN_NET),
                  "NET_BELOW_1.5PCT_PER_YEAR")
    if inc.get("t") is None or inc["t"] < T_FLOOR:
        return _v(V_NONINCREMENTAL, "BETA_MATCHED_SPY", "increment over beta-matched SPY NW t %s below %.1f"
                  % (_r(inc.get("t")), T_FLOOR), "INCREMENT_OVER_BETA_MATCHED_SPY_T_BELOW_2")
    for k, h in halves.items():
        if h.get("ann_net") is None or h["ann_net"] <= 0:
            return _v(V_UNSTABLE, "HALVES", "%s net %s/yr is not positive" % (k, _r(h.get("ann_net"), 4)),
                      "HALF_NOT_POSITIVE")
    if not mult.get("passes"):
        return _v(V_MULTIPLICITY, "MULTIPLICITY", "BH q=0.10 fails at m=%d" % mult["m"],
                  "BH_Q010_FAILS_WITH_INHERITED_CELLS")
    return None


def confirmation_gate(c: dict) -> dict:
    if c.get("data_hold"):
        return _v(V_DATA_HOLD, "CONFIRMATION_DATA", c["data_hold"], "DATA")
    if c.get("ann_net") is None or c["ann_net"] <= 0:
        return _v(V_UNSTABLE, "CONFIRMATION_SIGN", "confirmation net %s/yr is not positive" % _r(c.get("ann_net"), 4),
                  "CONFIRMATION_SIGN_REVERSED")
    if c.get("nw_t") is None or c["nw_t"] < T_FLOOR:
        return _v(V_NO_EDGE, "CONFIRMATION_T", "confirmation NW t %s below %.1f" % (_r(c.get("nw_t")), T_FLOOR),
                  "CONFIRMATION_NW_T_BELOW_2")
    if c["ann_net"] < MATERIALITY_ANN_NET:
        return _v(V_MATERIALITY, "CONFIRMATION_MATERIALITY", "confirmation net %s/yr below %.3f"
                  % (_r(c["ann_net"], 4), MATERIALITY_ANN_NET), "CONFIRMATION_NET_BELOW_1.5PCT_PER_YEAR")
    return _v(V_QUALIFIED, None, "every preregistered qualification gate and the untouched confirmation passed; "
                                 "a HUMAN gate governs prospective registration and capital eligibility stays False")


def _brief(b: dict) -> dict:
    return {k: b.get(k) for k in ("deals", "skipped", "sessions", "first_session", "last_session", "ann_net",
                                  "nw_t", "p_one_sided", "sharpe", "ann_vol", "max_drawdown", "ann_cost_drag",
                                  "ann_financing", "mean_exposure", "max_exposure", "months",
                                  "deal_gross_return_mean", "deal_gross_return_median", "deal_loss_share",
                                  "deal_loss_below_minus_20pct_share", "exit_by_last_quoted_share")}


# --------------------------------------------------------------------------- #
# Evaluation and the executor contract
# --------------------------------------------------------------------------- #
def artifact_path() -> str:
    return str(research_root() / "results" / ARTIFACT_NAME)


def _result(v: dict, statistic: dict, economics: dict, mult: dict, identity) -> dict:
    return {"verdict": v["verdict"], "why": v["why"], "gate": v.get("gate"),
            "kill_rule_fired": v.get("kill_rule_fired"), "statistic": statistic, "economics": economics,
            "multiplicity": mult, "artifact": artifact_path(), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": stable_hash(identity or {})}


def hold_result(why: str, identity=None) -> dict:
    return _result(_v(V_DATA_HOLD, "DATA", why, "DATA"), {"lockbox_t": None}, {}, multiplicity(None), identity)


def evaluate(inputs: dict) -> dict:
    calendar = inputs["calendar"]
    cal = pd.DatetimeIndex(inputs["sessions"])
    rf_daily = np.asarray(inputs["rf_daily"], dtype=float)
    reader = PriceReader(inputs["price_loader"])
    identity = inputs.get("identity") or {}
    cov_q = coverage(calendar, *QUALIFICATION)
    out = {"coverage": {"QUALIFICATION": cov_q}, "head_coverage": inputs.get("head_coverage"),
           "audit": inputs.get("audit"), "confirmation": "UNTOUCHED"}
    hold = data_hold_reason(inputs, cov_q, min_deals=MIN_QUALIFICATION_DEALS, label="qualification")
    if hold:
        out["max_date_read"] = reader.max_date_read
        out["executor_result"] = _result(_v(V_DATA_HOLD, "DATA", hold, "DATA"),
                                         {"lockbox_t": None, "qualification_deals": cov_q.get("included"),
                                          "identity_coverage": cov_q.get("identity_coverage")},
                                         {"cost_bps_per_side": COST_BPS}, multiplicity(None), identity)
        return out
    bad = pit_violations(calendar, cal)
    if bad:
        out["executor_result"] = _result(_v(V_PIT, "PIT", "%d included deals would enter on or before their "
                                            "establishing date" % len(bad), "IDENTIFICATION_AFTER_ENTRY"),
                                         {"lockbox_t": None}, {}, multiplicity(None), identity)
        return out
    included = [c for c in calendar if c["state"] == ME.ST_INCLUDED]
    qual_read = str(cal[min(int(cal.searchsorted(pd.Timestamp(QUALIFICATION[1]), side="right")) - 1 + HOLD_CAP,
                            len(cal) - 1)].date())
    q = book(included, reader, cal, rf_daily, *QUALIFICATION, read_through=qual_read)
    if not q.get("deals"):
        out["executor_result"] = hold_result("no qualification deal could be entered", identity)
        return out
    spy_ex = inputs["spy_excess"]
    inc = beta_matched_increment(q["daily"], spy_ex)
    halves = {k: book(included, reader, cal, rf_daily, *w, read_through=qual_read) for k, w in HALVES.items()}
    ladder = {str(b): _brief(book(included, reader, cal, rf_daily, *QUALIFICATION, cost_bps=b,
                                  read_through=qual_read)) for b in COST_LADDER_BPS}
    mult = multiplicity(q.get("p_one_sided"))
    v = qualification_gate(q, inc, halves, mult)
    conf = None
    if v is None:
        cov_c = coverage(calendar, *CONFIRMATION)
        out["coverage"]["CONFIRMATION"] = cov_c
        hold_c = data_hold_reason(inputs, cov_c, min_deals=MIN_CONFIRMATION_DEALS, label="confirmation")
        if hold_c:
            v = confirmation_gate({"data_hold": hold_c})
        else:
            conf = book(included, reader, cal, rf_daily, *CONFIRMATION)
            v = confirmation_gate(conf)
    out.update({
        "qualification": _brief(q), "qualification_deal_records": q["records"],
        "beta_matched_spy_increment": inc, "halves": {k: _brief(h) for k, h in halves.items()},
        "cost_ladder_bps_per_side": ladder, "confirmation": _brief(conf) if conf else "UNTOUCHED",
        "max_date_read": reader.max_date_read, "symbols_read": reader.symbols_read})
    statistic = {"lockbox_t": conf.get("nw_t") if conf else None,
                 "lockbox_window": "CONFIRMATION %s..%s" % CONFIRMATION,
                 "qualification_nw_t": q.get("nw_t"), "qualification_p_one_sided": q.get("p_one_sided"),
                 "beta_matched_increment_t": inc.get("t"), "beta": inc.get("beta"),
                 "qualification_deals": q.get("deals"), "qualification_months": q.get("months"),
                 "book_nw_lag": BOOK_NW_LAG}
    economics = {"units": "fraction of NAV; each deal carries a fixed %.4f NAV slot at entry" % SLOT_WEIGHT,
                 "cost_bps_per_side": COST_BPS, "qualification": _brief(q),
                 "halves_ann_net": {k: h.get("ann_net") for k, h in halves.items()},
                 "beta_matched_increment_ann": inc.get("ann_increment"),
                 "confirmation": _brief(conf) if conf else None}
    out["executor_result"] = _result(v, statistic, economics, mult, identity)
    return out


def design() -> dict:
    return {"universe": "every EDGAR full-index row of forms %s, 2002Q1-2026Q3" % ", ".join(MD.FORMS),
            "episode": "one target CIK's merger-form documents; a gap over %d days starts a new episode"
                       % ME.EPISODE_GAP_DAYS,
            "establishing_date": "first filing date by which the documents filed so far state an all-shares "
                                 "cash price, no stock / election / exchange-offer / CVR / partial terms, and a "
                                 "company-tagged trading symbol resolving to exactly one Norgate US equity "
                                 "quoted that day; within %d days of the first document" % ME.ESTABLISH_WINDOW_DAYS,
            "entry": "close of the first session strictly after the establishing filing date",
            "exit": "the target's last quoted session or entry + %d sessions" % HOLD_CAP,
            "sizing": "fixed %.4f NAV slot per deal (%d slots); a deal arriving with every slot occupied is "
                      "not traded" % (SLOT_WEIGHT, SLOTS),
            "costs": "%.1f bp per side on entry and exit notional" % COST_BPS,
            "financing": "3-month Treasury bill (%s, as of) on the marked capital of every open slot" % RF_COLUMN,
            "windows": {"QUALIFICATION": list(QUALIFICATION), "HALVES": {k: list(v) for k, v in HALVES.items()},
                        "CONFIRMATION": list(CONFIRMATION)},
            "benchmark": "beta-matched %s total return in excess of the bill" % BENCHMARK,
            "cost_ladder_bps_per_side": list(COST_LADDER_BPS)}


# --------------------------------------------------------------------------- #
# Inputs (read only) and the contract
# --------------------------------------------------------------------------- #
def audit_state() -> dict:
    p = MD.data_dir() / AUDIT_FILE
    if not p.exists():
        return {"verified": False, "reason": "AUDIT_MISSING"}
    raw = p.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    body = json.loads(raw.decode("utf-8"))
    return {"verified": AUDIT_SHA256 is not None and sha == AUDIT_SHA256, "sha256": sha,
            "precision": body.get("precision"), "n_sample": body.get("n_sample")}


def load_inputs() -> dict:
    from alpha_agent.r63 import panels as P
    rows = MD.load_index()
    parsed = MD.load_parsed()
    universe = ME.load_universe()
    calendar = ME.build_calendar(rows, parsed, universe)
    accs = {r["accession"] for r in rows}
    spy = P.load_norgate_total_return((BENCHMARK,)).get(BENCHMARK)
    fred = P.load_fred_daily()
    rf = fred[RF_COLUMN] if RF_COLUMN in getattr(fred, "columns", []) else None
    sessions = spy.index if spy is not None else pd.DatetimeIndex([])
    rf_daily = (rf.sort_index().reindex(sessions, method="ffill").ffill() / 100.0 / PPY).to_numpy() \
        if rf is not None else np.zeros(len(sessions))
    spy_ret = spy.pct_change() if spy is not None else None
    spy_excess = (spy_ret - pd.Series(rf_daily, index=sessions)) if spy is not None else None
    return {"calendar": calendar, "calendar_hash": ME.calendar_hash(calendar),
            "sessions": sessions, "rf_daily": rf_daily, "rf": rf, "spy": spy,
            "spy_excess": spy_excess, "price_loader": P.load_norgate_total_return,
            "head_coverage": (len(accs & set(parsed)) / len(accs)) if accs else 0.0,
            "audit": audit_state(),
            "identity": {"index_rows": len(rows), "parsed": len(parsed),
                         "universe_bases": len(universe), "calendar_episodes": len(calendar)}}


def run(*, verbose: bool = True, write: bool = True, inputs: Optional[dict] = None) -> dict:
    inputs = load_inputs() if inputs is None else inputs
    detail = evaluate(inputs)
    body = {"schema": "alpha_recovery_merger_arbitrage_cash_deal/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "preregistration": PREREGISTRATION,
            "preregistration_altered_after_results": False, "kill_rule_frozen": KILL_RULE_FROZEN,
            "design": design(), "scorer": SCORER, "capital_eligible": False}
    body.update(detail)
    res = body["executor_result"]
    body["verdict"], body["why"] = res["verdict"], res["why"]
    if write:
        write_artifact(ARTIFACT_NAME, body)
    if verbose:
        print("%s %s: %s" % (MECHANISM_ID, res["verdict"], res["why"]), flush=True)
    return body


def run_mechanism(*, mechanism: dict) -> dict:
    """The R59 mechanism-executor contract. Refuses another mechanism or a changed kill rule."""
    mid = (mechanism or {}).get("mechanism_id")
    if mid != MECHANISM_ID:
        raise ValueError("executor for %s was handed %r" % (MECHANISM_ID, mid))
    rule = ((mechanism or {}).get("pnl_gate") or {}).get("KILL_RULE")
    if rule != KILL_RULE_FROZEN:
        raise ValueError("the catalog KILL_RULE differs from the preregistered rule; an executor does not "
                         "judge a changed contract")
    try:
        body = run(verbose=False, write=True)
    except (OSError, KeyError, ValueError, ImportError) as exc:
        return hold_result("an owned input is unavailable or unreadable: %r" % (exc,))
    return dict(body["executor_result"])
