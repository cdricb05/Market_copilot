"""alpha_agent.r46.nav - ONE shadow strategy NAV, and the controls it must beat.

The canonical research-only NAV starts at ``STARTING_CAPITAL`` on the session
the first allocation was decided and rolls forward one session at a time on
the one NAV clock (the SPY session calendar). Each policy in
:mod:`alpha_agent.r46.allocation` has its own NAV under the same engine, and
two passive benchmarks are rolled by the same engine from prices, so the
sophisticated policy is judged against cash, equal-weight, risk-balanced and
passive alternatives on identical arithmetic.

Arithmetic, per session ``s`` and policy ``p`` (dollars):

    financing       = begin x rf_annual / 252            (collateral earns cash)
    open cost       = - capital_t x cost_t               (booked on entry)
    mark change     = capital_t x (G_t(s) - G_t(prev))   (entry-anchored gross)
    close           = capital_t x (G_t(exit) - G_t(prev)); realised += capital_t x net_t
    ending          = begin + financing + sum(...)

``capital_t`` is fixed when the trade opens: NAV at the decision session that
funded it x the strategy's frozen weight x the trade's share of the strategy.
Because the gross is entry-anchored, a trade's cumulative dollar P&L at close
equals ``capital_t x net_t`` - the judge's number times the capital - exactly.

Append-only: a (policy, session) row is written once. Re-running the roll
for a session already on the ledger appends nothing; a replay from the
ledgers reproduces the same rows. Prior NAV history is never rewritten.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Optional

from . import CAMPAIGN_ID, artifact_body, campaign_dir, write_json
from . import allocation as AL
from . import clock as CK
from . import pnl as PN
from . import trades as TR

CALCULATION_OWNER = "alpha_agent.r46.nav"

ARTIFACT = "R46_4_SHADOW_NAV.json"
COMPARISON_ARTIFACT = "R46_4_SHADOW_POLICY_COMPARISON.json"
LEDGER = "r46_4_shadow_nav.json"

STARTING_CAPITAL = 1_000_000.0
STARTING_CAPITAL_NOTE = ("a normalised research scale; the scale itself "
                         "creates no economic conclusion")

BENCH_SPY = "PASSIVE_SPY_v1"
BENCH_6040 = "PASSIVE_60_40_SPY_TLT_v1"
BENCHMARKS = {BENCH_SPY: {"SPY": 1.0},
              BENCH_6040: {"SPY": 0.6, "TLT": 0.4}}
SERIES_IDS = tuple(AL.POLICIES) + tuple(BENCHMARKS)


def _desk():
    from paper_trader.api import paper_trading_desk as desk
    return desk


def rows(campaign_id: str = CAMPAIGN_ID) -> list:
    return _desk()._read_ledger(TR.shadow_dir(campaign_id), LEDGER)


def latest_nav(series_id: str, campaign_id: str = CAMPAIGN_ID) -> dict:
    best = None
    for r in rows(campaign_id):
        if r.get("series_id") != series_id:
            continue
        if best is None or r["session"] > best["session"]:
            best = r
    return best or {}


def nav_by_policy(campaign_id: str = CAMPAIGN_ID) -> dict:
    out = {}
    for pid in AL.POLICIES:
        r = latest_nav(pid, campaign_id)
        out[pid] = float(r["ending_nav"]) if r else STARTING_CAPITAL
    return out


def inception_session(campaign_id: str = CAMPAIGN_ID) -> Optional[_dt.date]:
    """The first allocation decision session - the NAV's day zero."""
    ds = [str(r.get("decision_session")) for r in AL.rows(campaign_id)]
    return _dt.date.fromisoformat(min(ds)) if ds else None


# --------------------------------------------------------------------------- #
def _trade_book(campaign_id: str, policy_id: str, upto: str) -> list:
    """Funded trades for one policy, with their marks and close, as of a
    session string - everything the roll needs, read once."""
    marks: dict = {}
    for m in TR.marks(campaign_id):
        marks.setdefault(m["research_trade_id"], []).append(m)
    for v in marks.values():
        v.sort(key=lambda m: m["session"])
    closes = {c["research_trade_id"]: c for c in TR.closes(campaign_id)}
    book = []
    for o in TR.opens(campaign_id):
        cap = ((o.get("capital_by_policy") or {}).get(policy_id) or {}).get(
            "capital_usd")
        if not cap or float(cap) <= 0:
            continue
        nav_entry = max(str(o["entry_session"]), str(o.get("opened_as_of")))
        c = closes.get(o["research_trade_id"])
        nav_exit = (max(str(c["exit_session"]), str(c.get("closed_as_of")))
                    if c else None)
        book.append({"open": o, "capital": float(cap), "nav_entry": nav_entry,
                     "nav_exit": nav_exit, "close": c,
                     "marks": marks.get(o["research_trade_id"], [])})
    return book


def _g_at(t: dict, session: str) -> Optional[float]:
    """Entry-anchored gross at ``session`` from the mark on that session."""
    for m in t["marks"]:
        if m["session"] == session:
            return float(m.get("gross_return") or 0.0)
    return None


def _g_last_booked(t: dict, before: str) -> float:
    """The gross last booked into the NAV strictly before ``before``."""
    g = None
    for m in t["marks"]:
        if t["nav_entry"] <= m["session"] < before:
            g = float(m.get("gross_return") or 0.0)
    if g is not None:
        return g
    if t["nav_entry"] < before:
        gi = _g_at(t, t["nav_entry"])
        return gi if gi is not None else 0.0
    return 0.0


def _roll_policy(policy_id: str, sessions: list, begin_nav: float,
                 begin_session: Optional[str], book: list, rf_annual,
                 calendar_positions: dict) -> list:
    out = []
    nav = begin_nav
    hwm = begin_nav
    prior_rows_cum_realised = 0.0
    prior_rows_cum_cost = 0.0
    for s in sessions:
        ss = str(s)
        financing = (nav * float(rf_annual) / 252.0) if rf_annual else 0.0
        pnl_open_cost = 0.0
        pnl_marks = 0.0
        pnl_close = 0.0
        realised_booked = 0.0
        n_open, gross_exp, net_exp = 0, 0.0, 0.0
        for t in book:
            o, cap = t["open"], t["capital"]
            if t["nav_entry"] > ss:
                continue
            if t["nav_exit"] is not None and t["nav_exit"] < ss:
                continue
            if t["nav_entry"] == ss:
                pnl_open_cost -= cap * float(o["cost_return"])
                if t["nav_exit"] == ss:
                    # Opened and closed on the same NAV session (a one-session
                    # horizon whose entry printed late): book the whole trade.
                    c = t["close"]
                    pnl_close += cap * float(c.get("gross_return") or 0.0)
                    realised_booked += cap * float(c.get("net_return") or 0.0)
                    continue
                g = _g_at(t, ss) if ss > str(o["entry_session"]) else 0.0
                pnl_marks += cap * (g if g is not None else 0.0)
            else:
                if t["nav_exit"] == ss:
                    c = t["close"]
                    g_exit = float(c.get("gross_return") or 0.0)
                    pnl_close += cap * (g_exit - _g_last_booked(t, ss))
                    realised_booked += cap * float(c.get("net_return") or 0.0)
                    continue
                g = _g_at(t, ss)
                if g is not None:
                    pnl_marks += cap * (g - _g_last_booked(t, ss))
            if t["nav_exit"] is None or t["nav_exit"] > ss:
                n_open += 1
                gross_exp += cap * float(
                    o.get("gross_exposure_per_unit_capital") or 0.0)
                net_exp += cap * float(
                    o.get("net_exposure_per_unit_capital") or 0.0)
        ending = nav + financing + pnl_open_cost + pnl_marks + pnl_close
        hwm = max(hwm, ending)
        prior_rows_cum_realised += realised_booked
        prior_rows_cum_cost += -pnl_open_cost
        # Unrealised value of what is still open at the end of the session.
        unreal = 0.0
        for t in book:
            if t["nav_entry"] > ss or (t["nav_exit"] is not None
                                       and t["nav_exit"] <= ss):
                continue
            g = _g_last_booked(t, ss + "~")   # latest mark at or before ss
            unreal += t["capital"] * (g - float(t["open"]["cost_return"]))
        out.append({
            "series_id": policy_id,
            "kind": "POLICY",
            "session": ss,
            "beginning_nav": round(nav, 6),
            "financing_pnl": round(financing, 6),
            "transaction_cost_pnl": round(pnl_open_cost, 6),
            "mark_to_market_pnl": round(pnl_marks, 6),
            "close_pnl": round(pnl_close, 6),
            "today_net_pnl": round(ending - nav, 6),
            "today_net_pnl_ex_financing": round(ending - nav - financing, 6),
            "realised_pnl_booked_today": round(realised_booked, 6),
            "ending_nav": round(ending, 6),
            "daily_return": round(ending / nav - 1.0, 10) if nav else None,
            "cumulative_return": round(ending / STARTING_CAPITAL - 1.0, 10),
            "high_water_mark": round(hwm, 6),
            "drawdown": round(ending / hwm - 1.0, 10) if hwm else 0.0,
            "unrealised_pnl_end": round(unreal, 6),
            "n_open_trades": n_open,
            "gross_exposure_usd": round(gross_exp, 6),
            "net_exposure_usd": round(net_exp, 6),
            "gross_exposure_share": (round(gross_exp / ending, 10)
                                     if ending else None),
            "net_exposure_share": (round(net_exp / ending, 10)
                                   if ending else None),
            "risk_free_annual": rf_annual,
            "evidence_class": PN.EVIDENCE_TRUE_FORWARD,
            "calculation_owner": CALCULATION_OWNER,
        })
        nav = ending
    return out


def _roll_benchmark(bench_id: str, sessions: list, begin_nav: float,
                    begin_session: Optional[_dt.date], series_fn) -> list:
    weights = BENCHMARKS[bench_id]
    out = []
    nav = begin_nav
    hwm = begin_nav
    prev = begin_session
    for s in sessions:
        ret = 0.0
        ok = True
        for sym, w in weights.items():
            ser = series_fn(sym)
            _, p1 = PN.mark_on_or_before(ser, s)
            _, p0 = PN.mark_on_or_before(ser, prev) if prev else (None, None)
            if p0 is None or p1 is None or p0 <= 0:
                ok = False
                break
            ret += w * (p1 / p0 - 1.0)
        ending = nav * (1.0 + ret) if ok else nav
        hwm = max(hwm, ending)
        out.append({
            "series_id": bench_id, "kind": "BENCHMARK", "session": str(s),
            "beginning_nav": round(nav, 6), "financing_pnl": 0.0,
            "transaction_cost_pnl": 0.0,
            "mark_to_market_pnl": round(ending - nav, 6), "close_pnl": 0.0,
            "today_net_pnl": round(ending - nav, 6),
            "today_net_pnl_ex_financing": round(ending - nav, 6),
            "realised_pnl_booked_today": 0.0,
            "ending_nav": round(ending, 6),
            "daily_return": round(ret, 10) if ok else None,
            "cumulative_return": round(ending / STARTING_CAPITAL - 1.0, 10),
            "high_water_mark": round(hwm, 6),
            "drawdown": round(ending / hwm - 1.0, 10) if hwm else 0.0,
            "unrealised_pnl_end": None, "n_open_trades": None,
            "gross_exposure_usd": round(ending, 6),
            "net_exposure_usd": round(ending, 6),
            "gross_exposure_share": 1.0, "net_exposure_share": 1.0,
            "priced": ok,
            "weights": weights,
            "evidence_class": PN.EVIDENCE_TRUE_FORWARD,
            "calculation_owner": CALCULATION_OWNER,
        })
        nav = ending
        prev = s
    return out


# --------------------------------------------------------------------------- #
def roll(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID, series_fn=None,
         risk_free_annual: float = None) -> dict:
    """Roll every policy and benchmark NAV through ``as_of``. Append-only."""
    from . import marketdata as MD
    sf = series_fn or MD.closes
    rf = risk_free_annual
    if rf is None:
        rf = MD.risk_free_annual().get("annual")
    inception = inception_session(campaign_id)
    if inception is None:
        return {"state": "NO_INCEPTION", "n_appended": 0,
                "reason": "no allocation has been decided yet; the shadow NAV "
                          "starts on the first decision session"}
    calendar = TR.nav_calendar(sf, start=inception, end=as_of)
    existing: dict = {}
    for r in rows(campaign_id):
        existing.setdefault(r["series_id"], {})[r["session"]] = r
    new_rows = []
    for sid in SERIES_IDS:
        have = existing.get(sid, {})
        if str(inception) not in have:
            new_rows.append({
                "series_id": sid,
                "kind": "POLICY" if sid in AL.POLICIES else "BENCHMARK",
                "session": str(inception), "inception": True,
                "beginning_nav": STARTING_CAPITAL, "financing_pnl": 0.0,
                "transaction_cost_pnl": 0.0, "mark_to_market_pnl": 0.0,
                "close_pnl": 0.0, "today_net_pnl": 0.0,
                "today_net_pnl_ex_financing": 0.0,
                "realised_pnl_booked_today": 0.0,
                "ending_nav": STARTING_CAPITAL, "daily_return": 0.0,
                "cumulative_return": 0.0, "high_water_mark": STARTING_CAPITAL,
                "drawdown": 0.0, "unrealised_pnl_end": 0.0,
                "n_open_trades": 0, "gross_exposure_usd": 0.0,
                "net_exposure_usd": 0.0, "gross_exposure_share": 0.0,
                "net_exposure_share": 0.0,
                "starting_capital_note": STARTING_CAPITAL_NOTE,
                "evidence_class": PN.EVIDENCE_TRUE_FORWARD,
                "calculation_owner": CALCULATION_OWNER,
            })
            have = dict(have, **{str(inception): new_rows[-1]})
        last_session = max(have) if have else str(inception)
        last_row = have[last_session]
        todo = [s for s in calendar if str(s) > last_session]
        if not todo:
            continue
        if sid in AL.POLICIES:
            book = _trade_book(campaign_id, sid, str(as_of))
            new_rows.extend(_roll_policy(
                sid, todo, float(last_row["ending_nav"]), last_session, book,
                rf, {}))
        else:
            new_rows.extend(_roll_benchmark(
                sid, todo, float(last_row["ending_nav"]),
                _dt.date.fromisoformat(last_session), sf))
    appended = (_desk()._append_ledger(TR.shadow_dir(campaign_id), LEDGER,
                                       new_rows) if new_rows else [])
    return {"state": "ROLLED" if appended else "NOTHING_DUE",
            "as_of": str(as_of), "inception": str(inception),
            "n_appended": len(appended), "n_sessions_on_calendar": len(calendar),
            "risk_free_annual": rf, "idempotent": True,
            "calculation_owner": CALCULATION_OWNER}


# --------------------------------------------------------------------------- #
def series(series_id: str, campaign_id: str = CAMPAIGN_ID) -> list:
    return sorted((r for r in rows(campaign_id) if r["series_id"] == series_id),
                  key=lambda r: r["session"])


def _summary(sid: str, srows: list) -> dict:
    if not srows:
        return {"series_id": sid, "state": "NOT_STARTED"}
    last = srows[-1]
    rets = [r.get("daily_return") for r in srows[1:]
            if r.get("daily_return") is not None]
    vol = None
    sharpe = None
    if len(rets) >= 20:
        import numpy as np
        sd = float(np.std(rets, ddof=1))
        if sd > 0:
            vol = sd * math.sqrt(252.0)
            sharpe = float(np.mean(rets) / sd * math.sqrt(252.0))
    return {
        "series_id": sid,
        "kind": last.get("kind"),
        "inception": srows[0]["session"],
        "latest_session": last["session"],
        "n_sessions": len(srows),
        "starting_capital": STARTING_CAPITAL,
        "nav": last["ending_nav"],
        "cumulative_return": last["cumulative_return"],
        "cumulative_net_pnl": round(last["ending_nav"] - STARTING_CAPITAL, 6),
        "cumulative_financing": round(sum(r.get("financing_pnl") or 0.0
                                          for r in srows), 6),
        "cumulative_cost_drag": round(-sum(r.get("transaction_cost_pnl")
                                           or 0.0 for r in srows), 6),
        "cumulative_realised_pnl": round(sum(r.get("realised_pnl_booked_today")
                                             or 0.0 for r in srows), 6),
        "unrealised_pnl": last.get("unrealised_pnl_end"),
        "today_net_pnl": last.get("today_net_pnl"),
        "today_net_pnl_ex_financing": last.get("today_net_pnl_ex_financing"),
        "max_drawdown": min((r.get("drawdown") or 0.0) for r in srows),
        "current_drawdown": last.get("drawdown"),
        "high_water_mark": last.get("high_water_mark"),
        "annualised_vol": vol,
        "sharpe_annualised": sharpe,
        "gross_exposure_share": last.get("gross_exposure_share"),
        "net_exposure_share": last.get("net_exposure_share"),
        "n_open_trades": last.get("n_open_trades"),
    }


def _policy_competition(sid: str, srows: list, as_of: _dt.date,
                        campaign_id: str) -> dict:
    """Release 46.5 - the section-9 facts per policy, from the SAME ledgers."""
    if not srows:
        return {"gross_pnl": None, "net_pnl": None, "cost_drag": None,
                "realised_pnl": None, "unrealised_pnl": None,
                "turnover_usd": None, "n_allocated": None, "deployment": None,
                "latest_daily_return": None, "n_sessions": 0}
    gross = float(sum((r.get("mark_to_market_pnl") or 0.0)
                      + (r.get("close_pnl") or 0.0) for r in srows))
    cost = float(-sum(r.get("transaction_cost_pnl") or 0.0 for r in srows))
    turnover = 0.0
    if sid in AL.POLICIES:
        for o in TR.opens(campaign_id):
            if str(o.get("entry_session")) > str(as_of):
                continue
            cap = float(((o.get("capital_by_policy") or {}).get(sid) or {})
                        .get("capital_usd") or 0.0)
            turnover += cap * 2.0 * float(
                o.get("gross_exposure_per_unit_capital") or 0.0)
    dec = AL.latest(sid, before=None, campaign_id=campaign_id) \
        if sid in AL.POLICIES else {}
    return {"gross_pnl": round(gross, 6),
            "net_pnl": round(float(srows[-1]["ending_nav"]) - STARTING_CAPITAL,
                             6),
            "cost_drag": round(cost, 6),
            "realised_pnl": round(float(sum(r.get("realised_pnl_booked_today")
                                            or 0.0 for r in srows)), 6),
            "unrealised_pnl": srows[-1].get("unrealised_pnl_end"),
            "turnover_usd": round(turnover, 6),
            "n_allocated": dec.get("n_allocated"),
            "deployment": dec.get("deployment"),
            "latest_daily_return": srows[-1].get("daily_return"),
            "n_sessions": len(srows)}


def build(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID) -> dict:
    """The NAV read models: the canonical headline and the policy comparison."""
    summaries = {sid: _summary(sid, series(sid, campaign_id))
                 for sid in SERIES_IDS}
    competition = {sid: _policy_competition(sid, series(sid, campaign_id),
                                            as_of, campaign_id)
                   for sid in SERIES_IDS}
    canon = summaries[AL.CANONICAL_POLICY]
    cash = summaries[AL.POLICY_CASH]
    canon_nav = canon.get("nav")
    cash_nav = cash.get("nav")
    residual_vs_cash = (None if canon_nav is None or cash_nav is None
                        else round(canon_nav - cash_nav, 6))
    body = artifact_body(
        "r46_4_shadow_nav/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        canonical_policy=AL.CANONICAL_POLICY,
        starting_capital=STARTING_CAPITAL,
        starting_capital_note=STARTING_CAPITAL_NOTE,
        inception=(str(inception_session(campaign_id))
                   if inception_session(campaign_id) else None),
        shadow_nav=canon_nav,
        shadow_return=canon.get("cumulative_return"),
        cumulative_net_forward_pnl=canon.get("cumulative_net_pnl"),
        today_net_pnl=canon.get("today_net_pnl"),
        realised_pnl=canon.get("cumulative_realised_pnl"),
        unrealised_pnl=canon.get("unrealised_pnl"),
        cost_drag=canon.get("cumulative_cost_drag"),
        financing_earned=canon.get("cumulative_financing"),
        max_drawdown=canon.get("max_drawdown"),
        current_drawdown=canon.get("current_drawdown"),
        residual_alpha_pnl_vs_cash_control=residual_vs_cash,
        gross_exposure_share=canon.get("gross_exposure_share"),
        net_exposure_share=canon.get("net_exposure_share"),
        n_open_trades=canon.get("n_open_trades"),
        by_series=summaries,
        chain=_desk().verify_ledger(TR.shadow_dir(campaign_id), LEDGER),
        never_rewrites_prior_history=True,
        realised_and_unrealised_are_reported_separately=True,
        evidence_class=PN.EVIDENCE_TRUE_FORWARD,
        research_only=True,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)

    ranked = sorted(summaries.values(), key=lambda s: -(s.get("nav") or 0.0))
    # Release 46.5 - the policy COMPETITION: does complex allocation beat
    # simple equal weight or cash? Decidable only on realised forward NAV
    # with a sample; until then the answer is stated as not yet decidable.
    n_sess = max((s.get("n_sessions") or 0) for s in summaries.values())
    n_closed = sum(1 for c in TR.closes(campaign_id)
                   if str(c.get("exit_session")) <= str(as_of))
    canon_nav_ = summaries[AL.CANONICAL_POLICY].get("nav")
    eq_nav = summaries[AL.POLICY_EQUAL].get("nav")
    rk_nav = summaries[AL.POLICY_RISK].get("nav")
    decidable = bool(n_closed >= 20 and n_sess >= 20)
    leader = ranked[0]["series_id"] if ranked else None
    comparison = artifact_body(
        "r46_4_shadow_policy_comparison/1", CALCULATION_OWNER,
        as_of=str(as_of),
        canonical_policy=AL.CANONICAL_POLICY,
        policies=list(AL.POLICIES),
        benchmarks={k: v for k, v in BENCHMARKS.items()},
        ranked_by_nav=[dict({"series_id": s["series_id"], "nav": s.get("nav"),
                             "cumulative_return": s.get("cumulative_return"),
                             "max_drawdown": s.get("max_drawdown"),
                             "sharpe": s.get("sharpe_annualised")},
                            **competition[s["series_id"]])
                       for s in ranked],
        competition={
            "question": "does complex allocation actually beat simple equal "
                        "weight or cash?",
            "decidable": decidable,
            "decidable_when": "at least 20 closed research trades and 20 "
                              "forward NAV sessions",
            "n_forward_sessions": n_sess,
            "n_closed_research_trades": n_closed,
            "current_leader_by_nav": leader,
            "canonical_minus_equal_weight_usd": (
                None if canon_nav_ is None or eq_nav is None
                else round(canon_nav_ - eq_nav, 6)),
            "canonical_minus_equal_risk_usd": (
                None if canon_nav_ is None or rk_nav is None
                else round(canon_nav_ - rk_nav, 6)),
            "answer": ("NOT_YET_DECIDABLE" if not decidable else
                       "CANONICAL_LEADS" if leader == AL.CANONICAL_POLICY
                       else "SIMPLE_POLICY_LEADS"),
            "assumes_nothing": True,
        },
        canonical_beats_cash=(None if residual_vs_cash is None
                              else bool(residual_vs_cash > 0)),
        canonical_minus_cash_usd=residual_vs_cash,
        canonical_minus_equal_weight_usd=(
            None if canon_nav is None
            or summaries[AL.POLICY_EQUAL].get("nav") is None
            else round(canon_nav - summaries[AL.POLICY_EQUAL]["nav"], 6)),
        canonical_minus_equal_risk_usd=(
            None if canon_nav is None
            or summaries[AL.POLICY_RISK].get("nav") is None
            else round(canon_nav - summaries[AL.POLICY_RISK]["nav"], 6)),
        canonical_minus_passive_spy_usd=(
            None if canon_nav is None
            or summaries[BENCH_SPY].get("nav") is None
            else round(canon_nav - summaries[BENCH_SPY]["nav"], 6)),
        canonical_minus_passive_60_40_usd=(
            None if canon_nav is None
            or summaries[BENCH_6040].get("nav") is None
            else round(canon_nav - summaries[BENCH_6040]["nav"], 6)),
        rules_frozen_per_policy_version=True,
        weights_never_optimised_on_forward_results=True,
        same_engine_for_every_series=True,
    )
    write_json(campaign_dir(campaign_id) / COMPARISON_ARTIFACT, comparison)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "COMPARISON_ARTIFACT", "LEDGER",
           "STARTING_CAPITAL", "BENCHMARKS", "SERIES_IDS", "rows",
           "latest_nav", "nav_by_policy", "inception_session", "roll",
           "series", "build"]
