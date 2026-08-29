"""alpha_agent.r46.harvest - matured forward economics, and marks, kept apart.

Release 46.4 built the machine that turns a matured prediction into money:
the judge appends the outcome, the trade ledger takes the judge's row as the
close, the strategy stream, the shadow NAV and the board all read it. This
module is the ONE place that answers, per session, the question Release 46.5
exists to ask - *which pre-registered strategies have actually made or lost
TRUE_FORWARD money, net of costs?* - and it answers it with two numbers that
are never added together:

``MATURED_FORWARD_EVIDENCE``
    closed research trades only. Gross, cost, net, control, residual alpha,
    net at 2x costs, net at stress costs, dollars where the trade was funded.
    Every figure is TAKEN from the judge's outcome row through the trade
    close the P&L owner wrote - nothing is recomputed here.

``MARK_TO_MARKET``
    open research trades at their last point-in-time mark on or before the
    session. Reported so an operator can see what is at risk; explicitly NOT
    statistical evidence and never counted toward a verdict.

It also proves, every session, that the four places a closed trade's money is
recorded agree: the judge's outcome row, the trade close, the strategy stream
and the shadow NAV's realised booking. A disagreement is reported as
``ONE_ECONOMIC_TRUTH = False`` with the offending trade named, never absorbed.

Nothing here matures a prediction. Maturity is the judge's alone, on the
instrument's own realised bar calendar; this module reads what has matured
and refuses to call the rest anything but pending.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Optional

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import allocation as AL
from . import clock as CK
from . import ledger as LG
from . import nav as NV
from . import trades as TR

CALCULATION_OWNER = "alpha_agent.r46.harvest"

ARTIFACT = "R46_5_FORWARD_HARVEST.json"

#: The two evidence classes this module reports. Never summed.
MATURED = "MATURED_FORWARD_EVIDENCE"
MTM = "MARK_TO_MARKET"

#: The frozen forward-P&L evidence vocabulary.
EVIDENCE_STILL_WAITING = "STILL_WAITING_FOR_REALITY"
EVIDENCE_FIRST = "FIRST_MATURED_ECONOMICS"
EVIDENCE_ACCRUING = "MATURED_ECONOMICS_ACCRUING"
EVIDENCE_STATES = (EVIDENCE_STILL_WAITING, EVIDENCE_FIRST, EVIDENCE_ACCRUING)

#: Below this many closed trades the estate holds its FIRST economics and
#: nothing may be called accruing. The number is the contract's early-evidence
#: floor, not a new threshold.
FIRST_ECONOMICS_BELOW = 5

RECONCILIATION_TOLERANCE_USD = 1e-4
RECONCILIATION_TOLERANCE_UNIT = 1e-9


def _fin(v) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _capital(o: dict, policy_id: str) -> float:
    return float(((o.get("capital_by_policy") or {}).get(policy_id) or {})
                 .get("capital_usd") or 0.0)


def matured_trades(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
                   policy_id: str = None) -> list:
    """Every research trade CLOSED on or before ``as_of`` - the judge's numbers."""
    pid = policy_id or AL.CANONICAL_POLICY
    opens = {o["research_trade_id"]: o for o in TR.opens(campaign_id)}
    outs = {str(o.get("prediction_id")): o for o in LG.outcomes(campaign_id)}
    rows = []
    for c in TR.closes(campaign_id):
        if str(c.get("exit_session")) > str(as_of):
            continue
        o = opens.get(c["research_trade_id"]) or {}
        judge = outs.get(str(c.get("prediction_id"))) or {}
        cap = _capital(o, pid)
        share = float(o.get("weight_within_strategy") or 0.0)
        net = _fin(c.get("net_return")) or 0.0
        gross = _fin(c.get("gross_return")) or 0.0
        cost = _fin(c.get("cost_return")) or 0.0
        ctl = _fin(c.get("control_return"))
        resid = _fin(c.get("residual_alpha_vs_control"))
        net2 = _fin(c.get("net_return_at_2x"))
        nets = _fin(c.get("net_return_at_stress"))
        rows.append({
            "research_trade_id": c["research_trade_id"],
            "prediction_id": c.get("prediction_id"),
            "challenger_id": c.get("challenger_id"),
            "horizon": o.get("horizon"),
            "asset_class": o.get("asset_class"),
            "economic_family": o.get("economic_family"),
            "information_family": o.get("information_family"),
            "dependence_cluster": o.get("dependence_cluster"),
            "entry_session": o.get("entry_session"),
            "exit_session": c.get("exit_session"),
            "sessions_held": c.get("sessions_held"),
            "evidence_class": MATURED,
            # ---- unit economics, per 1.0 of the trade's capital ------------- #
            "gross_return": gross,
            "transaction_cost": cost,
            "net_return": net,
            "control_return": ctl,
            "residual_alpha": resid,
            "net_return_at_2x_costs": net2,
            "net_return_at_stress_costs": nets,
            "residual_alpha_at_2x_costs": _fin(c.get("residual_alpha_at_2x")),
            "hit": bool(c.get("hit")),
            # ---- the strategy's share and the shadow dollars ---------------- #
            "weight_within_strategy": share,
            "funded": bool(o.get("funded")),
            "capital_usd": cap,
            "gross_pnl_usd": (cap * gross if cap > 0 else None),
            "cost_pnl_usd": (-cap * cost if cap > 0 else None),
            "net_pnl_usd": (cap * net if cap > 0 else None),
            "residual_alpha_pnl_usd": (cap * resid if cap > 0 and resid is not None
                                       else None),
            "net_pnl_usd_at_2x_costs": (cap * net2 if cap > 0 and net2 is not None
                                        else None),
            "net_pnl_usd_at_stress_costs": (cap * nets if cap > 0
                                            and nets is not None else None),
            # ---- one truth: the judge's row IS the close ------------------- #
            "judge_net_return": _fin(judge.get("realised_net_return")),
            "judge_matches_close": (judge.get("realised_net_return") is not None
                                    and abs(float(judge["realised_net_return"])
                                            - net) <= RECONCILIATION_TOLERANCE_UNIT),
            "pnl_owner_reconciliation": ((c.get("reconciliation") or {})
                                         .get("state")),
            "scored_at_utc": c.get("scored_at_utc"),
            "closed_at_utc": c.get("closed_at_utc"),
        })
    rows.sort(key=lambda r: (str(r["exit_session"]), str(r["challenger_id"])))
    return rows


def open_marks(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
               policy_id: str = None) -> list:
    """Every OPEN research trade at its last mark on or before ``as_of``."""
    pid = policy_id or AL.CANONICAL_POLICY
    closed = {c["research_trade_id"] for c in TR.closes(campaign_id)
              if str(c.get("exit_session")) <= str(as_of)}
    marks: dict = {}
    for m in TR.marks(campaign_id):
        if str(m.get("session")) <= str(as_of):
            marks.setdefault(m["research_trade_id"], []).append(m)
    for v in marks.values():
        v.sort(key=lambda m: m["session"])
    rows = []
    for o in TR.opens(campaign_id):
        tid = o["research_trade_id"]
        if tid in closed or str(o.get("entry_session")) > str(as_of):
            continue
        mm = marks.get(tid) or []
        last = mm[-1] if mm else None
        cost = _fin(o.get("cost_return")) or 0.0
        cap = _capital(o, pid)
        if last is None:
            gross, net, resid = 0.0, -cost, None
            peak_net, dd = -cost, 0.0
            sessions_held = 0
        else:
            gross = _fin(last.get("gross_return")) or 0.0
            net = _fin(last.get("net_return")) or 0.0
            resid = _fin(last.get("residual_alpha_vs_control"))
            path = [_fin(m.get("net_return")) or 0.0 for m in mm]
            peak_net = max([-cost] + path)
            dd = min(0.0, net - peak_net)
            sessions_held = last.get("sessions_held")
        rows.append({
            "research_trade_id": tid,
            "prediction_id": o.get("prediction_id"),
            "challenger_id": o.get("challenger_id"),
            "horizon": o.get("horizon"),
            "asset_class": o.get("asset_class"),
            "information_family": o.get("information_family"),
            "entry_session": o.get("entry_session"),
            "exit_session_expected": o.get("exit_session_expected"),
            "last_mark_session": (last or {}).get("session"),
            "sessions_held": sessions_held,
            "n_marks": len(mm),
            "evidence_class": MTM,
            "unrealised_gross_return": gross,
            "cost_drag": cost,
            "unrealised_net_return": net,
            "unrealised_residual_alpha": resid,
            "unrealised_net_return_at_2x_costs": gross - 2.0 * cost,
            "current_drawdown_from_peak_net": dd,
            "funded": bool(o.get("funded")),
            "capital_usd": cap,
            "unrealised_gross_pnl_usd": (cap * gross if cap > 0 else None),
            "unrealised_net_pnl_usd": (cap * net if cap > 0 else None),
            "unrealised_residual_alpha_pnl_usd": (
                cap * resid if cap > 0 and resid is not None else None),
            "cost_drag_usd": (cap * cost if cap > 0 else None),
            "is_matured_evidence": False,
        })
    rows.sort(key=lambda r: (str(r["entry_session"]), str(r["challenger_id"])))
    return rows


def _sum(rows: list, key: str) -> float:
    return float(sum(float(r[key]) for r in rows if r.get(key) is not None))


def _unit_mtm(marks: list, campaign_id: str) -> dict:
    """Share-weighted unrealised unit economics of the open book."""
    unit = {"gross": 0.0, "net": 0.0, "residual_alpha": 0.0, "cost_drag": 0.0}
    opens_by_id = {o["research_trade_id"]: o for o in TR.opens(campaign_id)}
    for r in marks:
        share = float((opens_by_id.get(r["research_trade_id"]) or {})
                      .get("weight_within_strategy") or 0.0)
        unit["gross"] += share * r["unrealised_gross_return"]
        unit["net"] += share * r["unrealised_net_return"]
        unit["cost_drag"] += share * r["cost_drag"]
        if r["unrealised_residual_alpha"] is not None:
            unit["residual_alpha"] += share * r["unrealised_residual_alpha"]
    return unit


def _share_sum(rows: list, key: str) -> float:
    return float(sum(float(r["weight_within_strategy"]) * float(r[key])
                     for r in rows if r.get(key) is not None))


def reconcile(as_of: _dt.date, matured: list, campaign_id: str = CAMPAIGN_ID,
              policy_id: str = None, registry: dict = None) -> dict:
    """Judge == close == strategy stream == NAV realised booking, or say why not.

    ``registry`` is threaded rather than re-loaded: the stream owner needs the
    field to summarise, and a root whose registry is not on disk would
    otherwise report every strategy as missing from its own stream - a
    reconciliation break invented by the checker rather than found by it.
    """
    pid = policy_id or AL.CANONICAL_POLICY
    problems = []
    for r in matured:
        if not r["judge_matches_close"]:
            problems.append({"trade": r["research_trade_id"],
                             "where": "judge_vs_close",
                             "judge": r["judge_net_return"],
                             "close": r["net_return"]})
        if r["pnl_owner_reconciliation"] not in (None, "RECONCILED",
                                                  "NOT_RECOMPUTABLE"):
            problems.append({"trade": r["research_trade_id"],
                             "where": "pnl_owner",
                             "state": r["pnl_owner_reconciliation"]})
    # Strategy stream: realised per strategy is share x judge net. Prefer the
    # number the stream owner PUBLISHED for this session - that is the figure
    # every other surface reads - and fall back to rebuilding it only when no
    # published summary for this session exists.
    from . import strategy_pnl as SP
    published = read_json(campaign_dir(campaign_id) / SP.ARTIFACT,
                          default=None) or {}
    if str(published.get("as_of")) == str(as_of) and published.get("strategies"):
        spnl, source = published, "published_artifact"
    else:
        spnl, source = SP.build(as_of, campaign_id, registry,
                                write=False), "rebuilt"
    by_cid: dict = {}
    for r in matured:
        by_cid[r["challenger_id"]] = by_cid.get(r["challenger_id"], 0.0) \
            + r["weight_within_strategy"] * r["net_return"]
    stream_by_cid = {s["challenger_id"]: float(s.get("realised_net_return")
                                               or 0.0)
                     for s in (spnl.get("strategies") or ())}
    if by_cid and not stream_by_cid:
        # The stream could not be summarised at all (no published artifact for
        # this session and no registry to rebuild from). That is ONE fact
        # about the checker's inputs, not one break per strategy.
        problems.append({"where": "strategy_stream",
                         "reason": "STREAM_NOT_SUMMARISABLE",
                         "detail": "no strategy-P&L artifact for this session "
                                   "and no registry supplied to rebuild one"})
    else:
        for cid, v in by_cid.items():
            if cid not in stream_by_cid:
                problems.append({"challenger_id": cid,
                                 "where": "strategy_stream",
                                 "reason": "CLOSED_TRADE_HAS_NO_STREAM",
                                 "closes": v})
            elif abs(stream_by_cid[cid] - v) > 1e-9:
                problems.append({"challenger_id": cid,
                                 "where": "strategy_stream",
                                 "stream": stream_by_cid[cid], "closes": v})
    # NAV: realised booked through as_of equals capital x judge net, summed.
    nav_rows = [r for r in NV.series(pid, campaign_id)
                if str(r.get("session")) <= str(as_of)]
    nav_realised = float(sum(float(r.get("realised_pnl_booked_today") or 0.0)
                             for r in nav_rows))
    close_realised = float(sum(r["net_pnl_usd"] for r in matured
                               if r["net_pnl_usd"] is not None))
    if abs(nav_realised - close_realised) > RECONCILIATION_TOLERANCE_USD:
        problems.append({"where": "nav_realised_booking",
                         "nav": nav_realised, "closes": close_realised})
    return {"ONE_ECONOMIC_TRUTH": not problems, "problems": problems,
            "n_matured_checked": len(matured),
            "strategy_stream_source": source,
            "nav_realised_usd": nav_realised,
            "closed_trade_realised_usd": close_realised,
            "checked": ["judge_outcome_row == trade_close",
                        "pnl_owner_reconciliation",
                        "strategy_stream_realised == share x judge_net",
                        "nav_realised_booked == capital x judge_net"]}


def next_maturity(campaign_id: str = CAMPAIGN_ID) -> Optional[str]:
    """THE next expected maturity. One owner; every reader composes this."""
    scored = {str(o.get("prediction_id")) for o in LG.outcomes(campaign_id)}
    dates = sorted(str(p.get("horizon_end_expected"))
                   for p in LG.predictions(campaign_id)
                   if str(p.get("prediction_id")) not in scored
                   and p.get("horizon_end_expected"))
    return dates[0] if dates else None


#: Release 46.6.2 - why a bare date is not enough.
#:
#: After the 2026-08-28 cycle scored a 2026-08-28 maturity, the board still
#: advertised ``next_maturity = 2026-08-28``, which reads like a stuck clock.
#: It was not. Exactly ONE prediction still expected that date -
#: ``r46_3_vx_term_carry_1d`` on ``&VX``, entry 2026-08-27, horizon 1 - and at
#: the instant the judge ran, that instrument's own 2026-08-28 bar had not
#: printed, so :func:`alpha_agent.r46.judge.resolve` correctly answered
#: NOT_MATURED. The bar landed later the same evening and the row became
#: scoreable, i.e. it was waiting for DATA, not stuck.
#:
#: An operator cannot tell those two apart from a date. So the owner now
#: returns the date WITH the reason it is still outstanding, and every read
#: model composes this instead of recomputing the minimum for itself.
MATURITY_ESTIMATE_NOTE = (
    "horizon_end_expected is a CALENDAR estimate: "
    "alpha_agent.r46.clock.expected_maturity_date counts weekdays after the "
    "entry session. Scoring never uses it - the judge counts the instrument's "
    "OWN realised sessions. A prediction whose estimate has arrived is "
    "therefore not necessarily scoreable, and a next maturity that does not "
    "move is evidence about DATA, not about a stalled tournament.")


def next_maturity_detail(campaign_id: str = CAMPAIGN_ID, *,
                         resolve: bool = False) -> dict:
    """The next expected maturity AND why it is still outstanding.

    Pure over the ledgers by default - a read model must not call a provider
    on every request. ``resolve=True`` adds the judge's per-row verdict for
    the rows at that date, which is what an audit wants and a dashboard does
    not.
    """
    scored = {str(o.get("prediction_id")) for o in LG.outcomes(campaign_id)}
    pending = [p for p in LG.predictions(campaign_id)
               if str(p.get("prediction_id")) not in scored
               and p.get("horizon_end_expected")]
    if not pending:
        return {"next_maturity": None, "n_pending": 0, "n_at_next": 0,
                "rows": [], "why": "nothing is outstanding",
                "estimate_note": MATURITY_ESTIMATE_NOTE,
                "calculation_owner": CALCULATION_OWNER}
    nxt = min(str(p["horizon_end_expected"]) for p in pending)
    at = [p for p in pending if str(p["horizon_end_expected"]) == nxt]
    rows = []
    for p in at[:40]:
        legs = ((p.get("position_expression") or {}).get("legs")) or []
        row = {
            "prediction_id": p.get("prediction_id"),
            "challenger_id": p.get("challenger_id"),
            "horizon": p.get("horizon"),
            "entry_session_date": p.get("effective_as_of"),
            "instruments": sorted({str(l.get("instrument")) for l in legs})[:6],
            "emitted_at_utc": p.get("emitted_at_utc"),
        }
        if resolve:
            try:
                from . import judge as JD
                r = JD.resolve(p)
                row["judge_state"] = r.get("state")
                row["judge_reason"] = r.get("reason")
                row["judge_missing_legs"] = r.get("missing")
            except Exception as exc:            # noqa: BLE001 - fail soft
                row["judge_state"] = "UNRESOLVED"
                row["judge_reason"] = "%s: %s" % (type(exc).__name__,
                                                  str(exc)[:160])
        rows.append(row)
    return {
        "next_maturity": nxt,
        "n_pending": len(pending),
        "n_at_next": len(at),
        "rows": rows,
        "why": ("%d prediction(s) still expect %s. A prediction is scored on "
                "its own instrument's realised sessions, so it stays "
                "outstanding until that instrument has printed the bar - "
                "which is why this date can stand after a cycle has already "
                "scored a maturity on it." % (len(at), nxt)),
        "estimate_note": MATURITY_ESTIMATE_NOTE,
        "resolved_against_market_data": bool(resolve),
        "calculation_owner": CALCULATION_OWNER,
    }


def evidence_state(n_closed: int) -> str:
    if n_closed <= 0:
        return EVIDENCE_STILL_WAITING
    if n_closed < FIRST_ECONOMICS_BELOW:
        return EVIDENCE_FIRST
    return EVIDENCE_ACCRUING


def build(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
          policy_id: str = None, registry: dict = None,
          write: bool = True) -> dict:
    pid = policy_id or AL.CANONICAL_POLICY
    matured = matured_trades(as_of, campaign_id, pid)
    marks = open_marks(as_of, campaign_id, pid)
    recon = reconcile(as_of, matured, campaign_id, pid, registry)
    funded_m = [r for r in matured if r["capital_usd"] > 0]
    funded_o = [r for r in marks if r["capital_usd"] > 0]
    by_strategy: dict = {}
    for r in matured:
        e = by_strategy.setdefault(r["challenger_id"], {
            "challenger_id": r["challenger_id"], "n_matured": 0, "n_hits": 0,
            "unit_net": 0.0, "unit_residual_alpha": 0.0, "unit_cost": 0.0,
            "unit_net_at_2x": 0.0, "usd_net": 0.0, "usd_residual_alpha": 0.0})
        e["n_matured"] += 1
        e["n_hits"] += 1 if r["hit"] else 0
        e["unit_net"] += r["weight_within_strategy"] * r["net_return"]
        e["unit_cost"] += r["weight_within_strategy"] * r["transaction_cost"]
        if r["residual_alpha"] is not None:
            e["unit_residual_alpha"] += (r["weight_within_strategy"]
                                         * r["residual_alpha"])
        if r["net_return_at_2x_costs"] is not None:
            e["unit_net_at_2x"] += (r["weight_within_strategy"]
                                    * r["net_return_at_2x_costs"])
        e["usd_net"] += r["net_pnl_usd"] or 0.0
        e["usd_residual_alpha"] += r["residual_alpha_pnl_usd"] or 0.0
    body = artifact_body(
        "r46_5_forward_harvest/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        policy_id=pid,
        FORWARD_PNL_EVIDENCE=evidence_state(len(matured)),
        evidence_vocabulary=list(EVIDENCE_STATES),
        first_economics_below_n_closed=FIRST_ECONOMICS_BELOW,
        # ---- MATURED: the only numbers that may ever decide anything ------ #
        matured={
            "evidence_class": MATURED,
            "n_matured": len(matured),
            "n_funded": len(funded_m),
            "n_unfunded_unit_economics": len(matured) - len(funded_m),
            "hit_rate": ((sum(1 for r in matured if r["hit"]) / len(matured))
                         if matured else None),
            "unit_share_weighted": {
                "gross": _share_sum(matured, "gross_return"),
                "transaction_cost": _share_sum(matured, "transaction_cost"),
                "net": _share_sum(matured, "net_return"),
                "control": _share_sum(matured, "control_return"),
                "residual_alpha": _share_sum(matured, "residual_alpha"),
                "net_at_2x_costs": _share_sum(matured, "net_return_at_2x_costs"),
                "net_at_stress_costs": _share_sum(matured,
                                                  "net_return_at_stress_costs"),
            },
            "usd_funded": {
                "gross": _sum(funded_m, "gross_pnl_usd"),
                "transaction_cost": _sum(funded_m, "cost_pnl_usd"),
                "net": _sum(funded_m, "net_pnl_usd"),
                "residual_alpha": _sum(funded_m, "residual_alpha_pnl_usd"),
                "net_at_2x_costs": _sum(funded_m, "net_pnl_usd_at_2x_costs"),
                "net_at_stress_costs": _sum(funded_m,
                                            "net_pnl_usd_at_stress_costs"),
            },
            "by_strategy": sorted(by_strategy.values(),
                                  key=lambda e: -e["unit_residual_alpha"]),
            "trades": matured,
        },
        # ---- MARK_TO_MARKET: at risk, not evidence ------------------------- #
        mark_to_market={
            "evidence_class": MTM,
            "is_matured_statistical_evidence": False,
            "n_open": len(marks),
            "n_funded": len(funded_o),
            "n_marked_at_least_once": sum(1 for r in marks if r["n_marks"]),
            "unit_share_weighted": _unit_mtm(marks, campaign_id),
            "usd_funded": {
                "gross": _sum(funded_o, "unrealised_gross_pnl_usd"),
                "net": _sum(funded_o, "unrealised_net_pnl_usd"),
                "residual_alpha": _sum(funded_o,
                                       "unrealised_residual_alpha_pnl_usd"),
                "cost_drag": _sum(funded_o, "cost_drag_usd"),
            },
            "worst_current_drawdown_from_peak_net": (
                min((r["current_drawdown_from_peak_net"] for r in marks),
                    default=None)),
            "trades": marks,
        },
        matured_and_mark_to_market_are_never_summed=True,
        reconciliation=recon,
        next_maturity=next_maturity(campaign_id),
        nothing_here_matures_a_prediction=True,
        no_backdating=True,
        research_only=True, orders_created=0, portfolio_mutations=0,
    )
    if write:
        write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "MATURED", "MTM",
           "EVIDENCE_STATES", "EVIDENCE_STILL_WAITING", "EVIDENCE_FIRST",
           "EVIDENCE_ACCRUING", "FIRST_ECONOMICS_BELOW", "matured_trades",
           "open_marks", "reconcile", "next_maturity", "evidence_state",
           "build"]
