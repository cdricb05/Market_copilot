"""alpha_agent.r46.attribution - what made money, what lost it, what cost it.

Canonical forward P&L attribution for the CANONICAL shadow policy, in dollars,
by challenger, asset class, economic family, information family, horizon,
decision date and ex-ante regime. Every dollar in the shadow NAV's P&L is a
funded research trade's dollar, and every funded trade carries the descriptors
it was opened with, so attribution is a sum, never a model.

Trades the shadow portfolio never funded (those that entered before the first
allocation existed) are attributed separately at UNIT economics and labelled
``UNFUNDED_UNIT_ECONOMICS``; they are never added to the dollar totals.

Contributions reported per group: gross P&L, cost, net P&L, control P&L,
residual alpha P&L, turnover, risk contribution (from the one risk state),
and contribution to the current drawdown (the group's dollar P&L since the
portfolio's high-water-mark session).
"""
from __future__ import annotations

import datetime as _dt

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import allocation as AL
from . import clock as CK
from . import nav as NV
from . import regime as RGM
from . import risk as RK
from . import trades as TR

CALCULATION_OWNER = "alpha_agent.r46.attribution"

ARTIFACT = "R46_4_PNL_ATTRIBUTION.json"
REGIME_ARTIFACT = "R46_4_REGIME_PNL.json"

GROUPINGS = ("challenger_id", "asset_class", "economic_family",
             "information_family", "horizon", "decision_date",
             "regime_risk_appetite", "regime_volatility", "regime_equity_trend",
             "regime_credit_stress")

UNFUNDED = "UNFUNDED_UNIT_ECONOMICS"


def _trade_rows(as_of: _dt.date, campaign_id: str, policy_id: str) -> list:
    marks: dict = {}
    for m in TR.marks(campaign_id):
        if m["session"] <= str(as_of):
            marks.setdefault(m["research_trade_id"], []).append(m)
    for v in marks.values():
        v.sort(key=lambda m: m["session"])
    closes = {c["research_trade_id"]: c for c in TR.closes(campaign_id)
              if str(c.get("exit_session")) <= str(as_of)}
    hwm_session = _hwm_session(as_of, campaign_id, policy_id)
    out = []
    for o in TR.opens(campaign_id):
        if str(o["entry_session"]) > str(as_of):
            continue
        tid = o["research_trade_id"]
        cap = float(((o.get("capital_by_policy") or {}).get(policy_id) or {})
                    .get("capital_usd") or 0.0)
        c = closes.get(tid)
        mm = marks.get(tid, [])
        last = c if c is not None else (mm[-1] if mm else None)
        gross_u = float((last or {}).get("gross_return") or 0.0)
        cost_u = float(o.get("cost_return") or 0.0)
        net_u = gross_u - cost_u
        ctl_u = float((last or {}).get("control_return") or 0.0)
        resid_u = net_u - ctl_u
        unit = cap <= 0
        scale = 1.0 if unit else cap
        # Contribution to the current drawdown: P&L since the HWM session.
        g_at_hwm = 0.0
        if hwm_session and str(o["entry_session"]) <= hwm_session:
            for m in mm:
                if m["session"] <= hwm_session:
                    g_at_hwm = float(m.get("gross_return") or 0.0)
            since = gross_u - g_at_hwm
        else:
            since = net_u
        reg = RGM.regime_for(str(o["entry_session"]), campaign_id)
        out.append({
            "research_trade_id": tid,
            "challenger_id": o["challenger_id"],
            "asset_class": o.get("asset_class"),
            "economic_family": o.get("economic_family"),
            "information_family": o.get("information_family"),
            "dependence_cluster": o.get("dependence_cluster"),
            "horizon": o.get("horizon"),
            "decision_date": str(o["entry_session"]),
            "regime_risk_appetite": reg.get("risk_appetite") or "UNKNOWN",
            "regime_volatility": reg.get("volatility_regime") or "UNKNOWN",
            "regime_equity_trend": reg.get("equity_trend") or "UNKNOWN",
            "regime_credit_stress": reg.get("credit_stress") or "UNKNOWN",
            "funded": not unit,
            "capital_usd": cap,
            "state": "CLOSED" if c is not None else "OPEN",
            "gross_pnl": scale * gross_u,
            "cost_pnl": -scale * cost_u,
            "net_pnl": scale * net_u,
            "control_pnl": scale * ctl_u,
            "residual_alpha_pnl": scale * resid_u,
            "turnover": scale * 2.0 * float(
                o.get("gross_exposure_per_unit_capital") or 0.0),
            "drawdown_contribution": scale * since,
            "realised": c is not None,
        })
    return out


def _hwm_session(as_of: _dt.date, campaign_id: str, policy_id: str):
    srows = NV.series(policy_id, campaign_id)
    srows = [r for r in srows if r["session"] <= str(as_of)]
    if not srows:
        return None
    best, best_s = None, None
    for r in srows:
        if best is None or float(r["ending_nav"]) >= best:
            best, best_s = float(r["ending_nav"]), r["session"]
    return best_s


def _group(rows: list, key: str, risk_contrib: dict) -> list:
    groups: dict = {}
    for r in rows:
        k = str(r.get(key))
        g = groups.setdefault(k, {"key": k, "n_trades": 0, "n_open": 0,
                                  "n_closed": 0, "gross_pnl": 0.0,
                                  "cost_pnl": 0.0, "net_pnl": 0.0,
                                  "control_pnl": 0.0,
                                  "residual_alpha_pnl": 0.0, "turnover": 0.0,
                                  "drawdown_contribution": 0.0,
                                  "capital_usd": 0.0, "challengers": set()})
        g["n_trades"] += 1
        g["n_open" if r["state"] == "OPEN" else "n_closed"] += 1
        for f in ("gross_pnl", "cost_pnl", "net_pnl", "control_pnl",
                  "residual_alpha_pnl", "turnover", "drawdown_contribution",
                  "capital_usd"):
            g[f] += float(r.get(f) or 0.0)
        g["challengers"].add(r["challenger_id"])
    out = []
    for g in groups.values():
        rc = None
        if key == "challenger_id":
            rc = risk_contrib.get(g["key"])
        else:
            vals = [risk_contrib.get(c) for c in g["challengers"]
                    if risk_contrib.get(c) is not None]
            rc = float(sum(vals)) if vals else None
        g["risk_contribution"] = rc
        g["challengers"] = sorted(g["challengers"])
        for f in ("gross_pnl", "cost_pnl", "net_pnl", "control_pnl",
                  "residual_alpha_pnl", "turnover", "drawdown_contribution",
                  "capital_usd"):
            g[f] = round(g[f], 6)
        out.append(g)
    out.sort(key=lambda g: -g["net_pnl"])
    return out


def build(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
          policy_id: str = None) -> dict:
    pid = policy_id or AL.CANONICAL_POLICY
    rows = _trade_rows(as_of, campaign_id, pid)
    funded = [r for r in rows if r["funded"]]
    unfunded = [r for r in rows if not r["funded"]]
    risk_state = read_json(campaign_dir(campaign_id) / RK.ARTIFACT,
                           default=None) or {}
    rc = risk_state.get("risk_contribution") or {}

    by = {key: _group(funded, key, rc) for key in GROUPINGS}
    by_unit = {key: _group(unfunded, key, {}) for key in
               ("challenger_id", "asset_class", "economic_family",
                "information_family", "horizon")}
    tot = {f: round(sum(float(r.get(f) or 0.0) for r in funded), 6)
           for f in ("gross_pnl", "cost_pnl", "net_pnl", "control_pnl",
                     "residual_alpha_pnl", "turnover",
                     "drawdown_contribution")}
    contrib = by["challenger_id"]
    body = artifact_body(
        "r46_4_pnl_attribution/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        policy_id=pid,
        n_funded_trades=len(funded),
        n_unfunded_trades=len(unfunded),
        totals_usd=tot,
        what_made_money=[c for c in contrib if c["net_pnl"] > 0][:10],
        what_lost_money=[c for c in reversed(contrib) if c["net_pnl"] < 0][:10],
        what_cost_money=sorted(contrib, key=lambda g: g["cost_pnl"])[:10],
        top_contributors=contrib[:5],
        worst_detractors=list(reversed(contrib))[:5],
        by=by,
        unfunded_unit_economics={"label": UNFUNDED,
                                 "note": "trades that entered before the "
                                         "first allocation existed; unit "
                                         "returns, never added to dollars",
                                 "by": by_unit},
        groupings=list(GROUPINGS),
        attribution_is_a_sum_not_a_model=True,
        evidence_class="TRUE_FORWARD",
        trades=funded,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)

    regime_body = artifact_body(
        "r46_4_regime_pnl/1", CALCULATION_OWNER,
        as_of=str(as_of),
        policy_id=pid,
        ex_ante_regimes_only=True,
        by_risk_appetite=by["regime_risk_appetite"],
        by_volatility=by["regime_volatility"],
        by_equity_trend=by["regime_equity_trend"],
        by_credit_stress=by["regime_credit_stress"],
        n_funded_trades=len(funded),
        note="P&L grouped by the regime recorded ON the decision session; "
             "labels are never revised with later information",
    )
    write_json(campaign_dir(campaign_id) / REGIME_ARTIFACT, regime_body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "REGIME_ARTIFACT", "GROUPINGS",
           "build"]
