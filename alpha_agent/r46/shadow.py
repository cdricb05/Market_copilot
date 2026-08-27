"""alpha_agent.r46.shadow - the P&L layer of ONE tournament step, in order.

Not a second orchestration path. :mod:`alpha_agent.r46.advance` remains the
step the canonical Daily Research Cycle calls; this module is the stage it
runs between "score what matured" and "emit what is next", so that the money
question is answered on the same clock, in the same run, from the same
ledgers, with the ordering section 42 requires:

    1. record the ex-ante regime for the decision session
    2. open / mark / close research trades through the session
       (funding each new trade from the allocation decided BEFORE its entry)
    3. rebuild strategy P&L streams
    4. roll every shadow NAV through the session
    5. assemble the evidence view (outcomes matured on or before the session)
    6. compute the risk state on the streams that exist
    7. DECIDE the next allocation - zero-base, frozen rules, no later evidence
    8. roll the NAV again (a first decision creates the inception rows)
    9. rebuild the NAV, risk, attribution, opportunity and P&L-board read
       models on the decided weights

Emission happens AFTER this stage returns, in ``advance``. Every stage is
wrapped: a failure is reported by name and the others still run.
"""
from __future__ import annotations

import datetime as _dt

from . import CAMPAIGN_ID, artifact_body
from . import allocation as AL
from . import attribution as AT
from . import clock as CK
from . import contract as C
from . import harvest as HV
from . import nav as NV
from . import opportunity as OC
from . import pnl_board as PB
from . import regime as RGM
from . import risk as RK
from . import strategy_pnl as SP
from . import trades as TR
from . import verdicts as VD

CALCULATION_OWNER = "alpha_agent.r46.shadow"

#: Release 46.5 appends three read models AFTER the board: the forward
#: harvest (matured vs mark-to-market), the strategy verdicts (winner /
#: loser separation on matured trades only) and the realised-correlation
#: state (the frozen blend rule, and where the common sample stands).
STAGES = ("regime", "sync_trades", "strategy_pnl", "nav_roll",
          "evidence_view", "risk_state", "allocation", "nav_inception_roll",
          "nav_read_model", "risk_read_model", "attribution", "opportunity",
          "pnl_board", "trade_snapshot", "forward_harvest",
          "strategy_verdicts", "realised_correlation")


def _safe(fn, failures: list, label: str):
    try:
        return fn()
    except Exception as exc:                    # noqa: BLE001 - isolation
        failures.append({"stage": label, "error": type(exc).__name__,
                         "detail": str(exc)[:240]})
        return None


def evidence_view(board: dict) -> dict:
    """Per-strategy evidence from the ONE leaderboard's cells."""
    out: dict = {}
    for r in (board or {}).get("rows") or ():
        if r.get("origin") != "R46_SEED":
            continue
        cid = r["challenger_id"]
        h = int(r.get("horizon") or 0)
        need = C.FORWARD_EVIDENCE_GATES["min_effective_independent"].get(h, 40)
        e = out.setdefault(cid, {"cells": {}, "mean_net_alpha_bps": None})
        e["cells"][str(h)] = {
            "state": r.get("state"),
            "raw_matured": r.get("raw_matured"),
            "effective_independent": r.get("effective_independent"),
            "required_effective_independent": need,
            "net_alpha_bps": r.get("net_alpha_bps"),
            "t_stat": r.get("t_stat"),
        }
    for e in out.values():
        vals = [c["net_alpha_bps"] for c in e["cells"].values()
                if c.get("net_alpha_bps") is not None]
        e["mean_net_alpha_bps"] = (sum(vals) / len(vals)) if vals else None
    return out


def advance_pnl(as_of: _dt.date, registry: dict, board: dict,
                campaign_id: str = CAMPAIGN_ID, series_fn=None,
                now: _dt.datetime = None, risk_free_annual: float = None) -> dict:
    started = now or CK.now_utc()
    failures: list = []
    entries = {c["challenger_id"]: c
               for c in (registry.get("challengers") or ())}

    _safe(lambda: RGM.record(as_of, campaign_id, series_fn), failures, "regime")

    sync = _safe(lambda: TR.sync(
        as_of, campaign_id, registry, series_fn,
        funding_fn=lambda cid, entry, h: AL.funding_for(cid, entry, h,
                                                        campaign_id),
        risk_free_annual=risk_free_annual, synced_at=started),
        failures, "sync_trades") or {}

    spnl = _safe(lambda: SP.build(as_of, campaign_id, registry),
                 failures, "strategy_pnl") or {}
    streams = SP.net_series(SP.unit_streams(campaign_id, as_of)) \
        if spnl else {}
    econ = {s["challenger_id"]: s["economic_state"]
            for s in (spnl.get("strategies") or ())}

    roll1 = _safe(lambda: NV.roll(as_of, campaign_id, series_fn,
                                  risk_free_annual),
                  failures, "nav_roll") or {}

    evidence = _safe(lambda: evidence_view(board), failures,
                     "evidence_view") or {}

    prior_weights = (AL.latest(AL.CANONICAL_POLICY, before=None,
                               campaign_id=campaign_id) or {}).get(
        "weights") or {}
    risk_pre = _safe(lambda: RK.build(as_of, list(entries.values()), streams,
                                      prior_weights, campaign_id, series_fn,
                                      write=False),
                     failures, "risk_state") or {}
    vols = {cid: (v or {}).get("annual_vol") or RK.BOOK_VOL_PRIOR[
        "DEFAULT_BOOK"] for cid, v in (risk_pre.get("volatility") or {}).items()}
    for cid in entries:
        vols.setdefault(cid, RK.BOOK_VOL_PRIOR["DEFAULT_BOOK"])

    alloc = _safe(lambda: AL.decide(
        as_of, entries, evidence, vols, econ, NV.nav_by_policy(campaign_id),
        campaign_id, decided_at=started), failures, "allocation") or {}

    roll2 = _safe(lambda: NV.roll(as_of, campaign_id, series_fn,
                                  risk_free_annual),
                  failures, "nav_inception_roll") or {}

    nav_body = _safe(lambda: NV.build(as_of, campaign_id), failures,
                     "nav_read_model") or {}
    new_weights = alloc.get("canonical_weights") or {}
    risk_body = _safe(lambda: RK.build(as_of, list(entries.values()), streams,
                                       new_weights, campaign_id, series_fn,
                                       write=True),
                      failures, "risk_read_model") or {}
    attr = _safe(lambda: AT.build(as_of, campaign_id), failures,
                 "attribution") or {}
    opp = _safe(lambda: OC.build(as_of, entries, evidence, campaign_id),
                failures, "opportunity") or {}
    pboard = _safe(lambda: PB.build(as_of, campaign_id, board), failures,
                   "pnl_board") or {}
    snap = _safe(lambda: TR.snapshot(as_of, campaign_id), failures,
                 "trade_snapshot") or {}

    # ---- Release 46.5: harvest, verdicts, realised correlation ------------- #
    harvest = _safe(lambda: HV.build(as_of, campaign_id, registry=registry),
                    failures, "forward_harvest") or {}
    verdicts = _safe(lambda: VD.build(as_of, campaign_id, registry, board),
                     failures, "strategy_verdicts") or {}
    corr = _safe(lambda: RK.correlation_state(
        as_of, list(entries.values()), streams, new_weights, campaign_id),
        failures, "realised_correlation") or {}

    counts = (snap.get("counts") or {})
    return artifact_body(
        "r46_4_shadow_advance/1", CALCULATION_OWNER,
        as_of=str(as_of),
        started_utc=CK.iso(started),
        finished_utc=CK.iso(CK.now_utc()),
        stages=list(STAGES),
        stage_failures=failures,
        n_stage_failures=len(failures),
        # ---- the money facts ---------------------------------------------- #
        shadow_nav=nav_body.get("shadow_nav"),
        shadow_return=nav_body.get("shadow_return"),
        today_net_pnl=nav_body.get("today_net_pnl"),
        cumulative_net_forward_pnl=nav_body.get("cumulative_net_forward_pnl"),
        residual_alpha_pnl_vs_cash=nav_body.get(
            "residual_alpha_pnl_vs_cash_control"),
        realised_pnl=nav_body.get("realised_pnl"),
        unrealised_pnl=nav_body.get("unrealised_pnl"),
        cost_drag=nav_body.get("cost_drag"),
        max_drawdown=nav_body.get("max_drawdown"),
        inception=nav_body.get("inception"),
        # ---- trades --------------------------------------------------------- #
        trades_opened=sync.get("n_opened"),
        trades_marked=sync.get("n_marked"),
        trades_closed=sync.get("n_closed"),
        open_research_trades=(counts.get(TR.TRADE_OPEN, 0)
                              + counts.get(TR.TRADE_MARKED, 0)
                              + counts.get(TR.TRADE_MATURED, 0)),
        closed_research_trades=counts.get(TR.TRADE_CLOSED, 0),
        signal_emitted=counts.get(TR.SIGNAL_EMITTED, 0),
        funded_trades=snap.get("n_funded"),
        unfunded_open_trades=snap.get("n_unfunded_open"),
        # ---- allocation and risk ------------------------------------------- #
        canonical_policy=AL.CANONICAL_POLICY,
        n_allocated=len(new_weights),
        top_shadow_allocations=alloc.get("top_allocations"),
        canonical_cash_weight=alloc.get("canonical_cash_weight"),
        effective_independent_pnl_streams=risk_body.get(
            "effective_independent_streams_allocated"),
        nominal_streams=risk_body.get("nominal_streams"),
        correlation_source=risk_body.get("correlation_source"),
        economic_state_counts=spnl.get("economic_state_counts"),
        opportunity_counts=opp.get("counts"),
        best_net_pnl_strategy=pboard.get("best_net_pnl_strategy"),
        worst_net_pnl_strategy=pboard.get("worst_net_pnl_strategy"),
        best_residual_alpha_strategy=pboard.get(
            "best_residual_alpha_strategy"),
        best_capital_efficiency_strategy=pboard.get(
            "best_capital_efficiency_strategy"),
        nav_roll={"first": roll1, "after_decision": roll2},
        # ---- Release 46.5 ---------------------------------------------------- #
        forward_pnl_evidence=harvest.get("FORWARD_PNL_EVIDENCE"),
        n_matured_trades=(harvest.get("matured") or {}).get("n_matured"),
        matured_net_usd=((harvest.get("matured") or {}).get("usd_funded")
                         or {}).get("net"),
        matured_residual_alpha_usd=((harvest.get("matured") or {})
                                    .get("usd_funded") or {}).get(
            "residual_alpha"),
        one_economic_truth=(harvest.get("reconciliation") or {}).get(
            "ONE_ECONOMIC_TRUTH"),
        next_maturity=harvest.get("next_maturity"),
        verdict_counts=verdicts.get("counts"),
        shadow_scale_candidates=verdicts.get("shadow_scale_candidates"),
        shadow_reduce_candidates=verdicts.get("shadow_reduce_candidates"),
        realised_correlation_source=corr.get("source_clusters"),
        realised_correlation_weight=corr.get("realised_weight_clusters"),
        realised_correlation_common_sessions=corr.get(
            "n_common_sessions_clusters"),
        ledgers_intact=bool((snap.get("chain") or {}).get("all_intact")),
        research_only=True, orders_created=0, portfolio_mutations=0,
        model_promotions=0,
    )


__all__ = ["CALCULATION_OWNER", "STAGES", "evidence_view", "advance_pnl"]
