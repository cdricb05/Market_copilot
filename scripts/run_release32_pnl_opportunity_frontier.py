"""Run the Release 32 PnL Opportunity Frontier campaign, end to end.

RESEARCH ONLY and PRODUCTION READ-ONLY. This script reads owned data and writes
immutable artifacts under the Release-32 research root. It does not run Daily
Close, create a proposal or decision, modify holdings or cash, promote a model,
activate a sleeve, enable automation, restart production, or spend money.

    python scripts\\run_release32_pnl_opportunity_frontier.py --all

Stages can be run individually while iterating; ``--all`` runs them in the only
order that is valid, because a contract frozen after its results is not a
contract.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO.parent) not in sys.path:
    sys.path.insert(0, str(REPO.parent))

from paper_trader.alpha_agent import r32                       # noqa: E402
from paper_trader.alpha_agent.r32 import campaign as _campaign  # noqa: E402
from paper_trader.alpha_agent.r32 import contract as _contract  # noqa: E402
from paper_trader.alpha_agent.r32 import frontier as _frontier  # noqa: E402
from paper_trader.alpha_agent.r32 import funnel as _funnel      # noqa: E402
from paper_trader.alpha_agent.r32 import governance as _gov     # noqa: E402
from paper_trader.alpha_agent.r32 import information_state as _istate  # noqa: E402
from paper_trader.alpha_agent.r32 import judge as _judge        # noqa: E402
from paper_trader.alpha_agent.r32 import panels as _panels      # noqa: E402
from paper_trader.alpha_agent.r32 import purchase_gate as _gate  # noqa: E402
from paper_trader.alpha_agent.r32 import sleeve as _sleeve      # noqa: E402
from paper_trader.alpha_agent.r32 import sources as _sources    # noqa: E402
from paper_trader.alpha_agent.r32.sleeves import equity_selection  # noqa: E402
from paper_trader.alpha_agent.r32.sleeves import event_driven      # noqa: E402


def log(msg: str) -> None:
    print(f"[{_dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# --------------------------------------------------------------------------- #
def stage_contracts(campaign_id: str) -> dict:
    """Freeze every contract BEFORE a single result is observed."""
    now = _dt.datetime.now().isoformat(timespec="seconds")
    c = _contract.build(campaign_id=campaign_id, created_at=now, repo=REPO)
    _contract.freeze(c)
    log(f"contract frozen  hash={c['contract_hash'][:12]}")
    for label, body, freeze in (
        ("information_state", _istate.build_contract(campaign_id=campaign_id),
         _istate.freeze),
        ("sleeve_contract", _sleeve.build_contract(campaign_id=campaign_id),
         _sleeve.freeze),
        ("judge", _judge.build_contract(campaign_id=campaign_id), _judge.freeze),
        ("governance", _gov.build_contract(campaign_id=campaign_id),
         _gov.freeze),
    ):
        freeze(body)
        log(f"{label} frozen")
    return c


def stage_sources(campaign_id: str, *, full: bool) -> dict:
    """Measure and classify every owned source. This is Phase 1."""
    nd = _sources._norgate()
    measurements = {}
    dbs = {"norgate_economic": "Economic"}
    if full:
        dbs.update({"norgate_us_indices": "US Indices",
                    "norgate_forex_spot": "Forex Spot",
                    "norgate_cash_commodities": "Cash Commodities",
                    "norgate_world_indices": "World Indices",
                    "norgate_continuous_futures": "Continuous Futures"})
    for source_id, db in dbs.items():
        log(f"measuring {db} ...")
        m = _sources.measure_database(db, nd=nd)
        m.pop("rows", None)          # the manifest keeps counts, not every row
        measurements[source_id] = m
        by = m.get("by_admissibility", {})
        log(f"  {db}: {m.get('symbols')} symbols -> {by}")
    body = _sources.build(campaign_id=campaign_id, measurements=measurements)
    _sources.freeze(body)
    log(f"data_source_registry frozen  hash={body['registry_hash'][:12]}")
    return body


def stage_research(campaign_id: str, source_registry: dict) -> dict:
    """Panels, screening, qualification, lockbox, multiple testing, verdict."""
    log("loading panels ...")
    ctx = _campaign.Context(campaign_id=campaign_id).load()
    manifest = _panels.build_manifest(campaign_id=campaign_id, panels=ctx.panels)
    _panels.freeze(manifest)
    for name, row in manifest["panels"].items():
        log(f"  panel {name}: {row}" if not row.get("ok")
            else f"  panel {name}: n={row['n_dates']} "
                 f"{row['first']}..{row['last']}")

    fun = _funnel.Funnel(campaign_id=campaign_id,
                         judge_behaviour_hash=_judge.behaviour_hash())
    sleeve_results = {}
    for sleeve in _contract.NEW_SLEEVES:
        log(f"sleeve {sleeve} ...")
        res = _campaign.run_sleeve(sleeve, ctx=ctx, fun=fun)
        sleeve_results[sleeve] = res
        log(f"  screened={res.get('screened')} "
            f"qualified={res.get('qualified')} "
            f"families={res.get('surviving_families')} "
            f"state={res.get('state')}")

    log("opening lockbox (once per finalist) ...")
    lockbox = _campaign.run_lockbox(sleeve_results, ctx=ctx, fun=fun)
    for sleeve, rows in lockbox.items():
        for r in rows:
            # The primary statistic is excess over the VOLATILITY-MATCHED
            # control. Logging excess over cash here would advertise the very
            # number that made campaign v1 wrong.
            t = (r.get("vs_volatility_matched_control") or {}).get("t_stat")
            tc = (r.get("vs_cash") or {}).get("t_stat")
            log(f"  {sleeve:<24} {str(r.get('label'))[:48]:<48} "
                f"n={r.get('n')} t_vs_matched={_fmt(t)} (t_vs_cash={_fmt(tc)})")

    mt = _campaign.run_multiple_testing(fun, lockbox)
    log(f"multiple testing: denominator={mt['denominator']} "
        f"tested={mt.get('tested')} survivors={mt.get('n_survivors')}")

    overlap = _campaign.common_overlap(sleeve_results, lockbox)
    inherited = equity_selection.load_inherited()
    log(f"inherited control: {inherited.get('verdict')} "
        f"(rerun={inherited.get('rerun_in_r32')})")

    verdict = _campaign.build_verdict(
        campaign_id=campaign_id, sleeve_results=sleeve_results,
        lockbox=lockbox, mt=mt, overlap=overlap, inherited=inherited,
        fun=fun, source_registry=source_registry, panel_manifest=manifest)
    _campaign.freeze(verdict)
    fun.freeze()
    r32.write_json(
        r32.campaign_dir(campaign_id) / _campaign.SLEEVE_RESULTS_ARTIFACT,
        r32.artifact_body(_campaign.SLEEVE_RESULTS_SCHEMA,
                          {"campaign_id": campaign_id,
                           "sleeves": {k: _strip(v)
                                       for k, v in sleeve_results.items()},
                           "lockbox": {k: [_strip(r) for r in v]
                                       for k, v in lockbox.items()}}))

    # Cross-sleeve correlation needs paths on IDENTICAL dates. Each panel has
    # its own decision calendar, so the per-sleeve lockbox paths share almost no
    # dates and a correlation computed from them is empty, not zero.
    shared = _campaign.common_overlap_paths(lockbox, ctx=ctx)
    paths = shared["paths"]
    log(f"common calendar: {len(ctx.common_calendar)} shared sessions; "
        f"{len(paths)} sleeves re-scored on it (reporting only)")
    for s, w in shared["windows"].items():
        log(f"  {s:<24} n={w.get('n')} {w.get('first','')}..{w.get('last','')} "
            f"t_vs_matched={_fmt(w.get('t_vs_volatility_matched_control'))}")
    gaps = information_gaps(sleeve_results, ctx)
    gate = _gate.build(campaign_id=campaign_id, gaps=gaps)
    _gate.freeze(gate)
    front = _frontier.build(campaign_id=campaign_id, verdict=verdict,
                            sleeve_paths=paths,
                            overlap=dict(overlap, shared_calendar=shared["windows"],
                                         shared_sessions=len(ctx.common_calendar),
                                         reporting_only=True),
                            inherited=inherited, information_gaps=gaps)
    _frontier.freeze(front)
    log(f"frontier frozen  strongest={front['strongest_sleeve']}")
    log(f"VERDICT {verdict['primary_verdict']} / {verdict['secondary_verdict']}")
    return verdict


def information_gaps(sleeve_results: dict, ctx) -> list:
    """What blocked a sleeve, expressed as a purchase-gate input."""
    gaps = []
    for req in event_driven.UNOWNED_EVENT_REQUIREMENTS:
        gaps.append({
            "gap": req["requirement"],
            "blocked_sleeve": _contract.SLEEVE_EVENT_DRIVEN,
            "why_it_matters": "corporate-event timing is the one opportunity "
                              "family this project cannot observe at all",
            "owned_substitute_tried": req["evidence"],
            "state": (_gate.STATE_WAITING_FOR_SAMPLE
                      if "analyst" in req["requirement"]
                      else _gate.STATE_BLOCKED_COVERAGE),
            "conditions": {
                CONDITIONS_BLOCKED_SLEEVE: True,
                CONDITIONS_MATERIAL: True,
                CONDITIONS_SUBSTITUTE: True,
            },
        })
    gaps.append({
        "gap": "vintage-dated macroeconomic history before 2000",
        "blocked_sleeve": _contract.SLEEVE_EQUITY_BETA_TIMING,
        "why_it_matters": "the owned macro database is stamped at the start of "
                          "the period it measures and carries revised values, "
                          "so macro state cannot be used as history at all",
        "owned_substitute_tried": "ALFRED vintages, which begin around 2000",
        "state": _gate.STATE_BLOCKED_PIT,
        "conditions": {CONDITIONS_BLOCKED_SLEEVE: True,
                       CONDITIONS_MATERIAL: True,
                       CONDITIONS_SUBSTITUTE: True},
    })
    return gaps


CONDITIONS_BLOCKED_SLEEVE = _gate.CONDITIONS[0]
CONDITIONS_MATERIAL = _gate.CONDITIONS[1]
CONDITIONS_SUBSTITUTE = _gate.CONDITIONS[6]


def _strip(obj):
    if isinstance(obj, dict):
        return {k: _strip(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_strip(v) for v in obj]
    return obj


def _fmt(x) -> str:
    try:
        return f"{float(x):+.2f}"
    except (TypeError, ValueError):
        return "n/a"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--campaign-id", default=_contract.CAMPAIGN_ID)
    ap.add_argument("--contracts", action="store_true")
    ap.add_argument("--sources", action="store_true")
    ap.add_argument("--research", action="store_true")
    ap.add_argument("--full-source-scan", action="store_true",
                    help="measure every owned database, not only Economic")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args(argv)

    cid = args.campaign_id
    log(f"Release 32 campaign {cid}")
    log(f"research root: {r32.research_root()}")

    registry = None
    if args.all or args.contracts:
        stage_contracts(cid)
    if args.all or args.sources:
        registry = stage_sources(cid, full=args.full_source_scan)
    if args.all or args.research:
        registry = registry or _sources.load(cid) or {}
        stage_research(cid, registry)
    if not (args.all or args.contracts or args.sources or args.research):
        ap.print_help()
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
