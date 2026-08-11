#!/usr/bin/env python
r"""Stage 14 — orthogonal alpha discovery campaign (bounded operator run).

Executes the SIX hypotheses frozen in
``D:\Temp\paper_trader_stage14_orthogonal_alpha_discovery\STAGE14_PREREGISTRATION.md``
through the RELEASED machinery (see ``alpha_agent.stage14_orthogonal``):

  enqueue (ONE ResearchQueue, lane ``stage14.orthogonal.*``, origin
  ``stage14-orthogonal`` — outside every cadence allowlist) -> claim ->
  released Stage-5 scorer + gates -> Stage 9.4 enrichment (orthogonality /
  regime / walk-forward / deflated-Sharpe / selection) -> released tournament
  ingestion (``row_to_contract_metrics`` + ``classify_evidence`` + lifecycle
  transitions) -> released BH-FDR -> immutable evidence JSON on D:.

Safety: research-only. Writes ONLY to the D: research roots (ResearchQueue db,
tournament registry db, evidence directory). Operational-ledger fingerprints
are taken before and after via the ONE canonical owner
(``alpha_agent.runtime.fingerprint_ledgers``) and the run FAILS if they differ.
No orders, no fills, no book/target/NAV mutation, no promotion, no cadence.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import autonomous_research as ar     # noqa: E402
from paper_trader.alpha_agent import historical_price_panel as hpp  # noqa: E402
from paper_trader.alpha_agent import orthogonality as orth          # noqa: E402
from paper_trader.alpha_agent import price_factors as pf            # noqa: E402
from paper_trader.alpha_agent import runtime as rt                  # noqa: E402
from paper_trader.alpha_agent import stage9_4 as s94                # noqa: E402
from paper_trader.alpha_agent import stage14_orthogonal as s14      # noqa: E402
from paper_trader.alpha_agent import tournament as tt               # noqa: E402
from paper_trader.alpha_agent.historical_identity import IdentityStore  # noqa: E402

STAGE8_CONFIG = _REPO / "configs" / "alpha_agent" / "stage8_autonomy.json"
STAGE5_CONFIG = _REPO / "configs" / "alpha_agent" / "stage5_experiment_factory.json"
INGESTION_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion")
PRE2015_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\pre2015\ingestion")
IDENTITY_DB = Path(r"D:\Stock_Prediction_app_data\alpha_agent\identity\historical_identity.sqlite")
# Frozen champion-control artifacts (Phase 10-L panel + momentum monthly panel).
CHAMPION_PANEL_CSV = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10l_historical_sector_neutral_scored_panel_reconstruction"
    r"\historical_sector_neutral_scored_panel.csv")
MOMENTUM_PANEL_CSV = Path(
    r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha\_inputs"
    r"\momentum_monthly_panel.csv")
DEFAULT_OUTPUT_DIR = Path(
    r"D:\Temp\paper_trader_stage14_orthogonal_alpha_discovery\evidence")

COST_GRID = [5, 10, 25, 50]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _compact_row(row: dict) -> dict:
    return {k: v for k, v in row.items()
            if k not in ("_periods_series", "market_features")}


def _write_json(path: Path, doc) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=1, sort_keys=True, default=str),
                    encoding="utf-8")


def _ticker_maps(id_store, formation_dates) -> dict:
    out: dict = {}
    if id_store is None:
        return out
    cache_by_month: dict = {}
    for fd in sorted(set(formation_dates)):
        month = str(fd)[:7]
        if month not in cache_by_month:
            try:
                cache_by_month[month] = hpp.assetid_to_effective_ticker(
                    id_store, str(fd))
            except Exception:
                cache_by_month[month] = {}
        out[str(fd)] = cache_by_month[month]
    return out


def run_campaign(*, output_dir: Path, skip_registry: bool = False) -> dict:
    started = _now()
    stage8_cfg = _load_json(STAGE8_CONFIG)
    stage5_cfg = _load_json(STAGE5_CONFIG) if STAGE5_CONFIG.exists() else {}
    tournament_cfg_path = _REPO / (stage8_cfg.get("tournament", {})
                                   .get("config") or
                                   "configs/alpha_agent/stage9_tournament.json")
    t_cfg = tt.load_config(tournament_cfg_path)
    queue_db = (stage8_cfg.get("autonomy") or {}).get("queue_db")
    if not queue_db:
        raise SystemExit("stage8_autonomy.json missing autonomy.queue_db")

    fp_before = rt.fingerprint_ledgers(stage5_cfg)

    # ---- owned panel (open+close, merged main + pre2015) ------------------- #
    roots = [r for r in (PRE2015_ROOT, INGESTION_ROOT) if Path(r).exists()]
    oc_panel = s14.build_merged_open_close_panel(roots)
    close_panel = s14.close_panel_from_open_close(oc_panel)
    overnight_arrays = s14.build_overnight_arrays(oc_panel)
    panel_note = {"assets": len(oc_panel),
                  "bars": sum(len(v) for v in oc_panel.values()),
                  "roots": [str(r) for r in roots]}

    # ---- ONE released ResearchQueue: enqueue + claim (idempotent) --------- #
    queue = ar.ResearchQueue(queue_db)
    job_ids: dict = {}
    for h in s14.HYPOTHESES:
        job_ids[h["id"]] = queue.enqueue(
            ar.CAT_EXPERIMENT, lane=s14.LANE_PREFIX + h["feature"],
            payload={"stage": "stage14", "hypothesis_id": h["id"],
                     "feature": h["feature"], "family": h["family"],
                     "kind": h["kind"],
                     "preregistration":
                         "STAGE14_PREREGISTRATION.md (frozen 2026-08-10)"},
            origin=s14.ORIGIN, priority=5)

    controls = s14.load_champion_controls(CHAMPION_PANEL_CSV,
                                          MOMENTUM_PANEL_CSV)
    id_store = IdentityStore(IDENTITY_DB) if IDENTITY_DB.exists() else None

    # ---- membership spans/events (Lane B inputs, computed once) ------------ #
    stores = s14.read_membership_spans(roots)
    session_index = {d: i for i, d in enumerate(
        sorted({d for s in close_panel.values() for d, _c in s}))}
    merged_spans, boundaries = s14.merge_spans_across_stores(
        stores, session_index) if stores else ({}, {"starts": set(),
                                                    "ends": set()})
    additions, deletions = s14.extract_membership_events(
        merged_spans, boundaries) if merged_spans else ([], [])

    ov_value_fn = s14.make_overnight_value_fn(overnight_arrays)
    seas = [h for h in s14.HYPOTHESES if h["id"] == "S14-C1"][0]
    seas_value_fn = s14.make_seasonality_value_fn(
        lookback_years=seas["lookback_years"],
        exclusion_sessions=seas["exclusion_sessions"],
        min_obs=seas["min_obs"])
    lane_sizes = {}
    for h in s14.HYPOTHESES:
        lane_sizes[h["family"]] = lane_sizes.get(h["family"], 0) + 1

    executed: dict = {}
    completed_for_ingest: list = []
    primary_stats: list = []
    claimed = 0
    while True:
        job = queue.claim_next(origins=[s14.ORIGIN],
                               lane_prefixes=[s14.LANE_PREFIX],
                               categories=[ar.CAT_EXPERIMENT])
        if job is None:
            break
        claimed += 1
        hyp_id = (job.payload or {}).get("hypothesis_id")
        h = next((x for x in s14.HYPOTHESES if x["id"] == hyp_id), None)
        if h is None:
            queue.block_specific(job.job_id, "unknown stage14 hypothesis")
            continue
        try:
            if h["kind"] == "cross_sectional":
                value_fn = (seas_value_fn if h["id"] == "S14-C1"
                            else ov_value_fn)
                built = pf.build_factor_cross_sections(
                    close_panel, feature=h["feature"],
                    horizon_days=h["horizon_days"], rebalance=h["rebalance"],
                    value_fn=value_fn)
                row = s14.score_cross_sectional(
                    h["feature"], built, horizon_days=h["horizon_days"],
                    rebalance=h["rebalance"], cost_grid=COST_GRID)
                cc = s14.champion_comparison(
                    built, controls,
                    _ticker_maps(id_store, built.get("formation_dates") or []))
                if cc.get("corr_vs_champion_mean") is not None:
                    # released convention: corr_vs_champion = 1 - complementarity
                    row["champion_complementarity"] = \
                        1.0 - cc["corr_vs_champion_mean"]
                if row.get("rank_ic_t") is not None:
                    row = s94.enrich_price_factor_row(
                        row, family_size=lane_sizes[h["family"]], cfg={})
                    row["partial_rank_ic"] = cc.get("partial_rank_ic_mean")
                    indep = orth.independent_information_score(
                        corr_champion=(None if cc.get("corr_vs_champion_mean")
                                       is None
                                       else cc["corr_vs_champion_mean"]),
                        corr_retained=None,
                        partial_rank_ic=cc.get("partial_rank_ic_mean"),
                        incremental_net_return=row.get(
                            "incremental_net_return"), cfg={})
                    row["independent_information_score"] = \
                        indep["independent_information_score"]
                    row["stage9_4_independent_information"] = indep
                row["stage14_champion_comparison"] = cc
                row["stage14_expected_sign"] = h["expected_sign"]
                detail = {"real_work": "stage14_cross_sectional_experiment",
                          "hypothesis_id": h["id"], "feature": h["feature"],
                          "family": h["family"],
                          "horizon_days": h["horizon_days"],
                          "rebalance": h["rebalance"],
                          "candidate_id": h.get("candidate_id"),
                          "panel_assets": panel_note["assets"],
                          "row": _compact_row(row),
                          "decision": row.get("decision"),
                          "no_automatic_promotion": True}
                executed[h["id"]] = {"row": row, "detail": detail,
                                     "built_periods": built["periods"]}
                completed_for_ingest.append(
                    {"job_id": job.job_id, "feature": h["feature"],
                     "row": _compact_row(row)})
                primary_stats.append(
                    {"id": h["id"], "family": h["family"],
                     "t": row.get("rank_ic_t"), "n": row.get("periods")})
                queue.complete(job.job_id, result=detail)
            else:
                events = additions if h["side"] == "ADDITION" else deletions
                study = s14.run_membership_event_study(
                    events, h["side"], h["expected_sign"], merged_spans,
                    close_panel)
                study["hypothesis_id"] = h["id"]
                detail = {"real_work": "stage14_membership_event_study",
                          "hypothesis_id": h["id"], "feature": h["feature"],
                          "family": h["family"], "side": h["side"],
                          "n_events_input": study["n_events_input"],
                          "primary_horizon": study["primary_horizon"],
                          "primary_clustered_t": study["primary_clustered_t"],
                          "evidence_status": study["evidence_status"],
                          "no_automatic_promotion": True}
                executed[h["id"]] = {"study": study, "detail": detail}
                primary_stats.append(
                    {"id": h["id"], "family": h["family"],
                     "t": study["primary_clustered_t"],
                     "n": study.get("primary_n_cohorts")})
                queue.complete(job.job_id, result=detail)
        except Exception as exc:  # noqa: BLE001 — isolate one bad hypothesis
            queue.block_specific(job.job_id,
                                 "stage14 compute error: %s"
                                 % type(exc).__name__)
            executed[h["id"]] = {"error": type(exc).__name__,
                                 "error_detail": str(exc)[:500]}

    # ---- released FDR over the frozen families ----------------------------- #
    fdr = s14.stage14_fdr(primary_stats)

    # ---- canonical tournament registry: seed new + ingest evidence --------- #
    registry_report: dict = {"skipped": bool(skip_registry)}
    if not skip_registry:
        registry = tt.CandidateRegistry(t_cfg["tournament_db"])
        try:
            seeded: dict = {}
            seas_spec = {"catalogue": s14.STAGE14_VERSION,
                         "expected_sign": 1,
                         "factor_family": "SEASONALITY",
                         "feature": "same_month_seasonality",
                         "formula": ("mean of the name's TARGET-month (month "
                                     "after formation month) monthly returns "
                                     "over the prior 10y, months ending >252 "
                                     "sessions before formation, >=5 obs"),
                         "horizon_days": 21,
                         "hypothesis_id": "same_month_seasonality",
                         "rebalance": "monthly", "search_family_size": 1,
                         "template": "campaign_price_factor"}
            seeded["S14-C1"] = registry.seed_candidate(
                name="Same-calendar-month seasonality (21d)",
                family=tt.FAM_PRICE_MOMENTUM, spec=seas_spec,
                data_dependencies=["owned_norgate_daily_bars"],
                universe="S&P 500 Current & Past (Norgate survivorship-safe)",
                pit_status=tt.PIT_OWNED_PRICE,
                component_signals=["same_month_seasonality"])
            for h in s14.HYPOTHESES:
                if h["kind"] != "membership_event":
                    continue
                spec = {"catalogue": s14.STAGE14_VERSION,
                        "expected_sign": h["expected_sign"],
                        "factor_family": "MEMBERSHIP_EVENT",
                        "feature": h["feature"],
                        "formula": ("post-effective-date abnormal return of "
                                    "S&P 500 %s events vs EW members, entry "
                                    "strictly after effective date, primary "
                                    "63 sessions" % h["side"]),
                        "horizon_days": s14.EVENT_PRIMARY_HORIZON,
                        "hypothesis_id": h["feature"],
                        "rebalance": "event",
                        "search_family_size": 2,
                        "template": "stage14_event_study"}
                seeded[h["id"]] = registry.seed_candidate(
                    name=("S&P 500 %s post-effective event (63d)"
                          % h["side"].lower()),
                    family=tt.FAM_EVENT_INSIDER, spec=spec,
                    data_dependencies=["owned_norgate_daily_bars",
                                       "owned_norgate_membership_spans"],
                    universe=("S&P 500 membership transitions "
                              "(Norgate survivorship-safe)"),
                    pit_status=tt.PIT_OWNED_PRICE,
                    component_signals=[h["feature"]])
            ingest = tt.ingest_completed_experiments(
                registry, t_cfg, completed=completed_for_ingest,
                source="stage14_orthogonal_campaign",
                evidence_date=started[:10])
            event_updates = []
            for h in s14.HYPOTHESES:
                if h["kind"] != "membership_event":
                    continue
                got = executed.get(h["id"]) or {}
                study = got.get("study")
                cid = seeded.get(h["id"])
                if not study or not cid:
                    continue
                prim = study["horizons"][s14.EVENT_PRIMARY_HORIZON]
                metrics = {
                    "study_kind": "event_time_cohorts",
                    "primary_horizon": s14.EVENT_PRIMARY_HORIZON,
                    "clustered_t": study["primary_clustered_t"],
                    "n_events": prim["n_events_measured"],
                    "n_cohorts": prim["n_cohorts"],
                    "gross_mean_abnormal": prim["gross_mean_abnormal"],
                    "net_signed_mean_abnormal_50bps":
                        prim["net_signed_mean_abnormal_50bps"],
                    "subperiod_consistency": prim["subperiod_consistency"],
                    "expected_sign": h["expected_sign"],
                    "point_in_time_valid": True,
                    "survivorship_safe": True,
                    "horizons": {str(k): {kk: vv for kk, vv in v.items()
                                          if kk not in ("per_year",)}
                                 for k, v in study["horizons"].items()},
                }
                blocker = prim.get("adequacy_blocker")
                if blocker:
                    gate = {"target_state": tt.DATA_HOLD,
                            "evidence_status": tt.EVIDENCE_INCOMPLETE,
                            "blocker": blocker, "failed_gates": [],
                            "complete": False}
                elif study["passes_frozen_gates"]:
                    gate = {"target_state": tt.KEEP_FOR_RESEARCH,
                            "evidence_status": tt.EVIDENCE_COMPLETE_STRONG,
                            "blocker": None, "failed_gates": [],
                            "complete": True}
                else:
                    failed = []
                    t = study["primary_clustered_t"]
                    if t is None or abs(t) < 2.0 or \
                            (h["expected_sign"] * (t or 0.0)) <= 0:
                        failed.append("clustered_t")
                    if (prim.get("net_signed_mean_abnormal_50bps") or 0) <= 0:
                        failed.append("net_signed_mean_abnormal_50bps")
                    if prim.get("subperiod_consistency") is None or \
                            prim["subperiod_consistency"] < 0.60:
                        failed.append("subperiod_consistency")
                    gate = {"target_state": tt.REJECTED,
                            "evidence_status": tt.EVIDENCE_COMPLETE_WEAK,
                            "blocker": None, "failed_gates": failed,
                            "complete": True}
                registry.record_evaluation(
                    cid, metrics=metrics, scores=tt.null_scores(), gate=gate,
                    pit={"point_in_time_valid": True,
                         "survivorship_safe": True},
                    experiment_id=job_ids.get(h["id"]),
                    evidence_date=started[:10])
                state = registry.get(cid)["lifecycle_state"]
                if gate["target_state"] == tt.DATA_HOLD:
                    if state in (tt.PROPOSED, tt.DATA_HOLD):
                        registry.transition(cid, tt.DATA_HOLD,
                                            blocker=blocker,
                                            reason="stage14 adequacy gate")
                elif state in (tt.PROPOSED, tt.DATA_HOLD, tt.TESTING):
                    if state != tt.TESTING:
                        registry.transition(cid, tt.TESTING,
                                            reason="stage14 evidence")
                    registry.transition(cid, gate["target_state"],
                                        reason="stage14 frozen event gates")
                event_updates.append({"candidate_id": cid,
                                      "to": registry.get(cid)
                                      ["lifecycle_state"]})
            registry_report = {"seeded": seeded, "ingest": ingest,
                               "event_updates": event_updates}
        finally:
            registry.close()

    fp_after = rt.fingerprint_ledgers(stage5_cfg)
    ledgers_unchanged = (fp_before == fp_after)

    summary = {
        "stage": "stage14_orthogonal_alpha_discovery",
        "version": s14.STAGE14_VERSION,
        "started": started, "finished": _now(),
        "preregistration": "STAGE14_PREREGISTRATION.md (frozen 2026-08-10)",
        "panel": panel_note,
        "membership": {"stores": len(stores),
                       "additions": len(additions),
                       "deletions": len(deletions)},
        "queue": {"db": str(queue_db), "job_ids": job_ids,
                  "claimed": claimed},
        "hypotheses_declared": len(s14.HYPOTHESES),
        "hypotheses_executed": len([k for k, v in executed.items()
                                    if "error" not in v]),
        "errors": {k: v for k, v in executed.items() if "error" in v},
        "fdr": fdr,
        "registry": registry_report,
        "operational_ledger_unchanged": ledgers_unchanged,
        "no_orders": True, "no_promotion": True, "no_cadence": True,
        "champion": "composite_sn (unchanged; manual review only)",
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "stage14_campaign_summary.json", summary)
    for hid, got in executed.items():
        _write_json(output_dir / ("stage14_%s.json" % hid.replace("-", "_")),
                    {k: v for k, v in got.items() if k != "row"}
                    | ({"row": _compact_row(got["row"])} if "row" in got
                       else {}))
    _write_json(output_dir / "stage14_fdr.json", fdr)
    if not ledgers_unchanged:
        raise SystemExit("OPERATIONAL LEDGER FINGERPRINT CHANGED — "
                         "stage14 run must be investigated")
    return summary


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--json", action="store_true")
    p.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    p.add_argument("--skip-registry", action="store_true",
                   help="measure + FDR only; no tournament registry writes")
    args = p.parse_args(argv)
    summary = run_campaign(output_dir=Path(args.output_dir),
                           skip_registry=args.skip_registry)
    if args.json:
        print(json.dumps(summary, indent=1, sort_keys=True, default=str))
    else:
        print("stage14 campaign complete: %d/%d executed; ledgers unchanged=%s"
              % (summary["hypotheses_executed"],
                 summary["hypotheses_declared"],
                 summary["operational_ledger_unchanged"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
