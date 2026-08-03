"""
Stage 10.3 — SURVIVORSHIP-SAFE HISTORICAL MARKET_BAR PANEL REPAIR. Deterministic
property tests for the canonical assetid-anchored owned-price panel reader
(``historical_price_panel``), the per-rebalance PRICE-readiness contract, the
Stage 10.3 campaign lanes/handlers/planner (``market_bar_jobs``), the tournament
price-gate + panel-epoch experiment generation, and the semantics-preserving
``PitFundamentalsStore`` fiscal-key index. No network, no real Norgate, no
operational mutation: tiny in-memory owned MARKET_BAR files, a real identity store
and a real research queue exercise every safety invariant.

Covers the 28 numbered acceptance properties of the Stage 10.3 contract.
"""
import json
from pathlib import Path

from alpha_agent import historical_identity as H
from alpha_agent import historical_price_panel as HPP
from alpha_agent import market_bar_jobs as MBJ
from alpha_agent import pit_fundamentals as PF
from alpha_agent import fundamental_signals as FS
from alpha_agent import fundamental_evidence as FEV
from alpha_agent import tournament as TT
from alpha_agent import autonomous_research as AR

REPO = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# Fixtures / helpers.
# --------------------------------------------------------------------------- #
def _clock():
    box = {"n": 0}

    def c():
        box["n"] += 1
        return "2026-08-03T00:%02d:00+00:00" % (box["n"] % 60)
    return c


def _dates(n, start="2020-01-06", step_days=7):
    import datetime as dt
    d0 = dt.date.fromisoformat(start)
    return [(d0 + dt.timedelta(days=step_days * k)).isoformat() for k in range(n)]


def _id_store(tmp_path, specs, *, snapshot_dates=None):
    """``specs`` = list of (idx, is_current, member_start, member_end, resolved,
    assetid_or_None). Builds a real assetid-anchored identity store."""
    store = H.IdentityStore(str(tmp_path / "id.sqlite"), clock=_clock())
    for (i, is_cur, mstart, mend, resolved, aid) in specs:
        sid = "ngid:%d" % i
        ident = H.SecurityIdentity(
            security_id=sid,
            norgate_assetid=(str(aid) if aid is not None else None),
            norgate_symbol="S%d" % i, ticker="S%d" % i, issuer_name="Co%d" % i,
            share_class="COMMON", base_type="Equity", exchange="NYSE",
            security_start_date="2010-01-01",
            security_end_date=None if is_cur else mend,
            delisting_date=None if is_cur else mend, is_current=is_cur,
            membership_intervals=[{"index_name": "S&P 500",
                                   "member_start": mstart, "member_end": mend}])
        store.upsert_security(ident)
        if resolved:
            store.record_mapping(H.MappingResult(
                sid, "%010d" % (1000 + i), H.METHOD_FILING_TICKER_OVERLAP, 3,
                0.9, H.STATUS_RESOLVED, "2010-01-01", None, {"t": 3}))
    if snapshot_dates:
        store.record_coverage_snapshot(as_of="2024-12-31",
                                       rebalance_dates=snapshot_dates,
                                       index_name="S&P 500")
    return store


def _simple_specs(n, assetid_offset=100):
    # current, resolved, one assetid each.
    return [(i, True, "2010-01-01", None, True, assetid_offset + i)
            for i in range(n)]


def _pit(ciks, *, with_prior=False):
    st = PF.PitFundamentalsStore()
    fys = [2017, 2018] if with_prior else [2018]
    for c in ciks:
        for fy in fys:
            for tag, val in (("Assets", 1000 + fy), ("Liabilities", 400),
                             ("StockholdersEquity", 600),
                             ("GrossProfit", 300), ("Revenues", 900),
                             ("CostOfRevenue", 600)):
                st.add_fact({"cik": c, "concept": tag, "unit": "USD",
                             "value": val, "period_end": "%d-12-31" % fy,
                             "fy": fy, "fp": "FY", "filed": "%d-02-15" % (fy + 1),
                             "form": "10-K"})
    return st


def _write_bars(root, rows, *, source_id="norgate_local", run="r1"):
    """rows = list of (assetid_or_None, ticker, date, close). Writes normalized
    MARKET_BAR JSONL in the Stage 2 layout under ``root``/normalized/MARKET_BAR."""
    base = Path(root) / "normalized" / "MARKET_BAR"
    buckets: dict = {}
    for aid, tkr, date, close in rows:
        y, m, d = date.split("-")
        p = base / y / m / d / ("%s.jsonl" % run)
        rec = {"record_type": "MARKET_BAR", "source_id": source_id,
               "ticker": tkr, "security_id": (str(aid) if aid is not None
                                              else None),
               "effective_at": date,
               "normalized_payload": {"Date": date, "Close": close,
                                      "Unadjusted Close": close * 0.5}}
        buckets.setdefault(p, []).append(json.dumps(rec))
    for p, lines in buckets.items():
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    return base


def _series_panel(assetids, dates, base=100.0):
    return {str(a): [(d, base + i) for i, d in enumerate(dates)]
            for a in assetids}


def _mb_ctx(tmp_path, store, ingestion_root, *, dates=None, cfg9=None,
           pit_store=None):
    c2a = HPP.build_cik_to_assetid(store)
    return MBJ.MarketBarJobContext(
        store=store, ingestion_root=str(ingestion_root),
        artifact_root=str(tmp_path / "artifacts"),
        rebalance_dates=dates or [], cfg9=cfg9 or {},
        pit_store=pit_store if pit_store is not None else _pit(list(c2a.keys())),
        stage103={"enabled": True, "planner_enabled": True,
                  "sources": ["norgate_local"], "horizon_days": 63,
                  "priority": 3,
                  "price_readiness": {"min_scored_periods": 12,
                                      "min_cross_section_names": 3,
                                      "min_names_per_period": 3}},
        index_name="S&P 500", signals=("asset_growth", "balance_sheet_quality",
                                        "gross_profitability"),
        sources=("norgate_local",), horizon_days=1, clock=_clock())


class _Job:
    def __init__(self, lane, payload=None, job_id="j1"):
        self.lane = lane
        self.payload = payload or {}
        self.job_id = job_id
        self.origin = MBJ.ORIGIN_103


_THR = {"min_scored_periods": 12, "min_cross_section_names": 3,
        "min_names_per_period": 3}


# =========================================================================== #
# 1. Current-survivor substitution impossible: a name whose owned price series
#    begins AFTER a formation date contributes NOTHING to that historical date.
# =========================================================================== #
def test_p01_no_current_survivor_substitution(tmp_path):
    dates = _dates(13)
    store = _id_store(tmp_path, _simple_specs(5))
    c2a = HPP.build_cik_to_assetid(store)
    aids = list(c2a.values())
    panel = _series_panel(aids, dates)
    # a "future IPO": assetid only priced from date index 6 onward.
    late = aids[0]
    panel[late] = [(d, 100.0 + i) for i, d in enumerate(dates) if i >= 6]
    pit = _pit(list(c2a.keys()))
    rr = HPP.per_rebalance_price_readiness(pit, panel, c2a, "balance_sheet_quality",
                                           horizon_days=1, thresholds=_THR)
    early = rr["per_date"][0]
    assert early["cross_section"] == 4   # the late name is absent on date 0
    assert late in panel                 # it exists, just not priced early


# 2. Delisted security prices are INCLUDED where historically eligible.
def test_p02_delisted_included_where_eligible(tmp_path):
    dates = _dates(13)
    specs = _simple_specs(4)
    specs.append((99, False, "2010-01-01", dates[8], True, 199))  # delisted late
    store = _id_store(tmp_path, specs)
    c2a = HPP.build_cik_to_assetid(store)
    assert "199" in c2a.values()          # delisted name is in the join
    panel = _series_panel(list(c2a.values()), dates)
    # delisted series ends at index 8 (no later bars -> no forward after).
    panel["199"] = [(d, 100.0 + i) for i, d in enumerate(dates) if i <= 8]
    pit = _pit(list(c2a.keys()))
    rr = HPP.per_rebalance_price_readiness(pit, panel, c2a, "balance_sheet_quality",
                                           horizon_days=1, thresholds=_THR)
    assert rr["per_date"][0]["cross_section"] == 5   # present early (in life)
    assert rr["per_date"][8]["cross_section"] == 4   # gone after delisting


# 3. Ticker text ALONE cannot resolve a price series: the panel is assetid-keyed;
#    a ticker-only (no assetid) record is excluded; same ticker + different assetid
#    stay separate.
def test_p03_ticker_alone_cannot_resolve(tmp_path):
    root = tmp_path / "ing"
    _write_bars(root, [
        (101, "T", "2020-01-06", 10.0),   # assetid 101
        (102, "T", "2020-01-06", 20.0),   # SAME ticker, different assetid
        (None, "T", "2020-01-06", 99.0),  # ticker-only norgate? excluded (no aid)
    ])
    _write_bars(root, [(None, "T", "2020-01-06", 55.0)], source_id="eodhd")
    panel = HPP.build_assetid_price_panel(root)
    assert set(panel.keys()) == {"101", "102"}   # two distinct identities
    assert panel["101"][0][1] == 10.0 and panel["102"][0][1] == 20.0


# 4. Assetid mapping survives ticker changes: the CIK->assetid join is stable
#    regardless of the security's ticker.
def test_p04_assetid_survives_ticker_change(tmp_path):
    store = _id_store(tmp_path, [(7, True, "2010-01-01", None, True, 777)])
    c2a = HPP.build_cik_to_assetid(store)
    cik = "%010d" % (1000 + 7)
    assert c2a[H.norm_cik(cik)] == "777"   # keyed by stable assetid, not ticker


# 5. Predecessor history is not rewritten to a successor: ticker reuse keeps
#    separate assetid identities and separate CIKs.
def test_p05_ticker_reuse_kept_separate(tmp_path):
    # two securities, same ticker text "S1" would collide, but different assetids.
    specs = [(1, False, "2010-01-01", "2016-06-30", True, 11),
             (2, True, "2017-01-01", None, True, 22)]
    store = _id_store(tmp_path, specs)
    c2a = HPP.build_cik_to_assetid(store)
    assert c2a[H.norm_cik("%010d" % 1001)] == "11"
    assert c2a[H.norm_cik("%010d" % 1002)] == "22"
    assert len(set(c2a.values())) == 2     # never merged into one series


# 6. Ambiguous / missing price mappings remain EXPLICIT (a security with no
#    assetid is never invented into the join).
def test_p06_missing_assetid_excluded(tmp_path):
    specs = [(1, True, "2010-01-01", None, True, 11),
             (2, True, "2010-01-01", None, True, None)]  # no assetid
    store = _id_store(tmp_path, specs)
    c2a = HPP.build_cik_to_assetid(store)
    assert c2a.get(H.norm_cik("%010d" % 1002)) is None
    assert len(c2a) == 1


# 7. Formation prices use only permitted (on-or-before) dates.
def test_p07_formation_price_on_or_before(tmp_path):
    dates = _dates(5)
    series = [(d, 100.0 + i) for i, d in enumerate(dates)]
    pdates, closes = FEV._price_index(series)
    # forward return from date index 1: uses close at 1 as the base (<= as_of).
    r = FEV._forward_return(pdates, closes, dates[1], 1)
    assert abs(r - (closes[2] / closes[1] - 1.0)) < 1e-12


# 8. Forward returns use STRICTLY future prices.
def test_p08_forward_strictly_future(tmp_path):
    dates = _dates(5)
    series = [(d, 100.0 + i) for i, d in enumerate(dates)]
    pdates, closes = FEV._price_index(series)
    # horizon 2 from index 0 -> close[2]/close[0]-1 (strictly after formation).
    r = FEV._forward_return(pdates, closes, dates[0], 2)
    assert abs(r - (closes[2] / closes[0] - 1.0)) < 1e-12


# 9. Missing forward prices remain MISSING (no fabrication).
def test_p09_missing_forward_is_missing(tmp_path):
    dates = _dates(3)
    series = [(d, 100.0 + i) for i, d in enumerate(dates)]
    pdates, closes = FEV._price_index(series)
    assert FEV._forward_return(pdates, closes, dates[2], 1) is None  # no future bar


# 10. Adjusted-price handling matches the evaluator contract (Close, not the
#     unadjusted field).
def test_p10_adjusted_close_used(tmp_path):
    root = tmp_path / "ing"
    _write_bars(root, [(101, "A", "2020-01-06", 42.0)])
    panel = HPP.build_assetid_price_panel(root)
    assert panel["101"][0][1] == 42.0     # Close, not Unadjusted Close (21.0)


# 11. Duplicate MARKET_BAR records are idempotently suppressed.
def test_p11_duplicate_bars_suppressed(tmp_path):
    root = tmp_path / "ing"
    _write_bars(root, [(101, "A", "2020-01-06", 10.0)], run="r1")
    _write_bars(root, [(101, "A", "2020-01-06", 10.0)], run="r2")  # exact dup
    panel = HPP.build_assetid_price_panel(root)
    assert panel["101"] == [("2020-01-06", 10.0)]   # de-duplicated


# 12. Changed source bytes create a new coverage epoch (revision).
def test_p12_new_data_new_epoch(tmp_path):
    root = tmp_path / "ing"
    _write_bars(root, [(101, "A", "2020-01-06", 10.0)])
    e1 = HPP.panel_coverage_epoch(root)
    _write_bars(root, [(101, "A", "2020-02-03", 11.0)])  # a NEW dated partition
    e2 = HPP.panel_coverage_epoch(root)
    assert e1 != e2


# 13/21. Exactly ONE job per planner step; restart resumes from durable meta flags
#        (after all stamped the planner returns None — no replay).
def test_p13_p21_one_job_and_restart_idempotent(tmp_path):
    root = tmp_path / "ing"
    _write_bars(root, [(100 + i, "S%d" % i, d, 100.0 + j)
                       for i in range(3)
                       for j, d in enumerate(_dates(4))])
    store = _id_store(tmp_path, _simple_specs(3))
    ctx = _mb_ctx(tmp_path, store, root, dates=_dates(4))
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    seen = []
    for _ in range(8):
        plan = MBJ.plan_next_market_bar_job(q, ctx, cfg=ctx.stage103)
        if plan is None:
            break
        seen.append(plan["lane"])
        job = q.claim_next(categories=[AR.CAT_DATA_VALIDATION],
                           origins=[MBJ.ORIGIN_103],
                           lane_prefixes=[MBJ.MARKET_BAR_LANE_PREFIX])
        assert job is not None
        outcome, detail = MBJ.dispatch_market_bar_job(job, ctx)
        q.apply_outcome(job.job_id, outcome, result=detail)
    assert seen == [MBJ.LANE_MB_COVERAGE_DIAGNOSE, MBJ.LANE_MB_PANEL_VALIDATE,
                    MBJ.LANE_MB_READINESS_RECHECK, MBJ.LANE_MB_EXPERIMENT_RECHECK]
    # restart: everything measured for the epoch -> planner returns None (no replay)
    assert MBJ.plan_next_market_bar_job(q, ctx, cfg=ctx.stage103) is None


# 14. A completed job is never replayed (dispatch idempotency).
def test_p14_completed_not_replayed(tmp_path):
    root = tmp_path / "ing"
    _write_bars(root, [(100, "S0", "2020-01-06", 10.0)])
    store = _id_store(tmp_path, _simple_specs(1))
    ctx = _mb_ctx(tmp_path, store, root, dates=_dates(4))
    job = _Job(MBJ.LANE_MB_COVERAGE_DIAGNOSE)
    o1, _ = MBJ.dispatch_market_bar_job(job, ctx)
    o2, d2 = MBJ.dispatch_market_bar_job(job, ctx)   # same payload
    assert o1 == AR.OUTCOME_COMPLETED
    assert d2.get("idempotent_skip") is True


# 15. Per-date readiness uses the historical (panel-derived) denominator, not a
#     fixed current count.
def test_p15_per_date_historical_denominator(tmp_path):
    dates = _dates(13)
    store = _id_store(tmp_path, _simple_specs(5))
    c2a = HPP.build_cik_to_assetid(store)
    panel = _series_panel(list(c2a.values()), dates)
    pit = _pit(list(c2a.keys()))
    rr = HPP.per_rebalance_price_readiness(pit, panel, c2a, "balance_sheet_quality",
                                           horizon_days=1, thresholds=_THR)
    assert len(rr["per_date"]) == rr["formation_dates"] == 12
    assert all(p["as_of"] in dates for p in rr["per_date"])


# 16. Aggregate price-ROW counts cannot open readiness (only real scored periods).
def test_p16_row_counts_cannot_open(tmp_path):
    dates = _dates(12)   # -> 11 formation dates
    store = _id_store(tmp_path, _simple_specs(50))   # many names, many rows
    c2a = HPP.build_cik_to_assetid(store)
    panel = _series_panel(list(c2a.values()), dates)
    pit = _pit(list(c2a.keys()))
    rr = HPP.per_rebalance_price_readiness(pit, panel, c2a, "balance_sheet_quality",
                                           horizon_days=1, thresholds=_THR)
    assert sum(len(v) for v in panel.values()) >= 500   # lots of rows
    assert rr["valid_scored_periods"] == 11
    assert rr["sufficient"] is False


# 17. Current-only prices cannot open readiness when < 12 real periods.
def test_p17_current_only_cannot_open(tmp_path):
    dates = _dates(12)   # 11 formation dates
    store = _id_store(tmp_path, _simple_specs(30))   # all current
    c2a = HPP.build_cik_to_assetid(store)
    panel = _series_panel(list(c2a.values()), dates)
    pit = _pit(list(c2a.keys()))
    rr = HPP.per_rebalance_price_readiness(pit, panel, c2a, "balance_sheet_quality",
                                           horizon_days=1, thresholds=_THR)
    assert rr["sufficient"] is False


# 18. 11 real scored periods remain CLOSED (the exact prior blocker).
def test_p18_eleven_periods_closed(tmp_path):
    dates = _dates(12)   # 11 formation dates
    store = _id_store(tmp_path, _simple_specs(5))
    c2a = HPP.build_cik_to_assetid(store)
    panel = _series_panel(list(c2a.values()), dates)
    pit = _pit(list(c2a.keys()))
    rr = HPP.per_rebalance_price_readiness(pit, panel, c2a, "balance_sheet_quality",
                                           horizon_days=1, thresholds=_THR)
    assert rr["valid_scored_periods"] == 11
    assert rr["sufficient"] is False
    assert rr["blocker"] == "INSUFFICIENT_PRICE_SCORED_PERIODS"


# 19. 12 real scored periods OPEN the experiment path.
def test_p19_twelve_periods_open(tmp_path):
    dates = _dates(13)   # 12 formation dates
    store = _id_store(tmp_path, _simple_specs(5))
    c2a = HPP.build_cik_to_assetid(store)
    panel = _series_panel(list(c2a.values()), dates)
    pit = _pit(list(c2a.keys()))
    rr = HPP.per_rebalance_price_readiness(pit, panel, c2a, "balance_sheet_quality",
                                           horizon_days=1, thresholds=_THR)
    assert rr["valid_scored_periods"] == 12
    assert rr["sufficient"] is True and rr["blocker"] is None


# 20. A REPAIRED (extended) panel genuinely adds the 12th scored period (11 -> 12).
def test_p20_repaired_date_enters_evaluator(tmp_path):
    store = _id_store(tmp_path, _simple_specs(5))
    c2a = HPP.build_cik_to_assetid(store)
    pit = _pit(list(c2a.keys()))
    d12 = _dates(12)
    p11 = _series_panel(list(c2a.values()), d12)
    r11 = HPP.per_rebalance_price_readiness(pit, p11, c2a, "balance_sheet_quality",
                                            horizon_days=1, thresholds=_THR)
    d13 = _dates(13)
    p12 = _series_panel(list(c2a.values()), d13)   # one more owned quarter
    r12 = HPP.per_rebalance_price_readiness(pit, p12, c2a, "balance_sheet_quality",
                                            horizon_days=1, thresholds=_THR)
    assert r11["valid_scored_periods"] == 11 and not r11["sufficient"]
    assert r12["valid_scored_periods"] == 12 and r12["sufficient"]


# 22. The AlphaAgent (tournament tick) GENERATES the fundamental experiment on the
#     repaired panel only when BOTH fundamental + price readiness are measured
#     sufficient; the spec carries the price_panel_epoch.
def test_p22_generator_generates_with_price_gate(tmp_path):
    store = _id_store(tmp_path, _simple_specs(25))
    store.set_meta("stage102_readiness_snapshot", json.dumps({
        "asset_growth": {"sufficient": True, "mapping_available": True},
        "balance_sheet_quality": {"sufficient": True, "mapping_available": True},
        "gross_profitability": {"sufficient": False, "mapping_available": True}}))
    store.set_meta("stage103_price_readiness_snapshot", json.dumps({
        "asset_growth": {"sufficient": True},
        "balance_sheet_quality": {"sufficient": True},
        "gross_profitability": {"sufficient": False}}))
    store.set_meta("stage103_panel_epoch", "EPOCHV1")
    cfg = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                     .read_text(encoding="utf-8-sig"))
    cfg["stage9_5"]["historical_universe"]["identity_store_db"] = \
        str(tmp_path / "id.sqlite")
    cfg["stage9_5"]["fundamental_experiments"]["max_per_cycle"] = 5
    reg = TT.CandidateRegistry(str(tmp_path / "t.sqlite"))
    TT.seed_families(reg)
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    res = TT.generate_stage9_5_fundamental_followups(reg, cfg, queue=q)
    jobs = [j for j in q.list_jobs(limit=50)
            if j.lane == "tournament.stage9_5_fundamental"]
    feats = {(j.payload or {}).get("feature") for j in jobs}
    reg.close()
    assert "asset_growth" in feats and "balance_sheet_quality" in feats
    assert "gross_profitability" not in feats
    assert all((j.payload.get("spec") or {}).get("price_panel_epoch") == "EPOCHV1"
               for j in jobs)


# 22b. When price readiness is INSUFFICIENT the experiment is NOT generated even
#      though the fundamental readiness passes (the price gate can only close).
def test_p22b_price_gate_closes(tmp_path):
    store = _id_store(tmp_path, _simple_specs(25))
    store.set_meta("stage102_readiness_snapshot", json.dumps({
        "asset_growth": {"sufficient": True, "mapping_available": True},
        "balance_sheet_quality": {"sufficient": True, "mapping_available": True},
        "gross_profitability": {"sufficient": False, "mapping_available": True}}))
    store.set_meta("stage103_price_readiness_snapshot", json.dumps({
        "asset_growth": {"sufficient": False},
        "balance_sheet_quality": {"sufficient": False},
        "gross_profitability": {"sufficient": False}}))
    cfg = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                     .read_text(encoding="utf-8-sig"))
    cfg["stage9_5"]["historical_universe"]["identity_store_db"] = \
        str(tmp_path / "id.sqlite")
    reg = TT.CandidateRegistry(str(tmp_path / "t.sqlite"))
    TT.seed_families(reg)
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    res = TT.generate_stage9_5_fundamental_followups(reg, cfg, queue=q)
    reg.close()
    assert res["count"] == 0


# 23. Attempts transition 0 -> 1 exactly once when a job is claimed.
def test_p23_attempts_increment_once(tmp_path):
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    q.enqueue(AR.CAT_DATA_VALIDATION, lane=MBJ.LANE_MB_READINESS_RECHECK,
              payload={}, priority=3, origin=MBJ.ORIGIN_103)
    job = q.claim_next(categories=[AR.CAT_DATA_VALIDATION],
                       origins=[MBJ.ORIGIN_103],
                       lane_prefixes=[MBJ.MARKET_BAR_LANE_PREFIX])
    assert int(getattr(job, "attempts", 0)) == 1


# 24. Spec de-dup: the SAME (feature, epoch) spec generates once; a CHANGED
#     panel_epoch produces a NEW experiment (so a repaired panel re-runs).
def test_p24_spec_dedup_and_epoch_regen(tmp_path):
    reg = TT.CandidateRegistry(str(tmp_path / "t.sqlite"))
    TT.seed_families(reg)
    cand = TT._candidate_for_feature(reg, "asset_growth")
    base = {"feature": "asset_growth", "horizon_days": 63, "rebalance": "quarterly",
            "template": "fundamental_momentum_rank",
            "study_kind": "stage9_5_fundamental", "fundamental_of":
            cand["candidate_id"]}
    s1 = reg.try_register_generated(strategy="stage9_5_fundamental",
                                    spec={**base, "price_panel_epoch": "E1"},
                                    candidate_id=cand["candidate_id"])
    s1b = reg.try_register_generated(strategy="stage9_5_fundamental",
                                     spec={**base, "price_panel_epoch": "E1"},
                                     candidate_id=cand["candidate_id"])
    s2 = reg.try_register_generated(strategy="stage9_5_fundamental",
                                    spec={**base, "price_panel_epoch": "E2"},
                                    candidate_id=cand["candidate_id"])
    reg.close()
    assert s1 and not s1b and s2   # same epoch dedups; new epoch regenerates


# 25. Lifecycle / statistical gates are UNCHANGED (min_scored_periods stays 12,
#     the keep gates are untouched).
def test_p25_gates_unchanged(tmp_path):
    assert HPP.DEFAULT_PRICE_READINESS["min_scored_periods"] == 12
    cfg = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                     .read_text(encoding="utf-8-sig"))
    assert cfg["gates"]["keep_min_rank_ic_t"] == 2.0
    assert cfg["evidence_completeness"]["min_scored_periods"] == 12


# 26. No model is automatically promoted: the market_bar handlers carry
#     no_automatic_promotion and never move a candidate lifecycle.
def test_p26_no_auto_promotion(tmp_path):
    root = tmp_path / "ing"
    _write_bars(root, [(100, "S0", "2020-01-06", 10.0)])
    store = _id_store(tmp_path, _simple_specs(1))
    ctx = _mb_ctx(tmp_path, store, root, dates=_dates(4))
    for lane in (MBJ.LANE_MB_COVERAGE_DIAGNOSE, MBJ.LANE_MB_PANEL_VALIDATE):
        _, d = MBJ.dispatch_market_bar_job(_Job(lane), ctx)
        assert d.get("no_automatic_promotion") is True
        assert d.get("safety_switch_flipped", False) is False


# 27. No operational ledger changes: artifacts + meta go ONLY under the research
#     tmp roots (never a .paper_trader operational-ledger path).
def test_p27_no_operational_ledger_write(tmp_path):
    root = tmp_path / "ing"
    _write_bars(root, [(100, "S0", "2020-01-06", 10.0)])
    store = _id_store(tmp_path, _simple_specs(1))
    ctx = _mb_ctx(tmp_path, store, root, dates=_dates(4))
    _, d = MBJ.dispatch_market_bar_job(_Job(MBJ.LANE_MB_COVERAGE_DIAGNOSE), ctx)
    art = d.get("artifact") or ""
    assert str(tmp_path) in str(art)
    assert ".paper_trader" not in str(art)


# 28. Queue fairness: at most one live market_bar job (a second is not planned).
def test_p28_one_live_job_fairness(tmp_path):
    root = tmp_path / "ing"
    _write_bars(root, [(100, "S0", "2020-01-06", 10.0)])
    store = _id_store(tmp_path, _simple_specs(1))
    ctx = _mb_ctx(tmp_path, store, root, dates=_dates(4))
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    p1 = MBJ.plan_next_market_bar_job(q, ctx, cfg=ctx.stage103)
    assert p1 is not None
    # a live queued job exists -> the planner will not enqueue a second.
    assert MBJ._has_live_market_bar_job(q) is True
    p2 = MBJ.plan_next_market_bar_job(q, ctx, cfg=ctx.stage103)
    assert p2 is None


# Extra. The semantics-preserving PitFundamentalsStore fiscal-key index returns
# EXACTLY what an exhaustive scan over _obs would (perf change, not a semantics
# change) — the guarantee the whole Stage 10.3 evaluation depends on.
def test_index_equivalence(tmp_path):
    st = _pit(["0000001000", "0000001001"], with_prior=True)

    def brute_latest(cik, as_of, concept="assets"):
        best_pe = best_fk = None
        for (c, cc, fk) in st._obs:
            if c != str(cik) or cc != concept:
                continue
            pe = st._period_end_available(cik, concept, fk, as_of)
            if pe is None:
                continue
            if best_pe is None or pe > best_pe:
                best_pe, best_fk = pe, fk
        return best_fk

    for cik in ("0000001000", "0000001001"):
        for as_of in ("2018-06-30", "2019-06-30", "2020-06-30", "2016-01-01"):
            assert st.latest_fiscal_key(cik, as_of) == brute_latest(cik, as_of)
            fk = st.latest_fiscal_key(cik, as_of)
            if fk:
                # prior resolves to the comparable FY-1 identity when available.
                assert st.prior_fiscal_key(cik, as_of, fk) in (None, "2017-FY",
                                                               "2018-FY")
