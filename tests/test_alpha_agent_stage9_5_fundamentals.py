"""
Stage 9.5 - LIVE point-in-time SEC companyfacts acquisition + fundamental alpha
activation. Deterministic tests for the 30 required properties (Part 11):

 1  only the exact companyfacts lane/category/origin is claimable
 2  unrelated DATA_ACQUISITION jobs remain untouched
 3  batch size is bounded
 4  hard maximum batch size is enforced
 5  daily batch cap is enforced
 6  no-progress stop is enforced
 7  progress resets no-progress state
 8  duplicate continuation is suppressed
 9  restart resumes from the durable cursor
 10 raw artifacts use atomic writes
 11 artifact hashes are deterministic
 12 identical documents are idempotent
 13 changed documents are not silently overwritten
 14 accession-level fact deduplication works
 15 filed date controls availability
 16 amendments do not leak backward
 17 fiscal-period identity is preserved
 18 units are handled deterministically
 19 missing concepts stay explicit
 20 gross-profitability fallback order is deterministic
 21 asset growth uses comparable periods
 22 balance-sheet quality uses the registered formula
 23 insufficient coverage remains DATA_HOLD
 24 complete weak evidence becomes REJECTED
 25 complete strong evidence may become KEEP_FOR_RESEARCH
 26 experiment result is imported exactly once
 27 no model is automatically promoted
 28 no shadow book is backfilled
 29 operational trading ledgers remain byte-identical
 30 cadence tasks remain disabled during uncommitted work (config invariant)
"""
import json
import random
from pathlib import Path

import pytest

from alpha_agent import autonomous_research as ar
from alpha_agent import acquisition_campaign as acq
from alpha_agent import fundamental_evidence as fev
from alpha_agent import fundamental_readiness as fr
from alpha_agent import fundamental_signals as fsig
from alpha_agent import ingestion as ing
from alpha_agent import pit_fundamentals as pfd
from alpha_agent import runtime as rt
from alpha_agent import sec_companyfacts as cf
from alpha_agent import tournament as tt
from alpha_agent.collectors import RawArchive
from alpha_agent.source_contracts import sha256_hex

REPO = Path(__file__).resolve().parents[1]
STAGE9_CFG = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                        .read_text(encoding="utf-8-sig"))
STAGE8_CFG = json.loads((REPO / "configs/alpha_agent/stage8_autonomy.json")
                        .read_text(encoding="utf-8-sig"))


# --------------------------------------------------------------------------- #
# Fixtures / builders
# --------------------------------------------------------------------------- #
def _cf_doc(cik, facts):
    """facts: (tag, unit, val, end, fy, fp, filed, accn, form)."""
    by_tag = {}
    for tag, unit, val, end, fy, fp, filed, accn, form in facts:
        by_tag.setdefault(tag, {}).setdefault(unit, []).append(
            {"val": val, "end": end, "fy": fy, "fp": fp, "filed": filed,
             "accn": accn, "form": form})
    return {"cik": cik,
            "facts": {"us-gaap": {t: {"units": u} for t, u in by_tag.items()}}}


def _store_from_docs(docs):
    return cf.materialize(docs)["store"]


def _pit_and_panel(*, noise, drift, n_ciks=40, seed=7, years=range(2001, 2021)):
    """A synthetic owned PIT store + survivorship-safe panel where
    gross_profitability quality is monotone in future drift."""
    import datetime as dt
    rnd = random.Random(seed)
    store = pfd.PitFundamentalsStore()
    recs = []
    for n in range(n_ciks):
        cik = str(1000 + n)
        q = n / n_ciks
        for y in years:
            for c, v in (("Assets", 1000.0), ("Revenues", 200.0 + 400.0 * q),
                         ("CostOfRevenue", 100.0)):
                recs.append({"event_type": "XBRL_FACT", "normalized_payload": {
                    "cik": cik, "concept": c, "unit": "USD", "value": v,
                    "period_end": "%d-12-31" % y, "fy": y, "fp": "FY",
                    "filed": "%d-03-01" % (y + 1),
                    "accession": "a%s-%d" % (cik, y), "form": "10-K"}})
    store.add_records(recs)
    d = dt.date(2001, 1, 2)
    dates = []
    for _ in range(4000):
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d += dt.timedelta(days=1)
    panel = {}
    for n in range(n_ciks):
        q = n / n_ciks
        px = 100.0
        bars = []
        for ds in dates:
            px *= max(0.5, 1.0 + drift * (q - 0.5) + rnd.gauss(0, noise))
            bars.append((ds, px))
        panel["T%03d" % n] = bars
    c2t = {str(1000 + n): "T%03d" % n for n in range(n_ciks)}
    return store, panel, c2t


def _row(kind):
    store, panel, c2t = (_pit_and_panel(noise=0.004, drift=0.0006) if kind ==
                         "strong" else _pit_and_panel(noise=0.05, drift=0.0))
    return fev.evaluate_fundamental_evidence(
        store, panel, "gross_profitability", cik_to_ticker=c2t,
        horizon_days=63, cfg={"bootstrap_resamples": 100})


# =========================================================================== #
# Queue / drain / campaign (1-9)
# =========================================================================== #
def test_rc01_only_exact_companyfacts_triple_claimable(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"))
    good = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_companyfacts",
                     payload={"campaign": "sec_companyfacts"}, priority=5,
                     origin="campaign-continuation")
    q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_form4_8k",
              payload={"campaign": "sec_form4_8k"}, priority=5,
              origin="campaign-continuation")
    q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_companyfacts",
              payload={"x": 1}, priority=9, origin="campaign-planner")
    q.enqueue(ar.CAT_EXPERIMENT, lane="tournament.stage9_5_fundamental",
              payload={"x": 1}, priority=9, origin="stage9-tournament")
    cont = (STAGE8_CFG["autonomy"]["collect_drain"]["companyfacts_continuation"])
    claimed = q.claim_next(categories=cont["allowed_categories"],
                           origins=cont["allowed_origins"],
                           lane_prefixes=cont["allowed_lane_prefixes"])
    assert claimed is not None and claimed.job_id == good
    # no OTHER companyfacts-triple job exists to claim -> second claim is None
    assert q.claim_next(categories=cont["allowed_categories"],
                        origins=cont["allowed_origins"],
                        lane_prefixes=cont["allowed_lane_prefixes"]) is None


def test_rc02_unrelated_data_acquisition_untouched(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"))
    q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_companyfacts",
              payload={"campaign": "sec_companyfacts"}, priority=5,
              origin="campaign-continuation")
    form4 = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_form4_8k",
                      payload={}, priority=5, origin="campaign-continuation")
    norg = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.norgate_prices",
                     payload={}, priority=9, origin="campaign-planner")
    cont = STAGE8_CFG["autonomy"]["collect_drain"]["companyfacts_continuation"]
    q.claim_next(categories=cont["allowed_categories"],
                 origins=cont["allowed_origins"],
                 lane_prefixes=cont["allowed_lane_prefixes"])
    states = {j.job_id: j.state for j in q.list_jobs(limit=50)}
    assert states[form4] == ar.STATE_QUEUED
    assert states[norg] == ar.STATE_QUEUED


def test_rc03_batch_size_is_bounded(tmp_path):
    store = acq.CampaignStore(str(tmp_path / "c.sqlite"))
    store.ensure_campaign("sec_companyfacts", kind="sec_companyfacts_xbrl",
                          universe=["T%03d" % i for i in range(20)],
                          universe_source="test", batch_size=5)
    assert len(store.next_batch("sec_companyfacts", batch_size=5)) == 5


def test_rc04_hard_max_batch_size_enforced():
    assert cf.clamp_batch_size(5, hard_max=10) == 5
    assert cf.clamp_batch_size(99, hard_max=10) == 10   # hard ceiling
    assert cf.clamp_batch_size(0, hard_max=10) == 1     # floor
    assert cf.clamp_batch_size(None, hard_max=10, default=5) == 5


def test_rc05_daily_batch_cap_enforced():
    assert rt.sec_continuation_stop_reason(
        is_complete=False, completed_batches_today=4,
        consecutive_no_progress=0, daily_cap=4,
        no_progress_max=2) == "DAILY_BATCH_CAP_REACHED"
    assert rt.sec_continuation_stop_reason(
        is_complete=False, completed_batches_today=3,
        consecutive_no_progress=0, daily_cap=4, no_progress_max=2) is None


def test_rc06_no_progress_stop_enforced():
    assert rt.sec_continuation_stop_reason(
        is_complete=False, completed_batches_today=0,
        consecutive_no_progress=2, daily_cap=4,
        no_progress_max=2) == "NO_PROGRESS_LIMIT"


def test_rc07_progress_resets_no_progress_state(tmp_path):
    store = acq.CampaignStore(str(tmp_path / "c.sqlite"))
    store.ensure_campaign("sec_companyfacts", kind="k", universe=["A", "B"],
                          universe_source="t", batch_size=1)
    store.record_batch("sec_companyfacts", run_date="2026-08-01",
                       progress=False)
    store.record_batch("sec_companyfacts", run_date="2026-08-01",
                       progress=False)
    assert store.consecutive_no_progress("sec_companyfacts") == 2
    store.record_batch("sec_companyfacts", run_date="2026-08-01", progress=True)
    assert store.consecutive_no_progress("sec_companyfacts") == 0


def test_rc08_duplicate_continuation_suppressed(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"))
    cfg = {"autonomy": {"collect_drain": {"companyfacts_continuation": {
        "enabled": True, "bootstrap_priority": 5, "daily_completed_batch_cap": 4,
        "max_consecutive_no_progress_batches": 2}}},
        "production": {"campaign_db": str(tmp_path / "c.sqlite")}}
    j1 = rt.ensure_companyfacts_bootstrap(q, cfg)
    j2 = rt.ensure_companyfacts_bootstrap(q, cfg)
    assert j1 is not None and j2 is None       # second call is a no-op
    lane_jobs = [j for j in q.list_jobs(limit=50)
                 if j.lane == "acq.sec_companyfacts"]
    assert len(lane_jobs) == 1


def test_rc09_restart_resumes_from_durable_cursor(tmp_path):
    path = str(tmp_path / "c.sqlite")
    universe = ["T%03d" % i for i in range(20)]
    store = acq.CampaignStore(path)
    store.ensure_campaign("sec_companyfacts", kind="k", universe=universe,
                          universe_source="t", batch_size=5)
    first = store.next_batch("sec_companyfacts", batch_size=5)
    store.record_results("sec_companyfacts", succeeded=first)
    del store
    store2 = acq.CampaignStore(path)               # restart
    cov = store2.coverage("sec_companyfacts")
    assert cov["completed_symbol_count"] == 5
    second = store2.next_batch("sec_companyfacts", batch_size=5)
    assert set(second).isdisjoint(set(first))      # never re-issues completed


# =========================================================================== #
# Raw artifact preservation (10-13)
# =========================================================================== #
def _archive(tmp_path):
    root = tmp_path / "out"
    return RawArchive(root / "raw", root, set(), 33554432), root


def _store_raw(archive, content, native="companyfacts|CIK0000320193"):
    return archive.store(source_id="sec_edgar", content=content, extension="json",
                         retrieved_at="2026-08-01T00:00:00+00:00",
                         business_date="2026-08-01", source_native_id=native,
                         request_fp="fp", content_type="application/json",
                         http_status=200, retry_count=0, published_at=None,
                         license_note="public")


def test_rc10_raw_artifacts_atomic_writes(tmp_path):
    archive, root = _archive(tmp_path)
    rec = _store_raw(archive, b'{"cik":320193}')
    p = root / rec["storage_path"]
    assert p.exists() and p.read_bytes() == b'{"cik":320193}'
    # atomic completion: no leftover .tmp; metadata sidecar written
    assert list(p.parent.glob("*.tmp")) == []
    assert (p.parent / ("%s.metadata.json" % rec["raw_object_id"])).exists()


def test_rc11_artifact_hashes_deterministic(tmp_path):
    archive, _ = _archive(tmp_path)
    content = b'{"cik":320193,"facts":{}}'
    rec = _store_raw(archive, content)
    assert rec["content_hash"] == sha256_hex(content)
    assert sha256_hex(content) == sha256_hex(content)


def test_rc12_identical_documents_idempotent(tmp_path):
    archive, root = _archive(tmp_path)
    content = b'{"same":1}'
    r1 = _store_raw(archive, content)
    r2 = _store_raw(archive, content)
    assert r1["raw_object_id"] == r2["raw_object_id"]
    assert r2["duplicate"] is True
    assert archive.duplicates_prevented == 1
    files = [p for p in (root / "raw").rglob("*.json")
             if not p.name.endswith(".metadata.json")]
    assert len(files) == 1               # single stored object


def test_rc13_changed_documents_not_overwritten(tmp_path):
    archive, root = _archive(tmp_path)
    r1 = _store_raw(archive, b'{"v":1}')
    r2 = _store_raw(archive, b'{"v":2}')
    assert r1["raw_object_id"] != r2["raw_object_id"]
    files = [p for p in (root / "raw").rglob("*.json")
             if not p.name.endswith(".metadata.json")]
    assert len(files) == 2               # both versions preserved (immutable)


# =========================================================================== #
# PIT materialization (14-19)
# =========================================================================== #
def test_rc14_accession_level_dedup(tmp_path):
    fact = ("Assets", "USD", 1000.0, "2021-12-31", 2021, "FY", "2022-03-01",
            "acc-1", "10-K")
    doc = _cf_doc("0000000001", [fact, fact])   # duplicated in the doc
    payloads = cf.parse_companyfacts(doc)
    assets = [p for p in payloads if p["concept"] == "Assets"]
    assert len(assets) == 1


def test_rc15_filed_date_controls_availability():
    store = _store_from_docs([_cf_doc("0000000001", [
        ("Assets", "USD", 1000.0, "2021-12-31", 2021, "FY", "2022-03-01",
         "a1", "10-K")])])
    assert store.as_of("0000000001", "assets", "2021-FY", "2022-01-01") is None
    obs = store.as_of("0000000001", "assets", "2021-FY", "2022-06-01")
    assert obs is not None and obs.value == 1000.0


def test_rc16_amendments_do_not_leak_backward():
    store = _store_from_docs([_cf_doc("0000000001", [
        ("Assets", "USD", 100.0, "2021-12-31", 2021, "FY", "2022-03-01", "a1",
         "10-K"),
        ("Assets", "USD", 120.0, "2021-12-31", 2021, "FY", "2022-09-01", "a2",
         "10-K/A")])])
    early = store.as_of("0000000001", "assets", "2021-FY", "2022-05-01")
    late = store.as_of("0000000001", "assets", "2021-FY", "2022-10-01")
    assert early.value == 100.0          # original only, no future restatement
    assert late.value == 120.0           # restatement visible only after filed


def test_rc17_fiscal_period_identity_preserved():
    store = _store_from_docs([_cf_doc("0000000001", [
        ("Assets", "USD", 100.0, "2020-12-31", 2020, "FY", "2021-03-01", "a1",
         "10-K"),
        ("Assets", "USD", 110.0, "2021-12-31", 2021, "FY", "2022-03-01", "a2",
         "10-K")])])
    assert store.as_of("0000000001", "assets", "2020-FY", "2022-06-01").value \
        == 100.0
    assert store.as_of("0000000001", "assets", "2021-FY", "2022-06-01").value \
        == 110.0


def test_rc18_units_handled_deterministically():
    assert cf.normalize_unit("USD") == ("USD", True)
    assert cf.normalize_unit("shares")[1] is False
    # a non-USD (shares) fact for a target tag is NOT materialized as monetary.
    doc = _cf_doc("0000000001", [
        ("Assets", "USD", 1000.0, "2021-12-31", 2021, "FY", "2022-03-01", "a1",
         "10-K"),
        ("Assets", "shares", 5.0, "2021-12-31", 2021, "FY", "2022-03-01", "a2",
         "10-K")])
    payloads = cf.parse_companyfacts(doc)
    assert all(p["unit"] == "USD" for p in payloads)


def test_rc19_missing_concepts_explicit():
    store = _store_from_docs([_cf_doc("0000000001", [
        ("Assets", "USD", 1000.0, "2021-12-31", 2021, "FY", "2022-03-01", "a1",
         "10-K")])])
    miss = store.missing_concepts("gross_profitability")
    assert "revenue" in miss and "cost_of_revenue" in miss
    r = fsig.gross_profitability(store, cik="0000000001", fiscal_key="2021-FY",
                                 as_of="2022-06-01")
    assert r["value"] is None and set(r["missing"])


# =========================================================================== #
# Fundamental signals (20-22)
# =========================================================================== #
def test_rc20_gross_profitability_fallback_order():
    # BLOCKER 3: Gross Profit / Assets is PREFERRED when both paths are present.
    both = _store_from_docs([_cf_doc("0000000001", [
        ("Assets", "USD", 1000.0, "2021-12-31", 2021, "FY", "2022-03-01", "a1",
         "10-K"),
        ("GrossProfit", "USD", 250.0, "2021-12-31", 2021, "FY", "2022-03-01",
         "a1", "10-K"),
        ("Revenues", "USD", 500.0, "2021-12-31", 2021, "FY", "2022-03-01", "a1",
         "10-K"),
        ("CostOfRevenue", "USD", 200.0, "2021-12-31", 2021, "FY", "2022-03-01",
         "a1", "10-K")])])
    r = fsig.gross_profitability(both, cik="0000000001", fiscal_key="2021-FY",
                                 as_of="2022-06-01")
    assert r["basis"] == "gross_profit" and abs(r["value"] - 0.25) < 1e-9
    # Revenue-minus-cost is used ONLY when Gross Profit is unavailable.
    fb = _store_from_docs([_cf_doc("0000000002", [
        ("Assets", "USD", 1000.0, "2021-12-31", 2021, "FY", "2022-03-01", "b1",
         "10-K"),
        ("Revenues", "USD", 500.0, "2021-12-31", 2021, "FY", "2022-03-01", "b1",
         "10-K"),
        ("CostOfRevenue", "USD", 200.0, "2021-12-31", 2021, "FY", "2022-03-01",
         "b1", "10-K")])])
    r2 = fsig.gross_profitability(fb, cik="0000000002", fiscal_key="2021-FY",
                                  as_of="2022-06-01")
    assert r2["basis"] == "revenue_minus_cost_fallback" \
        and abs(r2["value"] - 0.3) < 1e-9
    # Missing Assets -> no signal (never imputed).
    noa = _store_from_docs([_cf_doc("0000000003", [
        ("GrossProfit", "USD", 250.0, "2021-12-31", 2021, "FY", "2022-03-01",
         "c1", "10-K")])])
    r3 = fsig.gross_profitability(noa, cik="0000000003", fiscal_key="2021-FY",
                                  as_of="2022-06-01")
    assert r3["value"] is None and r3["missing"] == ["assets"]
    # Inconsistent fiscal periods do NOT combine: a 2020 Gross Profit is not used
    # for the 2021 fiscal key (numerator/denominator share one period).
    xper = _store_from_docs([_cf_doc("0000000004", [
        ("Assets", "USD", 1000.0, "2021-12-31", 2021, "FY", "2022-03-01", "d1",
         "10-K"),
        ("GrossProfit", "USD", 400.0, "2020-12-31", 2020, "FY", "2021-03-01",
         "d0", "10-K")])])
    r4 = fsig.gross_profitability(xper, cik="0000000004", fiscal_key="2021-FY",
                                  as_of="2022-06-01")
    assert r4["value"] is None


def test_rc21_asset_growth_comparable_periods():
    store = _store_from_docs([_cf_doc("0000000001", [
        ("Assets", "USD", 100.0, "2020-12-31", 2020, "FY", "2021-03-01", "a1",
         "10-K"),
        ("Assets", "USD", 110.0, "2021-12-31", 2021, "FY", "2022-03-01", "a2",
         "10-K")])])
    cur = store.latest_fiscal_key("0000000001", "2022-06-01")
    prior = store.prior_fiscal_key("0000000001", "2022-06-01", cur)
    assert cur == "2021-FY" and prior == "2020-FY"
    r = fsig.asset_growth(store, cik="0000000001", fiscal_key=cur,
                          prior_fiscal_key=prior, as_of="2022-06-01")
    assert abs(r["value"] - 0.10) < 1e-9


def test_rc22_balance_sheet_quality_registered_formula():
    store = _store_from_docs([_cf_doc("0000000001", [
        ("Assets", "USD", 1000.0, "2021-12-31", 2021, "FY", "2022-03-01", "a1",
         "10-K"),
        ("Liabilities", "USD", 400.0, "2021-12-31", 2021, "FY", "2022-03-01",
         "a1", "10-K"),
        ("StockholdersEquity", "USD", 600.0, "2021-12-31", 2021, "FY",
         "2022-03-01", "a1", "10-K")])])
    r = fsig.balance_sheet_quality(store, cik="0000000001", fiscal_key="2021-FY",
                                   as_of="2022-06-01")
    assert r["basis"] == "equity_over_assets"
    assert abs(r["value"] - 0.6) < 1e-9 and abs(r["leverage"] - 0.4) < 1e-9


# =========================================================================== #
# Fundamental candidate lifecycle (23-28)
# =========================================================================== #
def _registry(tmp_path):
    reg = tt.CandidateRegistry(str(tmp_path / "t.sqlite"))
    tt.seed_families(reg)
    return reg


def _fund_candidate(reg, feature="gross_profitability"):
    return tt._candidate_for_feature(reg, feature)


def test_rc23_insufficient_coverage_stays_data_hold(tmp_path):
    reg = _registry(tmp_path)
    # no coverage recorded sufficient -> generator emits nothing
    out = tt.generate_stage9_5_fundamental_followups(reg, STAGE9_CFG, queue=None)
    assert out["count"] == 0
    reg.close()


def test_rc24_complete_weak_becomes_rejected(tmp_path):
    reg = _registry(tmp_path)
    cand = _fund_candidate(reg)
    res = tt.ingest_completed_experiments(
        reg, STAGE9_CFG, completed=[{"job_id": "j-weak",
                                     "feature": "gross_profitability",
                                     "row": _row("weak")}])
    assert res["imported"] == 1
    assert reg.get(cand["candidate_id"])["lifecycle_state"] == tt.REJECTED
    reg.close()


def test_rc25_complete_strong_may_become_keep(tmp_path):
    reg = _registry(tmp_path)
    cand = _fund_candidate(reg)
    tt.ingest_completed_experiments(
        reg, STAGE9_CFG, completed=[{"job_id": "j-strong",
                                     "feature": "gross_profitability",
                                     "row": _row("strong")}])
    assert reg.get(cand["candidate_id"])["lifecycle_state"] == \
        tt.KEEP_FOR_RESEARCH
    reg.close()


def test_rc26_experiment_imported_exactly_once(tmp_path):
    reg = _registry(tmp_path)
    row = _row("weak")
    item = {"job_id": "j1", "feature": "gross_profitability", "row": row}
    r1 = tt.ingest_completed_experiments(reg, STAGE9_CFG, completed=[item])
    r2 = tt.ingest_completed_experiments(reg, STAGE9_CFG, completed=[item])
    assert r1["imported"] == 1 and r2["imported"] == 0 and r2["skipped"] == 1
    reg.close()


def test_rc27_no_model_automatically_promoted(tmp_path):
    reg = _registry(tmp_path)
    cand = _fund_candidate(reg)
    tt.ingest_completed_experiments(
        reg, STAGE9_CFG, completed=[{"job_id": "j", "feature":
                                     "gross_profitability", "row": _row("strong")}])
    st = reg.get(cand["candidate_id"])["lifecycle_state"]
    # KEEP_FOR_RESEARCH is the strongest an autonomous ingest can reach; there is
    # NO automatic promoted / live state in the lifecycle.
    assert st == tt.KEEP_FOR_RESEARCH
    assert "PROMOTED" not in STAGE9_CFG["lifecycle_states"]
    assert "READY_FOR_MANUAL_REVIEW" != st
    reg.close()


def test_rc28_no_shadow_book_backfilled(tmp_path):
    reg = _registry(tmp_path)
    tt.ingest_completed_experiments(
        reg, STAGE9_CFG, completed=[{"job_id": "j", "feature":
                                     "gross_profitability", "row": _row("strong")}])
    # ingest alone NEVER activates or backfills a shadow book.
    counts = reg.counts_by_state()
    assert counts.get(tt.SHADOW_BOOK_ACTIVE, 0) == 0
    reg.close()


# =========================================================================== #
# Safety invariants (29-30)
# =========================================================================== #
def test_rc29_operational_ledgers_byte_identical(tmp_path):
    led = tmp_path / "op_ledger"
    led.mkdir()
    (led / "book.json").write_text('{"nav": 100000}', encoding="utf-8")
    cfg = {"operational_ledger_roots": [str(led)]}
    before = ing.ledger_fingerprints(cfg)
    # exercise the Stage 9.5 read-only capabilities.
    store, panel, c2t = _pit_and_panel(noise=0.02, drift=0.0004, n_ciks=8,
                                       years=range(2015, 2021))
    cf.coverage_report([], signals=list(fsig.SIGNALS))
    cf.pit_observation_digest(store)
    fev.evaluate_fundamental_evidence(store, panel, "gross_profitability",
                                      cik_to_ticker=c2t, horizon_days=63,
                                      cfg={"bootstrap_resamples": 50})
    after = ing.ledger_fingerprints(cfg)
    assert before == after


def test_rc30_cadence_tasks_declared_disabled_invariant():
    wt = STAGE8_CFG["windows_tasks"]
    assert set(wt["cadence_tasks"]) == {
        "AlphaAgent-Collect", "AlphaAgent-Morning-Report",
        "AlphaAgent-PostClose-Report", "AlphaAgent-Watchdog"}
    assert wt["control_tasks"] == ["AlphaAgent-Telegram"]
    assert wt["all_disabled_until_final_validation"] is True


# =========================================================================== #
# Wiring / activation invariants (extra coverage of the Stage 9.5 contract)
# =========================================================================== #
def test_rc31_config_activation_and_allowlist():
    cont = STAGE8_CFG["autonomy"]["collect_drain"]["companyfacts_continuation"]
    assert cont["enabled"] is True
    assert cont["allowed_origins"] == ["campaign-continuation"]
    assert cont["allowed_lane_prefixes"] == ["acq.sec_companyfacts"]
    assert cont["allowed_categories"] == ["DATA_ACQUISITION"]
    assert cont["batch_size"] == 5 and cont["max_batch_size"] == 10
    assert cont["daily_completed_batch_cap"] == 4
    assert cont["max_consecutive_no_progress_batches"] == 2
    # PASS A admits the bootstrap lane; campaign registered with companyfacts runner
    la = STAGE8_CFG["autonomy"]["collect_drain"]["allowed_lane_prefixes"]
    assert "acq.sec_companyfacts" in la
    camps = {c["id"]: c for c in STAGE8_CFG["production"]["campaigns"]}
    assert camps["sec_companyfacts"]["runner"] == "companyfacts"
    s95 = STAGE9_CFG["stage9_5"]
    assert s95["enabled"] is True
    assert s95["fundamental_experiments"]["enabled"] is True
    assert len(s95["companyfacts_campaign"]["first_concepts"]) == 11


def test_rc32_companyfacts_batch_flags_focus_lane():
    flags = cf.companyfacts_batch_flags(cf.FIRST_CONCEPTS)
    assert flags["collect_companyfacts"] is True
    for off in ("collect_submissions", "collect_companyconcept",
                "collect_form4_transactions", "collect_form8k_earnings",
                "collect_full_index", "probe_bulk_archives"):
        assert flags[off] is False
    assert flags["filing_window_business_days"] == 0
    assert set(flags["companyfacts_concepts"]) == set(cf.FIRST_CONCEPTS)


def test_rc33_pit_observation_digest_replay_stable():
    docs = [_cf_doc("0000000001", [
        ("Assets", "USD", 100.0, "2021-12-31", 2021, "FY", "2022-03-01", "a1",
         "10-K")])]
    d1 = cf.pit_observation_digest(_store_from_docs(docs))
    d2 = cf.pit_observation_digest(_store_from_docs(docs))
    assert d1 == d2 and len(d1) == 64
    docs2 = docs + [_cf_doc("0000000002", [
        ("Assets", "USD", 200.0, "2021-12-31", 2021, "FY", "2022-03-01", "b1",
         "10-K")])]
    assert cf.pit_observation_digest(_store_from_docs(docs2)) != d1


# =========================================================================== #
# Stage 9.5 RELEASE CLOSURE - separate SAFE ACQUISITION (9.5A) from
# SURVIVORSHIP-SAFE HISTORICAL EVALUATION (9.5B). Targeted properties 1-12:
#  1  current-universe acquisition can continue independently
#  2  historical experiment generation is disabled without safe mapping
#  3  aggregate current-CIK coverage cannot unlock historical evaluation
#  4  per-date coverage gates are enforced
#  5  gross-profitability fallback order is correct (see test_rc20)
#  6  decile spread uses 10 percent
#  7  insufficient cross-sectional size blocks decile evidence
#  8  asset-growth periods are comparable
#  9  campaign scope changes are append-only
#  10 previous campaign history remains queryable
#  11 no candidate leaves DATA_HOLD prematurely
#  12 no operational trading state changes
# =========================================================================== #
def _hist_universe(ciks, dates):
    """A synthetic survivorship-safe historical universe + ticker->CIK map that
    is FULLY present on every rebalance date (used to prove the per-date gate can
    also PASS when a real mapping is supplied)."""
    univ = {d: list(ciks) for d in dates}
    c2t = {d: {c: "T%s" % c for c in ciks} for d in dates}
    return univ, c2t


def _fund_store(ciks, dates):
    """A PIT store where every cik has Assets + GrossProfit filed before the
    first rebalance date (gross_profitability computable on every date)."""
    recs = []
    for c in ciks:
        for tag, v in (("Assets", 1000.0), ("GrossProfit", 250.0)):
            recs.append({"event_type": "XBRL_FACT", "normalized_payload": {
                "cik": c, "concept": tag, "unit": "USD", "value": v,
                "period_end": "2009-12-31", "fy": 2009, "fp": "FY",
                "filed": "2010-03-01", "accession": "a%s" % c, "form": "10-K"}})
    return cf.owned_pit_store(recs)


def test_rcc01_current_universe_acquisition_independent(tmp_path):
    # 9.5A acquisition runs (materializes PIT facts) while 9.5B historical
    # evaluation stays blocked - the two are decoupled.
    docs = {"0000000001": _cf_doc("0000000001", [
        ("Assets", "USD", 1000.0, "2021-12-31", 2021, "FY", "2022-03-01", "a1",
         "10-K")])}
    store = acq.CampaignStore(str(tmp_path / "c.sqlite"))
    cf.ensure_campaign(store, ["0000000001"], universe_source="test",
                       batch_size=5)
    out = cf.run_batch(store, fetch_fn=lambda c: docs.get(c),
                       run_date="2026-08-01")
    assert out["status"] == "RAN" and out["records_added"] >= 1
    # ... yet the 9.5B historical experiment remains disallowed.
    gate = fr.historical_fundamental_experiment_allowed(STAGE9_CFG)
    assert gate["allowed"] is False


def test_rcc02_historical_experiment_disabled_without_mapping(tmp_path):
    gate = fr.historical_fundamental_experiment_allowed(STAGE9_CFG)
    assert gate["allowed"] is False
    assert gate["mapping_available"] is False
    assert gate["diagnostic"] == fr.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY
    reg = tt.CandidateRegistry(str(tmp_path / "t.sqlite"))
    tt.seed_families(reg)
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"))
    res = tt.generate_stage9_5_fundamental_followups(reg, STAGE9_CFG, queue=q)
    assert res["count"] == 0
    assert res["blocker"] == fr.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY
    assert [j for j in q.list_jobs(limit=50)
            if j.lane == "tournament.stage9_5_fundamental"] == []
    reg.close()


def test_rcc03_aggregate_current_cik_cannot_unlock(tmp_path):
    reg = tt.CandidateRegistry(str(tmp_path / "t.sqlite"))
    tt.seed_families(reg)
    cand = tt._candidate_for_feature(reg, "gross_profitability")
    # Force aggregate coverage to look "sufficient" (many current CIKs).
    reg.record_data_coverage(
        cand["candidate_id"],
        coverage={"ciks_ready_gross_profitability": 999},
        evidence_date="2026-08-01",
        data_dependency="point_in_time_fundamentals", job_id="j",
        sufficient=True, next_action="n/a")
    assert (reg.latest_data_coverage(cand["candidate_id"]) or {}).get(
        "sufficient") is True
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"))
    res = tt.generate_stage9_5_fundamental_followups(reg, STAGE9_CFG, queue=q)
    st = reg.get(cand["candidate_id"])["lifecycle_state"]
    reg.close()
    # Aggregate current-CIK coverage does NOT unlock a survivorship-biased
    # historical experiment; the candidate is never promoted.
    assert res["count"] == 0
    assert res["blocker"] == fr.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY
    assert st in (tt.PROPOSED, tt.DATA_HOLD, tt.TESTING)
    assert st != tt.KEEP_FOR_RESEARCH


def test_rcc04_per_date_coverage_gates_enforced():
    dates = ["20%02d-06-30" % y for y in range(11, 26)]   # 15 dates
    ciks = [str(1000 + i) for i in range(25)]
    store = _fund_store(ciks, dates)
    univ, c2t = _hist_universe(ciks, dates)
    thr = fr.readiness_thresholds(STAGE9_CFG)
    # (a) mapping unavailable -> NOT ready regardless of coverage.
    r_off = fr.per_rebalance_readiness(
        store, "gross_profitability", rebalance_dates=dates,
        historical_universe_by_date=univ, cik_to_ticker_by_date=c2t,
        mapping_available=False, thresholds=thr)
    assert r_off["sufficient"] is False
    assert r_off["blocker"] == fr.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY
    # (b) mapping available + full per-date coverage -> ready.
    r_on = fr.per_rebalance_readiness(
        store, "gross_profitability", rebalance_dates=dates,
        historical_universe_by_date=univ, cik_to_ticker_by_date=c2t,
        mapping_available=True, thresholds=thr)
    assert r_on["sufficient"] is True and r_on["valid_scored_periods"] == 15
    # (c) mapping available but only 5 dates covered -> per-date gate blocks.
    thin_univ = {d: (univ[d] if i < 5 else []) for i, d in enumerate(dates)}
    thin_c2t = {d: (c2t[d] if i < 5 else {}) for i, d in enumerate(dates)}
    r_thin = fr.per_rebalance_readiness(
        store, "gross_profitability", rebalance_dates=dates,
        historical_universe_by_date=thin_univ, cik_to_ticker_by_date=thin_c2t,
        mapping_available=True, thresholds=thr)
    assert r_thin["sufficient"] is False
    assert r_thin["valid_scored_periods"] == 5


def test_rcc05_decile_spread_uses_ten_percent():
    store, panel, c2t = _pit_and_panel(noise=0.004, drift=0.0006)
    dec = fev.evaluate_fundamental_evidence(
        store, panel, "gross_profitability", cik_to_ticker=c2t,
        horizon_days=63, cfg={"bootstrap_resamples": 50})
    assert abs(dec["spread_quantile"] - 0.10) < 1e-9
    assert dec["is_decile_spread"] is True and dec["decile_spread_mean"] \
        is not None
    quint = fev.evaluate_fundamental_evidence(
        store, panel, "gross_profitability", cik_to_ticker=c2t,
        horizon_days=63, quantile=0.20, cfg={"bootstrap_resamples": 50})
    # A quintile evaluation must NOT masquerade as a decile spread.
    assert quint["is_decile_spread"] is False
    assert quint["decile_spread_mean"] is None
    assert quint["quantile_spread_mean"] is not None


def test_rcc06_insufficient_cross_section_blocks_decile():
    # Fewer than ten names -> no genuine decile spread (thin), keeps DATA_HOLD.
    store, panel, c2t = _pit_and_panel(noise=0.02, drift=0.0004, n_ciks=6,
                                       years=range(2015, 2021))
    row = fev.evaluate_fundamental_evidence(
        store, panel, "gross_profitability", cik_to_ticker=c2t,
        horizon_days=63, cfg={"bootstrap_resamples": 50})
    assert row["decile_spread_mean"] is None and row["spread_t"] is None


def test_rcc07_asset_growth_comparable_quarters():
    store = _store_from_docs([_cf_doc("0000000001", [
        ("Assets", "USD", 100.0, "2024-03-31", 2024, "Q1", "2024-05-01", "q1a",
         "10-Q"),
        ("Assets", "USD", 110.0, "2024-06-30", 2024, "Q2", "2024-08-01", "q2a",
         "10-Q"),
        ("Assets", "USD", 130.0, "2025-06-30", 2025, "Q2", "2025-08-01", "q2b",
         "10-Q")])])
    cur = store.latest_fiscal_key("0000000001", "2025-09-01")
    prior = store.prior_fiscal_key("0000000001", "2025-09-01", cur)
    assert cur == "2025-Q2"
    # Comparable prior is prior-YEAR Q2, never the adjacent 2024-Q1.
    assert prior == "2024-Q2"
    r = fsig.asset_growth(store, cik="0000000001", fiscal_key=cur,
                          prior_fiscal_key=prior, as_of="2025-09-01")
    assert abs(r["value"] - (130.0 - 110.0) / 110.0) < 1e-9


def test_rcc08_asset_growth_rejects_adjacent_quarter():
    # 2025-Q2 present but NO 2024-Q2: no adjacent-quarter fallback -> no signal.
    store = _store_from_docs([_cf_doc("0000000001", [
        ("Assets", "USD", 120.0, "2025-03-31", 2025, "Q1", "2025-05-01", "q1",
         "10-Q"),
        ("Assets", "USD", 130.0, "2025-06-30", 2025, "Q2", "2025-08-01", "q2",
         "10-Q")])])
    cur = store.latest_fiscal_key("0000000001", "2025-09-01")
    assert cur == "2025-Q2"
    assert store.prior_fiscal_key("0000000001", "2025-09-01", cur) is None
    r = fsig.asset_growth(store, cik="0000000001", fiscal_key=cur,
                          prior_fiscal_key=None, as_of="2025-09-01")
    assert r["value"] is None


def test_rcc09_campaign_scope_change_append_only(tmp_path):
    store = acq.CampaignStore(str(tmp_path / "c.sqlite"))
    store.ensure_campaign_revision(
        "sec_companyfacts", kind="k", universe=["A", "B", "C"],
        universe_source="survivorship", universe_fingerprint="FP_SURV",
        batch_size=5)
    first = store.next_batch("sec_companyfacts", batch_size=2)
    store.record_results("sec_companyfacts", succeeded=first)
    # A genuine scope change supersedes append-only (never deletes rows).
    rev = store.ensure_campaign_revision(
        "sec_companyfacts", kind="k", universe=["A", "D"],
        universe_source="current", universe_fingerprint="FP_CURR",
        batch_size=5)
    assert rev["superseded"] == "sec_companyfacts"
    assert store.active_revision("sec_companyfacts") == rev["campaign_id"]
    assert rev["campaign_id"] != "sec_companyfacts"
    # Old campaign symbol history is preserved and still queryable.
    old_cov = store.coverage("sec_companyfacts")
    assert old_cov["completed_symbol_count"] == len(first)
    assert old_cov["status"] == "SUPERSEDED"
    assert old_cov["superseded_by"] == rev["campaign_id"]


def test_rcc10_previous_campaign_history_queryable(tmp_path):
    store = acq.CampaignStore(str(tmp_path / "c.sqlite"))
    store.ensure_campaign_revision(
        "sec_companyfacts", kind="k", universe=["A", "B"],
        universe_source="survivorship", universe_fingerprint="FP1",
        batch_size=5)
    store.ensure_campaign_revision(
        "sec_companyfacts", kind="k", universe=["A", "C"],
        universe_source="current", universe_fingerprint="FP2", batch_size=5)
    revs = store.campaign_revisions("sec_companyfacts")
    assert len(revs) == 2
    statuses = {r["campaign_id"]: r["status"] for r in revs}
    assert statuses["sec_companyfacts"] == "SUPERSEDED"
    active = [r for r in revs if r["status"] == "ACTIVE"]
    assert len(active) == 1 and active[0]["revision"] == 2
    # No same-scope reconcile creates a new revision (append-only, idempotent).
    store.ensure_campaign_revision(
        "sec_companyfacts", kind="k", universe=["A", "C", "E"],
        universe_source="current", universe_fingerprint="FP2", batch_size=5)
    assert len(store.campaign_revisions("sec_companyfacts")) == 2


def test_rcc11_no_candidate_leaves_data_hold_prematurely(tmp_path):
    reg = tt.CandidateRegistry(str(tmp_path / "t.sqlite"))
    tt.seed_families(reg)
    cand = tt._candidate_for_feature(reg, "gross_profitability")
    st0 = reg.get(cand["candidate_id"])["lifecycle_state"]
    # Even with aggregate coverage sufficient, the safety switch emits 0 jobs and
    # the candidate's state is UNCHANGED (no premature exit on current-survivor
    # data), never a KEEP/SHADOW promotion.
    reg.record_data_coverage(
        cand["candidate_id"], coverage={"x": 1}, evidence_date="2026-08-01",
        data_dependency="point_in_time_fundamentals", job_id="j",
        sufficient=True, next_action="n/a")
    tt.generate_stage9_5_fundamental_followups(reg, STAGE9_CFG, queue=None)
    st1 = reg.get(cand["candidate_id"])["lifecycle_state"]
    reg.close()
    assert st1 == st0
    assert st1 in (tt.PROPOSED, tt.DATA_HOLD, tt.TESTING)
    assert st1 not in (tt.KEEP_FOR_RESEARCH, tt.SHADOW_BOOK_ACTIVE)


def test_rcc12_no_operational_trading_state_changes(tmp_path):
    led = tmp_path / "op_ledger"
    led.mkdir()
    (led / "book.json").write_text('{"nav": 100000}', encoding="utf-8")
    cfg = {"operational_ledger_roots": [str(led)]}
    before = ing.ledger_fingerprints(cfg)
    # Exercise every release-closure capability.
    fr.historical_fundamental_experiment_allowed(STAGE9_CFG)
    dates = ["20%02d-06-30" % y for y in range(11, 26)]
    ciks = [str(1000 + i) for i in range(12)]
    st = _fund_store(ciks, dates)
    univ, c2t = _hist_universe(ciks, dates)
    fr.per_rebalance_readiness(st, "gross_profitability", rebalance_dates=dates,
                               historical_universe_by_date=univ,
                               cik_to_ticker_by_date=c2t, mapping_available=True,
                               thresholds=fr.readiness_thresholds(STAGE9_CFG))
    cstore = acq.CampaignStore(str(tmp_path / "c.sqlite"))
    cstore.ensure_campaign_revision("sec_companyfacts", kind="k",
                                    universe=["A"], universe_source="s",
                                    universe_fingerprint="FP", batch_size=5)
    assert ing.ledger_fingerprints(cfg) == before
