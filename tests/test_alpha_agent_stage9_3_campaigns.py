"""Stage 9.3 - CONTROLLED EVIDENCE CAMPAIGNS and DATA_HOLD REDUCTION.

Deterministic tests for the controlled SEC Form 4 / 8-K campaign continuation
(scoped allowlist, daily-batch cap, no-progress stop, restart-safe idempotency),
the owned-coverage readiness measures, the point-in-time fundamentals / sector
builders, the honest candidate-lifecycle gate (no promotion, no backfill) and the
read-only campaign surfaces - proving the campaigns are BOUNDED, RESTART-SAFE and
MEASURABLE without weakening a gate, forcing a candidate, fabricating data or
mutating any operational trading state.

Covers the 25 required properties (PART 12).
"""
import json
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import autonomous_research as ar  # noqa: E402
from paper_trader.alpha_agent import acquisition_campaign as ac  # noqa: E402
from paper_trader.alpha_agent import runtime as rt  # noqa: E402
from paper_trader.alpha_agent import tournament as tt  # noqa: E402
from paper_trader.alpha_agent import pit_fundamentals as pf  # noqa: E402
from paper_trader.alpha_agent import pit_sector as ps  # noqa: E402
from paper_trader.alpha_agent import report_renderer as rr  # noqa: E402
from paper_trader.alpha_agent import telegram_control as tc  # noqa: E402
from paper_trader.alpha_agent.collectors import sec_edgar as se  # noqa: E402

_CLK = "2026-08-01T12:00:00+00:00"


def _clock():
    return _CLK


def _store(tmp_path):
    s = ac.CampaignStore(str(tmp_path / "campaigns.sqlite"), clock=_clock)
    s.ensure_campaign("sec_form4_8k", kind="sec_cik_filings",
                      universe=["A", "B", "C", "D"],
                      universe_source="test", batch_size=2)
    return s


def _ok(job):
    return ar.OUTCOME_COMPLETED, {"real_work": "unit"}


_SEC_TRIPLE = dict(origins=["campaign-continuation"],
                   lane_prefixes=["acq.sec_form4_8k"],
                   categories=["DATA_ACQUISITION"])


def _seed_continuations(q):
    """One correctly-scoped SEC continuation + three mis-scoped ones."""
    good = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_form4_8k",
                     payload={"campaign": "sec_form4_8k", "seq": 1},
                     origin="campaign-continuation")
    wrong_lane = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.norgate_prices",
                           payload={"campaign": "norgate_prices"},
                           origin="campaign-continuation")
    wrong_cat = q.enqueue(ar.CAT_DATA_VALIDATION, lane="acq.sec_form4_8k",
                          payload={"x": 1}, origin="campaign-continuation")
    wrong_lane2 = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="tournament.foo",
                            payload={"x": 1}, origin="campaign-continuation")
    return good, wrong_lane, wrong_cat, wrong_lane2


# --------------------------------------------------------------------------- #
# 1-2: scoped campaign-continuation allowlist.
# --------------------------------------------------------------------------- #
def test_p01_only_scoped_sec_continuation_is_claimable(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    good, _, _, _ = _seed_continuations(q)
    rep = ar.drain_jobs(q, {ar.CAT_DATA_ACQUISITION: _ok,
                            ar.CAT_DATA_VALIDATION: _ok},
                        max_jobs=1, **_SEC_TRIPLE)
    assert rep["jobs_claimed"] == 1 and rep["jobs_completed"] == 1
    assert rep["job_ids"] == [good]
    assert q.get(good).state == ar.STATE_COMPLETED


def test_p02_other_campaign_continuation_jobs_untouched(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    good, wl, wc, wl2 = _seed_continuations(q)
    ar.drain_jobs(q, {ar.CAT_DATA_ACQUISITION: _ok, ar.CAT_DATA_VALIDATION: _ok},
                  max_jobs=1, **_SEC_TRIPLE)
    for jid in (wl, wc, wl2):
        j = q.get(jid)
        assert j.state == ar.STATE_QUEUED, (jid, j.state)
        assert j.attempts == 0  # never claimed -> never incremented


# --------------------------------------------------------------------------- #
# 3-6: bounded caps derived from the durable batch log + pure decision table.
# --------------------------------------------------------------------------- #
def test_p03_daily_batch_cap_enforced(tmp_path):
    s = _store(tmp_path)
    for _ in range(6):
        s.record_batch("sec_form4_8k", run_date="2026-08-01", progress=True)
    assert s.batches_on_date("sec_form4_8k", "2026-08-01") == 6
    assert s.batches_on_date("sec_form4_8k", "2026-08-02") == 0  # per calendar day
    assert rt.sec_continuation_stop_reason(
        is_complete=False, completed_batches_today=6, consecutive_no_progress=0,
        daily_cap=6, no_progress_max=2) == "DAILY_BATCH_CAP_REACHED"
    assert rt.sec_continuation_should_chain(
        is_complete=False, completed_batches_today=6, daily_cap=6) is False


def test_p04_no_progress_stop_enforced(tmp_path):
    s = _store(tmp_path)
    for _ in range(2):
        s.record_batch("sec_form4_8k", run_date="2026-08-01", progress=False,
                       stop_reason="SOURCE_BLOCKED")
    assert s.consecutive_no_progress("sec_form4_8k") == 2
    assert rt.sec_continuation_stop_reason(
        is_complete=False, completed_batches_today=2, consecutive_no_progress=2,
        daily_cap=6, no_progress_max=2) == "NO_PROGRESS_LIMIT"


def test_p05_progress_resets_no_progress_counter(tmp_path):
    s = _store(tmp_path)
    s.record_batch("sec_form4_8k", run_date="2026-08-01", progress=False)
    s.record_batch("sec_form4_8k", run_date="2026-08-01", progress=False)
    assert s.consecutive_no_progress("sec_form4_8k") == 2
    s.record_batch("sec_form4_8k", run_date="2026-08-01", progress=True)
    assert s.consecutive_no_progress("sec_form4_8k") == 0
    assert rt.sec_continuation_stop_reason(
        is_complete=False, completed_batches_today=3, consecutive_no_progress=0,
        daily_cap=6, no_progress_max=2) is None


def test_p06_completed_campaign_stops_continuation(tmp_path):
    s = _store(tmp_path)
    s.record_results("sec_form4_8k", succeeded=["A", "B", "C", "D"])
    cov = s.coverage("sec_form4_8k")
    assert cov["is_complete"] is True
    assert rt.sec_continuation_stop_reason(
        is_complete=True, completed_batches_today=0, consecutive_no_progress=0,
        daily_cap=6, no_progress_max=2) == "CAMPAIGN_COMPLETE"
    assert rt.sec_continuation_should_chain(
        is_complete=True, completed_batches_today=0, daily_cap=6) is False


# --------------------------------------------------------------------------- #
# 7-8: idempotency + restart-safe resume.
# --------------------------------------------------------------------------- #
def test_p07_duplicate_continuation_suppressed(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    a = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_form4_8k",
                  payload={"campaign": "sec_form4_8k", "seq": 7},
                  origin="campaign-continuation")
    b = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_form4_8k",
                  payload={"campaign": "sec_form4_8k", "seq": 7},
                  origin="campaign-continuation")
    assert a == b  # identical live job -> deduped
    assert q.counts_by_state().get("QUEUED") == 1


def test_p08_restart_resumes_same_campaign(tmp_path):
    path = str(tmp_path / "campaigns.sqlite")
    s1 = ac.CampaignStore(path, clock=_clock)
    s1.ensure_campaign("sec_form4_8k", kind="sec_cik_filings",
                       universe=["A", "B", "C", "D"], universe_source="test",
                       batch_size=2)
    s1.record_results("sec_form4_8k", succeeded=["A", "B"])
    s1.record_batch("sec_form4_8k", run_date="2026-08-01", progress=True,
                    metrics={"ciks_completed": 2})
    # a fresh store on the SAME db resumes the identical cursor + history
    s2 = ac.CampaignStore(path, clock=_clock)
    cov = s2.coverage("sec_form4_8k")
    assert cov["completed_symbol_count"] == 2 and cov["remaining_symbol_count"] == 2
    assert s2.batch_count("sec_form4_8k") == 1
    assert s2.next_batch("sec_form4_8k") == ["C", "D"]  # resumes uncompleted


# --------------------------------------------------------------------------- #
# 9-11: SEC parsing - accession dedup, purchase vs sale, 8-K Item 2.02 filter.
# --------------------------------------------------------------------------- #
def test_p09_sec_filings_accession_deduplicated():
    a = se.SecEdgarCollector._accession_from_filename(
        "edgar/data/320193/0000320193-24-000123.txt")
    b = se.SecEdgarCollector._accession_from_filename(
        "0000320193-24-000123.txt")
    assert a == b == "0000320193-24-000123"  # canonical, stable dedup key


_FORM4 = b"""<ownershipDocument>
 <documentType>4</documentType>
 <issuer><issuerCik>0000320193</issuerCik><issuerName>APPLE INC</issuerName>
  <issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer>
 <reportingOwner><reportingOwnerId><rptOwnerCik>0001234567</rptOwnerCik>
  <rptOwnerName>DOE JOHN</rptOwnerName></reportingOwnerId>
  <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
 </reportingOwner>
 <nonDerivativeTable>
  <nonDerivativeTransaction>
   <securityTitle><value>Common Stock</value></securityTitle>
   <transactionDate><value>2026-06-01</value></transactionDate>
   <transactionCoding><transactionFormType>4</transactionFormType>
    <transactionCode>P</transactionCode></transactionCoding>
   <transactionAmounts><transactionShares><value>100</value></transactionShares>
    <transactionPricePerShare><value>10</value></transactionPricePerShare>
    <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
   </transactionAmounts></nonDerivativeTransaction>
  <nonDerivativeTransaction>
   <securityTitle><value>Common Stock</value></securityTitle>
   <transactionDate><value>2026-06-02</value></transactionDate>
   <transactionCoding><transactionFormType>4</transactionFormType>
    <transactionCode>S</transactionCode></transactionCoding>
   <transactionAmounts><transactionShares><value>50</value></transactionShares>
    <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
   </transactionAmounts></nonDerivativeTransaction>
 </nonDerivativeTable>
</ownershipDocument>"""


def test_p10_open_market_purchases_distinguished_from_sales():
    doc = se.parse_form4_xml(_FORM4)
    assert doc is not None
    txns = doc["transactions"]
    by_code = {t["transaction_code"]: t for t in txns}
    assert "P" in by_code and "S" in by_code
    assert by_code["P"]["acquired_disposed"] == "A"  # open-market purchase
    assert by_code["S"]["acquired_disposed"] == "D"  # sale


def test_p11_8k_item_202_filtered_correctly():
    has = se.extract_item_202(
        b"<html><body>Item 2.02 Results of Operations. The company reported "
        b"diluted earnings of $1.23 per share.</body></html>")
    assert has["has_item_202"] is True
    assert has["eps_actual"] == 1.23
    nope = se.extract_item_202(
        b"<html><body>Item 5.02 Departure of Directors.</body></html>")
    assert nope["has_item_202"] is False


# --------------------------------------------------------------------------- #
# 12-13: PIT sector - filing availability dates, no current-GICS look-ahead.
# --------------------------------------------------------------------------- #
def test_p12_pit_sector_uses_filing_availability_dates():
    s = ps.PitSicSeries()
    s.add("0000320193", sic="3571", available_at="2015-02-01",
          provenance="10-K")
    # sector is known only ON/AFTER the filing availability date
    assert s.sector_as_of("0000320193", "2015-03-01") == "Technology"


def test_p13_current_gics_never_used_historically():
    s = ps.PitSicSeries()
    s.add("0000320193", sic="3571", available_at="2015-02-01")
    # BEFORE the filing was available there is NO classification (never the
    # current/later SIC applied backward).
    assert s.sector_as_of("0000320193", "2014-06-01") == ps.UNKNOWN
    assert s.is_financial_as_of("0000320193", "2014-06-01") is None


# --------------------------------------------------------------------------- #
# 14-16: PIT fundamentals - filed boundaries, amendments no backward leak,
# explicit missing concepts.
# --------------------------------------------------------------------------- #
def _fact(cik, tag, value, filed, form="10-K", fy=2023, fp="FY"):
    return {"event_type": "XBRL_FACT", "normalized_payload": {
        "cik": cik, "concept": tag, "unit": "USD", "value": value,
        "period_end": "2023-12-31", "filed": filed, "fy": fy, "fp": fp,
        "form": form}}


def test_p14_pit_fundamentals_preserve_filed_availability_boundary():
    st = pf.PitFundamentalsStore()
    st.add_records([_fact("0000320193", "Assets", 1000, "2024-02-01")])
    assert st.as_of("0000320193", "assets", "2023-FY", "2024-01-15") is None
    o = st.as_of("0000320193", "assets", "2023-FY", "2024-03-01")
    assert o is not None and o.value == 1000


def test_p15_amendments_do_not_leak_backward():
    st = pf.PitFundamentalsStore()
    st.add_records([
        _fact("0000320193", "Assets", 1000, "2024-02-01"),
        _fact("0000320193", "Assets", 1200, "2024-08-01", form="10-K/A")])
    # before the restatement the ORIGINAL value is returned
    assert st.as_of("0000320193", "assets", "2023-FY", "2024-03-01").value == 1000
    # after it, the restated value - never leaked backward
    assert st.as_of("0000320193", "assets", "2023-FY", "2024-09-01").value == 1200
    assert st.observation_count() == 2  # both preserved


def test_p16_missing_concepts_remain_explicit():
    st = pf.PitFundamentalsStore()
    st.add_records([
        _fact("0000320193", "Assets", 1000, "2024-02-01"),
        _fact("0000320193", "MadeUpTag", 5, "2024-02-01")])
    miss = st.missing_concepts("balance_sheet_quality")
    assert "liabilities" in miss and "stockholders_equity" in miss
    summ = st.coverage_summary()
    assert "MadeUpTag" in summ["unmapped_us_gaap_tags"]  # never silently dropped


# --------------------------------------------------------------------------- #
# 17-20: honest lifecycle gate - no forced pass, no promotion.
# --------------------------------------------------------------------------- #
_CFG9 = {"evidence_completeness": {"min_universe_names_per_period": 20,
         "min_scored_periods": 12, "min_coverage_pct": 60.0,
         "require_point_in_time_valid": True, "require_survivorship_safe": True},
         "gates": {"keep_min_rank_ic": 0.010, "keep_min_rank_ic_t": 2.0,
         "keep_min_positive_ic_hit_rate": 0.52, "keep_min_spread_t": 2.0,
         "keep_min_net25_spread": 0.0, "keep_min_subperiod_consistency": 0.60,
         "keep_min_regime_consistency": 0.50, "keep_max_drawdown_pct": -35.0,
         "keep_max_turnover_per_rebalance": 2.0,
         "keep_max_sector_concentration_pct": 40.0,
         "keep_max_worst_period_return_pct": -25.0,
         "forbid_known_lookahead": True}}

_STRONG = {"coverage_pct": 90.0, "scored_periods": 24, "min_names_per_period": 30,
           "point_in_time_valid": True, "survivorship_safe": True,
           "rank_ic": 0.03, "rank_ic_t": 3.5, "positive_ic_hit_rate": 0.58,
           "spread_t": 3.2, "net25_spread": 0.4, "subperiod_consistency": 0.8,
           "regime_consistency": 0.7, "max_drawdown_pct": -12.0,
           "turnover_per_rebalance": 1.0, "sector_concentration_pct": 20.0,
           "worst_period_return_pct": -10.0}


def test_p17_insufficient_coverage_stays_data_hold():
    m = dict(_STRONG, coverage_pct=10.0)  # below min_coverage_pct
    g = tt.classify_evidence(m, _CFG9)
    assert g["target_state"] == tt.DATA_HOLD and g["complete"] is False


def test_p18_complete_weak_becomes_rejected():
    m = dict(_STRONG, rank_ic_t=0.5)  # complete coverage, weak t-stat
    g = tt.classify_evidence(m, _CFG9)
    assert g["target_state"] == tt.REJECTED and g["complete"] is True
    assert g["blocker"] and g["blocker"].startswith("REJECT")


def test_p19_strong_complete_may_become_keep():
    g = tt.classify_evidence(_STRONG, _CFG9)
    assert g["target_state"] == tt.KEEP_FOR_RESEARCH
    assert g["evidence_status"] == tt.EVIDENCE_COMPLETE_STRONG


def test_p20_no_model_is_automatically_promoted():
    # KEEP_FOR_RESEARCH is the strongest reachable state; there is no PROMOTED /
    # live state in the lifecycle at all.
    assert "PROMOTED" not in tt.LIFECYCLE_STATES
    for m in (_STRONG, dict(_STRONG, rank_ic_t=0.5), dict(_STRONG, coverage_pct=1)):
        assert tt.classify_evidence(m, _CFG9)["target_state"] in (
            tt.DATA_HOLD, tt.REJECTED, tt.KEEP_FOR_RESEARCH)
    # the coverage gate never itself promotes: sufficient=False forbids evaluation
    assert tt.coverage_gate_allows_evaluation({"sufficient": False}) is False
    assert tt.coverage_gate_allows_evaluation(None) is False
    assert tt.coverage_gate_allows_evaluation({"sufficient": True}) is True


# --------------------------------------------------------------------------- #
# 21: shadow books cannot backfill history.
# --------------------------------------------------------------------------- #
def test_p21_shadow_books_cannot_backfill_history(tmp_path):
    sb = tt.ShadowBook(str(tmp_path), "sb_test")
    sb.inception(candidate_id="c1", inception_date="2026-08-01", membership=["A"],
                 benchmark="SPY", cost_bps=25.0, spec={})
    # a mark dated on/before inception is refused (no retroactive history)
    with pytest.raises(tt.RetroactiveError):
        sb.record_mark(date="2026-07-31", nav=100000.0)
    with pytest.raises(tt.RetroactiveError):
        sb.record_mark(date="2026-08-01", nav=100000.0)
    # forward marks are accepted
    sb.record_mark(date="2026-08-02", nav=100100.0)


# --------------------------------------------------------------------------- #
# 22-23: reporting suppression + read-only campaign surfaces.
# --------------------------------------------------------------------------- #
def test_p22_campaign_reporting_suppresses_unchanged_noise():
    snap = {"sec_form4_8k": {"completed": 20, "target": 1895, "is_complete": False,
                             "consecutive_no_progress": 0,
                             "last_stop_reason": None}}
    assert rr.material_campaign_changes(snap, snap) == []  # unchanged -> silent
    moved = {"sec_form4_8k": dict(snap["sec_form4_8k"], completed=120)}
    assert any("milestone" in x for x in rr.material_campaign_changes(snap, moved))
    stopped = {"sec_form4_8k": dict(snap["sec_form4_8k"],
                                    last_stop_reason="NO_PROGRESS_LIMIT")}
    assert any("stopped" in x for x in rr.material_campaign_changes(snap, stopped))


def test_p23_telegram_surface_reads_canonical_campaign_state(tmp_path):
    s = _store(tmp_path)
    s.record_results("sec_form4_8k", succeeded=["A", "B"])
    cfg = {"production": {"campaign_db": str(tmp_path / "campaigns.sqlite")},
           "autonomy": {"collect_drain": {"sec_continuation": {"enabled": True}}}}
    prov = tc.build_default_providers(stage8_config=cfg, queue=None)
    out = prov["campaigns"]()
    assert "sec_form4_8k" in out and "2/4" in out  # canonical cursor
    detail = prov["campaign"]("sec_form4_8k")
    assert "sec_form4_8k" in detail and "cap 6" in detail


# --------------------------------------------------------------------------- #
# 24-25: no operational mutation surface + cadence-disabled config contract.
# --------------------------------------------------------------------------- #
def test_p24_campaign_modules_never_touch_operational_ledgers():
    for mod in (ac, pf):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        for forbidden in ("paper_trading_desk", "multi_horizon_alpha_ledger",
                          "current_alpha_paper_book", "postgres", "Daily Close"):
            assert forbidden not in src, (mod.__name__, forbidden)


def test_p25_cadence_tasks_disabled_contract():
    cfg = json.loads((_REPO / "configs" / "alpha_agent"
                      / "stage8_autonomy.json").read_text(encoding="utf-8-sig"))
    wt = cfg["windows_tasks"]
    assert wt["all_disabled_until_final_validation"] is True
    for t in ("AlphaAgent-Collect", "AlphaAgent-Morning-Report",
              "AlphaAgent-PostClose-Report", "AlphaAgent-Watchdog"):
        assert t in wt["cadence_tasks"]
