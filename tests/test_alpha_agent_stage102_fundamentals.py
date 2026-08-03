"""
Stage 10.2 — HISTORICAL PIT FUNDAMENTALS + AUTONOMOUS ALPHA UNLOCK. Deterministic
property tests for the full SEC companyfacts bulk ingestion + mapped-CIK PIT
materialization + store-derived activation + AlphaAgent-generated fundamental
experiment. No network, no real Norgate, no operational mutation: a tiny in-memory
companyfacts zip, a real identity store, the durable fact index, the PIT store,
the measured readiness contract and the canonical Stage 10.2 lanes/handlers/planner
exercise every safety invariant.

Covers the 35 numbered acceptance properties of the Stage 10.2 contract.
"""
import hashlib
import json
import zipfile
from pathlib import Path

import pytest

from alpha_agent import historical_identity as H
from alpha_agent import sec_companyfacts_index as CFI
from alpha_agent import sec_companyfacts as CF
from alpha_agent import pit_fundamentals as PF
from alpha_agent import fundamental_signals as FS
from alpha_agent import fundamental_readiness as FR
from alpha_agent import fundamental_jobs as FJ
from alpha_agent import autonomous_research as AR
from alpha_agent import acquisition_campaign as ACQ
from alpha_agent import sec_bulk_download as BULK

REPO = Path(__file__).resolve().parents[1]
DATES = ["%d-%s" % (y, mm) for y in range(2015, 2025)
         for mm in ("03-31", "06-30", "09-30", "12-31")]


# --------------------------------------------------------------------------- #
# Fixtures / helpers.
# --------------------------------------------------------------------------- #
def _clock():
    box = {"n": 0}

    def c():
        box["n"] += 1
        return "2026-08-02T00:%02d:00+00:00" % (box["n"] % 60)
    return c


def _f(tag, val, filed, *, unit="USD", end=None, fy=None, fp=None, accn=None,
       form="10-K", start=None, frame=None):
    return {"tag": tag, "unit": unit, "val": val, "filed": filed, "end": end,
            "fy": fy, "fp": fp, "accn": accn or ("acc-%s-%s" % (tag, filed)),
            "form": form, "start": start, "frame": frame}


def _cf_doc(cik, facts, entity=None):
    us: dict = {}
    for f in facts:
        node = us.setdefault(f["tag"], {"label": f["tag"], "units": {}})
        node["units"].setdefault(f["unit"], []).append(
            {"val": f["val"], "start": f.get("start"), "end": f.get("end"),
             "fy": f.get("fy"), "fp": f.get("fp"), "filed": f["filed"],
             "accn": f["accn"], "form": f.get("form", "10-K"),
             "frame": f.get("frame")})
    return {"cik": int(cik), "entityName": entity or ("Co%s" % cik),
            "facts": {"us-gaap": us}}


def _write_cf_zip(path, docs, extra=None):
    with zipfile.ZipFile(path, "w") as zf:
        for cik, doc in docs.items():
            zf.writestr("CIK%010d.json" % int(cik), json.dumps(doc))
        for name, content in (extra or {}).items():
            zf.writestr(name, content)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _index(tmp_path, docs, *, allow=None, extra=None, ahash=None,
           member_step=1000):
    Path(tmp_path).mkdir(parents=True, exist_ok=True)
    zp = tmp_path / "companyfacts.zip"
    h = _write_cf_zip(zp, docs, extra)
    idx = CFI.SecCompanyFactsIndex(tmp_path / "cf.sqlite", clock=_clock())
    allowlist = allow if allow is not None else [str(c) for c in docs]
    r = idx.index_companyfacts_archive(zp, archive_hash=ahash or h,
                                       allowlist_ciks=allowlist,
                                       member_step=member_step)
    return idx, zp, (ahash or h), r


def _pit_from_index(idx, ciks):
    return CF.historical_pit_store_from_index(idx, ciks)


def _resolved_id_store(tmp_path, n=25, *, current=None, delisted=None,
                       dates=DATES, snapshot=True):
    """A real identity store: ``n`` current resolved securities (members over the
    whole window). ``current``/``delisted`` override with explicit specs."""
    store = H.IdentityStore(tmp_path / "id.sqlite", clock=_clock())
    specs = []
    if current is None and delisted is None:
        for i in range(n):
            specs.append((i, True, "2010-01-01", None, True))  # resolved current
    else:
        for i, mend in (current or []):
            specs.append((i, True, "2010-01-01", None, True))
        for i, mend in (delisted or []):
            specs.append((i, False, "2010-01-01", mend, True))
    for i, is_cur, mstart, mend, resolved in specs:
        sid = "ngid:%d" % i
        ident = H.SecurityIdentity(
            security_id=sid, norgate_assetid=str(i), norgate_symbol="S%d" % i,
            ticker="S%d" % i, issuer_name="Co%d" % i, share_class="COMMON",
            base_type="Equity", exchange="NYSE",
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
    if snapshot:
        store.record_coverage_snapshot(as_of="2024-12-31",
                                       rebalance_dates=dates,
                                       index_name="S&P 500")
    return store


def _rich_companyfacts(tmp_path, ciks, *, fy_lo=2013, fy_hi=2023):
    docs = {}
    for cik_int in ciks:
        facts = []
        for fy in range(fy_lo, fy_hi + 1):
            filed = "%d-02-15" % (fy + 1)
            end = "%d-12-31" % fy
            facts.append(_f("Assets", 10000 + fy + cik_int, filed, end=end,
                            fy=fy, fp="FY", accn="a-as-%d-%d" % (cik_int, fy)))
            facts.append(_f("Revenues", 5000 + fy + cik_int, filed, end=end,
                            fy=fy, fp="FY", accn="a-rev-%d-%d" % (cik_int, fy)))
            facts.append(_f("CostOfRevenue", 2000 + fy + cik_int, filed, end=end,
                            fy=fy, fp="FY",
                            accn="a-cost-%d-%d" % (cik_int, fy)))
        docs[cik_int] = _cf_doc(cik_int, facts)
    idx, zp, h, _r = _index(tmp_path, docs, allow=["%010d" % c for c in docs],
                            member_step=5000)
    return idx, zp, h


def _stage102_cfg(tmp_path, *, enabled=True):
    return {"enabled": enabled, "planner_enabled": True,
            "bulk_root": str(tmp_path), "companyfacts_url": "http://x/cf.zip",
            "companyfacts_index_db": str(tmp_path / "cf.sqlite"),
            "artifact_root": str(tmp_path / "artifacts"),
            "index_member_step": 5000, "cik_batch": 500, "priority": 4}


def _ctx(tmp_path, store, idx, *, campaign_store=None, cfg9=None, dates=DATES):
    return FJ.FundamentalJobContext(
        store=store, companyfacts_index=idx, campaign_store=campaign_store,
        artifact_root=str(tmp_path / "artifacts"), rebalance_dates=dates,
        cfg9=cfg9 or {}, stage102=_stage102_cfg(tmp_path),
        index_name="S&P 500", signals=FJ.DEFAULT_SIGNALS,
        transport=None, read_normalized=lambda rt, *, limit=6000: [],
        clock=_clock())


def _drive(ctx, queue, cycles=20):
    """Run the real planner -> claim -> dispatch -> settle loop offline."""
    log = []
    for _ in range(cycles):
        plan = FJ.plan_next_fundamental_job(queue, ctx, cfg=ctx.stage102)
        if plan is None:
            break
        job = queue.claim_next(
            categories=[AR.CAT_DATA_ACQUISITION, AR.CAT_DATA_VALIDATION],
            origins=[FJ.ORIGIN_102], lane_prefixes=[FJ.FUNDAMENTAL_LANE_PREFIX])
        if job is None:
            break
        outcome, detail = FJ.dispatch_fundamental_job(job, ctx)
        queue.apply_outcome(job.job_id, outcome, result=detail)
        log.append((job.lane, outcome, detail))
    return log


def _blob_transport(blob, etag="e1", lm="lm1"):
    def t(req, timeout):
        if req.get("method") == "HEAD":
            return {"status": 200, "headers": {
                "Content-Length": str(len(blob)), "ETag": etag,
                "Last-Modified": lm, "Accept-Ranges": "bytes"}}
        rng = str(req.get("headers", {}).get("Range", "")).replace("bytes=", "")
        a, b = rng.split("-")
        return {"status": 206, "body": blob[int(a):int(b) + 1]}
    return t


# --------------------------------------------------------------------------- #
# 1. companyfacts ZIP download is restart-safe.
# --------------------------------------------------------------------------- #
def test_p01_companyfacts_download_restart_safe(tmp_path):
    blob = bytes(bytearray((i % 251) for i in range(200000)))
    dl = BULK.BulkArchiveDownloader(
        url="http://x/companyfacts.zip", dest_dir=tmp_path, name="companyfacts",
        segment_bytes=64 * 1024, chunk_bytes=16 * 1024, forbid_drives=(),
        transport=_blob_transport(blob))
    r1 = dl.download_segment()
    assert not r1.get("complete")
    assert 0 < r1["bytes_downloaded"] < len(blob)  # one bounded segment
    for _ in range(30):
        r = dl.download_segment()
        if r.get("complete"):
            break
    assert dl.final_path.exists()
    assert dl.final_path.stat().st_size == len(blob)  # resumed to completion


# 2. archive replay is idempotent.
def test_p02_index_replay_idempotent(tmp_path):
    docs = {320193: _cf_doc(320193, [_f("Assets", 1000, "2021-03-01",
                                        end="2020-12-31", fy=2020, fp="FY")])}
    idx, zp, h, r = _index(tmp_path, docs)
    d1 = idx.digest()
    idx.index_companyfacts_archive(zp, archive_hash=h,
                                   allowlist_ciks=["320193"], member_step=1000)
    assert idx.digest() == d1


# 3. changed bytes create a new revision.
def test_p03_changed_bytes_new_revision(tmp_path):
    docs = {320193: _cf_doc(320193, [_f("Assets", 1000, "2021-03-01",
                                        end="2020-12-31", fy=2020, fp="FY")])}
    idx, zp, h, _ = _index(tmp_path, docs)
    idx.index_companyfacts_archive(zp, archive_hash="DIFFERENT_HASH",
                                   allowlist_ciks=["320193"], member_step=1000)
    assert any(rv["kind"] == "ARCHIVE_REVISION" for rv in idx.revisions())


# 4. path traversal is rejected.
def test_p04_zip_path_traversal_rejected(tmp_path):
    docs = {320193: _cf_doc(320193, [_f("Assets", 1000, "2021-03-01",
                                        end="2020-12-31", fy=2020, fp="FY")])}
    idx, zp, h, r = _index(
        tmp_path, docs, allow=["320193", "0000000009"],
        extra={"evil/../CIK0000000009.json":
               json.dumps(_cf_doc(9, [_f("Assets", 1, "2021-01-01")]))})
    assert r["skipped_members"] >= 1
    assert idx.member(9) is None  # traversal member never cataloged


# 5. malformed members are isolated.
def test_p05_malformed_members_isolated(tmp_path):
    docs = {320193: _cf_doc(320193, [_f("Assets", 1000, "2021-03-01",
                                        end="2020-12-31", fy=2020, fp="FY")])}
    idx, zp, h, r = _index(
        tmp_path, docs, allow=["320193", "0000000001"],
        extra={"CIK0000000001.json": "{ bad json ]"})
    assert r["malformed_members"] == 1
    assert idx.member(320193)["materialized"] == 1  # good member still indexed


# 6. mapped CIK target set is deterministic.
def test_p06_mapped_cik_set_deterministic(tmp_path):
    store = _resolved_id_store(tmp_path, n=5)
    idx, *_ = _rich_companyfacts(tmp_path, [1000 + i for i in range(5)])
    ctx = _ctx(tmp_path, store, idx)
    a = FJ._resolved_ciks(ctx)
    b = FJ._resolved_ciks(ctx)
    assert a == b == sorted(a)
    assert a == ["%010d" % (1000 + i) for i in range(5)]


# 7. unresolved securities are excluded without current substitution.
def test_p07_unresolved_excluded(tmp_path):
    store = H.IdentityStore(tmp_path / "id.sqlite", clock=_clock())
    for i in range(5):
        sid = "ngid:%d" % i
        store.upsert_security(H.SecurityIdentity(
            security_id=sid, norgate_assetid=str(i), norgate_symbol="S%d" % i,
            ticker="S%d" % i, issuer_name="Co%d" % i, share_class="COMMON",
            base_type="Equity", exchange="NYSE",
            security_start_date="2010-01-01", security_end_date=None,
            delisting_date=None, is_current=True,
            membership_intervals=[{"index_name": "S&P 500",
                                   "member_start": "2010-01-01",
                                   "member_end": None}]))
        if i < 3:
            store.record_mapping(H.MappingResult(
                sid, "%010d" % (1000 + i), H.METHOD_FILING_TICKER_OVERLAP, 3,
                0.9, H.STATUS_RESOLVED, "2010-01-01", None, {"t": 3}))
        else:
            store.record_mapping(H.MappingResult(
                sid, None, H.METHOD_UNRESOLVED, H.TIER_UNRESOLVED_BACKLOG, 0.0,
                H.STATUS_UNRESOLVED, "2010-01-01", None, {"reason": "x"}))
    ctx = _ctx(tmp_path, store, None)
    assert FJ._resolved_ciks(ctx) == ["%010d" % (1000 + i) for i in range(3)]


# 8. campaign revision is append-only (supersession on scope change).
def test_p08_campaign_revision_append_only(tmp_path):
    cs = ACQ.CampaignStore(str(tmp_path / "camp.sqlite"), clock=_clock())
    r1 = cs.ensure_campaign_revision(
        FJ.DEFAULT_CAMPAIGN_ID, kind=FJ.CAMPAIGN_KIND, universe=["1", "2"],
        universe_source="s", universe_fingerprint="fp_A")
    r2 = cs.ensure_campaign_revision(
        FJ.DEFAULT_CAMPAIGN_ID, kind=FJ.CAMPAIGN_KIND, universe=["1", "2", "3"],
        universe_source="s", universe_fingerprint="fp_B")
    revs = cs.campaign_revisions(FJ.DEFAULT_CAMPAIGN_ID)
    statuses = {rv["status"] for rv in revs}
    assert len(revs) >= 2                          # append-only history
    assert "SUPERSEDED" in statuses and "ACTIVE" in statuses
    assert r1["campaign_id"] != r2["campaign_id"]  # scope change -> new revision


# 9. campaign/index cursor resumes after restart.
def test_p09_cursor_resumes_after_restart(tmp_path):
    docs = {1000 + i: _cf_doc(1000 + i, [_f("Assets", 10 + i, "2021-03-01",
                                            end="2020-12-31", fy=2020, fp="FY")])
            for i in range(4)}
    idx, zp, h, r = _index(tmp_path, docs)
    assert r["complete"]
    done = idx.archive_status("companyfacts")["members_done"]
    d1 = idx.digest()
    # "restart": a brand-new index object on the SAME durable sqlite file.
    idx2 = CFI.SecCompanyFactsIndex(tmp_path / "cf.sqlite")
    r2 = idx2.index_companyfacts_archive(
        zp, archive_hash=h, allowlist_ciks=["%010d" % c for c in docs],
        member_step=1000)
    assert r2["complete"]
    assert idx2.archive_status("companyfacts")["members_done"] == done
    assert idx2.digest() == d1  # cursor persisted; nothing re-materialised


# 10. duplicate continuation jobs are suppressed (one live job max).
def test_p10_no_duplicate_live_job(tmp_path):
    store = _resolved_id_store(tmp_path, n=3)
    idx, zp, h = _rich_companyfacts(tmp_path, [1000 + i for i in range(3)])
    (tmp_path / "companyfacts.zip").write_bytes(zp.read_bytes())
    ctx = _ctx(tmp_path, store, idx)
    queue = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    p1 = FJ.plan_next_fundamental_job(queue, ctx, cfg=ctx.stage102)
    assert p1 is not None
    assert FJ.plan_next_fundamental_job(queue, ctx, cfg=ctx.stage102) is None


# 11. each valid mapped CIK is attempted (materialised or exact missing).
def test_p11_every_mapped_cik_attempted(tmp_path):
    ciks = [1000 + i for i in range(6)]
    idx, zp, h = _rich_companyfacts(tmp_path, ciks)
    for c in ciks:
        m = idx.member("%010d" % c)
        assert m is not None and m["materialized"] == 1


# 12. filed date controls availability.
def test_p12_filed_date_controls_availability(tmp_path):
    docs = {1000: _cf_doc(1000, [_f("Assets", 1000, "2021-03-01",
                                    end="2020-12-31", fy=2020, fp="FY")])}
    idx, *_ = _index(tmp_path, docs)
    store = _pit_from_index(idx, ["0000001000"])
    assert store.as_of("0000001000", "assets", "2020-FY", "2021-02-01") is None
    obs = store.as_of("0000001000", "assets", "2020-FY", "2021-04-01")
    assert obs is not None and obs.value == 1000.0


# 13. amendments do not leak backward.
def test_p13_amendments_no_backward_leak(tmp_path):
    docs = {1000: _cf_doc(1000, [
        _f("Assets", 1000, "2021-03-01", end="2020-12-31", fy=2020, fp="FY",
           accn="orig"),
        _f("Assets", 1200, "2021-09-01", end="2020-12-31", fy=2020, fp="FY",
           accn="amend", form="10-K/A")])}
    idx, *_ = _index(tmp_path, docs)
    store = _pit_from_index(idx, ["0000001000"])
    assert store.as_of("0000001000", "assets", "2020-FY", "2021-06-01").value \
        == 1000.0                                        # original only
    assert store.as_of("0000001000", "assets", "2020-FY", "2021-10-01").value \
        == 1200.0                                        # amendment now visible


# 14. facts retain accession and source archive hash.
def test_p14_facts_retain_accession_and_source(tmp_path):
    docs = {1000: _cf_doc(1000, [_f("Assets", 1000, "2021-03-01",
                                    end="2020-12-31", fy=2020, fp="FY",
                                    accn="ACC-XYZ")])}
    idx, zp, h, _ = _index(tmp_path, docs)
    payloads = idx.facts_for_cik("0000001000")
    assert payloads and payloads[0]["accession"] == "ACC-XYZ"
    conn = idx._connect()
    try:
        row = conn.execute("SELECT source_archive_hash, source_member FROM "
                           "cf_fact LIMIT 1").fetchone()
        assert row["source_archive_hash"] == h and row["source_member"]
    finally:
        conn.close()


# 15. missing facts remain missing (never zero-filled).
def test_p15_missing_facts_remain_missing(tmp_path):
    docs = {1000: _cf_doc(1000, [_f("Assets", 1000, "2021-03-01",
                                    end="2020-12-31", fy=2020, fp="FY")])}
    idx, *_ = _index(tmp_path, docs)
    store = _pit_from_index(idx, ["0000001000"])
    miss = store.missing_concepts("gross_profitability")
    assert "revenue" in miss and "cost_of_revenue" in miss
    assert store.ciks_with_candidate("gross_profitability") == set()


# 16. unsupported (non-USD) units remain explicit — never a monetary value.
def test_p16_unsupported_units_excluded(tmp_path):
    docs = {1000: _cf_doc(1000, [
        _f("Assets", 1000, "2021-03-01", end="2020-12-31", fy=2020, fp="FY"),
        _f("Assets", 99, "2021-03-01", unit="shares", end="2020-12-31",
           fy=2020, fp="FY", accn="sh")])}
    idx, *_ = _index(tmp_path, docs)
    payloads = idx.facts_for_cik("0000001000")
    assert all(p["unit"] == "USD" for p in payloads)
    assert len(payloads) == 1


# 17. gross-profitability preferred/fallback order is correct.
def test_p17_gross_profitability_preferred_then_fallback(tmp_path):
    # Preferred: GrossProfit present -> basis gross_profit.
    docs = {1000: _cf_doc(1000, [
        _f("Assets", 1000, "2021-03-01", end="2020-12-31", fy=2020, fp="FY"),
        _f("Revenues", 800, "2021-03-01", end="2020-12-31", fy=2020, fp="FY",
           accn="r"),
        _f("CostOfRevenue", 300, "2021-03-01", end="2020-12-31", fy=2020,
           fp="FY", accn="c"),
        _f("GrossProfit", 500, "2021-03-01", end="2020-12-31", fy=2020, fp="FY",
           accn="g")])}
    idx, *_ = _index(tmp_path, docs)
    store = _pit_from_index(idx, ["0000001000"])
    r = FS.gross_profitability(store, cik="0000001000", fiscal_key="2020-FY",
                               as_of="2021-04-01")
    assert r["basis"] == "gross_profit" and abs(r["value"] - 0.5) < 1e-9
    # Fallback: no GrossProfit -> (revenue - cost)/assets.
    docs2 = {1001: _cf_doc(1001, [
        _f("Assets", 1000, "2021-03-01", end="2020-12-31", fy=2020, fp="FY"),
        _f("Revenues", 800, "2021-03-01", end="2020-12-31", fy=2020, fp="FY",
           accn="r"),
        _f("CostOfRevenue", 300, "2021-03-01", end="2020-12-31", fy=2020,
           fp="FY", accn="c")])}
    idx2, *_ = _index(tmp_path / "b", docs2)
    (tmp_path / "b").mkdir(exist_ok=True)
    store2 = _pit_from_index(idx2, ["0000001001"])
    r2 = FS.gross_profitability(store2, cik="0000001001", fiscal_key="2020-FY",
                               as_of="2021-04-01")
    assert r2["basis"] == "revenue_minus_cost_fallback"
    assert abs(r2["value"] - 0.5) < 1e-9


# 18. asset growth uses comparable (FY vs prior-year FY) periods.
def test_p18_asset_growth_comparable_period(tmp_path):
    docs = {1000: _cf_doc(1000, [
        _f("Assets", 1000, "2020-03-01", end="2019-12-31", fy=2019, fp="FY",
           accn="a19"),
        _f("Assets", 1200, "2021-03-01", end="2020-12-31", fy=2020, fp="FY",
           accn="a20")])}
    idx, *_ = _index(tmp_path, docs)
    store = _pit_from_index(idx, ["0000001000"])
    prior = store.prior_fiscal_key("0000001000", "2021-04-01", "2020-FY",
                                   concept="assets")
    assert prior == "2019-FY"
    r = FS.asset_growth(store, cik="0000001000", fiscal_key="2020-FY",
                        prior_fiscal_key=prior, as_of="2021-04-01")
    assert abs(r["value"] - 0.2) < 1e-9


# 19. quarterly year-over-year comparison is correct (Q2 vs prior-year Q2).
def test_p19_quarterly_yoy(tmp_path):
    docs = {1000: _cf_doc(1000, [
        _f("Assets", 1000, "2020-08-01", end="2020-06-30", fy=2020, fp="Q2",
           accn="q220"),
        _f("Assets", 900, "2019-08-01", end="2019-06-30", fy=2019, fp="Q2",
           accn="q219"),
        _f("Assets", 950, "2020-05-01", end="2020-03-31", fy=2020, fp="Q1",
           accn="q120")])}
    idx, *_ = _index(tmp_path, docs)
    store = _pit_from_index(idx, ["0000001000"])
    prior = store.prior_fiscal_key("0000001000", "2020-09-01", "2020-Q2",
                                   concept="assets")
    assert prior == "2019-Q2"          # prior-year same quarter, never Q1


# 20. current-survivor substitution is impossible (delisted names in their era).
def test_p20_no_current_survivor_substitution(tmp_path):
    store = _resolved_id_store(
        tmp_path, current=[(0, None), (1, None)],
        delisted=[(2, "2018-06-30")], dates=["2017-06-30", "2020-06-30"])
    early = {u["security_id"] for u in store.historical_universe_on(
        "2017-06-30", index_name="S&P 500")}
    late = {u["security_id"] for u in store.historical_universe_on(
        "2020-06-30", index_name="S&P 500")}
    assert "ngid:2" in early                      # delisted present in its era
    assert "ngid:2" not in late                   # gone after delisting


# 21. readiness is measured PER rebalance date.
def test_p21_readiness_per_rebalance_date(tmp_path):
    ciks = [1000 + i for i in range(25)]
    store = _resolved_id_store(tmp_path, n=25)
    idx, *_ = _rich_companyfacts(tmp_path, ciks)
    pit = _pit_from_index(idx, ["%010d" % c for c in ciks])
    rr = FR.measured_readiness_from_store(store, "gross_profitability", {},
                                          pit_store=pit, rebalance_dates=DATES,
                                          index_name="S&P 500")
    assert len(rr["per_date"]) == len(DATES)
    assert all("coverage_pct" in p for p in rr["per_date"])


# 22. aggregate fact counts alone cannot unlock (per-date coverage governs).
def test_p22_aggregate_counts_cannot_unlock(tmp_path):
    # 25-name universe but PIT facts for only 5 CIKs (high aggregate, low
    # per-date coverage 5/25 = 20% < 60%).
    store = _resolved_id_store(tmp_path, n=25)
    idx, *_ = _rich_companyfacts(tmp_path, [1000 + i for i in range(5)])
    pit = _pit_from_index(idx, ["%010d" % (1000 + i) for i in range(5)])
    assert pit.observation_count() > 100          # high AGGREGATE
    rr = FR.measured_readiness_from_store(store, "gross_profitability", {},
                                          pit_store=pit, rebalance_dates=DATES,
                                          index_name="S&P 500")
    assert rr["sufficient"] is False
    assert rr["median_coverage_pct"] < 60.0


# 23. current-company coverage alone cannot unlock (survivorship denominator).
def test_p23_current_company_cannot_unlock(tmp_path):
    # 7 current resolved + 5 delisted UNRESOLVED, all members on 2016-03-31.
    store = H.IdentityStore(tmp_path / "id.sqlite", clock=_clock())
    for i in range(12):
        cur = i < 7
        sid = "ngid:%d" % i
        store.upsert_security(H.SecurityIdentity(
            security_id=sid, norgate_assetid=str(i), norgate_symbol="S%d" % i,
            ticker="S%d" % i, issuer_name="Co%d" % i, share_class="COMMON",
            base_type="Equity", exchange="NYSE",
            security_start_date="2010-01-01",
            security_end_date=None if cur else "2019-06-30",
            delisting_date=None if cur else "2019-06-30", is_current=cur,
            membership_intervals=[{"index_name": "S&P 500",
                                   "member_start": "2010-01-01",
                                   "member_end": None if cur else "2019-06-30"}]))
        if cur:
            store.record_mapping(H.MappingResult(
                sid, "%010d" % (1000 + i), H.METHOD_FILING_TICKER_OVERLAP, 3,
                0.9, H.STATUS_RESOLVED, "2010-01-01", None, {"t": 3}))
    store.record_coverage_snapshot(as_of="2016-06-30",
                                   rebalance_dates=["2016-03-31"],
                                   index_name="S&P 500")
    ms = FR.historical_mapping_status_from_store(store, {})
    assert ms["available"] is False               # 7/12 = 58% < 60%


# 24. disabled mode remains closed regardless of measured state.
def test_p24_disabled_mode_closed(tmp_path):
    ciks = [1000 + i for i in range(25)]
    store = _resolved_id_store(tmp_path, n=25)
    idx, *_ = _rich_companyfacts(tmp_path, ciks)
    pit = _pit_from_index(idx, ["%010d" % c for c in ciks])
    cfg = {"stage9_5": {"fundamental_experiments":
                        {"activation_mode": "disabled"}}}
    gate = FR.historical_fundamental_experiment_allowed(
        cfg, store=store, signal="gross_profitability", pit_store=pit)
    assert gate["allowed"] is False
    assert gate["activation_mode"] == "disabled"
    assert gate["diagnostic"] == FR.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY


# 25. measured_auto remains closed with insufficient PIT evidence.
def test_p25_measured_auto_closed_without_pit(tmp_path):
    store = _resolved_id_store(tmp_path, n=25)  # mapping 100% but NO PIT facts
    cfg = {"stage9_5": {"fundamental_experiments":
                        {"activation_mode": "measured_auto"}}}
    empty_pit = PF.PitFundamentalsStore()
    gate = FR.historical_fundamental_experiment_allowed(
        cfg, store=store, signal="gross_profitability", pit_store=empty_pit)
    assert gate["mapping_available"] is True       # mapping is fine ...
    assert gate["readiness_sufficient"] is False   # ... but no PIT facts
    assert gate["allowed"] is False


# 26. measured_auto opens ONLY with complete canonical readiness.
def test_p26_measured_auto_opens_with_full_readiness(tmp_path):
    ciks = [1000 + i for i in range(25)]
    store = _resolved_id_store(tmp_path, n=25)
    idx, *_ = _rich_companyfacts(tmp_path, ciks)
    pit = _pit_from_index(idx, ["%010d" % c for c in ciks])
    cfg = {"stage9_5": {"fundamental_experiments":
                        {"activation_mode": "measured_auto"}}}
    gate = FR.historical_fundamental_experiment_allowed(
        cfg, store=store, signal="gross_profitability", pit_store=pit)
    assert gate["mapping_available"] is True
    assert gate["readiness_sufficient"] is True
    assert gate["allowed"] is True
    assert gate["diagnostic"] is None


# 27. the AGENT (tournament tick) generates the experiment, not Claude.
def test_p27_agent_generates_experiment(tmp_path):
    from alpha_agent import tournament as TT
    ciks = [1000 + i for i in range(25)]
    store = _resolved_id_store(tmp_path, n=25)
    _rich_companyfacts(tmp_path, ciks)
    cfg = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                     .read_text(encoding="utf-8-sig"))
    cfg["stage9_5"]["historical_universe"]["identity_store_db"] = \
        str(tmp_path / "id.sqlite")
    cfg["stage9_5"]["historical_universe"]["companyfacts_index_db"] = \
        str(tmp_path / "cf.sqlite")
    # sanity: the measured gate is OPEN under the fixture.
    gate = FR.historical_fundamental_experiment_allowed(
        cfg, store=store, signal="gross_profitability")
    assert gate["allowed"] is True and gate["activation_mode"] == "measured_auto"
    reg = TT.CandidateRegistry(str(tmp_path / "t.sqlite"))
    TT.seed_families(reg)
    cand = TT._candidate_for_feature(reg, "gross_profitability")
    reg.record_data_coverage(cand["candidate_id"],
                             coverage={"ciks": 25, "periods": 40},
                             sufficient=True)
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    res = TT.generate_stage9_5_fundamental_followups(reg, cfg, queue=q)
    reg.close()
    jobs = [j for j in q.list_jobs(limit=50)
            if j.lane == "tournament.stage9_5_fundamental"]
    assert res["count"] >= 1 and jobs
    assert jobs[0].origin == "stage9-tournament"   # the AGENT generated it


# 28. a claimed job's attempts increment exactly once.
def test_p28_attempts_increment_once(tmp_path):
    store = _resolved_id_store(tmp_path, n=3)
    idx, zp, h = _rich_companyfacts(tmp_path, [1000 + i for i in range(3)])
    (tmp_path / "companyfacts.zip").write_bytes(zp.read_bytes())
    ctx = _ctx(tmp_path, store, idx)
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    FJ.plan_next_fundamental_job(q, ctx, cfg=ctx.stage102)
    job = q.claim_next(categories=[AR.CAT_DATA_ACQUISITION,
                                   AR.CAT_DATA_VALIDATION],
                       origins=[FJ.ORIGIN_102],
                       lane_prefixes=[FJ.FUNDAMENTAL_LANE_PREFIX])
    assert job is not None and job.attempts == 1


# 29. experiment result import keying is exactly-once (deterministic hash).
def test_p29_result_import_exactly_once(tmp_path):
    from alpha_agent import tournament as TT
    row = {"feature": "gross_profitability", "rank_ic_t": 2.5, "spread_t": 2.1,
           "rank_ic_mean": 0.02, "net25_spread": 0.01}
    h1 = TT.result_hash_for(source="stage9_5_fundamental",
                            feature="gross_profitability", row=row,
                            evidence_date="2024-12-31", job_id="job1",
                            candidate_id="cand1")
    h2 = TT.result_hash_for(source="stage9_5_fundamental",
                            feature="gross_profitability", row=row,
                            evidence_date="2024-12-31", job_id="job1",
                            candidate_id="cand1")
    assert h1 == h2                                # identical row -> one key


# 30. existing lifecycle gates remain unchanged (no gate lowered).
def test_p30_lifecycle_gates_unchanged():
    cfg = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                     .read_text(encoding="utf-8-sig"))
    g = cfg["gates"]
    assert g["keep_min_rank_ic_t"] == 2.0
    assert g["keep_min_spread_t"] == 2.0
    assert g["keep_min_subperiod_consistency"] == 0.60
    assert cfg["stage9_5"]["per_rebalance_readiness"]["min_coverage_pct"] == 60.0
    assert cfg["stage9_5"]["per_rebalance_readiness"][
        "min_historical_mapping_coverage_pct"] == 60.0


# 31. no model is promoted automatically.
def test_p31_no_automatic_promotion(tmp_path):
    cfg = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                     .read_text(encoding="utf-8-sig"))
    assert cfg["safety"]["no_model_promotion"] is True
    assert "PROMOTED" not in cfg["lifecycle_states"]
    store = _resolved_id_store(tmp_path, n=3)
    idx, zp, h = _rich_companyfacts(tmp_path, [1000 + i for i in range(3)])
    (tmp_path / "companyfacts.zip").write_bytes(zp.read_bytes())
    ctx = _ctx(tmp_path, store, idx)
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    for lane, outcome, detail in _drive(ctx, q):
        assert detail.get("no_automatic_promotion", True) is True


# 32. no shadow book is backfilled (readiness/activation never flips a switch).
def test_p32_no_shadow_book_no_switch_flip(tmp_path):
    ciks = [1000 + i for i in range(25)]
    store = _resolved_id_store(tmp_path, n=25)
    idx, *_ = _rich_companyfacts(tmp_path, ciks)
    ctx = _ctx(tmp_path, store, idx)
    o1, d1 = FJ._fund_readiness_recheck(ctx, _job(FJ.LANE_FUND_READINESS_RECHECK))
    o2, d2 = FJ._fund_activation_recheck(
        ctx, _job(FJ.LANE_FUND_ACTIVATION_RECHECK))
    assert d1["safety_switch_flipped"] is False
    assert d2["safety_switch_flipped"] is False


# 33. queue fairness remains intact (categories/allowlist unchanged).
def test_p33_queue_fairness_intact():
    cfg = json.loads((REPO / "configs/alpha_agent/stage8_autonomy.json")
                     .read_text(encoding="utf-8-sig"))
    cd = cfg["autonomy"]["collect_drain"]
    assert cd["max_jobs_per_cycle"] == 1
    assert "stage10.2-fundamentals" in cd["allowed_origins"]
    assert "fundamentals." in cd["allowed_lane_prefixes"]
    assert cd["companyfacts_continuation"]["fairness_enabled"] is True
    # Every Stage 10.2 lane uses only already-admitted categories.
    assert FJ._ACQUISITION_LANES.union(FJ._VALIDATION_LANES).issuperset({
        FJ.LANE_FUND_BULK_INVENTORY, FJ.LANE_FUND_BULK_INDEX,
        FJ.LANE_FUND_PIT_MATERIALIZE, FJ.LANE_FUND_ACTIVATION_RECHECK})


# 34. exactly one job is planned/live per cycle.
def test_p34_one_job_per_cycle(tmp_path):
    store = _resolved_id_store(tmp_path, n=3)
    idx, zp, h = _rich_companyfacts(tmp_path, [1000 + i for i in range(3)])
    (tmp_path / "companyfacts.zip").write_bytes(zp.read_bytes())
    ctx = _ctx(tmp_path, store, idx)
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    FJ.plan_next_fundamental_job(q, ctx, cfg=ctx.stage102)
    live = [j for j in q.list_jobs(state=AR.STATE_QUEUED, limit=100)
            if j.origin == FJ.ORIGIN_102]
    assert len(live) == 1


# 35. stores live under the research root, never an operational-ledger root.
def test_p35_stores_outside_operational_root():
    cfg = json.loads((REPO / "configs/alpha_agent/stage8_autonomy.json")
                     .read_text(encoding="utf-8-sig"))
    s102 = cfg["stage10_2"]
    for key in ("companyfacts_index_db", "bulk_root", "artifact_root"):
        p = s102[key].replace("\\", "/").lower()
        assert "alpha_agent" in p
        assert ".paper_trader" not in p  # not the operational-ledger root


# 36. measured-auto PER-SIGNAL unlock: a below-threshold signal
#     (gross_profitability) NEVER vetoes the signals that genuinely pass
#     (asset_growth / balance_sheet_quality). Reproduces the LIVE Stage 10.2
#     measured readiness snapshot exactly (gp 40% < 60%; the other two above it).
def test_p36_measured_auto_per_signal_no_veto(tmp_path):
    from alpha_agent import tournament as TT
    store = _resolved_id_store(tmp_path, n=25)
    snap = {
        "gross_profitability": {"sufficient": False, "mapping_available": True,
                                "median_coverage_pct": 40.0,
                                "blocker": "INSUFFICIENT_COVERAGE_PCT"},
        "asset_growth": {"sufficient": True, "mapping_available": True,
                         "median_coverage_pct": 64.0},
        "balance_sheet_quality": {"sufficient": True, "mapping_available": True,
                                  "median_coverage_pct": 60.7}}
    store.set_meta("stage102_readiness_snapshot", json.dumps(snap))
    store.set_meta("stage102_activation_open", "1")
    cfg = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                     .read_text(encoding="utf-8-sig"))
    cfg["stage9_5"]["historical_universe"]["identity_store_db"] = \
        str(tmp_path / "id.sqlite")
    cfg["stage9_5"]["historical_universe"]["companyfacts_index_db"] = \
        str(tmp_path / "cf.sqlite")   # intentionally absent: the snapshot is used
    cfg["stage9_5"]["fundamental_experiments"]["max_per_cycle"] = 5
    reg = TT.CandidateRegistry(str(tmp_path / "t.sqlite"))
    TT.seed_families(reg)
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    res = TT.generate_stage9_5_fundamental_followups(reg, cfg, queue=q)
    reg.close()
    feats = {(j.payload or {}).get("feature") for j in q.list_jobs(limit=50)
             if j.lane == "tournament.stage9_5_fundamental"}
    assert res["count"] >= 2
    assert "asset_growth" in feats and "balance_sheet_quality" in feats
    assert "gross_profitability" not in feats   # honest veto holds for it alone


# 37. measured-auto with EVERY signal below threshold: honest blocker, zero jobs
#     (no false unlock; the switch is never flipped by a below-threshold signal).
def test_p37_measured_auto_all_insufficient_blocks(tmp_path):
    from alpha_agent import tournament as TT
    store = _resolved_id_store(tmp_path, n=25)
    snap = {s: {"sufficient": False, "mapping_available": True}
            for s in ("gross_profitability", "asset_growth",
                      "balance_sheet_quality")}
    store.set_meta("stage102_readiness_snapshot", json.dumps(snap))
    store.set_meta("stage102_activation_open", "0")
    cfg = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                     .read_text(encoding="utf-8-sig"))
    cfg["stage9_5"]["historical_universe"]["identity_store_db"] = \
        str(tmp_path / "id.sqlite")
    cfg["stage9_5"]["historical_universe"]["companyfacts_index_db"] = \
        str(tmp_path / "cf.sqlite")
    reg = TT.CandidateRegistry(str(tmp_path / "t.sqlite"))
    TT.seed_families(reg)
    q = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    res = TT.generate_stage9_5_fundamental_followups(reg, cfg, queue=q)
    reg.close()
    assert res["count"] == 0
    assert res.get("blocker") == FR.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY
    assert not [j for j in q.list_jobs(limit=50)
                if j.lane == "tournament.stage9_5_fundamental"]


def _job(lane, payload=None):
    return AR.Job(job_id="j_" + lane, dedupe_key="d", category="DATA_VALIDATION",
                  lane=lane, state="RUNNING", payload=payload or {})


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
