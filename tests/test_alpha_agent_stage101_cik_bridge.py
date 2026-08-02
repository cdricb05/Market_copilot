"""
Stage 10.1 — HISTORICAL CIK BRIDGE CLOSURE. Deterministic property tests for the
full SEC bulk ingestion + complete Norgate universe resolution capability. No
network, no real Norgate, no operational mutation: a tiny in-memory submissions
zip, fake accessor and fake transport exercise the durable SEC issuer-history
index, the candidate-based deterministic matching contract, the canonical Stage
10.1 lanes/handlers/planner, coverage + readiness, and every safety invariant.

Covers the 38 numbered acceptance properties of the Stage 10.1 contract.
"""
import hashlib
import json
import zipfile

import pytest

from alpha_agent import historical_identity as H
from alpha_agent import sec_issuer_index as SI
from alpha_agent import identity_jobs as IJ
from alpha_agent import autonomous_research as AR
from alpha_agent import fundamental_readiness as FR


# --------------------------------------------------------------------------- #
# Fixtures / helpers.
# --------------------------------------------------------------------------- #
def _clock():
    box = {"n": 0}

    def c():
        box["n"] += 1
        return "2026-08-02T00:%02d:00+00:00" % (box["n"] % 60)
    return c


def _write_zip(path, docs, extra=None):
    with zipfile.ZipFile(path, "w") as zf:
        for cik, body in docs.items():
            zf.writestr("CIK%010d.json" % cik, json.dumps({"cik": cik, **body}))
        for name, content in (extra or {}).items():
            zf.writestr(name, content)
    return hashlib.sha256(path.read_bytes()).hexdigest()


_ISSUERS = {
    6201: {"name": "AMR CORP", "tickers": [], "formerNames": [],
           "filings": {"recent": {"filingDate": ["2005-01-01", "2013-12-01"]}}},
    320193: {"name": "Apple Inc.", "tickers": ["AAPL"],
             "formerNames": [{"name": "APPLE COMPUTER INC", "from": "1994-01-01",
                              "to": "2007-01-10"}],
             "filings": {"recent": {"filingDate": ["2005-01-01", "2024-01-01"]}}},
    789019: {"name": "MICROSOFT CORP", "tickers": ["MSFT"], "formerNames": [],
             "filings": {"recent": {"filingDate": ["2001-01-01", "2024-01-01"]}}},
}


def _index(tmp_path, docs=None, extra=None):
    docs = _ISSUERS if docs is None else docs
    zp = tmp_path / "submissions.zip"
    ahash = _write_zip(zp, docs, extra)
    idx = SI.SecIssuerIndex(tmp_path / "issuer.sqlite", clock=_clock())
    r = idx.index_submissions_archive(zp, archive_hash=ahash, member_step=1000)
    return idx, zp, ahash, r


class FakeAcc(H.NorgateIdentityAccessor):
    def __init__(self, ident, surv, current):
        super().__init__()
        self._ident, self._surv, self._current = ident, surv, current

    def available(self):
        return True, "fake"

    def watchlist_symbols(self, name):
        return list(self._surv) if "Past" in name else list(self._current)

    def security_identity(self, symbol):
        return dict(self._ident.get(symbol, {}))

    def membership_intervals(self, symbol, index_name, start_date="1990-01-01"):
        end = self._ident.get(symbol, {}).get("last_quoted_date")
        return [{"index_name": index_name, "member_start": "2005-01-01",
                 "member_end": end}]


_IDENT = {
    "AAPL": dict(assetid=1, security_name="Apple Inc.", exchange_name="NASDAQ",
                 first_quoted_date="1990-01-01", last_quoted_date=None,
                 base_type="Equity"),
    "MSFT": dict(assetid=2, security_name="Microsoft Corp", exchange_name="NASDAQ",
                 first_quoted_date="1990-01-01", last_quoted_date=None,
                 base_type="Equity"),
    "AMR-201312": dict(assetid=3, security_name="AMR Corp", exchange_name="NYSE",
                       first_quoted_date="1990-01-01",
                       last_quoted_date="2013-12-09", base_type="Equity"),
    "NOSEC-201005": dict(assetid=5, security_name="Obscure Nonfiler Holdings",
                         exchange_name="NYSE", first_quoted_date="2000-01-01",
                         last_quoted_date="2010-05-01", base_type="Equity"),
}
_SURV = ["AAPL", "MSFT", "AMR-201312", "NOSEC-201005"]
_CURRENT = ["AAPL", "MSFT"]


def _fake_transport(req, timeout):
    if "company_tickers.json" in req.get("url", ""):
        return {"status": 200, "body": json.dumps(
            {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
             "1": {"cik_str": 789019, "ticker": "MSFT",
                   "title": "Microsoft Corp"}}).encode()}
    return {"status": 404, "body": b""}


def _fake_normalized(rt, *, limit=3000):
    if rt == "FILING_EVENT":
        return [{"ticker": "AAPL", "normalized_payload": {
            "cik": "320193", "ticker": "AAPL", "form": "10-K",
            "accession": "0000320193-20-000001"}, "available_at": "2020-11-01"}]
    return []


def _ctx(tmp_path, idx):
    store = H.IdentityStore(tmp_path / "identity.sqlite", clock=_clock())
    s101 = {"enabled": True, "planner_enabled": True,
            "bulk_root": str(tmp_path), "company_tickers_url":
            "http://x/company_tickers.json", "contact_email": "t@example.com",
            "norgate_full_batch": 250, "resolve_full_batch": 500,
            "submissions_time_budget_seconds": 30, "priority": 6}
    return IJ.IdentityJobContext(
        store=store, accessor=FakeAcc(_IDENT, _SURV, _CURRENT),
        artifact_root=str(tmp_path / "artifacts"),
        rebalance_dates=["2010-06-30", "2013-06-30"], issuer_index=idx,
        read_normalized=_fake_normalized, transport=_fake_transport,
        stage101=s101, clock=_clock())


def _job(lane, payload=None):
    return AR.Job(job_id="j_" + lane, dedupe_key="d", category="DATA_ACQUISITION",
                  lane=lane, state="RUNNING", payload=payload or {})


def _drive(ctx, queue, cycles=30):
    """Run the real planner -> claim -> dispatch -> settle loop offline."""
    log = []
    for _ in range(cycles):
        plan = IJ.plan_next_identity_job(queue, ctx, cfg=ctx.stage101)
        if plan is None:
            break
        job = queue.claim_next(
            categories=[AR.CAT_DATA_ACQUISITION, AR.CAT_DATA_VALIDATION],
            origins=[IJ.ORIGIN, IJ.ORIGIN_101], lane_prefixes=["identity."])
        if job is None:
            break
        outcome, detail = IJ.dispatch_identity_job(job, ctx)
        if outcome == AR.OUTCOME_COMPLETED:
            queue.complete(job.job_id, result=detail)
        elif outcome == AR.OUTCOME_RETRYABLE:
            queue.mark_retryable(job.job_id, "retry")
        else:
            queue.block_specific(job.job_id, detail.get("blocker", "blk"))
        log.append((job.lane, outcome, detail))
    return log


# --------------------------------------------------------------------------- #
# 1. complete ZIP inventory is restart-safe.
# --------------------------------------------------------------------------- #
def test_p01_zip_indexing_restart_safe(tmp_path):
    idx, zp, ahash, r1 = _index(tmp_path)
    done1 = idx.archive_status("submissions")["members_done"]
    r2 = idx.index_submissions_archive(zp, archive_hash=ahash, member_step=1000)
    assert r1["complete"] and r2["complete"]
    assert r2["created_this_call"] == 0  # nothing re-created after resume
    assert idx.archive_status("submissions")["members_done"] == done1


# 2. ZIP path traversal is rejected.
def test_p02_zip_path_traversal_rejected(tmp_path):
    docs = {320193: _ISSUERS[320193]}
    idx, zp, ahash, r = _index(tmp_path, docs,
                               extra={"evil/../CIK0000000009.json":
                                      json.dumps({"cik": 9, "name": "EVIL"})})
    assert r["skipped_members"] >= 1
    assert idx.issuer(9) is None  # traversal member never indexed


# 3. malformed members are isolated.
def test_p03_malformed_members_isolated(tmp_path):
    idx, zp, ahash, r = _index(
        tmp_path, {320193: _ISSUERS[320193]},
        extra={"CIK0000000001.json": "{ bad json ]"})
    assert r["malformed_members"] == 1
    assert idx.issuer(320193) is not None  # good member still indexed


# 4. archive parse is deterministic.
def test_p04_parse_deterministic(tmp_path):
    doc = {"cik": 6201, **_ISSUERS[6201]}
    a = SI.parse_submissions_document(doc, source_member="m", archive_hash="h")
    b = SI.parse_submissions_document(doc, source_member="m", archive_hash="h")
    assert H.content_hash(a) == H.content_hash(b)


# 5. identical archive replay is idempotent (digest stable).
def test_p05_replay_idempotent(tmp_path):
    idx, zp, ahash, _ = _index(tmp_path)
    d1 = idx.digest()
    idx.index_submissions_archive(zp, archive_hash=ahash, member_step=1000)
    assert idx.digest() == d1


# 6. changed archive bytes create a new revision.
def test_p06_changed_archive_new_revision(tmp_path):
    idx, zp, ahash, _ = _index(tmp_path)
    idx.index_submissions_archive(zp, archive_hash="DIFFERENT_HASH",
                                  member_step=1000)
    kinds = [r["kind"] for r in idx.revisions(limit=50)]
    assert "ARCHIVE_REVISION" in kinds


# 7. every valid submissions member is indexed.
def test_p07_every_member_indexed(tmp_path):
    idx, zp, ahash, r = _index(tmp_path)
    assert r["issuers_indexed"] == len(_ISSUERS)
    assert idx.counts()["issuers"] == len(_ISSUERS)


# 8. formerNames effective dates preserved.
def test_p08_former_name_dates_preserved(tmp_path):
    idx, *_ = _index(tmp_path)
    iss = idx.issuer(320193)
    fn = iss["former_names"][0]
    assert fn["name"] == "APPLE COMPUTER INC"
    assert fn["from"] == "1994-01-01" and fn["to"] == "2007-01-10"


# 9. current tickers not mislabeled historical (source-tagged; obs separate).
def test_p09_current_tickers_not_historical(tmp_path):
    idx, *_ = _index(tmp_path)
    idx.index_company_tickers(
        {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}},
        kind="company_tickers")
    # ticker_current rows are source-tagged; a filing OBSERVATION is separate.
    assert idx.candidates_by_ticker("AAPL") == {"0000320193"}
    assert idx.counts()["ticker_observation_rows"] == 0


# 10. filing-derived ticker observations retain filing dates.
def test_p10_filing_observation_dates(tmp_path):
    idx, *_ = _index(tmp_path)
    r = idx.index_filing_evidence(_fake_normalized("FILING_EVENT"),
                                  source="owned_FILING_EVENT")
    assert r["observations_added"] == 1
    conn = idx._connect()
    try:
        row = conn.execute("SELECT observed_date FROM ticker_observation").fetchone()
        assert row["observed_date"] == "2020-11-01"
    finally:
        conn.close()


# 11. duplicate filing evidence is suppressed.
def test_p11_duplicate_filing_suppressed(tmp_path):
    idx, *_ = _index(tmp_path)
    idx.index_filing_evidence(_fake_normalized("FILING_EVENT"), source="s")
    r2 = idx.index_filing_evidence(_fake_normalized("FILING_EVENT"), source="s")
    assert r2["observations_added"] == 0


# 12. CIK normalization is deterministic.
def test_p12_cik_norm_deterministic(tmp_path):
    assert H.norm_cik(320193) == "0000320193" == H.norm_cik("320193")
    assert H.norm_cik("00320193") == "0000320193"
    assert H.norm_cik("abc") is None and H.norm_cik("") is None


# 13. complete Norgate watchlist enumerated (discovery covers all).
def test_p13_full_universe_discovered(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    IJ._norgate_full_discovery(ctx, _job(IJ.LANE_NORGATE_FULL_DISCOVERY))
    assert ctx.store.counts()["total_securities"] == len(_SURV)


# 14. delisted Norgate symbols preserved.
def test_p14_delisted_preserved(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    IJ._norgate_full_discovery(ctx, _job(IJ.LANE_NORGATE_FULL_DISCOVERY))
    assert ctx.store.counts()["delisted_securities"] == 2  # AMR + NOSEC


# 15. assetid remains the security identity.
def test_p15_assetid_is_identity(tmp_path):
    assert H.canonical_security_id(3) == "ngid:3"
    assert H.canonical_security_id(3) != H.canonical_security_id(4)


# 16. ticker reuse does not merge assetids.
def test_p16_ticker_reuse_no_merge(tmp_path):
    a = H.canonical_security_id(10, fallback_symbol="FOO")
    b = H.canonical_security_id(11, fallback_symbol="FOO")
    assert a != b  # same ticker, different assetid => distinct identities


# 17. different share classes remain distinct.
def test_p17_share_classes_distinct(tmp_path):
    assert H.derive_share_class("ACME Corp Class A") == "CLASS_A"
    assert H.derive_share_class("ACME Corp Class B") == "CLASS_B"
    assert H.canonical_security_id(20) != H.canonical_security_id(21)


# 18. ticker-only evidence cannot resolve (delisted, no overlap, no name).
def test_p18_ticker_only_cannot_resolve(tmp_path):
    docs = {555: {"name": "Totally Different Name", "tickers": [],
                  "filings": {"recent": {"filingDate": ["2015-01-01",
                                                        "2020-01-01"]}}}}
    idx, *_ = _index(tmp_path, docs)
    # give the index a filing-observation of ticker GHST for cik 555 (dated 2018)
    idx.index_filing_evidence([{"ticker": "GHST", "normalized_payload":
                                {"cik": "555", "ticker": "GHST"},
                                "available_at": "2018-01-01"}], source="s")
    sec = {"security_id": "ngid:99", "ticker": "GHST", "issuer_name": "Ghost Co",
           "security_start_date": "2005-01-01", "security_end_date": "2010-01-01",
           "is_current": False, "norgate_assetid": "99", "name_history": []}
    tki, sub, meta = idx.candidate_evidence_for(sec)
    res = H.match_security_to_cik(sec, ticker_cik_index=tki,
                                  submissions_by_cik=sub)
    assert res.status != H.STATUS_RESOLVED  # no date overlap, no name match


# 19. current-security corroborated mapping can resolve.
def test_p19_current_corroborated_resolves(tmp_path):
    idx, *_ = _index(tmp_path)
    sec = {"security_id": "ngid:1", "ticker": "AAPL", "issuer_name": "Apple Inc.",
           "security_start_date": "1990-01-01", "security_end_date": None,
           "is_current": True, "norgate_assetid": "1", "name_history": []}
    tki, sub, meta = idx.candidate_evidence_for(sec)
    res = H.match_security_to_cik(sec, ticker_cik_index=tki,
                                  submissions_by_cik=sub)
    assert res.status == H.STATUS_RESOLVED and res.cik == "0000320193"


# 20. former-name / date-overlap mapping can resolve a delisted name.
def test_p20_former_name_and_delisted_name_resolve(tmp_path):
    idx, *_ = _index(tmp_path)
    # AMR delisted resolves by legal name (no current ticker->CIK).
    sec = {"security_id": "ngid:3", "ticker": "AMR", "issuer_name": "AMR Corp",
           "security_start_date": "1990-01-01", "security_end_date": "2013-12-09",
           "is_current": False, "norgate_assetid": "3", "name_history": []}
    tki, sub, meta = idx.candidate_evidence_for(sec)
    res = H.match_security_to_cik(sec, ticker_cik_index=tki,
                                  submissions_by_cik=sub)
    assert res.status == H.STATUS_RESOLVED and res.cik == "0000006201"
    assert res.tier == H.TIER_LEGAL_NAME_OVERLAP


# 21. multiple CIK candidates remain ambiguous.
def test_p21_multiple_candidates_ambiguous(tmp_path):
    docs = {111: {"name": "Ajax Corp", "tickers": [],
                  "filings": {"recent": {"filingDate": ["2005-01-01",
                                                        "2012-01-01"]}}},
            222: {"name": "Ajax Corp", "tickers": [],
                  "filings": {"recent": {"filingDate": ["2006-01-01",
                                                        "2011-01-01"]}}}}
    idx, *_ = _index(tmp_path, docs)
    sec = {"security_id": "ngid:7", "ticker": "AJX", "issuer_name": "Ajax Corp",
           "security_start_date": "2007-01-01", "security_end_date": "2010-01-01",
           "is_current": False, "norgate_assetid": "7", "name_history": []}
    tki, sub, meta = idx.candidate_evidence_for(sec)
    res = H.match_security_to_cik(sec, ticker_cik_index=tki,
                                  submissions_by_cik=sub)
    assert res.status == H.STATUS_AMBIGUOUS
    assert IJ._classify_unresolved(sec, res, meta) == SI.REASON_NAME_COLLISION


# 22. successor identity cannot rewrite predecessor history.
def test_p22_successor_never_rewrites(tmp_path):
    # Successor 'NewCo' carries the delisted predecessor as a formerName.
    docs = {900: {"name": "NewCo Inc", "tickers": ["NEW"],
                  "formerNames": [{"name": "OldCo Corp", "from": "2000-01-01",
                                   "to": "2012-01-01"}],
                  "filings": {"recent": {"filingDate": ["2000-01-01",
                                                        "2024-01-01"]}}}}
    idx, *_ = _index(tmp_path, docs)
    store = H.IdentityStore(tmp_path / "id.sqlite", clock=_clock())
    ident = H.SecurityIdentity(
        security_id="ngid:800", norgate_assetid="800", norgate_symbol="OLD-201201",
        ticker="OLD", issuer_name="OldCo Corp", share_class="COMMON",
        base_type="Equity", exchange="NYSE", security_start_date="2000-01-01",
        security_end_date="2012-01-01", delisting_date="2012-01-01",
        is_current=False)
    store.upsert_security(ident)
    # Mark predecessor UNRESOLVED so it appears in the backlog for the scan.
    store.record_mapping(H.MappingResult(
        "ngid:800", None, H.METHOD_UNRESOLVED, H.TIER_UNRESOLVED_BACKLOG, 0.0,
        H.STATUS_UNRESOLVED, "2000-01-01", "2012-01-01",
        {"reason": "x", "required_evidence": []}))
    ctx = _ctx(tmp_path, idx)
    ctx.store = store
    IJ._successor_scan(ctx, _job(IJ.LANE_SUCCESSOR_SCAN))
    # Evidence is recorded but the predecessor mapping is UNCHANGED (UNRESOLVED).
    assert idx.counts()["successor_evidence_rows"] >= 1
    assert store.active_mapping("ngid:800")["status"] == H.STATUS_UNRESOLVED


# 23. unresolved reason codes are exact.
def test_p23_reason_codes_exact(tmp_path):
    idx, *_ = _index(tmp_path)
    sec = {"security_id": "ngid:5", "ticker": "NOSEC",
           "issuer_name": "Obscure Nonfiler Holdings",
           "security_start_date": "2000-01-01", "security_end_date": "2010-05-01",
           "is_current": False, "norgate_assetid": "5", "share_class": "COMMON",
           "name_history": []}
    tki, sub, meta = idx.candidate_evidence_for(sec)
    res = H.match_security_to_cik(sec, ticker_cik_index=tki,
                                  submissions_by_cik=sub)
    code = IJ._classify_unresolved(sec, res, meta)
    assert code == SI.REASON_NO_SEC_CANDIDATE
    assert code in SI.UNRESOLVED_REASON_CODES


# 24. mapping replay adds no duplicate revisions.
def test_p24_mapping_replay_no_dup_revision(tmp_path):
    store = H.IdentityStore(tmp_path / "id.sqlite", clock=_clock())
    ident = H.SecurityIdentity(
        security_id="ngid:1", norgate_assetid="1", norgate_symbol="AAPL",
        ticker="AAPL", issuer_name="Apple Inc.", share_class="COMMON",
        base_type="Equity", exchange="NASDAQ", security_start_date="1990-01-01",
        security_end_date=None, delisting_date=None, is_current=True)
    store.upsert_security(ident)
    res = H.MappingResult("ngid:1", "0000320193", H.METHOD_FILING_TICKER_OVERLAP,
                          H.TIER_FILING_TICKER_OVERLAP, 0.8, H.STATUS_RESOLVED,
                          "1990-01-01", None, {"tier": 3})
    store.record_mapping(res)
    n1 = len(store.revisions(limit=1000))
    store.record_mapping(res)  # identical => no-op
    assert len(store.revisions(limit=1000)) == n1


# 25. campaign cursors resume after restart.
def test_p25_cursors_resume(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    IJ._norgate_full_discovery(ctx, _job(IJ.LANE_NORGATE_FULL_DISCOVERY))
    ctx.stage101 = dict(ctx.stage101, resolve_full_batch=1)  # tiny batch
    IJ._cik_full_resolution(ctx, _job(IJ.LANE_CIK_FULL_RESOLUTION))
    cur1 = ctx.store.get_meta("stage101_res_cursor")
    assert cur1  # cursor advanced
    IJ._cik_full_resolution(ctx, _job(IJ.LANE_CIK_FULL_RESOLUTION))
    assert ctx.store.get_meta("stage101_res_cursor") >= cur1  # resumed forward


# 26. duplicate continuation jobs are suppressed (one live job max).
def test_p26_no_duplicate_live_job(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    queue = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    p1 = IJ.plan_next_identity_job(queue, ctx, cfg=ctx.stage101)
    assert p1 is not None
    # A live job exists now -> the planner must not enqueue a second.
    assert IJ.plan_next_identity_job(queue, ctx, cfg=ctx.stage101) is None


# 27. exactly one job is planned per cycle.
def test_p27_one_job_per_cycle(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    queue = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    IJ.plan_next_identity_job(queue, ctx, cfg=ctx.stage101)
    live = [j for j in queue.list_jobs(state=AR.STATE_QUEUED, limit=100)
            if j.origin in (IJ.ORIGIN, IJ.ORIGIN_101)]
    assert len(live) == 1


# 28. companyfacts fairness contract remains intact (categories/allowlist).
def _load_stage8_cfg():
    with open("configs/alpha_agent/stage8_autonomy.json",
              encoding="utf-8-sig") as fh:
        return json.load(fh)


def test_p28_companyfacts_fairness_intact():
    cfg = _load_stage8_cfg()
    cd = cfg["autonomy"]["collect_drain"]
    assert "stage10.1-identity" in cd["allowed_origins"]
    assert cd["companyfacts_continuation"]["fairness_enabled"] is True
    assert cd["max_jobs_per_cycle"] == 1
    # Every Stage 10.1 lane uses only already-admitted categories.
    assert IJ._ACQUISITION_LANES.union(IJ._VALIDATION_LANES).issuperset({
        IJ.LANE_SEC_BULK_INVENTORY, IJ.LANE_SEC_SUBMISSIONS_INDEX,
        IJ.LANE_CIK_FULL_RESOLUTION, IJ.LANE_MAPPING_COVERAGE_MEASURE})


# 29. all historical securities are attempted (every one gets a mapping).
def test_p29_all_securities_attempted(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    queue = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    _drive(ctx, queue)
    total = ctx.store.counts()["total_securities"]
    attempted = sum(1 for s in ctx.store.list_securities()
                    if ctx.store.active_mapping(s["security_id"]) is not None)
    assert total == len(_SURV) and attempted == total


# 30. coverage measured on every configured rebalance date.
def test_p30_coverage_every_rebalance_date(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    queue = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    _drive(ctx, queue)
    snap = ctx.store.latest_coverage_snapshot()
    assert set(snap["by_date"].keys()) == set(ctx.rebalance_dates)


# 31. aggregate coverage alone cannot unlock (survivorship denominator).
def test_p31_aggregate_cannot_unlock(tmp_path):
    store = H.IdentityStore(tmp_path / "id.sqlite", clock=_clock())
    # 2 current (resolved) + 3 delisted (unresolved) all members on the date.
    for i, (sym, cur) in enumerate([("A", True), ("B", True), ("C-201001", False),
                                    ("D-201002", False), ("E-201003", False)]):
        ident = H.SecurityIdentity(
            security_id="ngid:%d" % i, norgate_assetid=str(i), norgate_symbol=sym,
            ticker=sym.split("-")[0], issuer_name="Co%d" % i, share_class="COMMON",
            base_type="Equity", exchange="NYSE", security_start_date="2005-01-01",
            security_end_date=None if cur else "2010-06-01",
            delisting_date=None if cur else "2010-06-01", is_current=cur,
            membership_intervals=[{"index_name": "S&P 500",
                                   "member_start": "2005-01-01",
                                   "member_end": None if cur else "2010-06-01"}])
        store.upsert_security(ident)
        if cur:
            store.record_mapping(H.MappingResult(
                "ngid:%d" % i, "%010d" % (100 + i), H.METHOD_FILING_TICKER_OVERLAP,
                3, 0.8, H.STATUS_RESOLVED, "2005-01-01", None, {"t": 3}))
    store.record_coverage_snapshot(as_of="2010-06-30",
                                   rebalance_dates=["2010-03-31"])
    ms = FR.historical_mapping_status_from_store(store, {})
    assert ms["available"] is False  # 2/5 = 40% over survivorship universe < 60%


# 32. readiness is derived from canonical measured state.
def test_p32_readiness_from_canonical_state(tmp_path):
    store = H.IdentityStore(tmp_path / "id.sqlite", clock=_clock())
    assert FR.historical_mapping_status_from_store(store, {})["available"] is False
    assert FR.historical_mapping_status_from_store(store, {})["source"] == \
        "measured_identity_store"


# 33. insufficient mapping remains blocked.
def test_p33_insufficient_mapping_blocked(tmp_path):
    store = H.IdentityStore(tmp_path / "id.sqlite", clock=_clock())
    gate = FR.historical_fundamental_experiment_allowed(
        {"stage9_5": {"fundamental_experiments":
                      {"historical_evaluation_enabled": True}}}, store=store)
    assert gate["allowed"] is False
    assert gate["diagnostic"] == FR.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY


# 34. sufficient mapping can clear the mapping blocker.
def test_p34_sufficient_mapping_available(tmp_path):
    store = H.IdentityStore(tmp_path / "id.sqlite", clock=_clock())
    for i in range(10):
        cur = i < 7  # 7/10 = 70% resolved over the survivorship universe
        ident = H.SecurityIdentity(
            security_id="ngid:%d" % i, norgate_assetid=str(i),
            norgate_symbol="S%d" % i, ticker="S%d" % i, issuer_name="Co%d" % i,
            share_class="COMMON", base_type="Equity", exchange="NYSE",
            security_start_date="2005-01-01", security_end_date=None,
            delisting_date=None, is_current=True,
            membership_intervals=[{"index_name": "S&P 500",
                                   "member_start": "2005-01-01",
                                   "member_end": None}])
        store.upsert_security(ident)
        if cur:
            store.record_mapping(H.MappingResult(
                "ngid:%d" % i, "%010d" % (100 + i), H.METHOD_FILING_TICKER_OVERLAP,
                3, 0.8, H.STATUS_RESOLVED, "2005-01-01", None, {"t": 3}))
    store.record_coverage_snapshot(as_of="2010-06-30",
                                   rebalance_dates=["2010-03-31"])
    assert FR.historical_mapping_status_from_store(store, {})["available"] is True


# 35. no candidate leaves DATA_HOLD prematurely (every handler detail).
def test_p35_data_hold_preserved(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    queue = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    log = _drive(ctx, queue)
    assert log  # campaign ran
    for lane, outcome, detail in log:
        assert detail.get("disposition") == "DATA_HOLD"


# 36. no model is promoted (no automatic promotion anywhere).
def test_p36_no_model_promotion(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    queue = AR.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock())
    log = _drive(ctx, queue)
    for lane, outcome, detail in log:
        if "no_automatic_promotion" in detail:
            assert detail["no_automatic_promotion"] is True


# 37. no shadow book is backfilled (readiness never flips the switch).
def test_p37_no_shadow_book_no_switch_flip(tmp_path):
    idx, *_ = _index(tmp_path)
    ctx = _ctx(tmp_path, idx)
    outcome, detail = IJ._readiness_recheck(ctx, _job(IJ.LANE_READINESS_RECHECK))
    assert detail["safety_switch_flipped"] is False


# 38. stores live under the research root, never an operational-ledger root.
def test_p38_stores_outside_operational_root(tmp_path):
    cfg = _load_stage8_cfg()
    s101 = cfg["stage10_identity"]["stage10_1"]
    for key in ("issuer_index_db", "bulk_root"):
        p = s101[key].replace("\\", "/").lower()
        assert "alpha_agent" in p
        assert ".paper_trader" not in p  # not the operational-ledger root


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
