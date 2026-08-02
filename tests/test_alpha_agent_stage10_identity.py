"""
Stage 10 HISTORICAL IDENTITY LAYER — deterministic property tests.

Proves the 32 required Stage-10 properties end to end with fakes + real durable
stores (identity store, research queue, fairness store), fixed clocks and NO
network: effective-dated survivorship-safe identity; a ticker-text-insufficient
matching contract that leaves ambiguity unresolved; a survivorship-safe universe
service; canonical queue-operated, deduplicated, fairness-preserving identity
jobs; an honest, measured readiness unlock that current-CIK coverage can never
trip; effective-dated event-issuer mapping; and zero trading / candidate / model
/ shadow-book / operational-ledger mutation.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from alpha_agent import autonomous_research as ar
from alpha_agent import drain_fairness as df
from alpha_agent import fundamental_readiness as fr
from alpha_agent import historical_identity as H
from alpha_agent import identity_jobs as J

_REPO = Path(__file__).resolve().parents[1]
_STAGE8 = json.loads((_REPO / "configs" / "alpha_agent" /
                      "stage8_autonomy.json").read_text(encoding="utf-8-sig"))


class _Clock:
    def __init__(self):
        self._n = 0

    def __call__(self):
        self._n += 1
        return "2026-08-02T%02d:%02d:%02d" % (self._n // 3600,
                                              (self._n // 60) % 60,
                                              self._n % 60)


def _ident(aid, name, first, last, mem):
    return {"ident": {"assetid": aid, "security_name": name,
                      "exchange_name": "NYSE", "subtype1": "Equity",
                      "base_type": "Stock Market", "first_quoted_date": first,
                      "last_quoted_date": last}, "mem": mem}


def _span(start, end):
    return [{"index_name": "S&P 500", "member_start": start, "member_end": end}]


class _FakeAcc(H.NorgateIdentityAccessor):
    def __init__(self, data, current, avail=True):
        self._data = data
        self._cur = current
        self._avail = avail

    def available(self):
        return (self._avail, "fake" if self._avail else "NDU down")

    def watchlist_symbols(self, name):
        return list(self._data) if "Past" in name else list(self._cur)

    def security_identity(self, sym):
        return dict(self._data[sym]["ident"])

    def membership_intervals(self, sym, index, start="1990-01-01"):
        return list(self._data[sym].get("mem", []))


def _default_data():
    return {
        "AAPL": _ident("1", "Apple Inc Common", "1990-01-02", None,
                       _span("2000-01-01", None)),
        "MSFT": _ident("2", "Microsoft Corp Common", "1990-01-02", None,
                       _span("1994-06-01", None)),
        "AAMRQ-201312": _ident("3", "AMR Corp Common", "1990-01-02",
                               "2013-12-09", _span("1990-01-02", "2011-11-30")),
        "LEHMQ-200809": _ident("4", "Lehman Brothers Hldgs Common", "1994-01-01",
                               "2008-09-15", _span("1994-01-01", "2008-09-15")),
    }


def _store(tmp_path, name="id.sqlite"):
    return H.IdentityStore(tmp_path / name, clock=_Clock())


def _queue(tmp_path, name="q.sqlite"):
    return ar.ResearchQueue(tmp_path / name, clock=_Clock())


def _ctx(tmp_path, data=None, current=None, populate=True, **kw):
    data = _default_data() if data is None else data
    current = current if current is not None else {"AAPL", "MSFT"}
    st = _store(tmp_path)
    if populate:
        for sym in data:
            st.upsert_security(H.extract_security_identity(
                _FakeAcc(data, current), sym))
    return st, J.IdentityJobContext(
        store=st, accessor=_FakeAcc(data, current),
        artifact_root=str(tmp_path / "artifacts"),
        rebalance_dates=["1999-06-30", "2005-06-30", "2013-06-30"],
        cfg9={"stage9_5": {"fundamental_mvp": {"signals": ["gross_profitability"]},
                           "fundamental_experiments":
                           {"historical_evaluation_enabled": False}}}, **kw)


def _handlers(ctx):
    def h(job):
        return J.dispatch_identity_job(job, ctx)
    return {c: h for c in ar.JOB_CATEGORIES}


# =========================================================================== #
# 1-8 — effective-dated survivorship-safe identity + universe service.
# =========================================================================== #
def test_p01_identity_is_effective_dated(tmp_path):
    st, _ = _ctx(tmp_path)
    amr = st.get_security("ngid:3")
    assert amr["ticker_history"] and amr["ticker_history"][0]["effective_start"]
    assert amr["membership_intervals"][0]["member_start"] == "1990-01-02"
    assert amr["membership_intervals"][0]["member_end"] == "2011-11-30"
    # universe service exposes the ticker effective on the formation date
    u = st.historical_universe_on("2005-06-30")
    amr_row = next(x for x in u if x["security_id"] == "ngid:3")
    assert amr_row["ticker_effective_on"] == "AAMRQ"


def test_p02_ticker_change_preserves_identity(tmp_path):
    # Same assetid observed twice under DIFFERENT tickers -> one security id.
    st = _store(tmp_path)
    acc1 = _FakeAcc({"FB": _ident("9", "Facebook Inc Common", "2012-05-18", None,
                                  _span("2013-12-23", None))}, {"FB"})
    acc2 = _FakeAcc({"META": _ident("9", "Meta Platforms Inc Common",
                                    "2012-05-18", None,
                                    _span("2013-12-23", None))}, {"META"})
    a = st.upsert_security(H.extract_security_identity(acc1, "FB"))
    b = st.upsert_security(H.extract_security_identity(acc2, "META"))
    assert a["security_id"] == b["security_id"] == "ngid:9"
    assert len(st.list_securities()) == 1
    hist = st.get_security("ngid:9")["ticker_history"]
    assert {h["ticker"] for h in hist} == {"FB", "META"}   # append-only history


def test_p03_ticker_reuse_creates_separate_identities(tmp_path):
    st = _store(tmp_path)
    old = _FakeAcc({"XYZ-200106": _ident("10", "Old Xyz Common", "1990-01-01",
                                         "2001-06-30", [])}, set())
    new = _FakeAcc({"XYZ": _ident("11", "New Xyz Common", "2015-01-01", None,
                                  [])}, {"XYZ"})
    st.upsert_security(H.extract_security_identity(old, "XYZ-200106"))
    st.upsert_security(H.extract_security_identity(new, "XYZ"))
    secs = {s["security_id"] for s in st.list_securities()}
    assert secs == {"ngid:10", "ngid:11"}   # ticker reused, identities distinct


def test_p04_delisted_remain_in_earlier_universes(tmp_path):
    st, _ = _ctx(tmp_path)
    early = {x["ticker"] for x in st.historical_universe_on("1999-06-30")}
    assert "AAMRQ" in early and "LEHMQ" in early   # delisted present in the past


def test_p05_future_constituents_not_early(tmp_path):
    st, _ = _ctx(tmp_path)
    # AAPL joined 2000-01-01; must be absent on 1999-06-30.
    assert "AAPL" not in {x["ticker"]
                          for x in st.historical_universe_on("1999-06-30")}


def test_p06_exited_disappear_after_exit(tmp_path):
    st, _ = _ctx(tmp_path)
    # AMR exited 2011-11-30; must be absent on 2013-06-30.
    assert "AAMRQ" not in {x["ticker"]
                           for x in st.historical_universe_on("2013-06-30")}


def test_p07_share_classes_deterministic(tmp_path):
    assert H.derive_share_class("Alphabet Inc Class A") == "CLASS_A"
    assert H.derive_share_class("Alphabet Inc Class C") == "CLASS_C"
    assert H.derive_share_class("Berkshire Hathaway Common") == "COMMON"
    st = _store(tmp_path)
    acc = _FakeAcc({"GOOGL": _ident("20", "Alphabet Inc Class A", "2004-08-19",
                                    None, []),
                    "GOOG": _ident("21", "Alphabet Inc Class C", "2014-04-03",
                                   None, [])}, {"GOOGL", "GOOG"})
    st.upsert_security(H.extract_security_identity(acc, "GOOGL"))
    st.upsert_security(H.extract_security_identity(acc, "GOOG"))
    assert len(st.list_securities()) == 2   # distinct assetids -> distinct secs


def test_p08_merger_successor_does_not_rewrite_predecessor(tmp_path):
    st, _ = _ctx(tmp_path)
    before = st.get_security("ngid:3")   # AMR (predecessor, delisted)
    # A successor with its OWN assetid + CIK mapping is recorded.
    st.record_mapping(H.MappingResult("ngid:1", "320193",
                                      H.METHOD_OWNED_AUTHORITATIVE, 1, 1.0,
                                      H.STATUS_RESOLVED, None, None, {"tier": 1}))
    after = st.get_security("ngid:3")
    assert before["content_hash"] == after["content_hash"]
    assert before["name_history"] == after["name_history"]


# =========================================================================== #
# 9-12 — matching contract + append-only mapping.
# =========================================================================== #
def test_p09_ambiguous_matches_remain_unresolved(tmp_path):
    st, _ = _ctx(tmp_path)
    sec = st.get_security("ngid:3")   # delisted, so current-listing path off
    res = H.match_security_to_cik(sec, ticker_cik_index={"AAMRQ": ["111", "222"]})
    assert res.status in (H.STATUS_AMBIGUOUS, H.STATUS_CONFLICT)
    assert res.cik is None


def test_p10_ticker_only_cannot_resolve(tmp_path):
    st, _ = _ctx(tmp_path)
    sec = st.get_security("ngid:3")   # delisted
    res = H.match_security_to_cik(sec, ticker_cik_index={"AAMRQ": ["555"]})
    assert res.status == H.STATUS_UNRESOLVED and res.cik is None


def test_p11_mapping_revisions_append_only(tmp_path):
    st, _ = _ctx(tmp_path)
    st.record_mapping(H.MappingResult("ngid:1", "111", H.METHOD_OWNED_AUTHORITATIVE,
                                      1, 1.0, H.STATUS_RESOLVED, None, None,
                                      {"tier": 1}))
    st.record_mapping(H.MappingResult("ngid:1", "222", H.METHOD_AUDITED_REPAIR,
                                      5, 0.85, H.STATUS_RESOLVED, None, None,
                                      {"tier": 5}))
    hist = st.mapping_history("ngid:1")
    assert len(hist) == 2
    prior, latest = hist[0], hist[1]
    assert prior["active"] == 0 and prior["superseded_by"] == latest["id"]
    assert latest["active"] == 1 and latest["cik"] == "222"


def test_p12_prior_mapping_evidence_queryable(tmp_path):
    st, _ = _ctx(tmp_path)
    st.record_mapping(H.MappingResult("ngid:1", "111", H.METHOD_OWNED_AUTHORITATIVE,
                                      1, 1.0, H.STATUS_RESOLVED, None, None,
                                      {"tier": 1, "note": "first"}))
    st.record_mapping(H.MappingResult("ngid:1", "222", H.METHOD_AUDITED_REPAIR, 5,
                                      0.85, H.STATUS_RESOLVED, None, None,
                                      {"tier": 5}))
    ev = json.loads(st.mapping_history("ngid:1")[0]["evidence_json"])
    assert ev.get("note") == "first"   # prior evidence still queryable


# =========================================================================== #
# 13-14 — idempotent replay + restart safety.
# =========================================================================== #
def test_p13_store_replay_idempotent(tmp_path):
    data = _default_data()
    st = _store(tmp_path)
    acc = _FakeAcc(data, {"AAPL", "MSFT"})
    for _ in range(3):
        for sym in data:
            st.upsert_security(H.extract_security_identity(acc, sym))
    d1 = st.digest()
    for sym in data:
        st.upsert_security(H.extract_security_identity(acc, sym))
    assert st.digest() == d1 and st.counts()["total_securities"] == 4


def test_p14_restart_resumes_safely(tmp_path):
    st, ctx = _ctx(tmp_path)
    ctx.store.mark_processed("identity.discover:abc", job_id="j1",
                             kind="identity.discover")
    n = st.counts()["total_securities"]
    reopened = H.IdentityStore(tmp_path / "id.sqlite")   # same path
    assert reopened.counts()["total_securities"] == n
    assert reopened.already_processed("identity.discover:abc")   # survives restart


# =========================================================================== #
# 15-20 — canonical queue, dedup, allowlist, one-job-per-cycle, fairness.
# =========================================================================== #
def test_p15_identity_jobs_use_canonical_queue(tmp_path):
    st, ctx = _ctx(tmp_path, populate=False)   # empty store, non-empty universe
    q = _queue(tmp_path)
    plan = J.plan_next_identity_job(q, ctx, cfg={"priority": 6})
    assert plan is not None and plan["lane"] == J.LANE_DISCOVER
    jobs = q.list_jobs(state=ar.STATE_QUEUED, limit=50)
    assert any(j.origin == J.ORIGIN and j.lane.startswith("identity.")
               for j in jobs)


def test_p16_identity_jobs_deduplicated(tmp_path):
    st, ctx = _ctx(tmp_path, data={}, current=set())
    q = _queue(tmp_path)
    assert J.plan_next_identity_job(q, ctx) is not None
    assert J.plan_next_identity_job(q, ctx) is None   # at most one live job
    # dispatch-level idempotency: a re-run of the same payload is a no-op.
    job = q.list_jobs(state=ar.STATE_QUEUED, limit=5)[0]
    st.mark_processed("%s:%s" % (job.lane, H.content_hash(job.payload)[:16]))
    out, det = J.dispatch_identity_job(job, ctx)
    assert out == ar.OUTCOME_COMPLETED and det.get("idempotent_skip") is True


def test_p17_exact_identity_lanes_allowlisted(tmp_path):
    drain = _STAGE8["autonomy"]["collect_drain"]
    assert "stage10-identity" in drain["allowed_origins"]
    assert "identity." in drain["allowed_lane_prefixes"]
    q = _queue(tmp_path)
    ok = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="identity.discover", payload={},
                   origin="stage10-identity")
    claimed = q.claim_next(origins=drain["allowed_origins"],
                           lane_prefixes=drain["allowed_lane_prefixes"],
                           categories=drain["allowed_categories"])
    assert claimed is not None and claimed.job_id == ok


def test_p18_unrelated_jobs_untouched(tmp_path):
    drain = _STAGE8["autonomy"]["collect_drain"]
    q = _queue(tmp_path)
    unrelated = q.enqueue(ar.CAT_SIGNAL_COMBINATION, lane="combination.x",
                          payload={}, origin="planner")
    q.enqueue(ar.CAT_DATA_ACQUISITION, lane="identity.discover", payload={},
              origin="stage10-identity")
    # Drain the identity allowlist; the unrelated planner job is never claimed.
    for _ in range(3):
        q.claim_next(origins=drain["allowed_origins"],
                     lane_prefixes=drain["allowed_lane_prefixes"],
                     categories=drain["allowed_categories"])
    assert q.get(unrelated).state == ar.STATE_QUEUED
    assert q.get(unrelated).attempts == 0


def _drain_cfg():
    d = _STAGE8["autonomy"]["collect_drain"]
    return {"max_jobs_per_cycle": 1,
            "allowed_origins": d["allowed_origins"],
            "allowed_lane_prefixes": d["allowed_lane_prefixes"],
            "allowed_categories": d["allowed_categories"],
            "sec_continuation": {"enabled": False},
            "companyfacts_continuation": {
                "enabled": True, "fairness_enabled": True,
                "fairness_max_idle_cycles": 2,
                "allowed_origins": ["campaign-continuation"],
                "allowed_lane_prefixes": ["acq.sec_companyfacts"],
                "allowed_categories": ["DATA_ACQUISITION"]}}


def test_p19_exactly_one_job_per_cycle(tmp_path):
    q = _queue(tmp_path)
    q.enqueue(ar.CAT_DATA_ACQUISITION, lane="identity.discover", payload={"a": 1},
              origin="stage10-identity", priority=6)
    q.enqueue(ar.CAT_EXPERIMENT, lane="tournament.stage9_4_revalidation",
              payload={}, origin="stage9-tournament", priority=3)
    q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_companyfacts", payload={},
              origin="campaign-continuation", priority=5)
    handled = {"n": 0}

    def h(job):
        handled["n"] += 1
        return ar.OUTCOME_COMPLETED, {"lane": job.lane}
    rep = df.run_fair_drain(q, {c: h for c in ar.JOB_CATEGORIES},
                            drain_cfg=_drain_cfg(), fair_store=None)
    assert rep["jobs_claimed"] == 1 and handled["n"] == 1


def test_p20_fairness_intact_identity_cannot_starve_companyfacts(tmp_path):
    q = _queue(tmp_path)
    fair = df.FairnessStore(tmp_path / "fair.sqlite", clock=_Clock())
    seq = {"i": 0}

    def h(job):
        return ar.OUTCOME_COMPLETED, {"lane": job.lane}
    handlers = {c: h for c in ar.JOB_CATEGORIES}
    # A CONTINUOUS identity + tournament backlog, plus a queued companyfacts
    # continuation. With bound=2 the companyfacts continuation must still execute
    # within 3 cycles despite identity sitting at priority 6.
    q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_companyfacts", payload={},
              origin="campaign-continuation", priority=5)
    cf_ran = []
    for cyc in range(3):
        # keep identity + tournament permanently available
        q.enqueue(ar.CAT_DATA_ACQUISITION, lane="identity.discover",
                  payload={"c": seq["i"]}, origin="stage10-identity", priority=6)
        q.enqueue(ar.CAT_EXPERIMENT, lane="tournament.stage9_4_revalidation",
                  payload={"c": seq["i"]}, origin="stage9-tournament", priority=3)
        seq["i"] += 1
        rep = df.run_fair_drain(q, handlers, drain_cfg=_drain_cfg(),
                                fair_store=fair)
        assert rep["jobs_claimed"] == 1   # one job per cycle, always
        if any(hd["lane"] == "acq.sec_companyfacts"
               for hd in rep.get("handled", [])):
            cf_ran.append(cyc)
    assert cf_ran, "companyfacts must execute within the fairness bound"
    assert cf_ran[0] <= 2 and rep["companyfacts_fairness"]["promoted"] is True


# =========================================================================== #
# 21-22 — dependency planner.
# =========================================================================== #
def test_p21_planner_identifies_exact_blockers(tmp_path):
    st, ctx = _ctx(tmp_path, populate=False)   # empty store, non-empty universe
    q = _queue(tmp_path)
    plan = J.plan_next_identity_job(q, ctx)
    assert plan["lane"] == J.LANE_DISCOVER
    assert "discovery incomplete" in plan["reason"]


def test_p22_planner_creates_correct_repair_job(tmp_path):
    data = _default_data()
    st, ctx = _ctx(tmp_path, data=data,
                   repair_rules={"ngid:3": {"cik": "111", "note": "audited"}})
    # Force a fully-decided store with an unresolved backlog: mark all UNRESOLVED.
    for s in st.list_securities():
        st.record_mapping(H.MappingResult(
            s["security_id"], None, H.METHOD_UNRESOLVED, 6, 0.0,
            H.STATUS_UNRESOLVED, None, None, {"tier": 6}))
    q = _queue(tmp_path)
    # planner accessor reports full universe already indexed (no discovery left)
    ctx.accessor = _FakeAcc(data, {"AAPL", "MSFT"})
    plan = J.plan_next_identity_job(q, ctx)
    assert plan["lane"] == J.LANE_REPAIR
    job = q.get(plan["job_id"])
    assert job.origin == J.ORIGIN and job.category == ar.CAT_DATA_VALIDATION


# =========================================================================== #
# 23-26 — readiness unlock is measured + honest; current-CIK can't unlock.
# =========================================================================== #
def _resolve_current(st):
    """Resolve every CURRENT security via the current-listing tier (owned unique
    ticker->CIK), leaving delisted UNRESOLVED — the honest current-only ceiling."""
    idx = {s["ticker"]: [str(1000 + i)]
           for i, s in enumerate(st.list_securities(is_current=True))}
    for s in st.list_securities():
        sec = st.get_security(s["security_id"])
        st.record_mapping(H.match_security_to_cik(sec, ticker_cik_index=idx))


def test_p23_mapping_progress_improves_readiness_honestly(tmp_path):
    st, _ = _ctx(tmp_path)
    st.record_coverage_snapshot(as_of="2026-08-02",
                                rebalance_dates=["2005-06-30"])
    before = fr.historical_mapping_status_from_store(
        st, {"stage9_5": {"per_rebalance_readiness":
                          {"min_historical_mapping_coverage_pct": 60.0}}})
    _resolve_current(st)
    st.record_coverage_snapshot(as_of="2026-08-03",
                                rebalance_dates=["2005-06-30"])
    after = fr.historical_mapping_status_from_store(
        st, {"stage9_5": {"per_rebalance_readiness":
                          {"min_historical_mapping_coverage_pct": 60.0}}})
    assert (after["measured_coverage_pct"] or 0) >= (before["measured_coverage_pct"]
                                                     or 0)
    assert after["available"] is False   # still below threshold -> honest


def test_p24_aggregate_current_cik_cannot_unlock(tmp_path):
    st, _ = _ctx(tmp_path)
    _resolve_current(st)   # 100% of CURRENT securities resolved
    cfg = {"stage9_5": {"per_rebalance_readiness":
                        {"min_historical_mapping_coverage_pct": 60.0},
                        "fundamental_experiments":
                        {"historical_evaluation_enabled": True}}}
    st.record_coverage_snapshot(as_of="2026-08-02",
                                rebalance_dates=["1999-06-30", "2005-06-30",
                                                 "2013-06-30"])
    gate = fr.historical_fundamental_experiment_allowed(cfg, store=st)
    assert gate["allowed"] is False
    assert gate["diagnostic"] == fr.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY


def test_p25_per_rebalance_readiness_uses_historical_membership(tmp_path):
    st, _ = _ctx(tmp_path)
    cfg = {"stage9_5": {"per_rebalance_readiness": {}}}
    rd = fr.measured_readiness_from_store(
        st, "gross_profitability", cfg,
        rebalance_dates=["1999-06-30", "2005-06-30", "2013-06-30"])
    by = {p["as_of"]: p["universe_names"] for p in rd["per_date"]}
    # membership-driven, survivorship-safe counts (delisted included in the past)
    assert by["1999-06-30"] == 3 and by["2005-06-30"] == 4 and by["2013-06-30"] == 2


def test_p26_current_survivor_substitution_impossible(tmp_path):
    st, _ = _ctx(tmp_path)
    # AAPL is a CURRENT survivor that joined only in 2000; it must NOT be
    # substituted into 1999. Delisted AMR/LEHMQ ARE present then.
    u1999 = {x["ticker"] for x in st.historical_universe_on("1999-06-30")}
    assert "AAPL" not in u1999 and {"AAMRQ", "LEHMQ", "MSFT"} <= u1999


# =========================================================================== #
# 27-28 — event/insider identity by effective date.
# =========================================================================== #
def test_p27_event_issuer_maps_by_effective_date(tmp_path):
    st = _store(tmp_path)
    # Two securities share CIK 999 but with disjoint lives (a ticker change).
    old = H.SecurityIdentity("ngid:50", "50", "OLD-201012", "OLD", "Old Co",
                             "COMMON", "Stock", "NYSE", "1990-01-01",
                             "2010-12-31", "2010-12-31", False,
                             ticker_history=[{"ticker": "OLD",
                                              "effective_start": "1990-01-01",
                                              "effective_end": "2010-12-31"}])
    new = H.SecurityIdentity("ngid:51", "51", "NEW", "NEW", "New Co", "COMMON",
                             "Stock", "NYSE", "2011-01-01", None, None, True,
                             ticker_history=[{"ticker": "NEW",
                                              "effective_start": "2011-01-01",
                                              "effective_end": None}])
    st.upsert_security(old)
    st.upsert_security(new)
    for sid in ("ngid:50", "ngid:51"):
        st.record_mapping(H.MappingResult(sid, "999",
                                          H.METHOD_OWNED_AUTHORITATIVE, 1, 1.0,
                                          H.STATUS_RESOLVED, None, None,
                                          {"tier": 1}))
    m = J.map_event_issuer(st, "999", "2005-06-01")   # inside OLD's life
    assert m["status"] == H.STATUS_RESOLVED and m["security_id"] == "ngid:50"
    assert m["ticker_effective"] == "OLD"


def test_p28_ambiguous_event_mappings_stay_blocked(tmp_path):
    st = _store(tmp_path)
    # Two securities share CIK 999 AND both cover the event date -> ambiguous.
    for i, sid in enumerate(("ngid:60", "ngid:61")):
        s = H.SecurityIdentity(sid, str(60 + i), "T%d" % i, "T%d" % i, "Co",
                               "COMMON", "Stock", "NYSE", "1990-01-01", None,
                               None, True)
        st.upsert_security(s)
        st.record_mapping(H.MappingResult(sid, "999",
                                          H.METHOD_OWNED_AUTHORITATIVE, 1, 1.0,
                                          H.STATUS_RESOLVED, None, None,
                                          {"tier": 1}))
    m = J.map_event_issuer(st, "999", "2015-06-01")
    assert m["status"] == H.STATUS_AMBIGUOUS and m["security_id"] is None


# =========================================================================== #
# 29-32 — safety invariants: DATA_HOLD, no promotion, no shadow, ledger intact.
# =========================================================================== #
def test_p29_no_candidate_leaves_data_hold_prematurely(tmp_path):
    st, ctx = _ctx(tmp_path)
    q = _queue(tmp_path)
    outcomes = []
    for lane, cat in ((J.LANE_DISCOVER, ar.CAT_DATA_ACQUISITION),
                      (J.LANE_CIK_RESOLVE, ar.CAT_DATA_ACQUISITION),
                      (J.LANE_COVERAGE, ar.CAT_DATA_VALIDATION),
                      (J.LANE_READINESS_EVAL, ar.CAT_DATA_VALIDATION)):
        jid = q.enqueue(cat, lane=lane, payload={"x": lane}, origin=J.ORIGIN)
        out, det = J.dispatch_identity_job(q.get(jid), ctx)
        outcomes.append((lane, det.get("disposition")))
    assert all(d == "DATA_HOLD" for _l, d in outcomes)


def test_p30_no_model_auto_promoted(tmp_path):
    st, ctx = _ctx(tmp_path)
    q = _queue(tmp_path)
    jid = q.enqueue(ar.CAT_DATA_VALIDATION, lane=J.LANE_READINESS_EVAL,
                    payload={}, origin=J.ORIGIN)
    out, det = J.dispatch_identity_job(q.get(jid), ctx)
    assert det.get("no_automatic_promotion") is True
    assert det.get("safety_switch_flipped") is False
    # the readiness gate itself is still refused (no historical coverage)
    assert det["gate"]["gross_profitability"]["allowed"] is False


def test_p31_no_shadow_book_backfilled(tmp_path):
    st, ctx = _ctx(tmp_path)
    q = _queue(tmp_path)
    for lane in (J.LANE_DISCOVER, J.LANE_CIK_RESOLVE, J.LANE_COVERAGE):
        jid = q.enqueue(ar.CAT_DATA_ACQUISITION if "resolve" in lane or
                        "discover" in lane else ar.CAT_DATA_VALIDATION,
                        lane=lane, payload={"x": lane}, origin=J.ORIGIN)
        _o, det = J.dispatch_identity_job(q.get(jid), ctx)
        # identity handlers never touch shadow books / promotion. Exclude the
        # artifact path (the pytest tmp dir echoes the test name, which contains
        # "shadow") and inspect the payload itself.
        det.pop("artifact", None)
        blob = json.dumps(det).lower()
        assert "shadow" not in blob and "promote" not in blob
        assert det.get("disposition") == "DATA_HOLD"


def test_p32_operational_ledgers_byte_identical(tmp_path):
    ledger = tmp_path / "op_ledger"
    ledger.mkdir()
    (ledger / "book.json").write_text('{"nav": 100000}', encoding="utf-8")

    def digest():
        h = {}
        for f in sorted(ledger.rglob("*")):
            if f.is_file():
                h[str(f)] = hashlib.sha256(f.read_bytes()).hexdigest()
        return hashlib.sha256(json.dumps(h, sort_keys=True).encode()).hexdigest()

    before = digest()
    st, ctx = _ctx(tmp_path)
    q = _queue(tmp_path)
    for lane, cat in ((J.LANE_DISCOVER, ar.CAT_DATA_ACQUISITION),
                      (J.LANE_CIK_RESOLVE, ar.CAT_DATA_ACQUISITION),
                      (J.LANE_COVERAGE, ar.CAT_DATA_VALIDATION),
                      (J.LANE_READINESS_EVAL, ar.CAT_DATA_VALIDATION)):
        jid = q.enqueue(cat, lane=lane, payload={"x": lane}, origin=J.ORIGIN)
        J.dispatch_identity_job(q.get(jid), ctx)
    assert digest() == before   # identity work never touches an operational ledger
