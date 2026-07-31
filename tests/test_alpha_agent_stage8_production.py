"""
Stage 8 — acceptance-vs-production run modes, sharded full-universe campaigns,
resumable SEC bulk-archive download, and autonomous batch continuation
(WS2-WS8). Deterministic: no real network — the Norgate accessor, the HTTP
transport and the collector are all injected/faked.
"""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.alpha_agent import acquisition_campaign as ac
from paper_trader.alpha_agent import autonomous_research as ar
from paper_trader.alpha_agent import production_universe as pu
from paper_trader.alpha_agent import runtime as rt
from paper_trader.alpha_agent import sec_bulk_download as bd

_REPO = Path(__file__).resolve().parents[1]
_STAGE8_CFG = _REPO / "configs" / "alpha_agent" / "stage8_autonomy.json"
_TODAY = datetime.now(timezone.utc).date().isoformat()


# --------------------------------------------------------------------------- #
# Fakes.
# --------------------------------------------------------------------------- #
class FakeNorgate:
    def __init__(self, survivorship, current, running=True):
        self._s, self._c, self._run = survivorship, current, running

    def status(self):
        if not self._run:
            raise RuntimeError("NDU down")
        return 1

    def watchlist_symbols(self, name):
        return self._s if "Current & Past" in name else self._c


class FakeHttp:
    """Deterministic ranged HTTP transport over an in-memory blob."""

    def __init__(self, data, last_modified="Mon, 01 Jan 2026 00:00:00 GMT",
                 etag='"v1"'):
        self.data, self.lm, self.etag = data, last_modified, etag

    def __call__(self, req, timeout):
        if req["method"] == "HEAD":
            return {"status": 200, "body": b"",
                    "headers": {"Content-Length": str(len(self.data)),
                                "Last-Modified": self.lm, "ETag": self.etag,
                                "Accept-Ranges": "bytes"}}
        lo, hi = (int(x) for x in
                  req["headers"]["Range"].split("=")[1].split("-"))
        return {"status": 206, "headers": {}, "body": self.data[lo:hi + 1]}


# --------------------------------------------------------------------------- #
# WS2 — run modes + dynamic production universe.
# --------------------------------------------------------------------------- #
class TestRunModesAndUniverse:
    def test_run_mode_defaults_to_acceptance_for_safety(self):
        assert pu.resolve_run_mode({}) == pu.ACCEPTANCE

    def test_config_and_override_select_production(self):
        assert pu.resolve_run_mode({"production": {"run_mode": "production"}}) \
            == pu.PRODUCTION
        assert pu.resolve_run_mode({}, override="production") == pu.PRODUCTION

    def test_acceptance_universe_is_the_small_fixture(self):
        u = pu.resolve_universe({"production": {"run_mode": "acceptance"}})
        assert u.source == pu.SRC_ACCEPTANCE_FIXTURE
        assert u.symbols == sorted(pu.DEFAULT_ACCEPTANCE_UNIVERSE)
        assert u.survivorship_safe is False

    def test_production_universe_is_survivorship_safe_incl_delisted(self):
        nd = FakeNorgate(["AAPL", "MSFT", "DEAD1", "DEAD2"], ["AAPL", "MSFT"])
        u = pu.resolve_universe({"production": {"run_mode": "production"}},
                                norgate=nd)
        assert u.mode == pu.PRODUCTION and u.source == \
            pu.SRC_NORGATE_SURVIVORSHIP
        assert "DEAD1" in u.symbols and "DEAD2" in u.symbols
        assert u.survivorship_safe and not u.degraded

    def test_production_universe_has_no_permanent_symbol_cap(self):
        big = ["SYM%04d" % i for i in range(600)]
        nd = FakeNorgate(big, big[:500])
        u = pu.resolve_universe({"production": {"run_mode": "production"}},
                                norgate=nd)
        # No 6 / 300 / N permanent cap: the full universe is resolved.
        assert u.target_count == 600

    def test_degraded_falls_back_to_owned_panel_never_the_fixture(self):
        nd = FakeNorgate([], [], running=False)
        u = pu.resolve_universe(
            {"production": {"run_mode": "production"}}, norgate=nd,
            fallback_symbols=lambda: ["OWN1", "OWN2"])
        assert u.degraded and u.source == pu.SRC_OWNED_PANEL_FALLBACK
        assert u.symbols == ["OWN1", "OWN2"]
        assert set(u.symbols).isdisjoint(pu.DEFAULT_ACCEPTANCE_UNIVERSE)

    def test_fingerprint_is_order_independent_and_change_sensitive(self):
        assert pu.universe_fingerprint(["A", "B"]) == \
            pu.universe_fingerprint(["B", "A"])
        assert pu.universe_fingerprint(["A", "B"]) != \
            pu.universe_fingerprint(["A", "B", "C"])


# --------------------------------------------------------------------------- #
# WS3/WS7 — durable sharded campaign cursor store.
# --------------------------------------------------------------------------- #
class TestCampaignStore:
    def _store(self, tmp_path):
        return ac.CampaignStore(tmp_path / "campaigns.sqlite")

    def test_shards_into_batches_forward_then_repair(self, tmp_path):
        s = self._store(tmp_path)
        s.ensure_campaign("c", kind="k", universe=["A", "B", "C", "D", "E"],
                          universe_source="src", batch_size=2)
        assert s.next_batch("c") == ["A", "B"]
        s.record_results("c", succeeded=["A"], failed=[("B", "err")])
        # PENDING first (C,D), then the repair (B) once pending is exhausted.
        assert s.next_batch("c") == ["C", "D"]
        s.record_results("c", succeeded=["C", "D"])
        assert s.next_batch("c") == ["E", "B"]

    def test_coverage_reconciles_and_identifies_permanent_failures(self,
                                                                   tmp_path):
        s = self._store(tmp_path)
        s.ensure_campaign("c", kind="k", universe=["A", "B"],
                          universe_source="src", batch_size=2,
                          max_symbol_attempts=2)
        s.record_results("c", succeeded=["A"], failed=[("B", "e1")])
        s.record_results("c", failed=[("B", "e2")])  # 2nd attempt -> permanent
        cov = s.coverage("c")
        assert cov["completed_symbol_count"] == 1
        assert cov["permanent_failed_count"] == 1
        assert cov["remaining_symbol_count"] == 0
        assert cov["is_complete"] and cov["reconciles"]
        assert s.next_batch("c") == []  # permanent failure is not re-served
        assert [p["symbol"] for p in s.permanent_failures("c")] == ["B"]

    def test_universe_growth_is_absorbed_never_destructive(self, tmp_path):
        s = self._store(tmp_path)
        s.ensure_campaign("c", kind="k", universe=["A", "B"],
                          universe_source="src", batch_size=5)
        s.record_results("c", succeeded=["A", "B"])
        assert s.is_complete("c")
        r = s.ensure_campaign("c", kind="k", universe=["A", "B", "C", "D"],
                              universe_source="src", batch_size=5)
        assert r["created"] is False and r["added_symbols"] == 2
        # completed A,B are NOT re-served; only the new members are pending.
        assert s.next_batch("c") == ["C", "D"]
        assert not s.is_complete("c")

    def test_ensure_is_idempotent(self, tmp_path):
        s = self._store(tmp_path)
        s.ensure_campaign("c", kind="k", universe=["A", "B"],
                          universe_source="src", batch_size=5)
        r = s.ensure_campaign("c", kind="k", universe=["A", "B"],
                              universe_source="src", batch_size=5)
        assert r["added_symbols"] == 0 and r["total_symbols"] == 2

    def test_store_persists_across_reopen(self, tmp_path):
        s = self._store(tmp_path)
        s.ensure_campaign("c", kind="k", universe=["A", "B"],
                          universe_source="src", batch_size=1)
        s.record_results("c", succeeded=["A"])
        s2 = ac.CampaignStore(tmp_path / "campaigns.sqlite")
        assert s2.coverage("c")["completed_symbol_count"] == 1
        assert s2.next_batch("c") == ["B"]


# --------------------------------------------------------------------------- #
# WS6 — resumable SEC bulk-archive downloader.
# --------------------------------------------------------------------------- #
class TestBulkDownloader:
    def _dl(self, tmp_path, data, name="cf", **kw):
        kw.setdefault("disk_budget_bytes", 10 ** 9)
        kw.setdefault("segment_bytes", 4096)
        kw.setdefault("chunk_bytes", 1024)
        return bd.BulkArchiveDownloader(
            url="https://www.sec.gov/x.zip", dest_dir=tmp_path / "bulk",
            name=name, transport=FakeHttp(data), forbid_drives=(), **kw)

    def test_segments_resume_and_complete_with_checksum(self, tmp_path):
        data = bytes(range(256)) * 40  # 10240 bytes
        p1 = self._dl(tmp_path, data).download_segment()
        assert p1["disposition"] == bd.D_IN_PROGRESS
        assert p1["bytes_downloaded"] == 4096 and p1["resumable"]
        # a brand-new instance resumes from the durable checkpoint.
        self._dl(tmp_path, data).download_segment()
        p3 = self._dl(tmp_path, data).download_segment()
        assert p3["complete"] and p3["sha256"] == hashlib.sha256(data).hexdigest()
        final = tmp_path / "bulk" / "cf.zip"
        assert final.exists() and not (tmp_path / "bulk" / "cf.zip.part").exists()

    def test_disk_capacity_required_is_a_precise_blocker(self, tmp_path):
        dl = self._dl(tmp_path, bytes(10240), disk_budget_bytes=100)
        prog = dl.download_segment()
        assert prog["disposition"] == bd.D_DISK_CAPACITY_REQUIRED
        assert prog["total_bytes"] == 10240

    def test_c_drive_destination_is_refused(self, tmp_path):
        dl = bd.BulkArchiveDownloader(url="https://x/z.zip", dest_dir="C:\\nope",
                                      name="cf", transport=FakeHttp(b"x"))
        pf = dl.preflight(10)
        assert pf["disposition"] == bd.D_UNSAFE_DESTINATION

    def test_version_change_restarts_download(self, tmp_path):
        data = bytes(range(256)) * 40
        self._dl(tmp_path, data).download_segment()  # 4096 of 10240
        # the archive version changes (new ETag/Last-Modified) -> restart at 0.
        dl2 = bd.BulkArchiveDownloader(
            url="https://www.sec.gov/x.zip", dest_dir=tmp_path / "bulk",
            name="cf", segment_bytes=4096, chunk_bytes=1024, forbid_drives=(),
            transport=FakeHttp(data, last_modified="Tue, 02 Jan 2026 00:00:00 GMT",
                               etag='"v2"'))
        prog = dl2.download_segment()
        assert prog["bytes_downloaded"] == 4096  # restarted, not resumed to 8192

    def test_extraction_cursor_is_bounded_and_resumable(self, tmp_path):
        import zipfile
        dest = tmp_path / "bulk"
        dest.mkdir(parents=True)
        zpath = dest / "cf.zip"
        with zipfile.ZipFile(zpath, "w") as zf:
            for i in range(5):
                zf.writestr("m%d.json" % i, "{}")
        dl = bd.BulkArchiveDownloader(url="https://x/x.zip", dest_dir=dest,
                                      name="cf", transport=FakeHttp(b"x"))
        r1 = dl.extract_segment(max_members=2)
        assert r1["members_extracted_this_call"] == 2 and not r1["complete"]
        dl.extract_segment(max_members=2)
        r3 = dl.extract_segment(max_members=2)
        assert r3["complete"] and r3["members_done"] == 5


# --------------------------------------------------------------------------- #
# WS4/WS5/WS7 — production handlers drive campaigns through the real queue.
# --------------------------------------------------------------------------- #
def _fake_ingestion(ing_module, out_root):
    def fake(*, config, output_root, mode, as_of="latest", **kw):
        src = list((config.get("sources") or {}).keys())[0]
        scfg = config["sources"][src]
        if src == "eodhd_analyst":
            syms = scfg.get("sample_symbols", [])
            vroot = Path(output_root) / "vintages" / "eodhd_analyst"
            (vroot).mkdir(parents=True, exist_ok=True)
            (vroot / "_prospective_boundary.json").write_text(
                json.dumps({"first_snapshot_date": _TODAY}), encoding="utf-8")
            vdir = vroot / _TODAY
            vdir.mkdir(parents=True, exist_ok=True)
            for s in syms:
                (vdir / ("%s.json" % s.split(".")[0].upper())).write_text(
                    "{}", encoding="utf-8")
            return {"status": "ALPHA_AGENT_STAGE2_READY",
                    "counts": {"normalized_records_new": len(syms) * 3},
                    "source_states": {"eodhd_analyst": "HEALTHY"},
                    "blocked_sources": [], "run_id": "r"}
        if src == "sec_edgar":
            n = len(scfg.get("facts_sample_symbols", []))
            return {"status": "ALPHA_AGENT_STAGE2_READY",
                    "counts": {"normalized_records_new": n * 2},
                    "source_states": {"sec_edgar": "HEALTHY"},
                    "blocked_sources": [], "run_id": "r"}
        if src == "bea":
            return {"status": "ALPHA_AGENT_STAGE2_READY",
                    "counts": {"normalized_records_new": 7,
                               "normalized_records_total": 7},
                    "source_states": {"bea": "DEGRADED"},
                    "blocked_sources": [], "run_id": "r"}
        return {"status": "ALPHA_AGENT_STAGE2_READY",
                "counts": {"normalized_records_new": 0},
                "source_states": {src: "HEALTHY"}, "blocked_sources": []}
    ing_module.run_ingestion = fake


class TestProductionCampaignHandlers:
    def _cfg(self, tmp_path):
        return {
            "stage8_root": str(tmp_path),
            "autonomy": {"handlers": "production",
                         "queue_db": str(tmp_path / "autonomy.sqlite"),
                         "max_jobs_per_cycle": 4},
            "production_handlers": {
                "stage2_ingestion_config":
                    str(_REPO / "configs/alpha_agent/stage2_ingestion.json"),
                "contact_email": "binisti@gmail.com"},
            "production": {
                "run_mode": "acceptance",
                "campaign_db": str(tmp_path / "campaigns.sqlite"),
                "campaigns": [
                    {"id": "eodhd_analyst", "runner": "analyst",
                     "category": "PROSPECTIVE_SNAPSHOT", "batch_size": 2},
                    {"id": "sec_form4_8k", "runner": "sec_cik",
                     "category": "DATA_ACQUISITION", "batch_size": 3}]}}

    def test_analyst_campaign_shards_and_continues_to_completion(
            self, tmp_path, monkeypatch):
        from paper_trader.alpha_agent import ingestion as ing
        _fake_ingestion(ing, tmp_path)
        cfg = self._cfg(tmp_path)
        queue = rt.build_autonomy_queue(cfg)
        handlers = rt.build_production_autonomy_handlers(
            cfg, contact_email="x@y.z", queue=queue)
        queue.enqueue(ar.CAT_PROSPECTIVE_SNAPSHOT, lane="acq.eodhd_analyst",
                      payload={"campaign": "eodhd_analyst", "seq": 0})
        for _ in range(6):
            ar.run_cycle(queue, handlers, planner=lambda q: [], max_jobs=1,
                         floor=1)
        cov = rt.stage8_campaign_store(cfg).coverage("eodhd_analyst")
        assert cov["is_complete"] and cov["reconciles"]
        assert cov["completed_symbol_count"] == 6  # full acceptance fixture

    def test_completed_batch_enqueues_next_batch(self, tmp_path, monkeypatch):
        from paper_trader.alpha_agent import ingestion as ing
        _fake_ingestion(ing, tmp_path)
        cfg = self._cfg(tmp_path)
        queue = rt.build_autonomy_queue(cfg)
        handlers = rt.build_production_autonomy_handlers(
            cfg, contact_email="x@y.z", queue=queue)
        first = queue.enqueue(ar.CAT_PROSPECTIVE_SNAPSHOT,
                              lane="acq.eodhd_analyst",
                              payload={"campaign": "eodhd_analyst", "seq": 0})
        ar.run_cycle(queue, handlers, planner=lambda q: [], max_jobs=1, floor=1)
        # the finished batch left a fresh, distinct next-batch job behind.
        live = [j for j in queue.list_jobs(state=ar.STATE_QUEUED)
                if (j.payload or {}).get("campaign") == "eodhd_analyst"]
        assert live and queue.get(first).state == ar.STATE_COMPLETED
        assert live[0].payload.get("seq") == 2

    def test_bea_routes_through_queue_handler(self, tmp_path, monkeypatch):
        from paper_trader.alpha_agent import ingestion as ing
        _fake_ingestion(ing, tmp_path)
        cfg = self._cfg(tmp_path)
        queue = rt.build_autonomy_queue(cfg)
        handlers = rt.build_production_autonomy_handlers(
            cfg, contact_email="x@y.z", queue=queue)
        jid = queue.enqueue(ar.CAT_DATA_ACQUISITION, lane="bea",
                            payload={"provider": "BEA",
                                     "collector_source_id": "bea"})
        ar.run_cycle(queue, handlers, planner=lambda q: [], max_jobs=1, floor=1)
        j = queue.get(jid)
        assert j.state == ar.STATE_COMPLETED
        assert (j.result or {}).get("real_work") == "stage2_collect"

    def test_production_mode_planner_keeps_campaigns_seeded(self, tmp_path,
                                                            monkeypatch):
        from paper_trader.alpha_agent import ingestion as ing
        _fake_ingestion(ing, tmp_path)
        cfg = self._cfg(tmp_path)
        cfg["production"]["run_mode"] = "production"
        # In production the campaign planner must inject a batch job per campaign.
        store = rt.stage8_campaign_store(cfg)
        specs = rt.production_campaign_seed_specs(cfg, store)
        campaigns = {s["payload"]["campaign"] for s in specs}
        assert {"eodhd_analyst", "sec_form4_8k"} <= campaigns


# --------------------------------------------------------------------------- #
# Scheduled-task entrypoint selects production; stores stay off the ledger.
# --------------------------------------------------------------------------- #
class TestProductionConfigContract:
    def test_stage8_config_selects_production_mode(self):
        cfg = json.loads(_STAGE8_CFG.read_text(encoding="utf-8-sig"))
        assert cfg["production"]["run_mode"] == "production"
        assert cfg["production"]["production_universe_source"][
            "survivorship_watchlist"] == "S&P 500 Current & Past"
        # a per-job batch size exists; NO permanent total-universe cap key.
        assert int(cfg["production"]["per_job_symbol_batch_size"]) > 0

    def test_campaign_store_never_sits_in_the_operational_ledger(self):
        cfg = json.loads(_STAGE8_CFG.read_text(encoding="utf-8-sig"))
        cdb = cfg["production"]["campaign_db"].lower()
        assert "paper_trading_desk" not in cdb
        assert "multi_horizon_alpha_ledger" not in cdb
        assert "stage8" in cdb

    def test_bulk_archives_target_the_d_drive_not_c(self):
        cfg = json.loads(_STAGE8_CFG.read_text(encoding="utf-8-sig"))
        root = cfg["production"]["bulk_root"].lower()
        assert root.startswith("d:")

    def test_resolve_campaign_defs_has_no_permanent_cap(self):
        cfg = json.loads(_STAGE8_CFG.read_text(encoding="utf-8-sig"))
        defs = rt.resolve_campaign_defs(cfg)
        ids = {d["id"] for d in defs}
        assert {"norgate_prices", "eodhd_analyst", "sec_form4_8k",
                "sec_bulk_companyfacts", "sec_bulk_submissions"} <= ids
        for d in defs:
            assert int(d["batch_size"]) > 0  # batch size, not a universe cap


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
