"""R96 Phase A — the forward-only archive lanes (alpha_agent.collectors.forward_archive)
and the widened analyst-vintage lane. Hermetic: fake transport, fake FTP, tmp_path."""
from __future__ import annotations

import gzip
import json
import sqlite3
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent.collectors import (  # noqa: E402
    COLLECTOR_CLASSES, CollectorContext, RawArchive)
from paper_trader.alpha_agent.collectors import forward_archive as FA  # noqa: E402
from paper_trader.alpha_agent.collectors.eodhd_analyst import EodhdAnalystCollector  # noqa: E402

KEY = "SECRET-KEY-123"
AS_OF = "2026-10-02"


def _news(sym, n, offset=0):
    return [{"date": "2026-10-0%dT1%d:00:00+00:00" % (1 + (i % 2), i % 10),
             "title": "%s headline %d" % (sym, offset + i),
             "content": "body %d" % (offset + i),
             "link": "https://example.com/%s/%d" % (sym, offset + i),
             "symbols": [sym], "tags": ["T"],
             "sentiment": {"polarity": 0.1}} for i in range(n)]


class FakeTransport:
    def __init__(self, news_sizes=None, econ_rows=5, status=None):
        self.calls = []
        self.news_sizes = news_sizes or {}
        self.econ_rows = econ_rows
        self.status = status or {}

    def __call__(self, req, timeout):
        url = req["url"]
        self.calls.append(url)
        u = urlparse(url)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        path = u.path.split("/api/")[-1]
        if path in self.status:
            return {"status": self.status[path], "headers": {}, "body": b"{}", "error": None}
        if path == "economic-events":
            off = int(q.get("offset", 0))
            if off >= 2000:
                return {"status": 422, "headers": {}, "body": b"{}", "error": None}
            n = self.econ_rows if off == 0 else 0
            rows = [{"type": "CPI %d" % i, "country": "US", "date": "%s 12:30:00" % q["from"],
                     "period": "Sep", "comparison": "mom", "actual": None, "estimate": 0.2 + i,
                     "previous": 0.1, "change": None, "change_percentage": None} for i in range(n)]
            return {"status": 200, "headers": {}, "body": json.dumps(rows).encode(), "error": None}
        if path == "news":
            sym = q["s"]
            total = self.news_sizes.get(sym, 3)
            off, lim = int(q.get("offset", 0)), int(q.get("limit", 50))
            n = max(0, min(lim, total - off))
            return {"status": 200, "headers": {}, "body": json.dumps(_news(sym, n, off)).encode(), "error": None}
        if path.startswith("fundamentals/"):
            body = {"General": {"Name": "X", "CIK": "1"},
                    "Earnings": {"Trend": {"2026-12-31": {"period": "0q", "date": "2026-12-31",
                                                          "earningsEstimateAvg": 1.0}},
                                 "History": {"2026-06-30": {"date": "2026-06-30", "reportDate": "2026-07-25",
                                                            "epsActual": 1.1, "epsEstimate": 1.0}}},
                    "AnalystRatings": {"Rating": 4.1}, "Highlights": {}}
            return {"status": 200, "headers": {}, "body": json.dumps(body).encode(), "error": None}
        return {"status": 404, "headers": {}, "body": b"", "error": None}


def _ctx(tmp_path, source_cfg, transport):
    cfg = {"limits": {"max_retries": 1, "backoff_base_seconds": 0.0},
           "secret_redaction": {"redacted_query_params": ["api_token"], "redaction_placeholder": "REDACTED"}}
    archive = RawArchive(tmp_path / "raw", tmp_path, set(), 10_000_000)
    return CollectorContext(config=cfg, source_cfg=source_cfg, archive=archive, transport=transport,
                            now_iso=lambda: "2026-10-02T15:00:00+00:00", clock=lambda: 0.0,
                            sleep=lambda s: None, secrets=[KEY], env={"EODHD_API_KEY": KEY})


def _universe(tmp_path, syms):
    p = tmp_path / "universe.json"
    p.write_text(json.dumps({"symbols": syms}), encoding="utf-8")
    return str(p)


def _eodhd_cfg(tmp_path, syms, **kw):
    cfg = {"enabled": True, "allowed_env_vars": ["EODHD_API_KEY"], "min_interval_seconds": 0.0,
           "families": ["eodhd_economic_events", "eodhd_news"], "universe_file": _universe(tmp_path, syms),
           "news_max_symbols_per_run": 10, "news_page_size": 2, "news_max_pages_per_symbol": 3,
           "econ_chunk_days": 30, "econ_lookback_days": 7, "econ_lookahead_days": 21}
    cfg.update(kw)
    return cfg


def test_lanes_are_registered_in_the_canonical_collector_table():
    assert COLLECTOR_CLASSES["eodhd_forward_archive"] is FA.EodhdForwardArchiveCollector
    assert COLLECTOR_CLASSES["public_forward_archive"] is FA.PublicForwardArchiveCollector


def test_news_archive_is_idempotent_and_deduplicates(tmp_path):
    t = FakeTransport(news_sizes={"AAA.US": 3, "BBB.US": 3})
    cfg = _eodhd_cfg(tmp_path, ["AAA.US", "BBB.US"])
    r1 = FA.EodhdForwardArchiveCollector(_ctx(tmp_path, cfg, t)).collect(AS_OF)
    n1 = r1["inventory"]["forward_archive"]["eodhd_news"]
    assert n1["symbols_done_today"] == 2 and n1["articles_new"] == 6
    calls_after_first = len(t.calls)
    r2 = FA.EodhdForwardArchiveCollector(_ctx(tmp_path, cfg, t)).collect(AS_OF)
    n2 = r2["inventory"]["forward_archive"]["eodhd_news"]
    assert n2["symbols_this_run"] == 0 and n2["articles_new"] == 0      # same day: nothing refetched
    news_calls = [u for u in t.calls[calls_after_first:] if "/news" in u]
    assert news_calls == []
    con = sqlite3.connect(str(tmp_path / "vintages/eodhd_forward_archive/news/news_archive.sqlite"))
    n = con.execute("select count(*) from articles").fetchone()[0]
    con.close()
    assert n == 6


def test_news_preserves_provider_time_ingest_time_and_source_hash(tmp_path):
    t = FakeTransport(news_sizes={"AAA.US": 1})
    FA.EodhdForwardArchiveCollector(_ctx(tmp_path, _eodhd_cfg(tmp_path, ["AAA.US"]), t)).collect(AS_OF)
    con = sqlite3.connect(str(tmp_path / "vintages/eodhd_forward_archive/news/news_archive.sqlite"))
    pub, ing, sha = con.execute("select publication_ts, first_ingested_at, source_payload_sha256 from articles").fetchone()
    con.close()
    assert pub.startswith("2026-10-01T") and ing == "2026-10-02T15:00:00+00:00" and len(sha) == 64


def test_a_full_last_page_is_recorded_truncated(tmp_path):
    t = FakeTransport(news_sizes={"AAA.US": 50})
    FA.EodhdForwardArchiveCollector(_ctx(tmp_path, _eodhd_cfg(tmp_path, ["AAA.US"]), t)).collect(AS_OF)
    con = sqlite3.connect(str(tmp_path / "vintages/eodhd_forward_archive/news/news_archive.sqlite"))
    row = con.execute("select status, truncated from symbol_days").fetchone()
    con.close()
    assert row == ("TRUNCATED", 1)


def test_econ_snapshot_is_immutable_and_measures_overwrites(tmp_path):
    t = FakeTransport(econ_rows=3)
    cfg = _eodhd_cfg(tmp_path, [], families=["eodhd_economic_events"])
    r1 = FA.EodhdForwardArchiveCollector(_ctx(tmp_path, cfg, t)).collect(AS_OF)
    e1 = r1["inventory"]["forward_archive"]["eodhd_economic_events"]
    assert e1["snapshot_written"] is True and e1["rows"] == 3
    snap = tmp_path / ("vintages/eodhd_forward_archive/economic_events/snapshots/%s.json.gz" % AS_OF)
    before = snap.read_bytes()
    r2 = FA.EodhdForwardArchiveCollector(_ctx(tmp_path, cfg, t)).collect(AS_OF)
    assert r2["inventory"]["forward_archive"]["eodhd_economic_events"]["snapshot_written"] is False
    assert snap.read_bytes() == before
    body = json.loads(gzip.decompress(before))
    assert body["source_hash"] and body["ingested_at"] == "2026-10-02T15:00:00+00:00"
    assert e1["overwrite_evidence"]["verdict"] == "UNDETERMINED_NEEDS_TWO_SNAPSHOT_DAYS"


def test_overwrite_detector_flags_a_post_release_estimate_change(tmp_path):
    db = tmp_path / "e.sqlite"
    con = FA._open_db(db, FA._ECON_SCHEMA)
    with con:
        for snap, est in (("2026-10-01", 0.2), ("2026-10-03", 0.3)):
            con.execute("INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        ("k1", snap, "CPI", "US", "2026-10-02 12:30:00", "Sep", "mom",
                         0.25, est, 0.1, None, None, None, "h", "now"))
            con.execute("INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?,?)",
                        (snap, "a", "b", 1, 1, 0, "h", None, "now"))
    con.close()
    ev = FA.econ_estimate_revisions(db)
    assert ev["verdict"] == "PROVIDER_OVERWRITES_ESTIMATES_AFTER_RELEASE"


def test_deep_offset_refusal_is_truncation_not_completion(tmp_path):
    class Full(FakeTransport):
        def __call__(self, req, timeout):
            r = super().__call__(req, timeout)
            if "economic-events" in req["url"] and "offset=2000" not in req["url"]:
                rows = [{"type": "X%d" % i, "country": "US", "date": "2026-10-01 00:00:00"} for i in range(1000)]
                return {"status": 200, "headers": {}, "body": json.dumps(rows).encode(), "error": None}
            return r
    cfg = _eodhd_cfg(tmp_path, [], families=["eodhd_economic_events"], econ_max_pages=3)
    r = FA.EodhdForwardArchiveCollector(_ctx(tmp_path, cfg, Full())).collect(AS_OF)
    assert r["inventory"]["forward_archive"]["eodhd_economic_events"]["truncated"] is True


def test_rate_limit_is_never_data_and_leaves_symbol_for_retry(tmp_path):
    t = FakeTransport(status={"news": 429})
    r = FA.EodhdForwardArchiveCollector(_ctx(tmp_path, _eodhd_cfg(tmp_path, ["AAA.US"], families=["eodhd_news"]), t)).collect(AS_OF)
    n = r["inventory"]["forward_archive"]["eodhd_news"]
    assert n["symbols_failed"] == 1 and n["symbols_done_today"] == 0     # retried next pass


def test_no_credential_in_any_persisted_byte_and_no_event_records(tmp_path):
    t = FakeTransport(news_sizes={"AAA.US": 2}, econ_rows=2)
    r = FA.EodhdForwardArchiveCollector(_ctx(tmp_path, _eodhd_cfg(tmp_path, ["AAA.US"]), t)).collect(AS_OF)
    assert r["records"] == []                                             # research corpus only
    for p in tmp_path.rglob("*"):
        if p.is_file():
            data = p.read_bytes()
            if p.suffix == ".gz":
                data = gzip.decompress(data)
            assert KEY.encode() not in data, p
    assert KEY not in json.dumps(r, default=str)


def test_missing_credential_blocks_without_network(tmp_path):
    t = FakeTransport()
    ctx = _ctx(tmp_path, _eodhd_cfg(tmp_path, ["AAA.US"]), t)
    ctx.env = {}
    r = FA.EodhdForwardArchiveCollector(ctx).collect(AS_OF)
    assert r["health"]["overall_state"] == "BLOCKED_CREDENTIAL" and t.calls == []


def test_public_archive_ibkr_and_spdr_are_write_once(tmp_path, monkeypatch):
    payload = b"#BOF|2026.10.02|11:47:23\n#SYM|CUR|NAME|CON|ISIN|REBATERATE|FEERATE|AVAILABLE|\nAAPL|USD|APPLE|1|US0378331005|4.0|0.25|>10000000|\n#EOF|1\n"
    calls = []

    def fake_ftp(host, user, fname, timeout):
        calls.append(host)
        return payload
    monkeypatch.setattr(FA.PublicForwardArchiveCollector, "ftp_fetch", staticmethod(fake_ftp))
    cfg = {"enabled": True, "families": ["ibkr_shortable_shares"], "ibkr_hosts": ["h1"], "ibkr_files": ["usa.txt"]}
    r1 = FA.PublicForwardArchiveCollector(_ctx(tmp_path, cfg, FakeTransport())).collect(AS_OF)
    i1 = r1["inventory"]["forward_archive"]["ibkr_shortable_shares"]
    assert i1["files_written"] == 1 and i1["files"]["usa.txt"]["data_rows"] == 1
    assert i1["files"]["usa.txt"]["provider_file_stamp"].startswith("#BOF|2026.10.02")
    r2 = FA.PublicForwardArchiveCollector(_ctx(tmp_path, cfg, FakeTransport())).collect(AS_OF)
    assert r2["inventory"]["forward_archive"]["ibkr_shortable_shares"]["files_already_archived"] == 1
    assert calls == ["h1"]                                                 # no second fetch


def test_ibkr_unreachable_is_blocked_external_access_not_a_crash(tmp_path, monkeypatch):
    def boom(host, user, fname, timeout):
        raise TimeoutError("blocked")
    monkeypatch.setattr(FA.PublicForwardArchiveCollector, "ftp_fetch", staticmethod(boom))
    cfg = {"enabled": True, "families": ["ibkr_shortable_shares"], "ibkr_hosts": ["h1", "h2"]}
    r = FA.PublicForwardArchiveCollector(_ctx(tmp_path, cfg, FakeTransport())).collect(AS_OF)
    i = r["inventory"]["forward_archive"]["ibkr_shortable_shares"]
    assert i["blocked_external_access"] is True and i["state"] == "FAILED"


def test_analyst_universe_is_vintage_only_and_free_when_complete(tmp_path):
    t = FakeTransport()
    cfg = {"enabled": True, "allowed_env_vars": ["EODHD_API_KEY"], "sample_symbols": ["S1.US"],
           "universe_file": _universe(tmp_path, ["U1.US", "U2.US", "U3.US"]),
           "universe_max_symbols_per_run": 2, "vintage_subdir": "vintages/eodhd_analyst"}
    r1 = EodhdAnalystCollector(_ctx(tmp_path, cfg, t)).collect(AS_OF)
    assert r1["inventory"]["vintages_written"] == 3                        # sample + 2 universe
    tickers_with_records = {rec["ticker"] for rec in r1["records"]}
    assert tickers_with_records == {"S1"}                                  # universe: vintage only
    v = json.loads((tmp_path / "vintages/eodhd_analyst" / AS_OF / "U1.json").read_text())
    assert v["earnings_history_recent"][0]["epsEstimate"] == 1.0
    EodhdAnalystCollector(_ctx(tmp_path, cfg, t)).collect(AS_OF)          # finishes U3
    n = len(t.calls)
    r3 = EodhdAnalystCollector(_ctx(tmp_path, cfg, t)).collect(AS_OF)
    assert len(t.calls) == n                                               # complete -> no probe, no call
    assert r3["inventory"]["vintages_written"] == 0
    assert r3["inventory"]["universe_symbols_this_run"] == 0
    assert len(list((tmp_path / "vintages/eodhd_analyst" / AS_OF).glob("*.json"))) == 4
