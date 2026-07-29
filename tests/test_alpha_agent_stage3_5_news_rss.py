"""
tests/test_alpha_agent_stage3_5_news_rss.py — Stage 3.5 News/RSS-Atom battery.

Covers the 64 mandated areas. Every network interaction uses an injected fake
transport — pytest NEVER makes a real HTTP request or LLM call, never reads the
real Stage 1/2/3 stores and never touches the operational desk (all fixtures
live under tmp roots). Deterministic clocks; no wall-clock in any assertion.
"""
from __future__ import annotations

import csv
import datetime as _dt
import json
import sqlite3
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import event_clustering as ec  # noqa: E402
from paper_trader.alpha_agent import feed_contracts as fc  # noqa: E402
from paper_trader.alpha_agent import feed_registry as fr  # noqa: E402
from paper_trader.alpha_agent import research_director as rd  # noqa: E402
from paper_trader.alpha_agent import source_contracts as sc  # noqa: E402
from paper_trader.alpha_agent.collectors.base import CollectorContext, RawArchive
from paper_trader.alpha_agent.collectors.rss_atom import (  # noqa: E402
    RssAtomCollector, resolve_feed_item_entity)

AS_OF = "2026-07-28"
FIXED = _dt.datetime(2026, 7, 28, 20, 0, 0, tzinfo=_dt.timezone.utc)
NOW_ISO = "2026-07-28T20:00:00+00:00"


# --------------------------------------------------------------------------- #
# Feed byte fixtures
# --------------------------------------------------------------------------- #
def _rss(items: str, title: str = "Feed") -> bytes:
    return ("""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>%s</title>%s</channel></rss>""" % (
        title, items)).encode("utf-8")


SEC_RSS = _rss(
    """<item><title>SEC charges Acme (AAPL) with disclosure failures</title>
<link>https://www.sec.gov/news/press/2026/acme.htm?utm_source=news</link>
<guid>https://www.sec.gov/news/press/2026/acme</guid>
<description>The Commission <b>today</b> announced settled charges.</description>
<pubDate>Mon, 27 Jul 2026 14:00:00 GMT</pubDate></item>""", "SEC Press")

ATOM = ("""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"><title>Fed</title>
<updated>2026-07-28T18:00:00Z</updated>
<entry><title>FOMC statement released</title>
<link href="https://www.federalreserve.gov/x/fomc.htm" rel="alternate"/>
<id>tag:fed,2026:fomc</id><summary>Rates unchanged.</summary>
<published>2026-07-28T18:00:00Z</published>
<updated>2026-07-28T18:00:00Z</updated></entry></feed>""").encode("utf-8")

NS_RSS = ("""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/"
 xmlns:dc="http://purl.org/dc/elements/1.1/"><channel><title>NS</title>
<item><title>Namespaced item</title><link>https://ex.gov/a</link>
<dc:creator>Agency Desk</dc:creator>
<content:encoded>&lt;p&gt;Body text&lt;/p&gt;</content:encoded>
<pubDate>Tue, 28 Jul 2026 10:00:00 GMT</pubDate></item></channel></rss>""").encode("utf-8")

MALFORMED = b"<?xml version='1.0'?><rss><channel><item><title>x</broken>"
HTML_BODY = b"<!DOCTYPE html><html><body>Access Denied</body></html>"
UNSAFE_XML = (b"<?xml version='1.0'?><!DOCTYPE rss [<!ENTITY x 'y'>]>"
              b"<rss><channel><item><title>&x;</title></item></channel></rss>")
NO_PUBDATE = _rss("""<item><title>No date item</title>
<link>https://ex.gov/nodate</link><guid>nd-1</guid></item>""")


def _resp(status=200, body=b"", ctype="application/rss+xml", etag=None,
          last_modified=None, redirect_chain=None):
    headers = {"content-type": ctype}
    if etag:
        headers["etag"] = etag
    if last_modified:
        headers["last-modified"] = last_modified
    out = {"status": status, "headers": headers, "body": body, "error": None}
    if redirect_chain:
        out["redirect_chain"] = redirect_chain
    return out


def _transport(routes: dict):
    """routes: url-substring -> resp dict OR callable(request)->resp. Default 404."""
    def t(request, timeout):
        url = request["url"]
        for key, val in routes.items():
            if key in url:
                return val(request) if callable(val) else dict(val)
        return _resp(404, b"nf", ctype="text/plain")
    return t


# --------------------------------------------------------------------------- #
# Collector + config builders
# --------------------------------------------------------------------------- #
def _feed(feed_id="sec_press", **over):
    base = {"feed_id": feed_id,
            "feed_url": "https://www.sec.gov/news/pressreleases.rss",
            "publisher": "SEC", "source_category": "REGULATOR",
            "official_source": True, "trust_level": "PRIMARY_OFFICIAL",
            "feed_format": "RSS_2_0", "license_status": "US_PUBLIC_DOMAIN",
            "enabled": True, "priority": 1, "covered_tickers": []}
    base.update(over)
    ok, norm, _ = fr.validate_feed(base)
    norm["known_tickers"] = base.get("known_tickers", [])
    return norm


def _collector(tmp_path, transport, known_tickers=(), max_bytes=1_000_000):
    limits = {"max_retries": 1, "backoff_base_seconds": 0.0,
              "backoff_multiplier": 1.0, "http_timeout_seconds": 5,
              "raw_object_max_bytes": max_bytes, "circuit_breaker_threshold": 4}
    config = {"limits": limits,
              "secret_redaction": {"redacted_query_params": ["api_key"],
                                   "redaction_placeholder": "REDACTED"}}
    arch = RawArchive(tmp_path / "raw", tmp_path, set(), max_bytes)
    ctx = CollectorContext(
        config=config, source_cfg={"min_interval_seconds": 0.0,
                                   "known_tickers": list(known_tickers)},
        archive=arch, transport=transport, now_iso=lambda: NOW_ISO,
        clock=lambda: 0.0, sleep=lambda s: None, secrets=[], user_agent="t/1.0",
        env={})
    return RssAtomCollector(ctx)


def _engine_cfg(stage2_root="", ledger_roots=None, sensitive=None):
    return {
        "output_contract": {"state_dir": "state", "state_db": "feed_state.sqlite",
                            "registry_dir": "registry", "raw_dir": "raw",
                            "normalized_dir": "normalized", "runs_dir": "runs",
                            "latest_file": "latest.json"},
        "stage2_ingestion_root": stage2_root,
        "operational_ledger_roots": ledger_roots or [],
        "sensitive_env_vars": sensitive or [],
        "limits": {"max_retries": 1, "raw_object_max_bytes": 1_000_000,
                   "circuit_breaker_threshold": 4, "summary_max_chars": 600},
        "per_feed_min_interval_seconds": 0.0,
        "clustering": {"window_days": 2, "stage2_cap_per_type": 50},
        "secret_redaction": {"redacted_query_params": ["api_key"],
                             "redaction_placeholder": "REDACTED"},
        "user_agent": {"product": "t/1.0"},
    }


def _feeds(*feeds, **kw):
    cfg = {"registry_version": "1.0.0",
           "active_book_tickers": kw.get("active_book_tickers", ["AAPL", "MSFT"]),
           "known_tickers": kw.get("known_tickers", ["AAPL", "MSFT"]),
           "feeds": list(feeds)}
    cfg.update({k: v for k, v in kw.items()
                if k not in ("active_book_tickers", "known_tickers")})
    return cfg


def _raw_feed_dict(feed_id="sec_press", **over):
    d = {"feed_id": feed_id,
         "feed_url": "https://www.sec.gov/news/pressreleases.rss",
         "publisher": "SEC", "source_category": "REGULATOR",
         "official_source": True, "trust_level": "PRIMARY_OFFICIAL",
         "feed_format": "RSS_2_0", "enabled": True, "priority": 1}
    d.update(over)
    return d


def _run_engine(tmp_path, feeds_config, transport, mode="collect",
                stage2_root="", ledger_roots=None, sensitive=None, env=None):
    cfg = _engine_cfg(str(stage2_root), ledger_roots, sensitive)
    return cfg, fr.run_news_rss(
        config=cfg, feeds_config=feeds_config, output_root=str(tmp_path / "out"),
        mode=mode, as_of=AS_OF, git_commit="TESTSHA", transport=transport,
        env=env or {}, now_fn=lambda: FIXED, sleep_fn=lambda s: None,
        clock_fn=lambda: 0.0, contact_email=None)


# --------------------------------------------------------------------------- #
# Normalized-record + Stage 2 / Stage 3.5 tree builders
# --------------------------------------------------------------------------- #
def _mk(rt, source_id, native, title=None, ticker=None, link=None,
        avail="2026-07-28T14:00:00+00:00", extra=None):
    payload = {"title": title} if title else {}
    if link:
        payload["canonical_link"] = link
    if extra:
        payload.update(extra)
    return sc.build_normalized_record(
        record_type=rt, source_id=source_id, source_native_id=native,
        raw_object_id=None, retrieved_at=NOW_ISO, available_at=avail,
        effective_at=avail[:10], ticker=ticker, event_type="%s:test" % rt,
        payload=payload, provenance="test %s" % source_id)


def _write_norm(root: Path, rec: dict, run="r1"):
    d = str(rec.get("effective_at") or "2026-07-28")[:10]
    yyyy, mm, dd = d.split("-")
    p = root / "normalized" / rec["record_type"] / yyyy / mm / dd / ("%s.jsonl" % run)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(sc.canonical_json(rec) + "\n")


def _stage2(root, health=None):
    return {"ok": True, "run_id": "stage2_x", "root": Path(root),
            "source_health": health or {}, "record_type_counts": {},
            "as_of": AS_OF}


def _make_s35_root(base: Path, *, token=fr.READY, clusters=(), records=(),
                   coverage=None, feed_health_rows=None):
    base.mkdir(parents=True, exist_ok=True)
    run_dir = base / "runs" / "stage3_5_x"
    run_dir.mkdir(parents=True, exist_ok=True)
    (base / "latest.json").write_text(json.dumps({
        "run_id": "stage3_5_x", "run_dir": "runs/stage3_5_x",
        "terminal_token": token, "status": token}), encoding="utf-8")
    with (run_dir / "event_clusters.jsonl").open("w", encoding="utf-8") as fh:
        for c in clusters:
            fh.write(sc.canonical_json(c) + "\n")
    (run_dir / "source_coverage_report.json").write_text(
        json.dumps(coverage or {"enabled_feeds": 5, "healthy_feeds": 5,
                                "clusters_created": len(clusters),
                                "multi_source_clusters": 0, "company_feeds": 0,
                                "government_regulatory_feeds": 5,
                                "entity_resolution": {"UNMATCHED": 1},
                                "gdelt_state": "NOT_RUN"}), encoding="utf-8")
    with (run_dir / "feed_health.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["feed_id", "attempted", "health",
                                           "duplicates_prevented",
                                           "latest_item_time"])
        w.writeheader()
        for row in (feed_health_rows or [{"feed_id": "sec_press",
                                          "attempted": True, "health": "HEALTHY",
                                          "duplicates_prevented": 0,
                                          "latest_item_time": "2026-07-28T14:00:00+00:00"}]):
            w.writerow(row)
    for rec in records:
        _write_norm(base, rec)
    return base


# ======================================================================= #
# 1-4 + 20-22: parser
# ======================================================================= #
def test_01_rss_parse():
    p = fc.parse_feed(SEC_RSS)
    assert p["format"] == fc.FF_RSS_2_0 and not p["malformed"]
    assert len(p["items"]) == 1 and "SEC charges" in p["items"][0]["title"]


def test_02_atom_parse():
    p = fc.parse_feed(ATOM)
    assert p["format"] == fc.FF_ATOM and len(p["items"]) == 1
    assert p["items"][0]["canonical_link"].endswith("fomc.htm")
    assert p["items"][0]["publication_time"].startswith("2026-07-28")


def test_03_namespaces():
    p = fc.parse_feed(NS_RSS)
    it = p["items"][0]
    assert it["author"] == "Agency Desk"
    assert "Body text" in it["bounded_summary"]  # content:encoded, HTML stripped


def test_04_malformed_controlled():
    p = fc.parse_feed(MALFORMED)
    assert p["malformed"] and p["items"] == [] and "MALFORMED" in p["parser_status"]


def test_04b_unsafe_xml_quarantined():
    p = fc.parse_feed(UNSAFE_XML)
    assert p["malformed"] and p["parser_status"] == "PS_QUARANTINE_UNSAFE_XML"


def test_20_html_stripped():
    assert fc.strip_html("<p>Hello <b>World</b></p><script>x()</script>") == "Hello World"


def test_21_summary_bounds():
    long = "x" * 5000
    p = fc.parse_feed(_rss("<item><title>t</title><link>https://a.gov/x</link>"
                           "<description>%s</description>"
                           "<pubDate>Mon, 27 Jul 2026 14:00:00 GMT</pubDate></item>"
                           % long))
    it = p["items"][0]
    assert len(it["bounded_summary"]) <= fc.DEFAULT_SUMMARY_MAX_CHARS
    assert it["content_truncated"] is True


def test_22_missing_pubtime_warned():
    p = fc.parse_feed(NO_PUBDATE)
    it = p["items"][0]
    assert it["publication_time"] is None
    assert any("PUBLICATION_TIME_ABSENT" in w for w in it["quality_warnings"])


def test_19_canonical_url():
    a = fc.canonical_link("HTTPS://Www.SEC.gov:443/News/A/?utm_source=x&b=2&a=1#frag")
    assert a == "https://www.sec.gov/News/A?a=1&b=2"
    assert fc.canonical_link("https://x.com/p/") == "https://x.com/p"


# ======================================================================= #
# 5-18: collector HTTP hygiene + conditional polling
# ======================================================================= #
def test_05_oversized_rejected(tmp_path):
    c = _collector(tmp_path, _transport({"sec.gov": _resp(200, b"y" * 5000)}),
                   max_bytes=100)
    s = c.collect_feed(_feed(), {}, AS_OF)
    assert s["rejected_reason"] == "RAW_OBJECT_TOO_LARGE" and s["records_new"] == 0


def test_06_wrong_content_type(tmp_path):
    c = _collector(tmp_path, _transport(
        {"sec.gov": _resp(200, b"<html></html>", ctype="text/html")}))
    s = c.collect_feed(_feed(), {}, AS_OF)
    assert s["rejected_reason"] == "CONTENT_TYPE_NOT_FEED"


def test_07_html_error_rejected(tmp_path):
    c = _collector(tmp_path, _transport(
        {"sec.gov": _resp(200, HTML_BODY, ctype="application/xml")}))
    s = c.collect_feed(_feed(), {}, AS_OF)
    assert s["rejected_reason"] == "HTML_ERROR_RESPONSE"


def test_07b_zero_byte_rejected(tmp_path):
    c = _collector(tmp_path, _transport({"sec.gov": _resp(200, b"")}))
    s = c.collect_feed(_feed(), {}, AS_OF)
    assert s["rejected_reason"] == "ZERO_BYTE_RESPONSE"


def test_08_http_304_no_new_data(tmp_path):
    def route(req):
        if req["headers"].get("If-None-Match") == 'W/"v1"':
            return _resp(304, b"", etag='W/"v1"')
        return _resp(200, SEC_RSS, etag='W/"v1"')
    c = _collector(tmp_path, _transport({"sec.gov": route}))
    s = c.collect_feed(_feed(), {"etag": 'W/"v1"'}, AS_OF)
    assert s["not_modified"] is True and s["health"] == "HEALTHY_NOT_MODIFIED"
    assert s["records_new"] == 0 and s["rejected_reason"] is None


def test_09_10_11_etag_lastmod_conditional(tmp_path):
    seen = {}
    def route(req):
        seen.update(req["headers"])
        return _resp(200, SEC_RSS, etag='W/"e2"',
                     last_modified="Mon, 27 Jul 2026 14:00:00 GMT")
    c = _collector(tmp_path, _transport({"sec.gov": route}))
    s = c.collect_feed(_feed(), {"etag": 'W/"e1"',
                                 "last_modified": "Sun, 26 Jul 2026 00:00:00 GMT"},
                       AS_OF)
    assert seen.get("If-None-Match") == 'W/"e1"'          # conditional headers sent
    assert seen.get("If-Modified-Since") == "Sun, 26 Jul 2026 00:00:00 GMT"
    assert s["etag"] == 'W/"e2"'                           # etag persisted forward
    assert s["last_modified"] == "Mon, 27 Jul 2026 14:00:00 GMT"


def test_12_redirect_validation(tmp_path):
    c = _collector(tmp_path, _transport(
        {"sec.gov": _resp(200, SEC_RSS, redirect_chain=["ftp://evil/x"])}))
    s = c.collect_feed(_feed(), {}, AS_OF)
    assert s["rejected_reason"] == "UNSAFE_REDIRECT"


def test_13_bounded_retries(tmp_path):
    calls = {"n": 0}
    def route(req):
        calls["n"] += 1
        return _resp(500, b"err")
    c = _collector(tmp_path, _transport({"sec.gov": route}))
    s = c.collect_feed(_feed(), {}, AS_OF)
    assert calls["n"] == 2                # 1 attempt + 1 retry (max_retries=1)
    assert s["health"] == "FAILED" and c.retries_total == 1


def test_14_circuit_breaker(tmp_path):
    c = _collector(tmp_path, _transport({"sec.gov": _resp(200, SEC_RSS)}))
    s = c.collect_feed(_feed(), {"circuit_breaker_state": "OPEN",
                                 "consecutive_failures": 4}, AS_OF)
    assert s["attempted"] is False and s["health"] == "CIRCUIT_OPEN"


def test_15_16_deterministic_raw_id_immutable(tmp_path):
    c = _collector(tmp_path, _transport({"sec.gov": _resp(200, SEC_RSS)}))
    c.collect_feed(_feed(), {}, AS_OF)
    id1 = c.raw_objects[0]["raw_object_id"]
    p = tmp_path / "raw"
    files_before = {f: f.read_bytes() for f in p.rglob("*.xml")}
    c2 = _collector(tmp_path, _transport({"sec.gov": _resp(200, SEC_RSS)}))
    c2.collect_feed(_feed(), {}, AS_OF)
    files_after = {f: f.read_bytes() for f in p.rglob("*.xml")}
    id2 = fc.SOURCE_ID  # sanity
    assert fc.SOURCE_ID == "rss_atom"
    # identical content -> same content-addressed id and file never rewritten.
    assert sc.make_raw_object_id("sec_press", SEC_RSS) == id1
    assert files_before == files_after


def test_17_stable_item_id():
    a = fc.parse_feed(SEC_RSS)["items"][0]["native_id"]
    b = fc.parse_feed(SEC_RSS)["items"][0]["native_id"]
    assert a == b and a.startswith("it_")


def test_18_duplicate_item_prevention(tmp_path):
    c = _collector(tmp_path, _transport({"a.gov": _resp(200, SEC_RSS)}))
    f1 = _feed("f1", feed_url="https://a.gov/1")
    f2 = _feed("f2", feed_url="https://a.gov/2")
    s1 = c.collect_feed(f1, {}, AS_OF)
    s2 = c.collect_feed(f2, {}, AS_OF)   # same item content -> deduped
    assert s1["records_new"] == 1 and s2["records_new"] == 0
    assert s2["duplicates_prevented"] == 1


# ======================================================================= #
# 23-30: entity resolution + normalization
# ======================================================================= #
def test_23_exact_mapping():
    feed = _feed("aapl_ir", source_category="COMPANY_IR", covered_tickers=["AAPL"])
    m = resolve_feed_item_entity(feed, {"title": "Update"}, {"AAPL"})
    assert m["state"] == sc.EM_MATCHED_EXACT and m["mapped_tickers"] == ["AAPL"]


def test_24_alias_mapping():
    feed = _feed("news", source_category="INDUSTRY")
    m = resolve_feed_item_entity(feed, {"title": "AAPL soars on results"}, {"AAPL"})
    assert m["state"] == sc.EM_MATCHED_ALIAS and m["mapped_tickers"] == ["AAPL"]


def test_25_ambiguous_mapping():
    feed = _feed("news", source_category="INDUSTRY")
    m = resolve_feed_item_entity(feed, {"title": "AAPL and MSFT partner"},
                                 {"AAPL", "MSFT"})
    assert m["state"] == sc.EM_AMBIGUOUS and m["mapped_tickers"] == ["AAPL", "MSFT"]


def test_26_unmatched_mapping():
    feed = _feed("reg", source_category="REGULATOR")
    m = resolve_feed_item_entity(feed, {"title": "Agency issues guidance"},
                                 {"AAPL"})
    assert m["state"] == sc.EM_UNMATCHED and m["mapped_tickers"] == []


def test_27_no_llm_entity_resolution():
    src = Path(fc.__file__).read_text(encoding="utf-8") \
        + Path(ec.__file__).read_text(encoding="utf-8")
    assert "anthropic" not in src.lower() and "llm_providers" not in src
    # deterministic: identical inputs -> identical outputs.
    feed = _feed("reg", source_category="REGULATOR")
    a = resolve_feed_item_entity(feed, {"title": "AAPL"}, {"AAPL"})
    b = resolve_feed_item_entity(feed, {"title": "AAPL"}, {"AAPL"})
    assert a == b


def test_28_news_event_normalization():
    feed = _feed("ind", source_category="INDUSTRY")
    it = fc.parse_feed(SEC_RSS)["items"][0]
    rec = fc.build_feed_event_record(feed=feed, item=it, raw_object_id="raw_x",
                                     retrieved_at=NOW_ISO,
                                     mapping={"mapped_tickers": [], "state": "UNMATCHED"},
                                     license_note="pd")
    assert rec["record_type"] == "NEWS_EVENT" and rec["source_id"] == "rss_atom"
    assert rec["normalized_payload"]["feed_id"] == "ind"


def test_29_regulatory_event_normalization():
    feed = _feed("reg", source_category="REGULATOR")
    it = fc.parse_feed(SEC_RSS)["items"][0]
    rec = fc.build_feed_event_record(feed=feed, item=it, raw_object_id="raw_x",
                                     retrieved_at=NOW_ISO,
                                     mapping={"mapped_tickers": [], "state": "UNMATCHED"},
                                     license_note="pd")
    assert rec["record_type"] == "REGULATORY_EVENT"


def test_30_press_release_normalization():
    feed = _feed("ir", source_category="COMPANY_IR", covered_tickers=["AAPL"])
    it = fc.parse_feed(SEC_RSS)["items"][0]
    rec = fc.build_feed_event_record(feed=feed, item=it, raw_object_id="raw_x",
                                     retrieved_at=NOW_ISO,
                                     mapping={"mapped_tickers": ["AAPL"],
                                              "state": "MATCHED_EXACT"},
                                     license_note="pd")
    assert rec["record_type"] == "PRESS_RELEASE" and rec["ticker"] == "AAPL"


# ======================================================================= #
# 31-38: clustering
# ======================================================================= #
def test_31_exact_clustering():
    r1 = _mk("NEWS_EVENT", "eodhd", "n1", "Acme earnings beat", "AAPL",
             link="https://x.com/a")
    r2 = _mk("NEWS_EVENT", "rss_atom", "n2", "Different words entirely", "AAPL",
             link="https://x.com/a")
    cl = ec.cluster_events([r1, r2], now_iso=NOW_ISO)
    multi = [c for c in cl if len(c["member_record_ids"]) > 1]
    assert len(multi) == 1 and multi[0]["clustering_confidence"] == "EXACT"


def test_32_high_confidence_clustering():
    r1 = _mk("NEWS_EVENT", "eodhd", "n1", "Acme reports record quarterly revenue",
             "AAPL", link="https://a.com/1")
    r2 = _mk("PRESS_RELEASE", "rss_atom", "n2",
             "Acme reports record quarterly revenue today", "AAPL",
             link="https://b.com/2")
    cl = ec.cluster_events([r1, r2], now_iso=NOW_ISO)
    multi = [c for c in cl if len(c["member_record_ids"]) > 1]
    assert len(multi) == 1 and multi[0]["clustering_confidence"] in ("HIGH", "MEDIUM")


def test_33_time_window_enforced():
    r1 = _mk("NEWS_EVENT", "eodhd", "n1", "Acme reports record revenue", "AAPL",
             avail="2026-07-01T10:00:00+00:00")
    r2 = _mk("NEWS_EVENT", "rss_atom", "n2", "Acme reports record revenue", "AAPL",
             avail="2026-07-28T10:00:00+00:00")
    cl = ec.cluster_events([r1, r2], window_days=2, now_iso=NOW_ISO)
    assert all(len(c["member_record_ids"]) == 1 for c in cl)


def test_34_no_ticker_only_clustering():
    r1 = _mk("NEWS_EVENT", "eodhd", "n1", "Acme unveils data center plan", "AAPL")
    r2 = _mk("NEWS_EVENT", "rss_atom", "n2", "Regulator opens antitrust probe", "AAPL")
    cl = ec.cluster_events([r1, r2], now_iso=NOW_ISO)
    assert all(len(c["member_record_ids"]) == 1 for c in cl)  # shared ticker != cluster


def test_35_conflicting_facts_surfaced():
    r1 = _mk("TRADING_HALT", "nasdaq_trader", "h1", "Acme halted", "AAPL",
             extra={"reason_code": "T1"})
    r2 = _mk("TRADING_HALT", "rss_atom", "h2", "Acme halted", "AAPL",
             extra={"reason_code": "LUDP"})
    cl = ec.cluster_events([r1, r2], now_iso=NOW_ISO)
    multi = [c for c in cl if len(c["member_record_ids"]) > 1]
    assert multi and any(f["field"] == "reason_code"
                         for f in multi[0]["conflicting_facts"])


def test_36_cross_source_eodhd_rss():
    r1 = _mk("NEWS_EVENT", "eodhd", "n1", "Acme guidance raised for fiscal year",
             "AAPL", link="https://a.com/x")
    r2 = _mk("NEWS_EVENT", "rss_atom", "n2", "Acme guidance raised for fiscal year",
             "AAPL", link="https://b.com/y")
    cl = ec.cluster_events([r1, r2], now_iso=NOW_ISO)
    multi = [c for c in cl if len(c["member_record_ids"]) > 1][0]
    assert set(multi["member_sources"]) == {"eodhd", "rss_atom"}
    assert multi["corroborating_source_count"] == 2


def test_37_cross_source_sec_rss():
    r1 = _mk("FILING_EVENT", "sec_edgar", "f1", "Acme 8-K material agreement",
             "AAPL")
    r2 = _mk("REGULATORY_EVENT", "rss_atom", "r1", "Acme 8-K material agreement filed",
             "AAPL")
    cl = ec.cluster_events([r1, r2], now_iso=NOW_ISO)
    multi = [c for c in cl if len(c["member_record_ids"]) > 1]
    assert multi and multi[0]["representative_record_id"] == r1["record_id"]  # SEC primary


def test_38_cluster_member_reconciliation():
    r1 = _mk("NEWS_EVENT", "eodhd", "n1", "Acme raises guidance sharply", "AAPL",
             link="https://a.com/x")
    r2 = _mk("NEWS_EVENT", "rss_atom", "n2", "Acme raises guidance sharply", "AAPL",
             link="https://a.com/x")
    cl = ec.cluster_events([r1, r2], now_iso=NOW_ISO)
    idx = ec.index_clusters(cl)
    assert idx[r1["record_id"]]["cluster_id"] == idx[r2["record_id"]]["cluster_id"]
    reps = [v["is_representative"] for v in idx.values()]
    assert reps.count(True) == 1


def test_38b_run_id_excludes_created_at():
    r1 = _mk("NEWS_EVENT", "eodhd", "n1", "t", "AAPL")
    a = ec.clusters_identity_projection(ec.cluster_events([r1], now_iso="A"))
    b = ec.clusters_identity_projection(ec.cluster_events([r1], now_iso="B"))
    assert a == b   # created_at excluded from identity


# ======================================================================= #
# 39-42: registry
# ======================================================================= #
def test_39_feed_registry_validation():
    ok, norm, reasons = fr.validate_feed(_raw_feed_dict())
    assert ok and norm["enabled"] and not reasons


def test_40_unofficial_feed_rejected():
    ok, norm, reasons = fr.validate_feed(
        _raw_feed_dict("blog", official_source=False, trust_level="UNKNOWN"))
    assert norm["enabled"] is False
    assert any("UNOFFICIAL" in r for r in reasons)


def test_40b_registry_disables_unofficial():
    reg = fr.load_registry(_feeds(
        _raw_feed_dict("good"),
        _raw_feed_dict("bad", official_source=False, trust_level="UNKNOWN")))
    ids = {f["feed_id"] for f in reg.enabled_feeds()}
    assert ids == {"good"}


def test_41_missing_company_feed_controlled(tmp_path):
    feeds = _feeds(_raw_feed_dict("sec_press"),
                   active_book_tickers=["AAPL", "MSFT"], known_tickers=["AAPL"])
    _, res = _run_engine(tmp_path, feeds, _transport({"sec.gov": _resp(200, SEC_RSS)}))
    disc = json.loads((Path(res["run_dir"]) / "feed_discovery_results.json")
                      .read_text())
    without = {r["ticker"] for r in disc["companies_without_feeds"]}
    assert without == {"AAPL", "MSFT"}   # NO_OFFICIAL_FEED_DISCOVERED, not a failure


def test_42_feed_health_states(tmp_path):
    feeds = _feeds(_raw_feed_dict("ok"),
                   _raw_feed_dict("gone", feed_url="https://gone.gov/x", priority=2))
    t = _transport({"sec.gov": _resp(200, SEC_RSS)})   # gone.gov -> 404
    _, res = _run_engine(tmp_path, feeds, t)
    with open(Path(res["run_dir"]) / "feed_health.csv", encoding="utf-8-sig",
              newline="") as fh:
        rows = list(csv.DictReader(fh))
    states = {r["feed_id"]: r["health"] for r in rows}
    assert states["ok"] == "HEALTHY" and states["gone"] == "FAILED"


# ======================================================================= #
# 43-50: engine lifecycle + safety
# ======================================================================= #
def test_43_incremental_checkpoints(tmp_path):
    seen = {"etag_sent": None}
    def route(req):
        seen["etag_sent"] = req["headers"].get("If-None-Match")
        if req["headers"].get("If-None-Match") == 'W/"v1"':
            return _resp(304, b"", etag='W/"v1"')
        return _resp(200, SEC_RSS, etag='W/"v1"')
    feeds = _feeds(_raw_feed_dict("sec_press"))
    cfg, res1 = _run_engine(tmp_path, feeds, _transport({"sec.gov": route}))
    assert res1["status"] == fr.READY
    res2 = fr.run_news_rss(config=cfg, feeds_config=feeds,
                           output_root=str(tmp_path / "out"), mode="incremental",
                           as_of=AS_OF, git_commit="TESTSHA",
                           transport=_transport({"sec.gov": route}), env={},
                           now_fn=lambda: FIXED, sleep_fn=lambda s: None,
                           clock_fn=lambda: 0.0)
    assert seen["etag_sent"] == 'W/"v1"'          # persisted etag replayed
    assert res2["status"] == fr.NO_NEW


def test_44_no_new_data(tmp_path):
    feeds = _feeds(_raw_feed_dict("sec_press"))
    t = _transport({"sec.gov": _resp(200, SEC_RSS)})
    cfg, _ = _run_engine(tmp_path, feeds, t)
    res = fr.run_news_rss(config=cfg, feeds_config=feeds,
                          output_root=str(tmp_path / "out"), mode="incremental",
                          as_of=AS_OF, git_commit="TESTSHA", transport=t, env={},
                          now_fn=lambda: FIXED, sleep_fn=lambda s: None,
                          clock_fn=lambda: 0.0)
    assert res["status"] == fr.NO_NEW   # identical content -> nothing new


def test_45_verify_writes_nothing(tmp_path):
    feeds = _feeds(_raw_feed_dict("sec_press"))
    cfg, res = _run_engine(tmp_path, feeds, _transport({"sec.gov": _resp(200, SEC_RSS)}))
    run_dir = Path(res["run_dir"])
    before = {f.name: sc.sha256_hex(f.read_bytes())
              for f in run_dir.iterdir() if f.is_file()}
    v = fr.verify_news_rss_run(config=cfg, output_root=str(tmp_path / "out"))
    after = {f.name: sc.sha256_hex(f.read_bytes())
             for f in run_dir.iterdir() if f.is_file()}
    assert v["status"] == fr.VERIFIED and before == after


def test_46_no_secrets_persisted(tmp_path):
    secret = "SECRETVALUE_abcdef123456"
    feeds = _feeds(_raw_feed_dict("sec_press"))
    cfg, res = _run_engine(tmp_path, feeds, _transport({"sec.gov": _resp(200, SEC_RSS)}),
                           sensitive=["MY_KEY"], env={"MY_KEY": secret})
    for f in Path(res["run_dir"]).rglob("*"):
        if f.is_file():
            assert secret.encode() not in f.read_bytes()


def test_47_no_postgresql():
    src = Path(fr.__file__).read_text(encoding="utf-8").lower()
    # No PostgreSQL driver import (the docstring may still say "no PostgreSQL").
    assert "import psycopg" not in src and "psycopg2" not in src
    assert "sqlalchemy" not in src and "connect('postgres" not in src


def test_48_64_operational_ledgers_unchanged(tmp_path):
    ledger_dir = tmp_path / "desk"
    ledger_dir.mkdir()
    (ledger_dir / "book.json").write_text('{"nav": 100000}', encoding="utf-8")
    before = sc.sha256_hex((ledger_dir / "book.json").read_bytes())
    feeds = _feeds(_raw_feed_dict("sec_press"))
    _, res = _run_engine(tmp_path, feeds, _transport({"sec.gov": _resp(200, SEC_RSS)}),
                         ledger_roots=[str(ledger_dir)])
    after = sc.sha256_hex((ledger_dir / "book.json").read_bytes())
    assert res["ledger_unchanged"] is True and before == after


def test_49_stage2_records_read_only(tmp_path):
    s2 = tmp_path / "s2"
    _write_norm(s2, _mk("NEWS_EVENT", "eodhd", "n1", "Acme news", "AAPL"))
    before = {f: f.read_bytes() for f in s2.rglob("*") if f.is_file()}
    feeds = _feeds(_raw_feed_dict("sec_press"))
    _run_engine(tmp_path, feeds, _transport({"sec.gov": _resp(200, SEC_RSS)}),
                stage2_root=s2)
    after = {f: f.read_bytes() for f in s2.rglob("*") if f.is_file()}
    assert before == after   # Stage 2 immutable records never mutated


def test_50_no_order_signal_tables(tmp_path):
    feeds = _feeds(_raw_feed_dict("sec_press"))
    cfg, res = _run_engine(tmp_path, feeds, _transport({"sec.gov": _resp(200, SEC_RSS)}))
    db = tmp_path / "out" / "state" / "feed_state.sqlite"
    conn = sqlite3.connect(str(db))
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    assert not any(k in t.lower() for t in tables
                   for k in ("order", "signal", "fill", "decision", "trade"))


# ======================================================================= #
# 51-59: Stage 3 director integration
# ======================================================================= #
def test_51_stage3_reads_verified_additional_root(tmp_path):
    root = _make_s35_root(tmp_path / "nr")
    out = rd.read_additional_roots({"additional_event_roots": [str(root)]})
    assert out["verified_count"] == 1 and out["rejected"] == []


def test_52_stage3_rejects_unverified_root(tmp_path):
    root = _make_s35_root(tmp_path / "nr", token="ALPHA_AGENT_STAGE3_5_BLOCKED — x")
    out = rd.read_additional_roots({"additional_event_roots": [str(root)]})
    assert out["verified_count"] == 0 and len(out["rejected"]) == 1


def test_52b_missing_root_rejected(tmp_path):
    out = rd.read_additional_roots(
        {"additional_event_roots": [str(tmp_path / "nope")]})
    assert out["verified_count"] == 0 and "latest.json" in out["rejected"][0]["reason"]


def test_53_54_55_accepts_rss_types_and_selects(tmp_path):
    reg = _mk("REGULATORY_EVENT", "rss_atom", "r1", "SEC issues rule", None)
    pr = _mk("PRESS_RELEASE", "rss_atom", "p1", "Acme announces buyback", "AAPL")
    root = _make_s35_root(tmp_path / "nr", records=[reg, pr])
    s2 = tmp_path / "s2"
    _write_norm(s2, _mk("NEWS_EVENT", "eodhd", "n1", "Market news", "MSFT"))
    extra = rd.read_additional_roots({"additional_event_roots": [str(root)]})
    sel = rd.select_input_records(
        {"input_selection": {}}, _stage2(s2), set(),
        extra_roots=extra["roots"], cluster_index=extra["clusters"])
    types = {r["record_type"] for r in sel["selected"]}
    assert "REGULATORY_EVENT" in types and "PRESS_RELEASE" in types
    assert sel["rss_atom_selected"] >= 2 and sel["rss_atom_considered"] >= 2


def test_56_57_avoids_clustered_duplicate_keeps_members(tmp_path):
    # Different titles (so plain title-dedup does NOT fire) but the SAME canonical
    # link, so cross-source clustering collapses them to one representative.
    r_sec = _mk("FILING_EVENT", "sec_edgar", "f1", "Acme 8-K material agreement",
                "AAPL", link="https://x.com/deal")
    r_rss = _mk("REGULATORY_EVENT", "rss_atom", "r1", "Regulator notes Acme deal",
                "AAPL", link="https://x.com/deal")
    cluster = ec.cluster_events([r_sec, r_rss], now_iso=NOW_ISO)
    multi = [c for c in cluster if len(c["member_record_ids"]) > 1]
    assert multi, "fixture must cluster"
    idx = ec.index_clusters(cluster)
    root = _make_s35_root(tmp_path / "nr", clusters=cluster, records=[r_rss])
    s2 = tmp_path / "s2"
    _write_norm(s2, r_sec)
    extra = rd.read_additional_roots({"additional_event_roots": [str(root)]})
    sel = rd.select_input_records({"input_selection": {}}, _stage2(s2), set(),
                                  extra_roots=extra["roots"],
                                  cluster_index=extra["clusters"])
    ids = {r["record_id"] for r in sel["selected"]}
    assert sel["clustered_duplicates_dropped"] == 1
    assert len(ids & {r_sec["record_id"], r_rss["record_id"]}) == 1  # one representative
    keeper = [r for r in sel["selected"]
              if r["record_id"] in (r_sec["record_id"], r_rss["record_id"])][0]
    assert set(keeper["_cluster_member_ids"]) == {r_sec["record_id"], r_rss["record_id"]}


def test_58_dev_cap_remains_six_includes_rss(tmp_path):
    recs = [_mk("REGULATORY_EVENT", "rss_atom", "r%d" % i, "Reg %d" % i)
            for i in range(3)]
    recs += [_mk("FILING_EVENT", "sec_edgar", "f%d" % i, "Filing %d" % i, "AAPL")
             for i in range(3)]
    recs += [_mk("INSIDER_FILING", "sec_edgar", "i%d" % i, None, "MSFT")
             for i in range(3)]
    root = _make_s35_root(tmp_path / "nr",
                          records=[r for r in recs if r["source_id"] == "rss_atom"])
    s2 = tmp_path / "s2"
    for r in recs:
        if r["source_id"] != "rss_atom":
            _write_norm(s2, r)
    extra = rd.read_additional_roots({"additional_event_roots": [str(root)]})
    sel = rd.select_input_records({"input_selection": {}}, _stage2(s2), set(),
                                  extra_roots=extra["roots"],
                                  cluster_index=extra["clusters"])
    dev = rd.apply_development_sample(sel, {
        "max_event_records_per_cycle": 6,
        "balanced_sample_targets": [["REGULATORY_EVENT", 1], ["NEWS_EVENT", 1],
                                    ["PRESS_RELEASE", 1], ["FILING_EVENT", 1],
                                    ["INSIDER_FILING", 1], ["TRADING_HALT", 1]]})
    assert len(dev["selected"]) <= 6
    assert dev["rss_atom_selected"] >= 1   # an RSS-derived record is present


def test_59_deferred_records_not_selected(tmp_path):
    recs = [_mk("REGULATORY_EVENT", "rss_atom", "r%d" % i, "Reg %d" % i)
            for i in range(10)]
    root = _make_s35_root(tmp_path / "nr", records=recs)
    extra = rd.read_additional_roots({"additional_event_roots": [str(root)]})
    sel = rd.select_input_records({"input_selection": {}},
                                  _stage2(tmp_path / "s2"), set(),
                                  extra_roots=extra["roots"],
                                  cluster_index=extra["clusters"])
    dev = rd.apply_development_sample(sel, {"max_event_records_per_cycle": 6,
                                            "balanced_sample_targets": [["REGULATORY_EVENT", 1]]})
    sel_ids = {r["record_id"] for r in dev["selected"]}
    deferred = {d[0] for d in dev["deferred_ids"]}
    assert deferred and not (deferred & sel_ids)   # deferred never in the sample


# ======================================================================= #
# 60-61: report contracts
# ======================================================================= #
def test_60_news_rss_report_contract(tmp_path):
    feeds = _feeds(_raw_feed_dict("sec_press"))
    _, res = _run_engine(tmp_path, feeds, _transport({"sec.gov": _resp(200, SEC_RSS)}))
    report = (Path(res["run_dir"]) / "stage3_5_news_rss_report.md").read_text()
    for token in ("Stage 3.5 News/RSS-Atom Collection Report", "Enabled feeds",
                  "Event clusters created", "GDELT status",
                  "MULTI_SOURCE_EVENT_CLUSTERS"):
        assert token in report
    scr = json.loads((Path(res["run_dir"]) / "source_coverage_report.json").read_text())
    assert "government_regulatory_feeds" in scr and "entity_resolution" in scr


def test_61_integrated_coverage_flags_dynamic():
    # No RSS records -> generalized RSS still reported absent.
    sel0 = {"type_considered": {"NEWS_EVENT": 3}, "type_selected": {"NEWS_EVENT": 1},
            "type_freshness": {}, "rss_atom_considered": 0, "rss_atom_selected": 0}
    cov0 = rd.build_news_rss_coverage({"source_health": {}}, sel0, {})
    assert cov0["generalized_rss_collection_exists"] is False
    sec0 = rd._news_rss_section({"news_rss_coverage": cov0})
    assert any("Generalized RSS collection exists:** NO" in ln for ln in sec0)
    # With RSS records -> generalized RSS reported operational (partial).
    sel1 = {"type_considered": {"REGULATORY_EVENT": 4, "PRESS_RELEASE": 1},
            "type_selected": {"REGULATORY_EVENT": 2}, "type_freshness": {},
            "rss_atom_considered": 5, "rss_atom_selected": 2,
            "clusters_selected": ["clu_a"], "multi_source_clusters_selected": []}
    extra = {"verified_count": 1, "cluster_count": 3,
             "feed_evidence": {"enabled_feeds": 5, "healthy_feeds": 5}}
    cov1 = rd.build_news_rss_coverage({"source_health": {}}, sel1, extra)
    assert cov1["generalized_rss_collection_exists"] is True
    assert cov1["company_direct_rss_atom_feeds_exist"] is True
    sec1 = rd._news_rss_section({"news_rss_coverage": cov1})
    assert any("Generalized RSS collection exists:** YES" in ln for ln in sec1)
    assert any("PARTIAL" in ln for ln in sec1)


# ======================================================================= #
# 62-63: safety gate + immutable determinism
# ======================================================================= #
def test_62_daily_close_ledger_gate(tmp_path):
    ledger_dir = tmp_path / "desk"
    ledger_dir.mkdir()
    (ledger_dir / "close.json").write_text('{"final_close_status": "HOLD"}',
                                           encoding="utf-8")
    fp = fr.ledger_fingerprints({"operational_ledger_roots": [str(ledger_dir)]})
    assert len(fp) == 1   # read-only fingerprint of the operational close ledger


def test_63_immutable_run_deterministic(tmp_path):
    # Identical content collected into TWO fresh stores re-derives the SAME run
    # id (the id is a pure function of content + versions, never wall-clock).
    feeds = _feeds(_raw_feed_dict("sec_press"))
    t = _transport({"sec.gov": _resp(200, SEC_RSS)})
    cfg = _engine_cfg("")

    def go(root):
        return fr.run_news_rss(config=cfg, feeds_config=feeds,
                               output_root=str(tmp_path / root), mode="collect",
                               as_of=AS_OF, git_commit="TESTSHA", transport=t,
                               env={}, now_fn=lambda: FIXED,
                               sleep_fn=lambda s: None, clock_fn=lambda: 0.0)
    r1 = go("a")
    r2 = go("b")
    assert r1["run_id"] == r2["run_id"] and r1["status"] == fr.READY


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
