"""
tests/test_alpha_agent_stage2_ingestion.py — Alpha Agent Stage 2 test battery.

Deterministic, hermetic tests for the data-acquisition foundation. Every
network interaction uses an injected fake transport — pytest NEVER calls an
external API. The Norgate vendor package is replaced by an injected fake
module. Fixture Stage 1 registry + operational-ledger trees live under tmp.

Covers the 42 mandated areas: safety (no LLM / no PostgreSQL / no Paper Trader
DB / no outside writes / no ledger or book mutation / no orders), secret
redaction, deterministic IDs, raw immutability + reuse, duplicate prevention,
SQLite + FK integrity, atomic checkpoints, resume, retry/backoff, rate limits,
circuit breaker, response hygiene, per-source parser contracts, controlled
credential/entitlement/vendor blocks, PIT timestamp separation, identity
states + conflicts, Stage 1 gap mapping, incremental NO_NEW, verify-writes-
nothing, the required output contract and no-model-execution.
"""
from __future__ import annotations

import csv
import json
import sqlite3
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import ingestion as ing  # noqa: E402
from paper_trader.alpha_agent import source_contracts as sc  # noqa: E402
from paper_trader.alpha_agent.collectors.base import RawArchive  # noqa: E402
from paper_trader.alpha_agent.collectors.gdelt import canonical_url  # noqa: E402

AS_OF = "2026-07-24"  # a Friday — deterministic business-day math
EODHD_SECRET = "TESTEODHDSECRET1234567890"
FRED_SECRET = "TESTFREDSECRET0987654321"
CONTACT = "ops@test.example"

_STAGE2_SOURCE_FILES = [
    _REPO / "alpha_agent" / "source_contracts.py",
    _REPO / "alpha_agent" / "ingestion.py",
    _REPO / "alpha_agent" / "collectors" / "__init__.py",
    _REPO / "alpha_agent" / "collectors" / "base.py",
    _REPO / "alpha_agent" / "collectors" / "norgate_local.py",
    _REPO / "alpha_agent" / "collectors" / "eodhd.py",
    _REPO / "alpha_agent" / "collectors" / "sec_edgar.py",
    _REPO / "alpha_agent" / "collectors" / "finra.py",
    _REPO / "alpha_agent" / "collectors" / "nasdaq_trader.py",
    _REPO / "alpha_agent" / "collectors" / "fred_alfred.py",
    _REPO / "alpha_agent" / "collectors" / "gdelt.py",
    _REPO / "scripts" / "run_alpha_data_ingestion.py",
]


# --------------------------------------------------------------------------- #
# Fake transport / clock / vendor module
# --------------------------------------------------------------------------- #
class FakeTransport:
    def __init__(self, routes):
        self.routes = routes
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, request, timeout):
        self.calls.append((request["url"], dict(request.get("headers") or {})))
        for frag, resp in self.routes:
            if frag in request["url"]:
                out = resp(request) if callable(resp) else dict(resp)
                base = {"status": 200, "headers": {}, "body": b"", "error": None}
                base.update(out)
                return base
        return {"status": 404, "headers": {}, "body": b"", "error": None}


def make_clock():
    t = [0.0]

    def clock():
        t[0] += 0.01
        return t[0]
    return clock


class FakeRecarray:
    def __init__(self, names, rows):
        self._names = names
        self._rows = rows
        self.dtype = SimpleNamespace(names=tuple(names))

    def __iter__(self):
        for row in self._rows:
            yield _FakeRow(row)


class _FakeRow:
    def __init__(self, row):
        self._row = row

    def __getitem__(self, key):
        return self._row[key]


def fake_norgate_module():
    m = types.ModuleType("norgatedata")
    m.__version__ = "1.0.74-test"
    m.StockPriceAdjustmentType = SimpleNamespace(TOTALRETURN="TR")
    m.PaddingType = SimpleNamespace(NONE="NONE")
    m.status = lambda: True
    m.databases = lambda: ["US Equities", "US Indices"]
    m.watchlists = lambda: ["S&P 500 Current & Past"]
    m.assetid = lambda s: {"AAPL": 101, "MSFT": 102}.get(s, 999)
    m.security_name = lambda s: "Test %s Corp" % s
    m.exchange_name = lambda s: "NASDAQ"
    bars = {
        "AAPL": [{"Date": "2026-07-23", "Open": 210.0, "High": 212.0,
                  "Low": 209.0, "Close": 211.5, "Volume": 1000},
                 {"Date": "2026-07-24", "Open": 211.0, "High": 214.0,
                  "Low": 210.5, "Close": 213.0, "Volume": 1100}],
        "MSFT": [{"Date": "2026-07-24", "Open": 500.0, "High": 505.0,
                  "Low": 498.0, "Close": 503.0, "Volume": 2000}],
    }
    names = ["Date", "Open", "High", "Low", "Close", "Volume"]

    def price_timeseries(symbol, **kwargs):
        return FakeRecarray(names, bars.get(symbol, []))

    def index_constituent_timeseries(symbol, indexname, **kwargs):
        return FakeRecarray(["Date", "Index Constituent"],
                            [{"Date": "2026-07-24", "Index Constituent": 1}])
    m.price_timeseries = price_timeseries
    m.index_constituent_timeseries = index_constituent_timeseries
    return m


class norgate_ctx:
    def __init__(self, module):
        self.module = module

    def __enter__(self):
        self.old = sys.modules.get("norgatedata")
        sys.modules["norgatedata"] = self.module
        return self.module

    def __exit__(self, *exc):
        if self.old is None:
            sys.modules.pop("norgatedata", None)
        else:
            sys.modules["norgatedata"] = self.old
        return False


# --------------------------------------------------------------------------- #
# Fixture data
# --------------------------------------------------------------------------- #

def _finra_body():
    return ("Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20260724|AAPL|1000|10|4000|B,Q,N\n"
            "20260724|MSFT|2000|5|5000|B,Q,N\n"
            "20260724|ZZZQ|10|0|20|Q\n"
            "20260723|BAD|1|0|2|Q\n").encode()


def _master_idx_body():
    return ("Description: Daily Index\nLast Data Received: x\n\n"
            "CIK|Company Name|Form Type|Date Filed|Filename\n"
            "--------------------------------------------\n"
            "320193|Apple Inc|8-K|2026-07-24|edgar/data/320193/0000320193-26-000099.txt\n"
            "320193|Apple Inc|4|2026-07-24|edgar/data/320193/0000320193-26-000100.txt\n"
            "789019|Microsoft Corp|10-Q|2026-07-24|edgar/data/789019/0000789019-26-000055.txt\n"
            "111111|Nobody Inc|8-K|2026-07-24|edgar/data/111111/0000111111-26-000001.txt\n"
            ).encode()


def _company_tickers_body():
    return json.dumps({
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft"},
        "2": {"cik_str": 999999, "ticker": "AAPL", "title": "Apple Duplicate"},
    }).encode()


def _nasdaqlisted_body():
    return ("Symbol|Security Name|Market Category|Test Issue|Financial Status|"
            "Round Lot Size|ETF|NextShares\n"
            "AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N\n"
            "ZTEST|Test Co|Q|Y|N|100|N|N\n"
            "File Creation Time: 0724202620:05|||||||\n").encode()


def _otherlisted_body():
    return ("ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|"
            "Test Issue|NASDAQ Symbol\n"
            "XOM|Exxon Mobil Corporation|N|XOM|N|100|N|XOM\n"
            "File Creation Time: 0724202620:05|||||||\n").encode()


def _halts_body():
    return ("""<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">
<channel><title>Trading Halts</title>
<item>
<title>HALTX</title>
<pubDate>Fri, 24 Jul 2026 14:30:00 GMT</pubDate>
<ndaq:IssueSymbol>HALTX</ndaq:IssueSymbol>
<ndaq:IssueName>Halted Co</ndaq:IssueName>
<ndaq:Market>NASDAQ</ndaq:Market>
<ndaq:ReasonCode>T1</ndaq:ReasonCode>
<ndaq:HaltDate>07/24/2026</ndaq:HaltDate>
<ndaq:HaltTime>09:30:00</ndaq:HaltTime>
<ndaq:ResumptionDate>07/24/2026</ndaq:ResumptionDate>
<ndaq:ResumptionQuoteTime>10:00:00</ndaq:ResumptionQuoteTime>
<ndaq:ResumptionTradeTime>10:05:00</ndaq:ResumptionTradeTime>
</item>
</channel></rss>""").encode()


def _fred_route(request):
    import urllib.parse
    query = dict(urllib.parse.parse_qsl(
        urllib.parse.urlsplit(request["url"]).query))
    sid = query.get("series_id", "X")
    body = json.dumps({"observations": [
        {"realtime_start": "2026-06-11", "realtime_end": "2026-07-10",
         "date": "2026-06-10", "value": "4.5", "series": sid},
        {"realtime_start": "2026-07-11", "realtime_end": "9999-12-31",
         "date": "2026-06-10", "value": "4.6", "series": sid},
        {"realtime_start": "2026-07-21", "realtime_end": "9999-12-31",
         "date": "2026-07-20", "value": ".", "series": sid},
    ]}).encode()
    return {"body": body}


def _eodhd_routes():
    eod = json.dumps([
        {"date": "2026-07-23", "open": 1, "high": 2, "low": 0.5, "close": 1.5,
         "adjusted_close": 1.5, "volume": 100},
        {"date": "2026-07-24", "open": 1.5, "high": 2.5, "low": 1.0,
         "close": 2.0, "adjusted_close": 2.0, "volume": 120},
    ]).encode()
    div = json.dumps([{"date": "2026-07-10", "declarationDate": "2026-06-25",
                       "recordDate": "2026-07-11", "paymentDate": "2026-07-20",
                       "value": 0.25, "currency": "USD"}]).encode()
    splits = json.dumps([{"date": "2026-07-15", "split": "2.000000/1.000000"}]).encode()
    earnings = json.dumps({"earnings": [
        {"code": "AAPL.US", "report_date": "2026-07-23", "date": "2026-06-30",
         "before_after_market": "AfterMarket", "actual": 2.1, "estimate": 2.0}]}).encode()
    insider = json.dumps([
        {"code": "AAPL.US", "date": "2026-07-22", "ownerName": "CEO A",
         "transactionDate": "2026-07-21", "transactionCode": "S",
         "transactionAmount": 100}]).encode()
    news = json.dumps([
        {"date": "2026-07-23T12:00:00+00:00", "title": "T1",
         "content": "C" * 900, "link": "https://news.example/1",
         "symbols": ["AAPL.US"], "sentiment": {"polarity": 0.5}}]).encode()
    fundamentals = json.dumps({
        "General": {"Code": "AAPL", "Name": "Apple Inc.", "CIK": "320193",
                    "Exchange": "NASDAQ", "CurrencyCode": "USD",
                    "GicSector": "Information Technology"},
        "Highlights": {"MarketCapitalization": 3.1e12,
                       "MostRecentQuarter": "2026-06-30"}}).encode()
    user = json.dumps({"name": "acct", "email": "account-pii@example.com",
                       "subscriptionType": "unlimited", "apiRequests": 5,
                       "dailyRateLimit": 100000}).encode()
    return [
        ("/api/user?", {"body": user}),
        ("/api/eod/", {"body": eod}),
        ("/api/div/", {"body": div}),
        ("/api/splits/", {"body": splits}),
        ("/api/calendar/earnings", {"body": earnings}),
        ("/api/insider-transactions", {"body": insider}),
        ("/api/news", {"body": news}),
        ("/api/fundamentals/", {"body": fundamentals}),
    ]


def default_routes():
    return _eodhd_routes() + [
        ("company_tickers.json", {"body": _company_tickers_body()}),
        ("daily-index", {"body": _master_idx_body()}),
        ("cdn.finra.org", {"body": _finra_body(),
                           "headers": {"last-modified":
                                       "Fri, 24 Jul 2026 22:00:00 GMT"}}),
        ("nasdaqlisted.txt", {"body": _nasdaqlisted_body()}),
        ("otherlisted.txt", {"body": _otherlisted_body()}),
        ("rss.aspx?feed=tradehalts", {"body": _halts_body()}),
        ("api.stlouisfed.org", _fred_route),
    ]


# --------------------------------------------------------------------------- #
# Config / environment builders
# --------------------------------------------------------------------------- #

def _write_stage1_fixture(root: Path) -> None:
    run_dir = root / "runs" / "stage1_test"
    run_dir.mkdir(parents=True)
    (root / "latest.json").write_text(json.dumps(
        {"run_id": "stage1_test", "run_dir": "runs/stage1_test"}),
        encoding="utf-8")
    rows = [
        ("short_activity", "BLOCKED_BY_MISSING_DATA", "no local data"),
        ("options", "BLOCKED_BY_MISSING_DATA", "no provider"),
        ("price_momentum", "TESTED_INCONCLUSIVE", ""),
        ("value", "NOT_YET_TESTED", ""),
        ("insider_activity", "NOT_YET_TESTED", ""),
        ("macro_and_regime", "PROVEN_DISTINCT", ""),
        ("portfolio_construction", "PROVEN_DISTINCT", ""),
    ]
    with (run_dir / "research_coverage_map.csv").open("w", newline="",
                                                      encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["information_family", "evidence_classification",
                         "required_missing_data"])
        writer.writerows(rows)
    (run_dir / "current_state_summary.json").write_text(
        json.dumps({"champion_model": "fundamental_momentum_50_50_v1"}),
        encoding="utf-8")


def _write_ledger_fixture(root: Path) -> list[Path]:
    desk = root / "paper_trading_desk"
    mhz = root / "multi_horizon_alpha_ledger"
    desk.mkdir(parents=True)
    mhz.mkdir(parents=True)
    files = [desk / "alpha_book_records.json", desk / "paper_books.json",
             mhz / "mhz_snapshots.json"]
    for i, path in enumerate(files):
        path.write_text(json.dumps({"fixture": i, "book_id": "alpha_paper_book_1"}),
                        encoding="utf-8")
    return files


def make_config(tmp: Path, enabled: list[str], **tweaks) -> dict:
    sources = {
        "norgate_local": {
            "enabled": "norgate_local" in enabled, "priority": 1,
            "kind": "local_vendor", "license_note": "Norgate local license",
            "min_interval_seconds": 0.0,
            "sample_symbols": ["AAPL", "MSFT"],
            "index_partition_symbols": ["AAPL"], "index_name": "S&P 500",
            "recent_bar_days": 5, "max_watchlists": 10,
            "freshness_threshold_days": 6},
        "eodhd": {
            "enabled": "eodhd" in enabled, "priority": 2,
            "kind": "entitled_provider", "license_note": "EODHD subscription",
            "base_url": "https://eodhd.com/api",
            "allowed_env_vars": ["EODHD_API_KEY"],
            "min_interval_seconds": 0.0,
            "sample_symbols": ["AAPL.US", "MSFT.US"],
            "fundamentals_sample_symbols": ["AAPL.US"],
            "recent_bar_days": 5, "corporate_actions_window_days": 30,
            "earnings_window_days": 7, "news_window_days": 3,
            "news_limit_per_symbol": 5, "insider_limit": 50,
            "entitlement_probe_families": ["eod", "dividends", "splits",
                                           "earnings", "fundamentals",
                                           "insider", "news"],
            "freshness_threshold_days": 6},
        "sec_edgar": {
            "enabled": "sec_edgar" in enabled, "priority": 3,
            "kind": "public_official", "license_note": "SEC public domain",
            "base_url_www": "https://www.sec.gov",
            "base_url_data": "https://data.sec.gov",
            "min_interval_seconds": 0.0, "filing_window_business_days": 1,
            "forms_of_interest": ["8-K", "10-Q", "10-K", "4"],
            "max_filings_per_day": 6000, "store_filing_text": False,
            "ticker_map_path": "/files/company_tickers.json",
            "freshness_threshold_days": 6},
        "finra": {
            "enabled": "finra" in enabled, "priority": 4,
            "kind": "public_official", "license_note": "FINRA public",
            "base_url": "https://cdn.finra.org/equity/regsho/daily",
            "markets": ["CNMS"], "window_business_days": 1,
            "min_interval_seconds": 0.0, "freshness_threshold_days": 6},
        "nasdaq_trader": {
            "enabled": "nasdaq_trader" in enabled, "priority": 5,
            "kind": "public_official", "license_note": "Nasdaq Trader public",
            "symbol_directory_urls": {
                "nasdaqlisted": "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
                "otherlisted": "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"},
            "halts_feed_url": "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
            "min_interval_seconds": 0.0, "freshness_threshold_days": 6},
        "fred_alfred": {
            "enabled": "fred_alfred" in enabled, "priority": 6,
            "kind": "public_api_keyed", "license_note": "FRED public",
            "base_url": "https://api.stlouisfed.org/fred",
            "allowed_env_vars": ["FRED_API_KEY", "PAPER_TRADER_FRED_API_KEY"],
            "min_interval_seconds": 0.0, "observation_window_days": 45,
            "use_alfred_vintages": True,
            "series_allowlist": [
                {"series_id": "DGS10", "macro_family": "interest_rates",
                 "title": "10Y"},
                {"series_id": "VIXCLS", "macro_family": "volatility_regime",
                 "title": "VIX"}],
            "freshness_threshold_days": 45},
        "gdelt": {
            "enabled": "gdelt" in enabled,
            "deferred": "gdelt" not in enabled, "priority": 7,
            "kind": "public_discovery", "license_note": "GDELT metadata only",
            "defer_reason": "core collectors first",
            "base_url": "https://api.gdeltproject.org/api/v2/doc/doc",
            "min_interval_seconds": 0.0, "max_articles": 10,
            "metadata_only": True, "snippet_max_chars": 60,
            "query_symbols": ["AAPL"], "freshness_threshold_days": 6},
    }
    config = {
        "stage": "2", "config_version": "1.0.0",
        "safety": {"paper_only": True, "no_orders": True, "no_automation": True,
                   "no_model_api": True, "no_postgres": True},
        "stage1_registry_root": str(tmp / "stage1"),
        "operational_ledger_roots": [
            str(tmp / "ledgers" / "paper_trading_desk"),
            str(tmp / "ledgers" / "multi_horizon_alpha_ledger")],
        "output_contract": {
            "layout_version": "1.0.0", "state_dir": "state",
            "state_db": "source_state.sqlite", "raw_dir": "raw",
            "normalized_dir": "normalized", "runs_dir": "runs",
            "latest_file": "latest.json",
            "required_run_files": [
                "source_inventory.json", "entitlement_audit.json",
                "source_health.csv", "collection_manifest.json",
                "raw_object_index.csv", "normalized_record_counts.csv",
                "data_quality_report.json", "coverage_gap_mapping.csv",
                "checkpoint_summary.json", "daily_ingestion_report.md",
                "run_manifest.json"]},
        "limits": {"http_timeout_seconds": 5, "max_retries": 1,
                   "backoff_base_seconds": 0.0, "backoff_multiplier": 2.0,
                   "raw_object_max_bytes": 8388608,
                   "max_raw_objects_per_source": 200,
                   "max_normalized_records_per_source": 120000,
                   "circuit_breaker_threshold": 5},
        "secret_redaction": {
            "redacted_query_params": ["api_token", "api_key", "apikey",
                                      "token", "key"],
            "redaction_placeholder": "REDACTED"},
        "user_agent": {"product": "paper-trader-alpha-agent/2.0-test",
                       "contact_resolution_policy": "git_config_user_email",
                       "mask_contact_in_logs": True},
        "sources": sources,
    }
    for dotted, value in tweaks.items():
        node = config
        parts = dotted.split(".")
        for part in parts[:-1]:
            node = node[part]
        node[parts[-1]] = value
    return config


def default_env():
    return {"EODHD_API_KEY": EODHD_SECRET, "FRED_API_KEY": FRED_SECRET}


def _snapshot(root: Path) -> dict:
    return {str(p.relative_to(root)): (p.stat().st_size, p.stat().st_mtime_ns)
            for p in sorted(root.rglob("*")) if p.is_file()}


def run_stage2(tmp: Path, *, mode="collect", enabled=None, routes=None,
               env=None, contact=CONTACT, as_of=AS_OF, config=None,
               transport=None, limits_tweaks=None, use_norgate=True):
    if not (tmp / "stage1").exists():
        _write_stage1_fixture(tmp / "stage1")
        _write_ledger_fixture(tmp / "ledgers")
    cfg = config or make_config(tmp, enabled or [
        "norgate_local", "eodhd", "sec_edgar", "finra", "nasdaq_trader",
        "fred_alfred"], **(limits_tweaks or {}))
    transport = transport or FakeTransport(routes if routes is not None
                                           else default_routes())
    sleeps: list[float] = []
    kwargs = dict(config=cfg, output_root=str(tmp / "ingestion"), mode=mode,
                  as_of=as_of, git_commit="testcommit0001",
                  transport=transport, env=env if env is not None else default_env(),
                  sleep_fn=sleeps.append, clock_fn=make_clock(),
                  contact_email=contact)
    if use_norgate:
        with norgate_ctx(fake_norgate_module()):
            result = ing.run_ingestion(**kwargs)
    else:
        result = ing.run_ingestion(**kwargs)
    return SimpleNamespace(result=result, cfg=cfg, transport=transport,
                           sleeps=sleeps, tmp=tmp,
                           out=tmp / "ingestion",
                           db=tmp / "ingestion" / "state" / "source_state.sqlite")


# --------------------------------------------------------------------------- #
# Module-scope baseline rig (one full fake production run, reused read-only)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def rig(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("stage2_base")
    _write_stage1_fixture(tmp / "stage1")
    ledger_files = _write_ledger_fixture(tmp / "ledgers")
    pre_stage1 = _snapshot(tmp / "stage1")
    pre_ledgers = _snapshot(tmp / "ledgers")
    r = run_stage2(tmp)
    r.pre_stage1, r.pre_ledgers = pre_stage1, pre_ledgers
    r.post_stage1 = _snapshot(tmp / "stage1")
    r.post_ledgers = _snapshot(tmp / "ledgers")
    r.ledger_files = ledger_files
    assert r.result["status"] == ing.READY, r.result
    r.run_dir = Path(r.result["run_dir"])
    return r


import contextlib


@contextlib.contextmanager
def _db(rig_obj):
    conn = sqlite3.connect("file:%s?mode=ro" % Path(rig_obj.db).as_posix(),
                           uri=True)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _health_rows(run_dir: Path) -> dict[str, dict]:
    with (run_dir / "source_health.csv").open(encoding="utf-8", newline="") as fh:
        return {row["source_id"]: row for row in csv.DictReader(fh)}


def _all_output_text(root: Path) -> bytes:
    blob = b""
    for path in sorted(root.rglob("*")):
        if path.is_file():
            blob += path.read_bytes()
    return blob


def _source_blob() -> str:
    return "\n".join(p.read_text(encoding="utf-8") for p in _STAGE2_SOURCE_FILES)


# --------------------------------------------------------------------------- #
# 1-3: forbidden integrations
# --------------------------------------------------------------------------- #

def test_01_no_llm_imports_or_calls():
    blob = _source_blob().lower()
    for token in ("openai", "anthropic", "chatcompletion", "completions.create",
                  "messages.create", "claude"):
        assert token not in blob, token


def test_02_no_postgres_imports():
    blob = _source_blob().lower()
    for token in ("psycopg", "sqlalchemy", "create_engine", "postgresql://",
                  "pg8000", "asyncpg"):
        assert token not in blob, token


def test_03_no_paper_trader_db_calls():
    blob = _source_blob()
    for token in ("from paper_trader.api", "import paper_trader.api",
                  "paper_trader.engine", "engine.market_data",
                  "operational_book", "alpha_book.", "daily_close"):
        assert token not in blob, token


# --------------------------------------------------------------------------- #
# 4-7: write containment / no mutation / no orders
# --------------------------------------------------------------------------- #

def test_04_no_writes_outside_output_root(rig):
    assert rig.pre_stage1 == rig.post_stage1
    assert rig.pre_ledgers == rig.post_ledgers


def test_05_operational_ledgers_unchanged(rig):
    assert rig.result["ledger_unchanged"] is True
    dq = json.loads((rig.run_dir / "data_quality_report.json").read_text(
        encoding="utf-8"))
    check = [c for c in dq["checks"] if c["check_id"] == 18][0]
    assert check["status"] == "PASS"


def test_06_active_book_not_mutated(rig):
    for path in rig.ledger_files:
        obj = json.loads(path.read_text(encoding="utf-8"))
        assert obj.get("book_id") == "alpha_paper_book_1"


def test_07_no_order_or_fill_creation(rig):
    blob = _source_blob().lower()
    for token in ("create_order", "submit_order", "place_order", "order_plan",
                  "fill_order"):
        assert token not in blob, token
    with _db(rig) as conn:
        types_seen = {row[0] for row in
                      conn.execute("SELECT DISTINCT record_type FROM"
                                   " normalized_records")}
    assert types_seen <= set(sc.RECORD_TYPES)


# --------------------------------------------------------------------------- #
# 8-9: secret redaction
# --------------------------------------------------------------------------- #

def test_08_secrets_redacted_from_request_fingerprints(rig):
    blob = _all_output_text(rig.out)
    assert EODHD_SECRET.encode() not in blob
    assert FRED_SECRET.encode() not in blob
    assert b"account-pii@example.com" not in blob  # /user probe never archived
    index_text = (rig.run_dir / "raw_object_index.csv").read_text(encoding="utf-8")
    assert "api_token=REDACTED" in index_text


def test_09_secrets_redacted_from_errors(tmp_path):
    routes = [("/api/user?", {"error": "connect exploded with %s" % EODHD_SECRET}),
              ("/api/", {"error": "connect exploded with %s" % EODHD_SECRET})]
    r = run_stage2(tmp_path, enabled=["eodhd"], routes=routes)
    conn = sqlite3.connect(str(r.db))
    messages = [row[0] for row in
                conn.execute("SELECT message FROM source_errors")]
    conn.close()
    assert messages, "expected recorded errors"
    assert all(EODHD_SECRET not in (m or "") for m in messages)
    assert any("***" in (m or "") for m in messages)


# --------------------------------------------------------------------------- #
# 10-14: deterministic IDs, reuse, immutability, duplicate prevention
# --------------------------------------------------------------------------- #

def test_10_deterministic_raw_object_ids():
    a = sc.make_raw_object_id("finra", b"payload")
    assert a == sc.make_raw_object_id("finra", b"payload")
    assert a != sc.make_raw_object_id("finra", b"payload2")
    assert a != sc.make_raw_object_id("sec_edgar", b"payload")
    assert a.startswith("raw_")


def test_11_identical_raw_payload_reuse(rig):
    rerun = run_stage2(rig.tmp)
    assert rerun.result["status"] == ing.READY
    assert rerun.result["already_existed"] is True
    assert rerun.result["run_id"] == rig.result["run_id"]
    assert rerun.result["counts"]["raw_objects_new"] == 0
    assert rerun.result["counts"]["duplicates_prevented"] > 0


def test_12_raw_object_immutability(tmp_path):
    archive = RawArchive(tmp_path / "raw", tmp_path, set(), 1 << 20)
    first = archive.store(source_id="s", content=b"abc", extension="txt",
                          retrieved_at="2026-07-24T00:00:00", business_date=AS_OF,
                          source_native_id="n", request_fp="GET x",
                          content_type="text/plain", http_status=200,
                          retry_count=0, published_at=None, license_note="L")
    path = tmp_path / first["storage_path"]
    original = path.read_bytes()
    stamp = path.stat().st_mtime_ns
    second = archive.store(source_id="s", content=b"abc", extension="txt",
                           retrieved_at="2026-07-25T00:00:00", business_date=AS_OF,
                           source_native_id="n", request_fp="GET x",
                           content_type="text/plain", http_status=200,
                           retry_count=0, published_at=None, license_note="L")
    assert second["duplicate"] is True
    assert second["raw_object_id"] == first["raw_object_id"]
    assert path.read_bytes() == original
    assert path.stat().st_mtime_ns == stamp
    assert archive.duplicates_prevented == 1


def test_13_deterministic_normalized_record_ids():
    kwargs = dict(record_type=sc.RT_SHORT_VOLUME, source_id="finra",
                  source_native_id="2026-07-24|AAPL|CNMS", raw_object_id=None,
                  retrieved_at="2026-07-24T23:00:00",
                  payload={"short_volume": 1000})
    a = sc.build_normalized_record(**kwargs)
    b = sc.build_normalized_record(**kwargs)
    assert a["record_id"] == b["record_id"]
    kwargs["payload"] = {"short_volume": 1001}
    c = sc.build_normalized_record(**kwargs)
    assert c["record_id"] != a["record_id"]


def test_14_duplicate_normalized_record_prevention(rig):
    with _db(rig) as conn:
        before = conn.execute("SELECT COUNT(*) FROM normalized_records").fetchone()[0]
    rerun = run_stage2(rig.tmp)
    assert rerun.result["counts"]["normalized_records_new"] == 0
    with _db(rig) as conn:
        after = conn.execute("SELECT COUNT(*) FROM normalized_records").fetchone()[0]
    assert before == after


# --------------------------------------------------------------------------- #
# 15-18: state DB integrity, checkpoints, resume
# --------------------------------------------------------------------------- #

def test_15_sqlite_integrity(rig):
    with _db(rig) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_16_foreign_key_integrity(rig):
    with _db(rig) as conn:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_17_atomic_source_checkpoints(tmp_path, rig):
    with _db(rig) as conn:
        rows = {row["source_id"]: row for row in
                conn.execute("SELECT * FROM source_checkpoints")}
    for source in ("norgate_local", "eodhd", "sec_edgar", "finra",
                   "nasdaq_trader", "fred_alfred"):
        assert json.loads(rows[source]["cursor_json"]), source
        assert rows[source]["last_success_at"], source
    conn = ing.open_state_db(tmp_path / "s.sqlite")

    def mini(cursor):
        return {"records": [], "raw_objects": [], "entitlements": [],
                "errors": [], "cursor": cursor, "last_attempt_at": "t1",
                "last_success_at": "t1", "consecutive_failures": 0,
                "circuit_state": "CLOSED"}
    scfg = {"enabled": True, "kind": "x", "priority": 1}
    ing._persist_source(conn, "src", scfg, mini({"a": 1}), "run", "t1", set())
    ing._persist_source(conn, "src", scfg, mini({}), "run", "t2", set())
    kept = conn.execute("SELECT cursor_json FROM source_checkpoints WHERE"
                        " source_id='src'").fetchone()[0]
    conn.close()
    assert json.loads(kept) == {"a": 1}  # empty cursor never wipes a checkpoint


def test_18_resume_after_interrupted_collection(tmp_path):
    broken = [("cdn.finra.org", {"error": "connection dropped"})] + \
        [r for r in default_routes() if r[0] != "cdn.finra.org"]
    first = run_stage2(tmp_path, enabled=["norgate_local", "finra"],
                       routes=broken)
    assert first.result["status"] == ing.PARTIAL
    conn = sqlite3.connect(str(first.db))
    finra_before = conn.execute("SELECT COUNT(*) FROM normalized_records WHERE"
                                " source_id='finra'").fetchone()[0]
    norgate_rows = conn.execute("SELECT COUNT(*) FROM normalized_records WHERE"
                                " source_id='norgate_local'").fetchone()[0]
    conn.close()
    assert finra_before == 0 and norgate_rows > 0
    second = run_stage2(tmp_path, enabled=["norgate_local", "finra"],
                        routes=default_routes())
    assert second.result["status"] == ing.READY
    conn = sqlite3.connect(str(second.db))
    finra_after = conn.execute("SELECT COUNT(*) FROM normalized_records WHERE"
                               " source_id='finra'").fetchone()[0]
    conn.close()
    assert finra_after > 0
    assert second.result["counts"]["duplicates_prevented"] > 0  # norgate reused


# --------------------------------------------------------------------------- #
# 19-24: retry, rate limit, breaker, response hygiene, malformed input
# --------------------------------------------------------------------------- #

def test_19_retry_backoff_behavior(tmp_path):
    state = {"n": 0}

    def flaky(request):
        state["n"] += 1
        if state["n"] <= 2:
            return {"status": 500, "body": b"err"}
        return {"body": _finra_body(),
                "headers": {"last-modified": "Fri, 24 Jul 2026 22:00:00 GMT"}}
    cfg_tweaks = {"limits.max_retries": 3, "limits.backoff_base_seconds": 0.5}
    r = run_stage2(tmp_path, enabled=["finra"],
                   routes=[("cdn.finra.org", flaky)],
                   limits_tweaks=cfg_tweaks)
    assert r.result["status"] == ing.READY
    health = _health_rows(Path(r.result["run_dir"]))["finra"]
    assert int(health["retry_count"]) == 2
    assert 0.5 in r.sleeps and 1.0 in r.sleeps  # exponential backoff observed


def test_20_rate_limit_behavior(tmp_path):
    r = run_stage2(tmp_path, enabled=["finra"],
                   routes=[("cdn.finra.org", {"status": 429, "body": b"slow"})],
                   limits_tweaks={"limits.max_retries": 0})
    assert r.result["status"] == ing.BLOCKED  # 429 never treated as data
    conn = sqlite3.connect(str(r.db))
    assert conn.execute("SELECT COUNT(*) FROM raw_objects").fetchone()[0] == 0
    rl = conn.execute("SELECT COUNT(*) FROM source_errors WHERE"
                      " error_type='RATE_LIMITED'").fetchone()[0]
    conn.close()
    assert rl > 0
    health = _health_rows(Path(r.result["run_dir"]))["finra"]
    assert health["rate_limit_status"] == "RATE_LIMITED"


def test_21_circuit_breaker(tmp_path):
    r = run_stage2(tmp_path, enabled=["finra"],
                   routes=[("cdn.finra.org", {"error": "down"})],
                   limits_tweaks={"limits.max_retries": 0,
                                  "limits.circuit_breaker_threshold": 2})
    conn = sqlite3.connect(str(r.db))
    cb = conn.execute("SELECT circuit_state, consecutive_failures FROM"
                      " source_checkpoints WHERE source_id='finra'").fetchone()
    suppressed = conn.execute("SELECT COUNT(*) FROM source_errors WHERE"
                              " error_type='CIRCUIT_OPEN'").fetchone()[0]
    conn.close()
    assert cb[0] == "OPEN" and cb[1] >= 2
    assert suppressed > 0  # later requests were short-circuited


def test_22_zero_byte_response_rejected(tmp_path):
    r = run_stage2(tmp_path, enabled=["finra"],
                   routes=[("cdn.finra.org", {"body": b""})],
                   limits_tweaks={"limits.max_retries": 0})
    conn = sqlite3.connect(str(r.db))
    assert conn.execute("SELECT COUNT(*) FROM raw_objects").fetchone()[0] == 0
    zb = conn.execute("SELECT COUNT(*) FROM source_errors WHERE"
                      " error_type='ZERO_BYTE_RESPONSE'").fetchone()[0]
    conn.close()
    assert zb > 0 and r.result["status"] == ing.BLOCKED


def test_23_html_error_response_rejected(tmp_path):
    r = run_stage2(tmp_path, enabled=["finra"],
                   routes=[("cdn.finra.org",
                            {"body": b"<html><body>Maintenance</body></html>"})],
                   limits_tweaks={"limits.max_retries": 0})
    conn = sqlite3.connect(str(r.db))
    html = conn.execute("SELECT COUNT(*) FROM source_errors WHERE"
                        " error_type='HTML_ERROR_RESPONSE'").fetchone()[0]
    stored = conn.execute("SELECT COUNT(*) FROM raw_objects").fetchone()[0]
    conn.close()
    assert html > 0 and stored == 0


def test_24_malformed_json_handled(tmp_path):
    r = run_stage2(tmp_path, enabled=["fred_alfred"],
                   routes=[("api.stlouisfed.org", {"body": b"garbage{{not json"})])
    # Graceful, no crash: raw evidence archived, source FAILED, zero records.
    assert r.result["status"] == ing.PARTIAL
    assert any("fred_alfred" in b for b in r.result["blocked_sources"])
    conn = sqlite3.connect(str(r.db))
    parse = conn.execute("SELECT COUNT(*) FROM source_errors WHERE"
                         " error_type='PARSE_ERROR'").fetchone()[0]
    records = conn.execute("SELECT COUNT(*) FROM normalized_records").fetchone()[0]
    conn.close()
    assert parse > 0 and records == 0


# --------------------------------------------------------------------------- #
# 25-29: per-source guardrails
# --------------------------------------------------------------------------- #

def test_25_sec_user_agent_required_and_masked(tmp_path, rig):
    sec_calls = [(url, headers) for url, headers in rig.transport.calls
                 if "sec.gov" in url]
    assert sec_calls
    for _url, headers in sec_calls:
        assert CONTACT in headers.get("User-Agent", "")
    blob = _all_output_text(rig.out)
    assert CONTACT.encode() not in blob  # only the masked form may persist
    r = run_stage2(tmp_path, enabled=["sec_edgar"], contact=None)
    health = _health_rows(Path(r.result["run_dir"]))["sec_edgar"]
    assert health["overall_state"] == "BLOCKED_CONFIGURATION"
    assert "BLOCKED_MISSING_USER_AGENT_CONTACT" in health["entitlement_summary"]
    assert not [u for u, _ in r.transport.calls if "sec.gov" in u]


def test_26_sec_conservative_rate_limiting(tmp_path):
    r = run_stage2(tmp_path, enabled=["sec_edgar"],
                   limits_tweaks={"sources.sec_edgar.min_interval_seconds": 0.34})
    assert r.result["status"] == ing.READY
    assert any(0.2 <= s <= 0.34 for s in r.sleeps)


def test_27_eodhd_missing_credential_controlled(tmp_path):
    r = run_stage2(tmp_path, enabled=["norgate_local", "eodhd"],
                   env={"FRED_API_KEY": FRED_SECRET})
    assert r.result["status"] == ing.PARTIAL
    assert any("eodhd" in b and "BLOCKED_CREDENTIAL" in b
               for b in r.result["blocked_sources"])
    assert not [u for u, _ in r.transport.calls if "eodhd.com" in u]


def test_28_eodhd_blocked_entitlement_controlled(tmp_path):
    routes = [("/api/news", {"status": 403, "body": b"{}"})] + \
        [r for r in _eodhd_routes() if r[0] != "/api/news"] + default_routes()
    r = run_stage2(tmp_path, enabled=["eodhd"], routes=routes)
    assert r.result["status"] == ing.READY  # entitled families still collected
    audit = json.loads((Path(r.result["run_dir"]) /
                        "entitlement_audit.json").read_text(encoding="utf-8"))
    states = audit["entitlement_state"]["eodhd"]
    assert states["news"] == "NOT_ENTITLED"
    assert states["eod"] == "ENTITLED"
    conn = sqlite3.connect(str(r.db))
    news = conn.execute("SELECT COUNT(*) FROM normalized_records WHERE"
                        " record_type='NEWS_EVENT'").fetchone()[0]
    bars = conn.execute("SELECT COUNT(*) FROM normalized_records WHERE"
                        " record_type='MARKET_BAR'").fetchone()[0]
    conn.close()
    assert news == 0 and bars > 0


def test_29_norgate_unavailable_controlled(tmp_path):
    with norgate_ctx(None):  # import norgatedata -> ImportError
        r = run_stage2(tmp_path, enabled=["norgate_local", "finra"],
                       use_norgate=False)
    assert r.result["status"] == ing.PARTIAL
    health = _health_rows(Path(r.result["run_dir"]))["norgate_local"]
    assert health["overall_state"] == "NOT_CONFIGURED"
    conn = sqlite3.connect(str(r.db))
    finra_rows = conn.execute("SELECT COUNT(*) FROM normalized_records WHERE"
                              " source_id='finra'").fetchone()[0]
    conn.close()
    assert finra_rows > 0  # the rest of the system kept operating


# --------------------------------------------------------------------------- #
# 30-34: parser contracts
# --------------------------------------------------------------------------- #

def test_30_finra_parser_contract(rig):
    with _db(rig) as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM normalized_records WHERE record_type='SHORT_VOLUME'")]
    assert len(rows) == 3  # mismatched-date row rejected
    by_ticker = {r["ticker"]: r for r in rows}
    aapl = json.loads(by_ticker["AAPL"]["payload_json"])
    assert aapl["short_volume"] == 1000 and aapl["total_volume"] == 4000
    assert aapl["short_volume_ratio"] == 0.25
    assert "NOT short interest" in aapl["measure_note"]
    assert by_ticker["AAPL"]["effective_at"] == AS_OF
    assert by_ticker["AAPL"]["available_at"] is not None  # from Last-Modified
    with _db(rig) as conn:
        mismatch = conn.execute("SELECT COUNT(*) FROM source_errors WHERE"
                                " error_type='BUSINESS_DATE_MISMATCH'").fetchone()[0]
    assert mismatch >= 1


def test_31_nasdaq_symbol_directory_parser(rig):
    with _db(rig) as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM normalized_records WHERE source_id='nasdaq_trader'"
            " AND record_type='SECURITY_IDENTITY'")]
    tickers = {r["ticker"] for r in rows}
    assert "AAPL" in tickers and "XOM" in tickers
    assert "ZTEST" not in tickers  # test issues excluded
    xom = [r for r in rows if r["ticker"] == "XOM"][0]
    assert xom["exchange"] == "NYSE"
    assert (xom["available_at"] or "").startswith("2026-07-24T20:05")


def test_32_nasdaq_halt_parser(rig):
    with _db(rig) as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM normalized_records WHERE record_type='TRADING_HALT'")]
    assert len(rows) == 1
    halt = rows[0]
    payload = json.loads(halt["payload_json"])
    assert halt["ticker"] == "HALTX"
    assert payload["reason_code"] == "T1"
    assert payload["halt_time"] == "09:30:00"
    assert payload["resumption_trade_time"] == "10:05:00"
    assert halt["available_at"] is not None  # pubDate preserved
    assert halt["effective_at"] == "2026-07-24"


def test_33_fred_missing_credential_controlled(tmp_path):
    r = run_stage2(tmp_path, enabled=["norgate_local", "fred_alfred"],
                   env={"EODHD_API_KEY": EODHD_SECRET})
    assert r.result["status"] == ing.PARTIAL
    assert any("fred_alfred" in b and "BLOCKED_CREDENTIAL" in b
               for b in r.result["blocked_sources"])
    assert not [u for u, _ in r.transport.calls if "stlouisfed" in u]


def test_34_gdelt_metadata_only_and_dedup(tmp_path):
    articles = {"articles": [
        {"url": "https://ex.com/a?utm_source=x", "title": "Duplicate story",
         "seendate": "20260723T120000Z", "domain": "ex.com",
         "language": "English", "sourcecountry": "US"},
        {"url": "https://EX.com/a", "title": "Duplicate story",
         "seendate": "20260723T120000Z", "domain": "ex.com"},
        {"url": "https://ex.com/b",
         "title": "B" * 500, "seendate": "20260722T090000Z", "domain": "ex.com"},
    ]}
    r = run_stage2(tmp_path, enabled=["gdelt"],
                   routes=[("gdeltproject.org", {"body": json.dumps(articles).encode()})])
    assert r.result["status"] == ing.READY
    conn = sqlite3.connect(str(r.db))
    rows = [json.loads(p[0]) for p in conn.execute(
        "SELECT payload_json FROM normalized_records WHERE source_id='gdelt'")]
    conn.close()
    assert len(rows) == 2  # canonical-URL + fingerprint dedup collapsed the pair
    for payload in rows:
        assert payload["metadata_only"] is True
        assert "content" not in payload and "body" not in payload
        assert len(payload["title_snippet"]) <= 60  # bounded snippet
    assert canonical_url("https://EX.com/a?utm_source=z") == "https://ex.com/a"


# --------------------------------------------------------------------------- #
# 35-38: point-in-time discipline, identity, Stage 1 gap mapping
# --------------------------------------------------------------------------- #

def test_35_point_in_time_timestamp_separation(rig):
    with _db(rig) as conn:
        fred = [dict(r) for r in conn.execute(
            "SELECT * FROM normalized_records WHERE record_type="
            "'MACRO_OBSERVATION' AND observed_at='2026-06-10'"
            " ORDER BY available_at")]
        earn = [dict(r) for r in conn.execute(
            "SELECT * FROM normalized_records WHERE record_type="
            "'EARNINGS_EVENT'")]
        div = [dict(r) for r in conn.execute(
            "SELECT * FROM normalized_records WHERE event_type='DIVIDEND'")]
    # ALFRED vintages: one observation date, two distinct availability dates.
    avails = {r["available_at"] for r in fred}
    assert {"2026-06-11", "2026-07-11"} <= avails
    assert all(r["observed_at"] == "2026-06-10" for r in fred)
    # Earnings: period_end kept separate; availability NOT fabricated from it.
    payload = json.loads(earn[0]["payload_json"])
    assert payload["period_end"] == "2026-06-30"
    assert payload["publication_time"] == "2026-07-23"
    assert earn[0]["available_at"] is None
    warnings = json.loads(earn[0]["quality_warnings_json"])
    assert any("period_end NEVER substituted" in w for w in warnings)
    # Dividend: availability = declaration date, distinct from ex-date effect.
    assert div[0]["available_at"] == "2026-06-25"
    assert div[0]["effective_at"] == "2026-07-10"


def test_36_ticker_cik_conflict_surfaced(rig):
    dq = json.loads((rig.run_dir / "data_quality_report.json").read_text(
        encoding="utf-8"))
    conflicts = dq["identity_conflicts"]
    assert any(c["conflict_type"] == "TICKER_CIK_CONFLICT" and
               c["ticker"] == "AAPL" for c in conflicts)
    check = [c for c in dq["checks"] if c["check_id"] == 8][0]
    assert check["status"] == "PASS"


def test_37_entity_mapping_states(rig):
    with _db(rig) as conn:
        finra = {r["ticker"]: r["entity_mapping_confidence"] for r in
                 conn.execute("SELECT ticker, entity_mapping_confidence FROM"
                              " normalized_records WHERE record_type="
                              "'SHORT_VOLUME'")}
        sec_map = {r["ticker"]: r["entity_mapping_confidence"] for r in
                   conn.execute("SELECT ticker, entity_mapping_confidence FROM"
                                " normalized_records WHERE"
                                " event_type='TICKER_CIK_MAP'")}
        filings = {r["source_native_id"]: r["entity_mapping_confidence"] for r in
                   conn.execute("SELECT source_native_id,"
                                " entity_mapping_confidence FROM"
                                " normalized_records WHERE source_id='sec_edgar'"
                                " AND record_type IN ('FILING_EVENT',"
                                "'INSIDER_FILING')")}
    assert finra["MSFT"] == "MATCHED_EXACT"
    assert finra["AAPL"] == "AMBIGUOUS"  # two CIKs registered for AAPL
    assert finra["ZZZQ"] == "UNMATCHED"
    assert sec_map["AAPL"] == "AMBIGUOUS"
    assert filings["0000789019-26-000055"] == "MATCHED_EXACT"
    assert filings["0000111111-26-000001"] == "UNMATCHED"


def test_38_stage1_gap_mapping(rig):
    with (rig.run_dir / "coverage_gap_mapping.csv").open(encoding="utf-8",
                                                         newline="") as fh:
        rows = {r["information_family"]: r for r in csv.DictReader(fh)}
    assert rows["short_activity"]["candidate_stage2_source"] == "finra"
    assert rows["short_activity"]["gap_status"] == "PARTIALLY_ADDRESSED"
    assert int(rows["short_activity"]["records_collected"]) == 3
    assert rows["options"]["gap_status"] == "STILL_BLOCKED"
    assert rows["price_momentum"]["gap_status"] == "PARTIALLY_ADDRESSED"
    assert rows["value"]["entitlement_state"] == "ENTITLED"
    assert rows["insider_activity"]["candidate_stage2_source"] == "sec_edgar"
    assert rows["portfolio_construction"]["gap_status"] == "NOT_APPLICABLE"


# --------------------------------------------------------------------------- #
# 39-45: incremental, verify, output contract, no-experiment, audit, report
# --------------------------------------------------------------------------- #

def test_39_incremental_no_new_data(rig):
    runs_before = sorted((rig.out / "runs").iterdir())
    r = run_stage2(rig.tmp, mode="incremental")
    assert r.result["status"] == ing.NO_NEW
    assert r.result["run_dir"] is None
    runs_after = sorted((rig.out / "runs").iterdir())
    assert runs_before == runs_after  # no immutable run created for a timestamp


def test_40_verify_writes_nothing_no_network(rig):
    before = _snapshot(rig.out)
    sentinel = FakeTransport([])
    result = ing.run_ingestion(config=rig.cfg, output_root=str(rig.out),
                               mode="verify", transport=sentinel)
    assert result["status"] == ing.VERIFIED, result
    assert sentinel.calls == []  # verify never touches the network
    assert _snapshot(rig.out) == before  # verify writes nothing


def test_41_required_output_contract_and_latest(rig):
    required = rig.cfg["output_contract"]["required_run_files"]
    for name in required:
        assert (rig.run_dir / name).exists(), name
    latest = json.loads((rig.out / "latest.json").read_text(encoding="utf-8"))
    assert latest["run_id"] == rig.result["run_id"]
    assert latest["stage"] == "2"
    assert latest["terminal_token"] == ing.READY
    assert latest["stage1_run_id"] == "stage1_test"
    manifest = json.loads((rig.run_dir / "run_manifest.json").read_text(
        encoding="utf-8"))
    assert manifest["immutable"] is True
    assert set(manifest["output_file_hashes"]) == set(required) - {
        "run_manifest.json"}


def test_42_no_alpha_experiment_or_model_execution(rig):
    blob = _source_blob().lower()
    for token in ("backtest(", "sklearn", "torch", "lightgbm", "xgboost",
                  "run_research(", "evaluate_signal(", "capture_snapshots",
                  "mature_outcomes", "run_daily_close"):
        assert token not in blob, token
    with _db(rig) as conn:
        types_seen = {row[0] for row in conn.execute(
            "SELECT DISTINCT record_type FROM normalized_records")}
    assert types_seen <= set(sc.RECORD_TYPES)
    report = (rig.run_dir / "daily_ingestion_report.md").read_text(encoding="utf-8")
    assert "New alpha experiment run:** NO" in report


def test_43_audit_mode_immutable_run(tmp_path):
    r = run_stage2(tmp_path, mode="audit", enabled=["norgate_local", "finra"])
    assert r.result["status"] == ing.READY
    run_dir = Path(r.result["run_dir"])
    assert (run_dir / "entitlement_audit.json").exists()
    conn = sqlite3.connect(str(r.db))
    records = conn.execute("SELECT COUNT(*) FROM normalized_records").fetchone()[0]
    conn.close()
    assert records == 0  # audit inspects, never collects records


def test_44_gdelt_deferred_by_default(rig):
    health = _health_rows(rig.run_dir)["gdelt"]
    assert health["overall_state"] == "NOT_RUN"
    inventory = json.loads((rig.run_dir / "source_inventory.json").read_text(
        encoding="utf-8"))
    assert inventory["sources"]["gdelt"]["inventory"]["deferred"] is True
    assert not [u for u, _ in rig.transport.calls if "gdelt" in u]


def test_45_daily_report_contract(rig):
    report = (rig.run_dir / "daily_ingestion_report.md").read_text(encoding="utf-8")
    assert "Secrets exposed:** NO" in report
    assert "records changed:** NO" in report
    assert rig.result["run_id"] in report
    assert "Next collection cursor per source" in report
    assert "Stage 3 readiness" in report
    assert "stage1_test" in report
