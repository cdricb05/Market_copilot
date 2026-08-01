"""
tests/test_alpha_agent_stage8_new_sources.py — Stage 8 NEW official-source
collectors (US Treasury, BLS, BEA) + extended SEC EDGAR data.sec.gov lanes
(submissions / companyfacts / companyconcept).

Every network interaction uses an injected fake transport — pytest NEVER calls an
external API. Focus: real record production, point-in-time discipline (filed /
acceptance datetime = availability; reference period never fabricated as
availability), credential blocking, and secret redaction of the query-string key.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import source_contracts as sc  # noqa: E402
from paper_trader.alpha_agent.collectors import COLLECTOR_CLASSES  # noqa: E402
from paper_trader.alpha_agent.collectors.base import (  # noqa: E402
    CollectorContext, RawArchive,
)
from paper_trader.alpha_agent.collectors.bea import BeaCollector  # noqa: E402
from paper_trader.alpha_agent.collectors.bls import BlsCollector  # noqa: E402
from paper_trader.alpha_agent.collectors.sec_edgar import (  # noqa: E402
    SecEdgarCollector,
)
from paper_trader.alpha_agent.collectors.us_treasury import (  # noqa: E402
    UsTreasuryCollector,
)

AS_OF = "2026-07-31"


class FakeTransport:
    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def __call__(self, request, timeout):
        self.calls.append(request["url"])
        for frag, resp in self.routes:
            if frag in request["url"]:
                out = resp(request) if callable(resp) else dict(resp)
                base = {"status": 200, "headers": {}, "body": b"", "error": None}
                base.update(out)
                return base
        return {"status": 404, "headers": {}, "body": b"", "error": None}


def _clock():
    t = [0.0]

    def c():
        t[0] += 0.01
        return t[0]
    return c


def _ctx(source_cfg, routes, *, env=None, identity=None, extra_config=None):
    import tempfile
    d = Path(tempfile.mkdtemp(prefix="pt-stage8-"))   # system temp; no repo write
    config = {
        "limits": {"max_retries": 1, "backoff_base_seconds": 0.0,
                   "backoff_multiplier": 1.0, "http_timeout_seconds": 5,
                   "raw_object_max_bytes": 1 << 22, "circuit_breaker_threshold": 5},
        "secret_redaction": {"redacted_query_params":
                             ["api_token", "api_key", "key", "registrationkey",
                              "userid", "UserID"],
                             "redaction_placeholder": "REDACTED"},
        "user_agent": {"product": "paper-trader-alpha-agent/2.0"},
        "sources": {},
        "_runtime": {"contact_email": "ops@test.example"},
    }
    config.update(extra_config or {})
    archive = RawArchive(d / "raw", d, set(), 1 << 22)
    return CollectorContext(
        config=config, source_cfg=source_cfg, archive=archive,
        transport=FakeTransport(routes), now_iso=lambda: "2026-07-31T00:00:00",
        clock=_clock(), sleep=lambda s: None, env=env or {},
        user_agent="paper-trader-alpha-agent/2.0 ops@test.example",
        identity=identity)


def _body(obj) -> bytes:
    return json.dumps(obj).encode("utf-8")


# --------------------------------------------------------------------------- #
# Registration
# --------------------------------------------------------------------------- #
def test_new_collectors_registered():
    for sid in ("us_treasury", "bls", "bea"):
        assert sid in COLLECTOR_CLASSES


# --------------------------------------------------------------------------- #
# US Treasury
# --------------------------------------------------------------------------- #
_TREASURY_CFG = {
    "enabled": True, "base_url": "https://api.fiscaldata.treasury.gov",
    "page_size": 50, "max_pages": 1,
    "endpoints": [{"path": "/v2/accounting/od/avg_interest_rates",
                   "macro_family": "treasury_rates", "date_field": "record_date",
                   "value_field": "avg_interest_rate_amt",
                   "series_fields": ["security_type_desc", "security_desc"]}],
}
_TREASURY_JSON = {"data": [
    {"record_date": "2026-06-30", "security_type_desc": "Marketable",
     "security_desc": "Treasury Bills", "avg_interest_rate_amt": "3.706"},
    {"record_date": "2026-06-30", "security_type_desc": "Marketable",
     "security_desc": "Treasury Notes", "avg_interest_rate_amt": "2.941"},
], "meta": {"total-count": 2}, "links": {"next": None}}


def test_us_treasury_collects_macro_observations_with_pit():
    ctx = _ctx(_TREASURY_CFG, [("avg_interest_rates",
                                {"body": _body(_TREASURY_JSON)})])
    res = UsTreasuryCollector(ctx).collect(AS_OF)
    recs = res["records"]
    assert len(recs) == 2
    for r in recs:
        assert r["record_type"] == sc.RT_MACRO_OBSERVATION
        assert r["source_id"] == "us_treasury"
        # observed/effective are the record_date; availability is NEVER fabricated
        assert r["observed_at"] == "2026-06-30"
        assert r["effective_at"] == "2026-06-30"
        assert r["available_at"] is None
        assert any("RELEASE_LAG_UNKNOWN" in w for w in r["quality_warnings"])
    keys = {r["normalized_payload"]["series_key"] for r in recs}
    assert keys == {"Marketable|Treasury Bills", "Marketable|Treasury Notes"}


# --------------------------------------------------------------------------- #
# BLS
# --------------------------------------------------------------------------- #
_BLS_CFG = {
    "enabled": True, "base_url": "https://api.bls.gov/publicAPI/v2",
    "start_year": 2025, "end_year": 2026,
    "series_allowlist": [
        {"series_id": "CUUR0000SA0", "macro_family": "inflation",
         "title": "CPI-U"}],
}


def _bls_json(series_id):
    return {"status": "REQUEST_SUCCEEDED", "Results": {"series": [{
        "seriesID": series_id, "data": [
            {"year": "2026", "period": "M06", "periodName": "June",
             "value": "324.0", "footnotes": [{}]},
            {"year": "2025", "period": "M13", "periodName": "Annual",
             "value": "319.6", "footnotes": [{}]},
        ]}]}}


def test_bls_collects_and_maps_periods_pit():
    ctx = _ctx(_BLS_CFG, [("timeseries/data/CUUR0000SA0",
                           {"body": _body(_bls_json("CUUR0000SA0"))})])
    res = BlsCollector(ctx).collect(AS_OF)
    recs = res["records"]
    dates = sorted(r["observed_at"] for r in recs)
    assert dates == ["2025-01-01", "2026-06-01"]      # M13 -> Jan; M06 -> Jun
    for r in recs:
        assert r["record_type"] == sc.RT_MACRO_OBSERVATION
        assert r["available_at"] is None               # release date not in payload
        assert any("RELEASE_LAG_UNKNOWN" in w for w in r["quality_warnings"])


def test_bls_status_failure_is_recorded_not_fatal():
    bad = {"status": "REQUEST_NOT_PROCESSED", "message": ["daily limit"]}
    ctx = _ctx(_BLS_CFG, [("timeseries/data", {"body": _body(bad)})])
    res = BlsCollector(ctx).collect(AS_OF)
    assert res["records"] == []
    assert any(e["error_type"] == "BLS_STATUS" for e in res["errors"])


def test_bls_registration_key_redacted_from_fingerprint():
    secret = "BLSKEY_SUPERSECRET_123456"
    ctx = _ctx({**_BLS_CFG, "allowed_env_vars": ["BLS_API_KEY"]},
               [("timeseries/data", {"body": _body(_bls_json("CUUR0000SA0"))})],
               env={"BLS_API_KEY": secret})
    res = BlsCollector(ctx).collect(AS_OF)
    assert res["records"]
    blob = json.dumps(res, default=str)
    assert secret not in blob                          # never stored in the clear
    for raw in res["raw_objects"]:
        assert secret not in raw.get("request_fingerprint", "")
        assert "REDACTED" in raw.get("request_fingerprint", "")


# --------------------------------------------------------------------------- #
# BEA (credential-gated)
# --------------------------------------------------------------------------- #
_BEA_CFG = {
    "enabled": True, "base_url": "https://apps.bea.gov/api/data",
    "allowed_env_vars": ["BEA_API_KEY"],
    "tables": [{"dataset": "NIPA", "table_name": "T10101", "frequency": "Q",
                "year": "ALL", "macro_family": "national_accounts"}],
}


def test_bea_blocked_without_key_is_honest():
    # Hermetic: no env var AND a DPAPI credential path that cannot exist, so the
    # runtime DPAPI fallback resolves to nothing regardless of whether THIS
    # machine has a real BEA UserID configured. Proves the honest no-key block.
    cfg = dict(_BEA_CFG, dpapi_credential_path=str(
        _REPO / "no_such_dir__bea" / "userid.dpapi"))
    ctx = _ctx(cfg, [("apps.bea.gov", {"body": _body({})})], env={})
    res = BeaCollector(ctx).collect(AS_OF)
    assert res["records"] == []
    # Absent FREE key = resolvable configuration gap (ACCESSIBLE_AFTER_REPAIR),
    # NOT an invalid/rejected credential.
    assert res["health"]["overall_state"] == sc.SH_BLOCKED_CONFIGURATION
    assert res["health"]["credential_present"] is False
    # No network call was even attempted (blocked before fetch).
    assert ctx.transport.calls == []


def test_bea_collects_with_key_and_parses_periods():
    bea_json = {"BEAAPI": {"Results": {"Data": [
        {"TimePeriod": "2025Q3", "LineDescription": "GDP", "SeriesCode": "A191RL",
         "DataValue": "2.8", "LineNumber": "1"},
        {"TimePeriod": "2025", "LineDescription": "GDP annual",
         "SeriesCode": "A191RL", "DataValue": "2.5", "LineNumber": "1"},
    ]}}}
    ctx = _ctx(_BEA_CFG, [("apps.bea.gov", {"body": _body(bea_json)})],
               env={"BEA_API_KEY": "BEAKEY123"})
    res = BeaCollector(ctx).collect(AS_OF)
    dates = sorted(r["observed_at"] for r in res["records"])
    assert dates == ["2025-01-01", "2025-07-01"]       # 2025 -> Jan; Q3 -> Jul
    assert res["health"]["credential_present"] is True


# --------------------------------------------------------------------------- #
# SEC EDGAR extended lanes (submissions / companyfacts / companyconcept)
# --------------------------------------------------------------------------- #
_SEC_CFG = {
    "enabled": True, "base_url_www": "https://www.sec.gov",
    "base_url_data": "https://data.sec.gov",
    "ticker_map_path": "/files/company_tickers.json",
    "filing_window_business_days": 1, "forms_of_interest": ["8-K"],
    "collect_submissions": True, "collect_companyfacts": True,
    "collect_companyconcept": True, "collect_full_index": True,
    "bulk_index_record_cap": 5, "facts_sample_symbols": ["AAPL"],
    "facts_symbol_limit": 1, "companyfacts_concepts": ["NetIncomeLoss"],
    "companyconcept_concept": "NetIncomeLoss",
}
_MASTER_IDX = (
    b"Description: EDGAR master index\n"
    b"CIK|Company Name|Form Type|Date Filed|Filename\n"
    b"--------------------------------------------------------------------\n"
    b"320193|Apple Inc|8-K|2026-07-05|edgar/data/320193/0000320193-26-90.txt\n"
    b"789019|Microsoft Corp|8-K|2026-07-06|edgar/data/789019/0000789019-26-70.txt\n"
    b"1045810|NVIDIA Corp|10-Q|2026-07-07|edgar/data/1045810/x.txt\n")
_TICKERS = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc"}}
_SUBMISSIONS = {"filings": {"recent": {
    "form": ["4", "8-K", "10-Q"],
    "accessionNumber": ["0000320193-26-01", "0000320193-26-02",
                        "0000320193-26-03"],
    "filingDate": ["2026-07-01", "2026-07-15", "2026-07-20"],
    "acceptanceDateTime": ["2026-07-01T18:30:00.000Z", "2026-07-15T16:05:00.000Z",
                           "2026-07-20T17:00:00.000Z"],
    "reportDate": ["2026-06-30", "", "2026-06-27"],
    "primaryDocument": ["form4.xml", "ex99.htm", "aapl.htm"],
}}}
_FACTS = {"facts": {"us-gaap": {"NetIncomeLoss": {"units": {"USD": [
    {"end": "2026-06-27", "val": 29789000000, "filed": "2026-07-31",
     "form": "10-Q", "fy": 2026, "fp": "Q3", "start": "2026-03-29"}]}}}}}
_CONCEPT = {"units": {"USD": [
    {"end": "2026-06-27", "val": 29789000000, "filed": "2026-07-31",
     "form": "10-Q", "fy": 2026, "fp": "Q3"}]}}


def _sec_ctx():
    return _ctx(_SEC_CFG, [
        ("company_tickers.json", {"body": _body(_TICKERS)}),
        ("daily-index", {"status": 404}),        # today's index: expected gap
        ("submissions/CIK", {"body": _body(_SUBMISSIONS)}),
        ("companyfacts/CIK", {"body": _body(_FACTS)}),
        ("companyconcept/CIK", {"body": _body(_CONCEPT)}),
        ("full-index", {"body": _MASTER_IDX}),
    ], identity=sc.IdentityResolver())


def test_sec_companyfacts_available_at_is_filed_not_period_end():
    res = SecEdgarCollector(_sec_ctx()).collect(AS_OF)
    facts = [r for r in res["records"]
             if r["record_type"] == sc.RT_FUNDAMENTAL_FACT
             and r["normalized_payload"].get("concept") == "NetIncomeLoss"
             and r["event_type"] == "XBRL_FACT"]
    assert facts, "expected companyfacts XBRL facts"
    f = facts[0]
    # TRUE point-in-time: availability is the FILED date, effective is period end.
    assert f["available_at"] == "2026-07-31"
    assert f["effective_at"] == "2026-06-27"
    assert f["available_at"] != f["effective_at"]
    assert f["normalized_payload"]["value"] == 29789000000


def test_sec_submissions_form4_and_8k_acceptance_is_pit_anchor():
    res = SecEdgarCollector(_sec_ctx()).collect(AS_OF)
    form4 = [r for r in res["records"]
             if r["record_type"] == sc.RT_INSIDER_FILING
             and str(r["source_native_id"]).startswith("sub|")]
    assert form4, "expected a Form 4 insider record from submissions"
    assert form4[0]["available_at"] == "2026-07-01T18:30:00.000Z"
    eightk = [r for r in res["records"]
              if r["event_type"] == "8-K"
              and r["record_type"] == sc.RT_FILING_EVENT]
    assert eightk and eightk[0]["available_at"] == "2026-07-15T16:05:00.000Z"
    assert eightk[0]["normalized_payload"]["is_8k"] is True
    assert eightk[0]["normalized_payload"]["item_202_note"]


def test_sec_companyconcept_lane_emits_pit_facts():
    res = SecEdgarCollector(_sec_ctx()).collect(AS_OF)
    concept = [r for r in res["records"] if r["event_type"] == "XBRL_CONCEPT"]
    assert concept
    assert concept[0]["available_at"] == "2026-07-31"      # filed
    assert concept[0]["normalized_payload"]["concept"] == "NetIncomeLoss"


def test_sec_full_index_bulk_lane_emits_bounded_filings():
    res = SecEdgarCollector(_sec_ctx()).collect(AS_OF)
    bulk = [r for r in res["records"]
            if str(r["source_native_id"]).startswith("bulk|")]
    assert bulk, "expected bulk full-index filing records"
    # forms_of_interest = 8-K only; both 8-K rows captured, 10-Q filtered out.
    assert all(r["event_type"] == "8-K" for r in bulk)
    assert len(bulk) == 2
    for r in bulk:
        assert r["normalized_payload"]["source_lane"] == "full_index_bulk"
        assert r["available_at"] is None            # index has filing date only


def test_sec_structured_lanes_gated_off_produce_no_facts():
    cfg = {**_SEC_CFG, "collect_submissions": False,
           "collect_companyfacts": False, "collect_companyconcept": False}
    ctx = _ctx(cfg, [
        ("company_tickers.json", {"body": _body(_TICKERS)}),
        ("daily-index", {"status": 404}),
    ], identity=sc.IdentityResolver())
    res = SecEdgarCollector(ctx).collect(AS_OF)
    assert not any(r["record_type"] == sc.RT_FUNDAMENTAL_FACT
                   for r in res["records"])


_SEC_ROUTES = [
    ("company_tickers.json", {"body": _body(_TICKERS)}),
    ("daily-index", {"status": 404}),
    ("submissions/CIK", {"body": _body(_SUBMISSIONS)}),
    ("companyfacts/CIK", {"body": _body(_FACTS)}),
    ("companyconcept/CIK", {"body": _body(_CONCEPT)}),
    ("full-index", {"body": _MASTER_IDX}),
]


def test_sec_collect_no_time_budget_is_unbounded_baseline():
    # SAFE-TIMEOUT (Stage 9.2 correction): absent collect_time_budget_seconds =
    # UNBOUNDED (byte-identical to prior behaviour) - structured lanes run and
    # the run is NOT marked deadline_reached.
    res = SecEdgarCollector(_sec_ctx()).collect(AS_OF)
    assert any(r["record_type"] == sc.RT_FUNDAMENTAL_FACT for r in res["records"])
    assert not res["inventory"].get("deadline_reached")
    assert res["cursor"].get("deadline_reached") is False


def test_sec_collect_honours_cooperative_time_budget():
    # A tiny cooperative budget makes the collector stop BETWEEN bounded network
    # requests (checked on this thread - never preempting an in-flight request),
    # mark the run deadline_reached, and skip the structured lanes. The
    # acquisition handler maps this to RETRYABLE and resumes the same batch. This
    # is the handler's OWN inline bound; nothing is abandoned and it is NOT a hard
    # kill.
    ctx = _ctx(_SEC_CFG, _SEC_ROUTES, identity=sc.IdentityResolver(),
               extra_config={"limits": {
                   "max_retries": 1, "backoff_base_seconds": 0.0,
                   "backoff_multiplier": 1.0, "http_timeout_seconds": 5,
                   "raw_object_max_bytes": 1 << 22,
                   "circuit_breaker_threshold": 5,
                   "collect_time_budget_seconds": 1e-9}})
    res = SecEdgarCollector(ctx).collect(AS_OF)
    assert res["inventory"].get("deadline_reached") is True
    assert res["cursor"].get("deadline_reached") is True
    # Structured-lane XBRL facts were NOT collected (lanes skipped after budget).
    assert not any(r["record_type"] == sc.RT_FUNDAMENTAL_FACT
                   for r in res["records"])
