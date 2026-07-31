"""
tests/test_alpha_agent_stage8_workstreams.py — Stage 8 FINAL closure workstreams.

WS1  EODHD analyst-vintage snapshot collector (immutable daily vintage, PIT
     availability = capture date, same-day idempotency, secret redaction).
WS2  SEC Form 4 transaction-level XML extraction.
WS3  SEC 8-K Item 2.02 / EX-99 earnings-release extraction.
WS4  SEC bulk-lane HEAD probe honesty (full-index vs company-facts/submissions
     bulk zips).
WS5  Point-in-time SIC sector classification (no look-ahead).
WS6  BEA secure-credential redaction.

Every network interaction uses an injected fake transport — pytest NEVER calls an
external API.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import source_contracts as sc  # noqa: E402
from paper_trader.alpha_agent.collectors.base import (  # noqa: E402
    CollectorContext, RawArchive,
)
from paper_trader.alpha_agent.collectors.eodhd_analyst import (  # noqa: E402
    EodhdAnalystCollector,
)
from paper_trader.alpha_agent.collectors.sec_edgar import (  # noqa: E402
    SecEdgarCollector,
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


def _ctx(source_cfg, routes, *, out_dir=None, env=None, identity=None,
         extra_config=None, transport=None):
    d = Path(out_dir or tempfile.mkdtemp(prefix="pt-ws-"))
    config = {
        "limits": {"max_retries": 1, "backoff_base_seconds": 0.0,
                   "backoff_multiplier": 1.0, "http_timeout_seconds": 5,
                   "raw_object_max_bytes": 1 << 22, "circuit_breaker_threshold": 5},
        "secret_redaction": {"redacted_query_params":
                             ["api_token", "api_key", "key", "registrationkey",
                              "userid", "UserID"],
                             "redaction_placeholder": "REDACTED"},
        "user_agent": {"product": "paper-trader-alpha-agent/2.0"},
        "sources": {}, "_runtime": {"contact_email": "ops@test.example"},
    }
    config.update(extra_config or {})
    archive = RawArchive(d / "raw", d, set(), 1 << 22)
    return CollectorContext(
        config=config, source_cfg=source_cfg, archive=archive,
        transport=transport or FakeTransport(routes),
        now_iso=lambda: "2026-07-31T00:00:00", clock=_clock(),
        sleep=lambda s: None, env=env or {},
        user_agent="paper-trader-alpha-agent/2.0 ops@test.example",
        identity=identity), d


def _body(obj) -> bytes:
    return json.dumps(obj).encode("utf-8")


# --------------------------------------------------------------------------- #
# WS1 — EODHD analyst-vintage collector
# --------------------------------------------------------------------------- #
_ANALYST_CFG = {
    "enabled": True, "base_url": "https://eodhd.com/api",
    "allowed_env_vars": ["EODHD_API_KEY"],
    "sample_symbols": ["AAPL.US", "JPM.US"],
    "analyst_filter": "General,Highlights,AnalystRatings,Earnings",
    "estimate_periods_cap": 4, "vintage_subdir": "vintages/eodhd_analyst",
}
_ANALYST_JSON = {
    "General": {"Code": "AAPL", "Name": "Apple Inc", "CIK": "320193"},
    "Highlights": {"WallStreetTargetPrice": 321.66},
    "AnalystRatings": {"Rating": 1.79, "TargetPrice": 250.5, "StrongBuy": 23,
                       "Buy": 15, "Hold": 8, "Sell": 1, "StrongSell": 0},
    "Earnings": {"Trend": {
        "2026-09-30": {"date": "2026-09-30", "period": "+1q",
                       "earningsEstimateAvg": "2.51", "earningsEstimateLow": "2.30",
                       "earningsEstimateHigh": "2.70",
                       "earningsEstimateNumberOfAnalysts": "28.0",
                       "epsRevisionsUpLast7days": "3",
                       "epsRevisionsDownLast30days": "1",
                       "revenueEstimateAvg": "102000000000"},
        "2026-12-31": {"date": "2026-12-31", "period": "+2q",
                       "earningsEstimateAvg": "2.85",
                       "earningsEstimateNumberOfAnalysts": "27.0",
                       "epsRevisionsUpLast30days": "5"}}},
}


def _analyst_routes():
    return [("fundamentals/", {"body": _body(_ANALYST_JSON)})]


def test_ws1_analyst_vintage_pit_availability_is_capture_date():
    ctx, _d = _ctx(_ANALYST_CFG, _analyst_routes(), env={"EODHD_API_KEY": "K" * 20})
    res = EodhdAnalystCollector(ctx).collect(AS_OF)
    recs = res["records"]
    assert recs, "expected analyst vintage records"
    kinds = {r["event_type"] for r in recs}
    assert "ANALYST_PRICE_TARGET_VINTAGE" in kinds
    assert "ANALYST_RATING_VINTAGE" in kinds
    assert "ANALYST_ESTIMATE_VINTAGE" in kinds
    for r in recs:
        assert r["record_type"] == sc.RT_FUNDAMENTAL_FACT
        assert r["source_id"] == "eodhd_analyst"
        # Availability is the SNAPSHOT capture date — never earlier, never fabricated.
        assert r["available_at"] == AS_OF
        assert r["effective_at"] == AS_OF
        assert r["normalized_payload"]["revision_vintage_date"] == AS_OF
    # price target value carried through
    pt = [r for r in recs if r["event_type"] == "ANALYST_PRICE_TARGET_VINTAGE"][0]
    assert pt["normalized_payload"]["wall_street_target_price"] == 321.66


def test_ws1_analyst_vintage_written_immutably_to_dated_dir():
    ctx, d = _ctx(_ANALYST_CFG, _analyst_routes(), env={"EODHD_API_KEY": "K" * 20})
    EodhdAnalystCollector(ctx).collect(AS_OF)
    vdir = d / "vintages" / "eodhd_analyst" / AS_OF
    files = sorted(p.name for p in vdir.glob("*.json"))
    assert files == ["AAPL.json", "JPM.json"]
    boundary = json.loads((d / "vintages" / "eodhd_analyst" /
                           "_prospective_boundary.json").read_text("utf-8"))
    assert boundary["first_snapshot_date"] == AS_OF
    assert boundary["backfill_before_floor_allowed"] is False


def test_ws1_same_day_second_run_is_idempotent():
    d = Path(tempfile.mkdtemp(prefix="pt-ws1-idem-"))
    ctx1, _ = _ctx(_ANALYST_CFG, _analyst_routes(), out_dir=d,
                   env={"EODHD_API_KEY": "K" * 20})
    r1 = EodhdAnalystCollector(ctx1).collect(AS_OF)
    assert len(r1["records"]) > 0
    vfile = d / "vintages" / "eodhd_analyst" / AS_OF / "AAPL.json"
    first_bytes = vfile.read_bytes()
    # Second same-day run: a fresh collector instance over the SAME output root.
    ctx2, _ = _ctx(_ANALYST_CFG, _analyst_routes(), out_dir=d,
                   env={"EODHD_API_KEY": "K" * 20})
    coll2 = EodhdAnalystCollector(ctx2)
    r2 = coll2.collect(AS_OF)
    assert len(r2["records"]) == 0, "second same-day run must emit no new records"
    assert r2["inventory"]["vintages_written"] == 0
    assert r2["inventory"]["vintages_idempotent_skipped"] == 2
    # No per-symbol fundamentals fetch on the idempotent run (only the 1 probe).
    fetches = [u for u in coll2.ctx.transport.calls if "fundamentals/" in u]
    assert len(fetches) == 1
    assert vfile.read_bytes() == first_bytes  # vintage unchanged (immutable)


def test_ws1_missing_credential_blocks_cleanly():
    ctx, _d = _ctx(_ANALYST_CFG, _analyst_routes(), env={})
    res = EodhdAnalystCollector(ctx).collect(AS_OF)
    assert res["health"]["overall_state"] == sc.SH_BLOCKED_CREDENTIAL
    assert res["records"] == []


def test_ws1_api_key_redacted_in_fingerprints():
    ctx, d = _ctx(_ANALYST_CFG, _analyst_routes(),
                  env={"EODHD_API_KEY": "SECRET_ANALYST_KEY_123456"})
    res = EodhdAnalystCollector(ctx).collect(AS_OF)
    blob = json.dumps(res, default=str)
    assert "SECRET_ANALYST_KEY_123456" not in blob
    for raw in res["raw_objects"]:
        assert "REDACTED" in raw["request_fingerprint"]
        assert "SECRET_ANALYST_KEY_123456" not in raw["request_fingerprint"]


# --------------------------------------------------------------------------- #
# WS2 — SEC Form 4 transaction-level XML extraction
# --------------------------------------------------------------------------- #
from paper_trader.alpha_agent.collectors.sec_edgar import (  # noqa: E402
    extract_item_202, extract_ownership_xml, parse_form4_xml,
)

_FORM4_XML = b"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2026-07-29</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001214128</rptOwnerCik>
      <rptOwnerName>COOK TIMOTHY D</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>0</isDirector>
      <isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
      <officerTitle>Chief Executive Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-07-29</value></transactionDate>
      <transactionCoding>
        <transactionFormType>4</transactionFormType>
        <transactionCode>S</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100000</value></transactionShares>
        <transactionPricePerShare><value>211.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>3200000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>"""


def test_ws2_parse_form4_xml_pure():
    p = parse_form4_xml(_FORM4_XML)
    assert p["document_type"] == "4"
    assert p["issuer"]["cik"] == "0000320193"
    assert p["issuer"]["trading_symbol"] == "AAPL"
    assert p["reporting_owner"]["is_officer"] is True
    assert p["reporting_owner"]["is_director"] is False
    assert p["reporting_owner"]["officer_title"] == "Chief Executive Officer"
    assert len(p["transactions"]) == 1
    t = p["transactions"][0]
    assert t["transaction_code"] == "S"
    assert t["acquired_disposed"] == "D"
    assert t["shares"] == "100000"
    assert t["price_per_share"] == "211.50"
    assert t["shares_owned_following"] == "3200000"
    assert t["direct_or_indirect"] == "D"
    assert t["is_derivative"] is False


def test_ws2_parse_non_ownership_returns_none():
    assert parse_form4_xml(b"<html><body>not xbrl</body></html>") is None


def test_ws2_extract_ownership_block_from_full_submission():
    wrapper = (b"-----BEGIN PRIVACY-ENHANCED MESSAGE-----\n<SEC-HEADER>junk"
               b"</SEC-HEADER>\n<DOCUMENT><TYPE>4<TEXT><XML>\n"
               + _FORM4_XML + b"\n</XML></TEXT></DOCUMENT>")
    block = extract_ownership_xml(wrapper)
    assert block is not None and block.startswith(b"<ownershipDocument")
    p = parse_form4_xml(wrapper)  # parser handles the full .txt wrapper directly
    assert p is not None and p["issuer"]["trading_symbol"] == "AAPL"


def test_ws2_form4_lane_emits_pit_transactions():
    ctx, _d = _ctx(
        {"enabled": True, "base_url_www": "https://www.sec.gov",
         "form4_xml_cap": 8},
        [(".txt", {"body": _FORM4_XML})])
    coll = SecEdgarCollector(ctx)
    coll._form4_candidates = [{
        "cik10": "0000320193", "ticker": "AAPL",
        "accession": "0000320193-26-000075", "primary_document": "form4.xml",
        "acceptance": "2026-07-29T18:30:00.000Z", "form": "4"}]
    coll._collect_form4_transactions({"User-Agent": "x"}, AS_OF,
                                     "2026-07-31T00:00:00")
    recs = coll.records
    assert len(recs) == 1
    r = recs[0]
    assert r["record_type"] == sc.RT_INSIDER_FILING
    # PIT availability is the SEC acceptance timestamp
    assert r["available_at"] == "2026-07-29T18:30:00.000Z"
    assert r["observed_at"] == "2026-07-29"
    pay = r["normalized_payload"]
    assert pay["transaction_code"] == "S"
    assert pay["acquired_disposed"] == "D"
    assert pay["is_officer"] is True
    assert pay["is_amendment"] is False
    assert coll.inventory["form4_transactions"] == 1
    assert coll.inventory["form4_distinct_issuers"] == 1


def test_ws2_form4_amendment_marked_and_not_overwriting():
    ctx, _d = _ctx({"enabled": True, "base_url_www": "https://www.sec.gov"},
                   [(".txt", {"body": _FORM4_XML})])
    coll = SecEdgarCollector(ctx)
    coll._form4_candidates = [{
        "cik10": "0000320193", "ticker": "AAPL",
        "accession": "0000320193-26-000099", "primary_document": "form4.xml",
        "acceptance": "2026-07-30T12:00:00.000Z", "form": "4/A"}]
    coll._collect_form4_transactions({"User-Agent": "x"}, AS_OF,
                                     "2026-07-31T00:00:00")
    assert coll.records[0]["normalized_payload"]["is_amendment"] is True
    assert "AMENDMENT" in coll.records[0]["provenance"]


# --------------------------------------------------------------------------- #
# WS3 — SEC 8-K Item 2.02 / EX-99 extraction
# --------------------------------------------------------------------------- #
_8K_TEXT = b"""<SEC-DOCUMENT>
Item 2.02. Results of Operations and Financial Condition.
On July 30, 2026, Apple Inc. reported diluted earnings of $2.51 per diluted share
and total net revenue of $102.5 billion for the quarter. The Company expects
continued momentum in the next quarter.
</SEC-DOCUMENT>"""


def test_ws3_extract_item_202_pure():
    info = extract_item_202(_8K_TEXT)
    assert info["has_item_202"] is True
    assert info["eps_actual"] == 2.51
    assert info["revenue_text"] is not None
    assert info["guidance_present"] is True


def test_ws3_extract_item_202_absent():
    info = extract_item_202(b"Item 5.02 Departure of Directors. No results here.")
    assert info["has_item_202"] is False
    assert info["eps_actual"] is None


def test_ws3_form8k_lane_emits_pit_earnings_event():
    ctx, _d = _ctx({"enabled": True, "base_url_www": "https://www.sec.gov",
                    "form8k_doc_cap": 6},
                   [(".txt", {"body": _8K_TEXT})])
    coll = SecEdgarCollector(ctx)
    coll._earn8k_candidates = [{
        "cik10": "0000320193", "ticker": "AAPL",
        "accession": "0000320193-26-000080", "primary_document": "aapl-8k.htm",
        "acceptance": "2026-07-30T16:35:00.000Z", "report_date": "2026-07-30",
        "form": "8-K"}]
    coll._collect_form8k_earnings({"User-Agent": "x"}, AS_OF,
                                  "2026-07-31T00:00:00")
    assert len(coll.records) == 1
    r = coll.records[0]
    assert r["record_type"] == sc.RT_EARNINGS_EVENT
    assert r["event_type"] == "8-K_ITEM_2.02"
    assert r["available_at"] == "2026-07-30T16:35:00.000Z"
    assert r["normalized_payload"]["eps_actual"] == 2.51
    assert coll.inventory["form8k_item202_hits"] == 1


# --------------------------------------------------------------------------- #
# WS4 — SEC bulk-lane HEAD probe honesty
# --------------------------------------------------------------------------- #
def test_ws4_bulk_probe_large_archive_is_precise_blocker():
    def _head(req):
        return {"status": 200, "headers": {"Content-Length": "1300000000"}}
    ctx, _d = _ctx({"enabled": True, "base_url_www": "https://www.sec.gov",
                    "bulk_manageable_bytes": 33554432,
                    "bulk_archives": {"companyfacts": "/x/companyfacts.zip"}},
                   [("companyfacts.zip", _head)])
    coll = SecEdgarCollector(ctx)
    coll._probe_bulk_archives({"User-Agent": "x"}, AS_OF)
    cf = coll.inventory["bulk_archive_probe"]["companyfacts"]
    assert cf["lane"] == "SEC_COMPANYFACTS_BULK"
    assert cf["content_length_bytes"] == 1300000000
    assert cf["manageable_within_cap"] is False
    assert cf["disposition"] == "OUT_OF_BAND_BULK_EXCEEDS_CAP"
    assert "1300000000 bytes" in cf["blocker"]
    assert "download" not in cf


def test_ws4_bulk_probe_small_archive_downloads_resumably():
    payload = b"PK" + b"z" * 500  # tiny fake zip
    calls = {"n": 0}

    def _route(req):
        if req.get("method") == "HEAD":
            return {"status": 200, "headers": {"Content-Length": str(len(payload))}}
        return {"status": 206, "headers": {}, "body": payload}
    ctx, d = _ctx({"enabled": True, "base_url_www": "https://www.sec.gov",
                   "bulk_manageable_bytes": 1 << 20, "bulk_chunk_bytes": 1 << 20,
                   "bulk_archives": {"submissions": "/x/submissions.zip"}},
                  [("submissions.zip", _route)])
    coll = SecEdgarCollector(ctx)
    coll._probe_bulk_archives({"User-Agent": "x"}, AS_OF)
    sub = coll.inventory["bulk_archive_probe"]["submissions"]
    assert sub["disposition"] == "RESUMABLE_DOWNLOAD"
    assert sub["download"]["bytes_downloaded"] == len(payload)
    assert sub["download"]["sha256"] is not None
    assert (d / "bulk" / "sec_edgar" / "submissions.zip").exists()
    assert (d / "bulk" / "sec_edgar" / "submissions.checkpoint.json").exists()


# --------------------------------------------------------------------------- #
# WS5 — point-in-time SIC sector classification
# --------------------------------------------------------------------------- #
from paper_trader.alpha_agent import pit_sector as ps  # noqa: E402
from paper_trader.alpha_agent.collectors.sec_edgar import (  # noqa: E402
    extract_assigned_sic,
)


def test_ws5_sic_to_sector_and_financials():
    assert ps.sic_to_sector("6021")["sector"] == "Financials"
    assert ps.is_financial_sic("6021") is True
    assert ps.is_financial_sic(3571) is False
    assert ps.sic_to_sector("3571")["sector"] == "Technology"
    assert ps.sic_to_sector(None)["sector"] == "Unknown"
    assert ps.sic_to_sector("6021")["mapping_version"] == ps.MAPPING_VERSION


def test_ws5_pit_series_no_lookahead():
    s = ps.PitSicSeries()
    # A name classified Industrials in 2018, reclassified Financials in 2024.
    s.add("AAA", sic="3559", available_at="2018-03-01")
    s.add("AAA", sic="6199", available_at="2024-05-01")
    # As of 2020 the ONLY known classification is the 2018 one — no look-ahead.
    assert s.sector_as_of("AAA", "2020-01-01") == "Industrials"
    assert s.is_financial_as_of("AAA", "2020-01-01") is False
    # As of 2025 the later reclassification is visible.
    assert s.is_financial_as_of("AAA", "2025-01-01") is True
    # Before any observation -> unknown (None), never assumed.
    assert s.is_financial_as_of("AAA", "2010-01-01") is None
    assert s.is_financial_as_of("ZZZ", "2025-01-01") is None


def test_ws5_extract_assigned_sic_from_header():
    body = (b"<SEC-HEADER>\nACCESSION NUMBER: 0000320193-26-000080\n"
            b"<FILER>\n<COMPANY-DATA>\nASSIGNED-SIC:\t\t\t3571\n</COMPANY-DATA>\n"
            b"</FILER>\n</SEC-HEADER>\n<DOCUMENT>...body...ASSIGNED-SIC 9999")
    assert extract_assigned_sic(body) == "3571"  # header only, not the body
    assert extract_assigned_sic(b"no sic here") is None


def test_ws5_extract_assigned_sic_modern_bracket_form():
    body = (b"<SEC-HEADER>\nSUBJECT COMPANY:\n\tCOMPANY DATA:\n"
            b"\t\tCOMPANY CONFORMED NAME:\t\t\tAPPLE INC\n"
            b"\t\tSTANDARD INDUSTRIAL CLASSIFICATION:\tELECTRONIC COMPUTERS [3571]\n"
            b"</SEC-HEADER>\n<DOCUMENT>body")
    assert extract_assigned_sic(body) == "3571"


def test_ws5_three_way_comparison_labels():
    symbols = ["AAA", "BBB", "CCC", "DDD"]
    current_gics = {"AAA": "Financials", "BBB": "Technology",
                    "CCC": "Financials", "DDD": "Energy"}
    pit = ps.PitSicSeries()
    pit.add("AAA", sic="6021", available_at="2020-01-01")   # financial (PIT)
    pit.add("BBB", sic="3571", available_at="2020-01-01")   # not financial
    # CCC/DDD have NO PIT observation -> not classified PIT
    cmp = ps.three_way_financials_comparison(
        symbols=symbols, current_gics=current_gics, pit_series=pit,
        as_of="2026-07-31")
    assert cmp["variants"]["full"]["universe"] == 4
    assert cmp["variants"]["current_gics"]["excluded"] == 2
    assert cmp["variants"]["current_gics"]["evidence_label"] == \
        "PROVISIONAL_CLASSIFICATION_LOOKAHEAD"
    assert cmp["variants"]["current_gics"]["leakage_safe"] is False
    pit_v = cmp["variants"]["pit_sic"]
    assert pit_v["classified"] == 2 and pit_v["excluded_financials"] == 1
    assert pit_v["leakage_safe"] is True
    assert pit_v["evidence_label"] == "LEAKAGE_SAFE_PIT_SIC_LOW_COVERAGE"


# --------------------------------------------------------------------------- #
# WS6 — BEA secure DPAPI credential resolution + no plaintext leakage
# --------------------------------------------------------------------------- #
from paper_trader.alpha_agent.collectors.bea import (  # noqa: E402
    BeaCollector, dpapi_unprotect,
)

_BEA_CFG = {
    "enabled": True, "base_url": "https://apps.bea.gov/api/data",
    "allowed_env_vars": ["BEA_API_KEY"],
    "tables": [{"dataset": "NIPA", "table_name": "T10101", "frequency": "Q",
                "year": "ALL", "macro_family": "national_accounts"}],
}


def test_ws6_dpapi_unprotect_never_raises_on_garbage():
    assert dpapi_unprotect("") is None
    assert dpapi_unprotect("not-hex-zzzz") is None
    assert dpapi_unprotect("deadbeef") is None  # valid hex, not a real DPAPI blob


def test_ws6_no_env_and_no_dpapi_file_blocks_cleanly():
    cfg = dict(_BEA_CFG,
               dpapi_credential_path=str(Path(tempfile.mkdtemp()) / "absent.dpapi"))
    ctx, _d = _ctx(cfg, [("apps.bea.gov", {"body": b"{}"})], env={})
    res = BeaCollector(ctx).collect(AS_OF)
    assert res["health"]["overall_state"] == sc.SH_BLOCKED_CONFIGURATION
    assert res["records"] == []


def test_ws6_bea_resolves_dpapi_path_from_config():
    coll = BeaCollector(_ctx(dict(_BEA_CFG, dpapi_credential_path="X:/nope.dpapi"),
                             [], env={})[0])
    assert coll._dpapi_path() == "X:/nope.dpapi"


def test_ws6_userid_redacted_when_key_present():
    _BEA_JSON = {"BEAAPI": {"Results": {"Data": [
        {"TimePeriod": "2025Q3", "DataValue": "3.0", "LineDescription": "GDP",
         "SeriesCode": "A191RL"}]}}}
    ctx, _d = _ctx(_BEA_CFG, [("apps.bea.gov", {"body": _body(_BEA_JSON)})],
                   env={"BEA_API_KEY": "SECRET_BEA_USERID_ABCDEF123456"})
    res = BeaCollector(ctx).collect(AS_OF)
    blob = json.dumps(res, default=str)
    assert "SECRET_BEA_USERID_ABCDEF123456" not in blob
    for raw in res["raw_objects"]:
        assert "SECRET_BEA_USERID_ABCDEF123456" not in raw["request_fingerprint"]
        assert "REDACTED" in raw["request_fingerprint"]
