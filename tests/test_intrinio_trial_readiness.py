"""
tests/test_intrinio_trial_readiness.py — Intrinio TRIAL readiness battery.

Covers the credential-gated Intrinio collector lane WITHOUT ever calling a real
API (every transport is an injected fake) and WITHOUT any real credential:

  * entitlement classification matrix (ACTIVE / AUTH_FAILURE / ENDPOINT_NOT_ENTITLED
    / RATE_LIMITED / NETWORK_FAILURE / NOT_PROVISIONED);
  * NOT_PROVISIONED makes ZERO network calls;
  * credential non-disclosure (key never in the serialized result; redacted from
    error text; never in any request URL — Bearer header only);
  * research-only enforcement (operational_use_allowed=True -> BLOCKED_LICENSE);
  * point-in-time discipline on collected records (filing date = availability,
    fiscal period_end NEVER substituted; Zacks current snapshot marked DATA_HOLD);
  * content-addressed immutable raw archive carries the TRIAL license note;
  * the canonical pure Data-Expansion gate returns INSUFFICIENT_EVIDENCE with no
    measured evidence; Stage-13A keeps the historical-Zacks question TRIAL_NOT_STARTED;
  * config + registry sanity, and the cadence-exclusion safety boundary.
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
from paper_trader.alpha_agent.collectors import COLLECTOR_CLASSES  # noqa: E402
from paper_trader.alpha_agent.collectors.base import (  # noqa: E402
    CollectorContext, RawArchive,
)
from paper_trader.alpha_agent.collectors import intrinio as ic  # noqa: E402
from paper_trader.alpha_agent.collectors.intrinio import IntrinioCollector  # noqa: E402
from paper_trader.engine import data_expansion_gate as gate  # noqa: E402

NOW = "2026-07-31T00:00:00"


class FakeTransport:
    def __init__(self, routes):
        self.routes = routes
        self.calls = []          # URLs
        self.headers = []        # header dicts

    def __call__(self, request, timeout):
        self.calls.append(request["url"])
        self.headers.append(dict(request.get("headers") or {}))
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


def _source_cfg(**over):
    cfg = {
        "source_id": "intrinio",
        "enabled": True,
        "requires_credential": True,
        "base_url": "https://api-v2.intrinio.com",
        "auth_mode": "bearer",
        "license_note": "Intrinio TRIAL - research-only; no redistribution; not operational.",
        "allowed_env_vars": ["INTRINIO_API_KEY", "PAPER_TRADER_INTRINIO_API_KEY"],
        # Isolate tests from the machine's REAL DPAPI credential file: once the
        # operator provisions the live trial, the default DPAPI fallback path
        # exists and "no env key" no longer means "no credential".
        "dpapi_credential_path": r"C:\nonexistent-paper-trader-test\intrinio_api_key.dpapi",
        "sample_symbols": ["AAPL"],
        "datasets": [
            {"dataset_key": "us_fundamentals", "data_category": "fundamentals",
             "data_expansion_dataset_id": "historical_pit_fundamentals_vendor",
             "entitlement_probe_path": "/companies?page_size=1",
             "sample_symbols": ["AAPL"],
             "fundamentals_path_template": "/companies/%s/fundamentals?page_size=4"},
            {"dataset_key": "zacks_estimates", "data_category": "analyst_revisions",
             "data_expansion_dataset_id": "analyst_estimate_revisions_history",
             "entitlement_probe_path": "/zacks/eps_estimates?page_size=1",
             "current_data_only": True, "sample_symbols": ["AAPL"],
             "estimates_path_template": "/zacks/eps_estimates?identifier=%s&page_size=8"},
        ],
    }
    cfg.update(over)
    return cfg


def _ctx(source_cfg, routes, *, env=None, secrets=None):
    d = Path(tempfile.mkdtemp(prefix="pt-intrinio-"))
    config = {
        "limits": {"max_retries": 0, "backoff_base_seconds": 0.0,
                   "backoff_multiplier": 1.0, "http_timeout_seconds": 5,
                   "raw_object_max_bytes": 1 << 22, "circuit_breaker_threshold": 5},
        "secret_redaction": {"redacted_query_params": ["api_key", "api_token"],
                             "redaction_placeholder": "REDACTED"},
    }
    archive = RawArchive(d / "raw", d, set(), 1 << 22)
    ft = FakeTransport(routes)
    ctx = CollectorContext(
        config=config, source_cfg=source_cfg, archive=archive, transport=ft,
        now_iso=lambda: NOW, clock=_clock(), sleep=lambda s: None,
        env=env or {}, user_agent="paper-trader-alpha-agent/2.0 ops@test.example",
        secrets=list(secrets or []), identity=sc.IdentityResolver())
    return ctx, ft, d


def _json_body(obj) -> bytes:
    return json.dumps(obj).encode("utf-8")


# --------------------------------------------------------------------------- #
# classify_probe pure matrix
# --------------------------------------------------------------------------- #
def test_classify_probe_matrix():
    assert ic.classify_probe(200, None, True) == ic.CLS_ACTIVE
    assert ic.classify_probe(200, None, False) == ic.CLS_UNKNOWN
    assert ic.classify_probe(401, None, False) == ic.CLS_AUTH_FAILURE
    assert ic.classify_probe(402, None, False) == ic.CLS_ENDPOINT_NOT_ENTITLED
    assert ic.classify_probe(403, None, False) == ic.CLS_ENDPOINT_NOT_ENTITLED
    assert ic.classify_probe(429, None, False) == ic.CLS_RATE_LIMITED
    assert ic.classify_probe(0, "TimeoutError: timed out", False) == ic.CLS_NETWORK_FAILURE
    assert ic.classify_probe(None, None, False) == ic.CLS_NETWORK_FAILURE
    assert ic.classify_probe(500, None, False) == ic.CLS_UNKNOWN


# --------------------------------------------------------------------------- #
# NOT_PROVISIONED — no credential, ZERO network calls
# --------------------------------------------------------------------------- #
def test_not_provisioned_zero_network():
    ctx, ft, _ = _ctx(_source_cfg(), routes=[], env={})   # no key in env
    res = IntrinioCollector(ctx).audit()
    assert ft.calls == []                                  # NO network I/O
    assert res["health"]["overall_state"] == sc.SH_BLOCKED_CONFIGURATION
    assert res["health"]["credential_present"] is False
    cls = res["inventory"]["entitlement_classification"]
    assert cls == {"us_fundamentals": ic.CLS_NOT_PROVISIONED,
                   "zacks_estimates": ic.CLS_NOT_PROVISIONED}
    assert res["inventory"]["trial_provisioned"] is False


# --------------------------------------------------------------------------- #
# Entitlement classification matrix through audit()
# --------------------------------------------------------------------------- #
def _audit_with_status(status, *, body=b"{}", error=None):
    resp = {"status": status, "body": body, "error": error}
    routes = [("companies", resp), ("zacks", resp)]
    ctx, ft, _ = _ctx(_source_cfg(), routes, env={"INTRINIO_API_KEY": "TRIALKEY_abcdef0123456789"})
    res = IntrinioCollector(ctx).audit()
    return res, ft


def test_audit_active():
    res, ft = _audit_with_status(200, body=_json_body({"companies": [{"id": "x"}]}))
    assert len(ft.calls) == 2
    cls = res["inventory"]["entitlement_classification"]
    assert cls["us_fundamentals"] == ic.CLS_ACTIVE
    assert cls["zacks_estimates"] == ic.CLS_ACTIVE
    assert res["health"]["overall_state"] == sc.SH_HEALTHY
    assert res["inventory"]["trial_provisioned"] is True


def test_audit_auth_failure():
    res, _ = _audit_with_status(401)
    cls = res["inventory"]["entitlement_classification"]
    assert cls["us_fundamentals"] == ic.CLS_AUTH_FAILURE
    assert res["health"]["overall_state"] == sc.SH_FAILED


def test_audit_endpoint_not_entitled():
    res, _ = _audit_with_status(403)
    cls = res["inventory"]["entitlement_classification"]
    assert cls["us_fundamentals"] == ic.CLS_ENDPOINT_NOT_ENTITLED
    assert cls["zacks_estimates"] == ic.CLS_ENDPOINT_NOT_ENTITLED


def test_audit_rate_limited():
    res, _ = _audit_with_status(429)
    cls = res["inventory"]["entitlement_classification"]
    assert cls["us_fundamentals"] == ic.CLS_RATE_LIMITED


def test_audit_network_failure():
    res, _ = _audit_with_status(0, body=b"", error="URLError: connection refused")
    cls = res["inventory"]["entitlement_classification"]
    assert cls["us_fundamentals"] == ic.CLS_NETWORK_FAILURE
    assert cls["zacks_estimates"] == ic.CLS_NETWORK_FAILURE


# --------------------------------------------------------------------------- #
# Credential non-disclosure
# --------------------------------------------------------------------------- #
def test_key_never_disclosed_and_bearer_header_only():
    key = "TRIALKEY_abcdef0123456789"
    resp = {"status": 200, "body": _json_body({"companies": [{"id": "x"}]})}
    routes = [("companies", resp), ("zacks", resp)]
    ctx, ft, _ = _ctx(_source_cfg(), routes, env={"INTRINIO_API_KEY": key})
    res = IntrinioCollector(ctx).audit()
    blob = json.dumps(res)
    assert key not in blob                                  # never in serialized result
    assert all(key not in url for url in ft.calls)          # never in any URL
    # authenticated exclusively via the Authorization: Bearer header
    assert any(h.get("Authorization") == "Bearer %s" % key for h in ft.headers)
    assert key in ctx.secrets                               # registered for redaction


def test_key_redacted_from_error_text():
    key = "TRIALKEY_abcdef0123456789"
    # Transport returns a network error whose text embeds the key.
    resp = {"status": 0, "body": b"", "error": "boom with " + key + " leaked"}
    routes = [("companies", resp), ("zacks", resp)]
    ctx, ft, _ = _ctx(_source_cfg(), routes, env={"INTRINIO_API_KEY": key})
    res = IntrinioCollector(ctx).audit()
    blob = json.dumps(res)
    assert key not in blob
    assert "REDACTED" in blob or "***" in blob


# --------------------------------------------------------------------------- #
# Research-only enforcement
# --------------------------------------------------------------------------- #
def test_operational_use_refused():
    ctx, ft, _ = _ctx(_source_cfg(operational_use_allowed=True), routes=[],
                      env={"INTRINIO_API_KEY": "TRIALKEY_abcdef0123456789"})
    res = IntrinioCollector(ctx).audit()
    assert res["health"]["overall_state"] == sc.SH_BLOCKED_LICENSE
    assert ft.calls == []


# --------------------------------------------------------------------------- #
# Collect — PIT discipline + TRIAL provenance + Zacks DATA_HOLD marking
# --------------------------------------------------------------------------- #
def _collect_ctx():
    fund_body = _json_body({"fundamentals": [
        {"end_date": "2026-03-31", "filing_date": "2026-05-01", "total_revenue": 1000}]})
    zacks_body = _json_body({"estimates": [
        {"fiscal_period_end": "2026-06-30", "mean": 1.23, "count": 30}]})
    routes = [
        ("/companies/AAPL/fundamentals", {"status": 200, "body": fund_body}),
        ("companies?page_size=1", {"status": 200, "body": _json_body({"companies": [{"id": "x"}]})}),
        ("/zacks/eps_estimates?identifier=AAPL", {"status": 200, "body": zacks_body}),
        ("zacks/eps_estimates?page_size=1", {"status": 200, "body": _json_body({"estimates": [{}]})}),
    ]
    return _ctx(_source_cfg(), routes, env={"INTRINIO_API_KEY": "TRIALKEY_abcdef0123456789"})


def test_collect_pit_and_trial_marking():
    ctx, ft, root = _collect_ctx()
    col = IntrinioCollector(ctx)
    res = col.collect("2026-07-31")
    recs = res["records"]
    funds = [r for r in recs if r["record_type"] == sc.RT_FUNDAMENTAL_FACT]
    zacks = [r for r in recs if r["record_type"] == sc.RT_EARNINGS_EVENT]
    assert funds and zacks
    f = funds[0]
    # filing date is availability; fiscal period_end is NEVER substituted for it.
    assert f["available_at"] == "2026-05-01"
    assert f["effective_at"] == "2026-03-31"
    assert f["available_at"] != f["effective_at"]
    z = zacks[0]
    # Current snapshot: availability is the retrieval time (a real forward obs),
    # and it is explicitly marked NOT a historical PIT revision.
    assert z["available_at"] == NOW
    assert z["event_type"] == "ANALYST_ESTIMATE_CURRENT_CONSENSUS"
    assert any("CURRENT_CONSENSUS_SNAPSHOT" in w for w in z["quality_warnings"])
    # Raw archive carries the TRIAL license note (content-addressed provenance).
    metas = list(root.rglob("*.metadata.json"))
    assert metas
    note_seen = any("TRIAL" in json.loads(m.read_text())["license_note"] for m in metas)
    assert note_seen


def test_collect_missing_filing_date_leaves_availability_null():
    fund_body = _json_body({"fundamentals": [{"end_date": "2026-03-31", "total_revenue": 5}]})
    routes = [("/companies/AAPL/fundamentals", {"status": 200, "body": fund_body}),
              ("companies?page_size=1", {"status": 200, "body": _json_body({"companies": [{}]})}),
              ("zacks", {"status": 403, "body": b"{}"})]
    ctx, ft, _ = _ctx(_source_cfg(), routes, env={"INTRINIO_API_KEY": "TRIALKEY_abcdef0123456789"})
    res = IntrinioCollector(ctx).collect("2026-07-31")
    funds = [r for r in res["records"] if r["record_type"] == sc.RT_FUNDAMENTAL_FACT]
    assert funds
    f = funds[0]
    assert f["available_at"] is None
    assert f["effective_at"] == "2026-03-31"
    assert any("AVAILABILITY_UNKNOWN" in w for w in f["quality_warnings"])


# --------------------------------------------------------------------------- #
# Phase-8 survivorship/identity readiness: unknown identity stays UNKNOWN.
# The trial has no live Intrinio identifiers yet, so the collector must NOT
# fabricate an owned identifier mapping (Norgate assetid / SEC CIK / security_id).
# Records are emitted UNMATCHED with null owned identifiers — mapping is deferred
# to the canonical IdentityResolver at live-trial time, never invented here.
# --------------------------------------------------------------------------- #
def test_identity_unknown_stays_unknown_not_fabricated():
    ctx, ft, _ = _collect_ctx()
    res = IntrinioCollector(ctx).collect("2026-07-31")
    recs = res["records"]
    assert recs
    for r in recs:
        assert r["entity_mapping_confidence"] == sc.EM_UNMATCHED
        assert r["security_id"] is None
        assert r["company_id"] is None


# --------------------------------------------------------------------------- #
# Canonical gate + Stage-13A honesty
# --------------------------------------------------------------------------- #
def test_gate_insufficient_evidence_without_measured_evidence():
    for did, cat, current in (("historical_pit_fundamentals_vendor", "fundamentals", False),
                              ("analyst_estimate_revisions_history", "analyst_revisions", True)):
        ds_meta = {"pit_guarantee": "CURRENT_ONLY"} if current else {}
        ic_input = {
            "dataset_id": did, "provider": "Intrinio", "data_category": cat,
            "dataset": ds_meta,
            "research_requirements": {"requires_point_in_time": not current},
            "evidence": {"available": False},
            "existing_ownership": {"entitlement_state": "NOT_ENTITLED"},
        }
        res = gate.evaluate_dataset(input_contract=ic_input)
        assert res["recommendation_state"] == gate.REC_INSUFFICIENT


def test_stage13a_historical_zacks_trial_not_started():
    from paper_trader.alpha_agent import analyst_revisions as ar
    dec = ar.purchase_decision(trial_started=False, adequacy={}, power={},
                               integrity={}, cost={}, value_assumptions={})
    assert dec["purchase_terminal"] == ar.PT_TRIAL_NOT_STARTED
    assert dec["manual_approval_required"] is True
    assert dec["auto_purchase"] is False


# --------------------------------------------------------------------------- #
# Config + registry + cadence-exclusion safety
# --------------------------------------------------------------------------- #
def test_trial_config_is_research_only():
    p = _REPO / "configs" / "alpha_agent" / "intrinio_trial.json"
    cfg = json.loads(p.read_text(encoding="utf-8"))
    # Phase-4 canonical trial-licensing state must be represented EXPLICITLY: no
    # trial data may silently become operational / portfolio / model-promotion data.
    assert cfg["provider"] == "Intrinio"
    assert cfg["license_state"] == "TRIAL"
    assert cfg["research_use_only"] is True
    assert cfg["operational_use_allowed"] is False
    assert cfg["portfolio_use_allowed"] is False
    assert cfg["model_promotion_allowed"] is False
    assert "provider_trials\\intrinio" in cfg["research_root"]
    ids = {d["data_expansion_dataset_id"] for d in cfg["source"]["datasets"]}
    assert ids == {"historical_pit_fundamentals_vendor", "analyst_estimate_revisions_history"}
    zacks = [d for d in cfg["source"]["datasets"] if d["dataset_key"] == "zacks_estimates"][0]
    assert zacks["current_data_only"] is True
    assert zacks["historical_revision_alpha_supported"] is False


def test_collector_registered():
    assert "intrinio" in COLLECTOR_CLASSES
    assert COLLECTOR_CLASSES["intrinio"] is IntrinioCollector


def test_intrinio_not_wired_into_collect_cadence():
    """Safety: the Intrinio TRIAL source must NOT be an enabled source in the
    scheduled Stage-2 ingestion config, so the Collect cadence never runs it."""
    p = _REPO / "configs" / "alpha_agent" / "stage2_ingestion.json"
    cfg = json.loads(p.read_text(encoding="utf-8"))
    sources = cfg.get("sources") or {}
    assert "intrinio" not in sources
