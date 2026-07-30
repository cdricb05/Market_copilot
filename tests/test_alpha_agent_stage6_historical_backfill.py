"""
Deterministic Stage 6 historical-backfill tests. Fakes only — no norgatedata,
no network, no DB, no LLM, no email, no scheduled task. Every provider
interaction is an injected fake; the operational-ledger immutability guard runs
against a temporary ledger directory.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import backfill_contracts as bc  # noqa: E402
from paper_trader.alpha_agent import historical_backfill as hb  # noqa: E402
from paper_trader.alpha_agent import experiment_contracts as ec  # noqa: E402
from paper_trader.alpha_agent import experiment_factory as ef  # noqa: E402
from paper_trader.alpha_agent import report_renderer as rr  # noqa: E402
from paper_trader.alpha_agent.source_contracts import (  # noqa: E402
    EM_MATCHED_EXACT, RT_CORPORATE_ACTION, RT_MARKET_BAR, RT_SECURITY_IDENTITY,
    RT_UNIVERSE_MEMBERSHIP, build_normalized_record)


# --------------------------------------------------------------------------- #
# Fakes.
# --------------------------------------------------------------------------- #
def _day(i: int) -> str:
    """Deterministic sortable synthetic trading-day date (skips weekends
    loosely; only needs to be monotonic + ~252/yr for month spans)."""
    import datetime as dt
    base = dt.date(2016, 1, 4)
    d = base + dt.timedelta(days=int(i * 7 / 5))  # ~5 trading days per 7 cal
    return d.isoformat()


class _FakeNorgate:
    """Injected Norgate adapter: local, deterministic, survivorship-aware."""

    def __init__(self, *, symbols: list[str], current: list[str], n_days: int,
                 available: bool = True):
        self._symbols = list(symbols)
        self._current = set(current)
        self._n_days = n_days
        self._available = available
        self.errors: list[dict] = []

    def available(self):
        return (self._available,
                "fake NDU running" if self._available else "fake NDU down")

    def resolve_universe(self, watchlist, cap, current_watchlist=None):
        full = sorted(set(self._symbols))
        if len(full) <= cap:
            return full, len(full), 0
        keep = sorted(s for s in full if s in self._current)
        past = sorted(s for s in full if s not in self._current)
        room = max(0, cap - len(keep))
        sel = sorted(set(keep) | set(past[:room]))
        return sel, len(full), len(full) - len(sel)

    def fetch_and_normalize(self, symbol, *, start, end, as_of, retrieved,
                            index_name, archive, families):
        idx = self._symbols.index(symbol) if symbol in self._symbols else 0
        mu = 0.0002 + 0.00003 * idx
        price = 10.0 + idx
        records = []
        raw = archive.store(
            source_id="norgate_local",
            content=json.dumps({"s": symbol, "n": self._n_days}).encode("utf-8"),
            extension="json", retrieved_at=retrieved, business_date=as_of,
            source_native_id="fake|%s" % symbol, request_fp="LOCAL fake",
            content_type="application/json", http_status=None, retry_count=0,
            published_at=None, license_note="fake")
        raw_id = raw["raw_object_id"]
        first_date = None
        last_date = None
        for i in range(self._n_days):
            eps = 0.00005 * (((idx * 7 + i * 13) % 11) - 5)
            price *= (1.0 + mu + eps)
            d = _day(i)
            if first_date is None:
                first_date = d
            last_date = d
            if "prices" in families:
                records.append(build_normalized_record(
                    record_type=RT_MARKET_BAR, source_id="norgate_local",
                    source_native_id="%s|%s" % (symbol, d), raw_object_id=raw_id,
                    retrieved_at=retrieved, observed_at=d, effective_at=d,
                    available_at=None, ticker=symbol, security_id="A%d" % idx,
                    event_type="EOD_BAR",
                    payload={"Date": d, "Close": round(price, 4),
                             "Open": round(price, 4), "High": round(price, 4),
                             "Low": round(price, 4), "Volume": 1000 + i,
                             "Turnover": 1.0, "Unadjusted Close": round(price, 4),
                             "Dividend": 0.0, "adjustment_requested": "TOTALRETURN"},
                    entity_mapping_confidence=EM_MATCHED_EXACT,
                    provenance="fake norgate bars"))
        if "security_identity" in families:
            records.append(build_normalized_record(
                record_type=RT_SECURITY_IDENTITY, source_id="norgate_local",
                source_native_id="identity|%s" % symbol, raw_object_id=raw_id,
                retrieved_at=retrieved, observed_at=retrieved, effective_at=as_of,
                available_at=retrieved, ticker=symbol, security_id="A%d" % idx,
                event_type="IDENTITY_SNAPSHOT",
                payload={"assetid": idx, "gics_sector": "Tech" if idx % 2
                         else "Energy"},
                entity_mapping_confidence=EM_MATCHED_EXACT,
                provenance="fake identity"))
        if "universe_membership" in families and first_date:
            records.append(build_normalized_record(
                record_type=RT_UNIVERSE_MEMBERSHIP, source_id="norgate_local",
                source_native_id="member|%s|%s" % (symbol, first_date),
                raw_object_id=raw_id, retrieved_at=retrieved,
                observed_at=first_date, effective_at=first_date,
                available_at=None, ticker=symbol,
                event_type="INDEX_MEMBERSHIP_SPAN",
                payload={"index_name": index_name, "member_from": first_date,
                         "member_to": None if symbol in self._current
                         else last_date},
                entity_mapping_confidence=EM_MATCHED_EXACT,
                provenance="fake membership"))
        fam_counts: dict = {}
        for r in records:
            fam = hb._RT_TO_FAMILY.get(r["record_type"], r["record_type"])
            fam_counts[fam] = fam_counts.get(fam, 0) + 1
        return {"records": records, "families": fam_counts, "no_data": False,
                "raw_new": 0 if raw["duplicate"] else 1}


def _write_fake_registry(root: Path) -> None:
    run = root / "runs" / "stage1_fake"
    run.mkdir(parents=True, exist_ok=True)
    (root / "latest.json").write_text(json.dumps(
        {"run_id": "stage1_fake", "run_dir": "runs/stage1_fake",
         "champion_model": "fundamental_momentum_50_50_v1"}), encoding="utf-8")
    rows = [
        {"information_family": "price_momentum", "hypotheses": "0",
         "unique_experiments": "59", "evidence_classification":
         "TESTED_INCONCLUSIVE", "required_missing_data": ""},
        {"information_family": "sector_exposure", "hypotheses": "2",
         "unique_experiments": "0", "evidence_classification":
         "PROVEN_DISTINCT", "required_missing_data": "mixed"},
        {"information_family": "value", "hypotheses": "3",
         "unique_experiments": "0", "evidence_classification": "NOT_YET_TESTED",
         "required_missing_data": ""},
        {"information_family": "macro_and_regime", "hypotheses": "3",
         "unique_experiments": "1", "evidence_classification": "NOT_YET_TESTED",
         "required_missing_data": ""},
        {"information_family": "earnings_surprise", "hypotheses": "14",
         "unique_experiments": "1", "evidence_classification": "REJECT_FAMILY",
         "required_missing_data": ""},
    ]
    cols = ["information_family", "hypotheses", "unique_experiments",
            "evidence_classification", "required_missing_data"]
    with (run / "research_coverage_map.csv").open("w", encoding="utf-8",
                                                  newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _base_cfg(tmp: Path, *, cap: int = 40, relaxed: bool = True) -> dict:
    ledger = tmp / "ledger"
    (ledger).mkdir(parents=True, exist_ok=True)
    (ledger / "book.json").write_text('{"nav": 100000}', encoding="utf-8")
    cfg = {
        "stage": "6",
        "backfill_root": str(tmp / "backfill"),
        "ingestion_root": str(tmp / "ingestion"),
        "stage1_registry_root": str(tmp / "registry"),
        "reopened_hypotheses_output": str(tmp / "backfill" /
                                          "reopened_hypotheses.jsonl"),
        "operational_ledger_roots": [str(ledger)],
        "universe": {"survivorship_watchlist": "S&P 500 Current & Past",
                     "current_watchlist": "S&P 500", "index_name": "S&P 500"},
        "date_range": {"start": "2016-01-01", "end": "latest"},
        "batch": {"ticker_batch_size": 10, "max_universe_symbols": cap},
        "providers": {
            "norgate_local": {"enabled": True, "license_note": "fake",
                              "families": ["prices", "universe_membership",
                                           "security_identity",
                                           "corporate_actions"]},
            "eodhd": {"enabled": True,
                      "families": ["fundamentals", "earnings", "insider", "news"],
                      "gap_detail": "no PIT history"}},
        "readiness": {},
        "reopen": {"max_candidates": 3},
    }
    if relaxed:
        cfg["readiness"] = {
            "price_momentum": {"min_years": 1.0, "min_pit_members": 20,
                               "min_monthly_periods": 12}}
    _write_fake_registry(tmp / "registry")
    return cfg


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("stage6")
    cfg = _base_cfg(tmp, cap=40)
    symbols = ["SYM%02d" % i for i in range(40)]
    current = symbols[:34]  # 6 delisted/past
    adapter = _FakeNorgate(symbols=symbols, current=current, n_days=900)
    result = hb.run_backfill(
        cfg, mode="backfill", as_of="2026-07-29",
        now_iso="2026-07-29T00:00:00+00:00", adapter=adapter,
        ledger_fingerprint=lambda: hb_ledger_fp(cfg))
    return {"tmp": tmp, "cfg": cfg, "result": result, "symbols": symbols,
            "current": current}


def hb_ledger_fp(cfg):
    out = {}
    for root in cfg.get("operational_ledger_roots", []):
        p = Path(root)
        if p.exists():
            for f in sorted(p.rglob("*")):
                if f.is_file():
                    out[str(f)] = _sha(f)
    return out


def _sha(path: Path) -> str:
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


# --------------------------------------------------------------------------- #
# Contract / config tests.
# --------------------------------------------------------------------------- #
def test_shipped_config_has_no_secret():
    cfg = bc.load_config(_REPO / "configs" / "alpha_agent" /
                         "stage6_historical_backfill.json")
    assert bc.scan_for_secrets(cfg) == []
    assert str(cfg["stage"]) == "6"


def test_config_missing_key_rejected(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"stage": "6"}), encoding="utf-8")
    with pytest.raises(bc.ConfigError):
        bc.load_config(p)


def test_config_embedded_secret_rejected(tmp_path):
    cfg = _base_cfg(tmp_path)
    cfg["api_token"] = "sk-ant-abcdef1234567890"
    p = tmp_path / "s.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    with pytest.raises(bc.ConfigError):
        bc.load_config(p)


def test_deterministic_ids():
    a = bc.stage6_run_id("2026-07-29", "cfg", "univ")
    b = bc.stage6_run_id("2026-07-29", "cfg", "univ")
    assert a == b and a.startswith("stage6_")
    assert bc.stage6_run_id("2026-07-30", "cfg", "univ") != a
    t1 = bc.ticker_batch_id("r", "norgate_local", ["B", "A"], "s", "e")
    t2 = bc.ticker_batch_id("r", "norgate_local", ["A", "B"], "s", "e")
    assert t1 == t2  # order-independent
    assert bc.universe_fingerprint(["A", "B"]) == \
        bc.universe_fingerprint(["B", "A"])


def test_no_forbidden_operational_tokens_in_source():
    for mod in ("historical_backfill.py", "backfill_contracts.py"):
        text = (_REPO / "alpha_agent" / mod).read_text(encoding="utf-8").lower()
        for tok in ("psycopg", "postgresql://", "127.0.0.1:9000", ":9000/",
                    "create_order", "submit_order", "create_signal",
                    "create_fill", "promote_model", "daily_close(",
                    "import anthropic", "openai"):
            assert tok not in text, "%s leaked %r" % (mod, tok)


# --------------------------------------------------------------------------- #
# Readiness-gate unit tests (strict thresholds).
# --------------------------------------------------------------------------- #
def test_price_readiness_strict_gate():
    gate = bc.READINESS_GATES["price_momentum"]
    good = {"prices": {"min_effective_date": "2015-01-02",
                       "max_effective_date": "2026-07-29",
                       "unique_tickers": 500, "unique_months": 138,
                       "survivorship_safe": True}}
    res = bc.evaluate_readiness(gate, good)
    assert res["usable"] and "price_momentum_rank" in res["unlocks"]
    short = {"prices": dict(good["prices"], max_effective_date="2018-01-02",
                            unique_months=24)}
    assert not bc.evaluate_readiness(gate, short)["usable"]
    thin = {"prices": dict(good["prices"], unique_tickers=100)}
    assert not bc.evaluate_readiness(gate, thin)["usable"]


def test_fundamental_readiness_requires_availability_timestamps():
    gate = bc.READINESS_GATES["fundamental_momentum"]
    cov = {"prices": {"min_effective_date": "2015-01-02",
                      "max_effective_date": "2026-07-29", "unique_tickers": 500,
                      "unique_months": 138, "survivorship_safe": True},
           "fundamentals": {"unique_quarters": 40, "unique_tickers": 400,
                            "record_count": 5000,
                            "has_availability_timestamps": False}}
    res = bc.evaluate_readiness(gate, cov)
    assert not res["usable"]
    assert "availability" in res["blocker"].lower()
    cov["fundamentals"]["has_availability_timestamps"] = True
    assert bc.evaluate_readiness(gate, cov)["usable"]


def test_event_readiness_gates_unmet_without_events():
    for name in ("earnings_drift", "insider_event", "news_event"):
        gate = bc.READINESS_GATES[name]
        cov = {"prices": {"min_effective_date": "2015-01-02",
                          "max_effective_date": "2026-07-29",
                          "unique_tickers": 500, "unique_months": 138,
                          "survivorship_safe": True}}
        for fam in gate["families"]:
            cov.setdefault(fam, {"record_count": 0})
        assert not bc.evaluate_readiness(gate, cov)["usable"]


# --------------------------------------------------------------------------- #
# Reopen-lane unit tests.
# --------------------------------------------------------------------------- #
def _cov_rows():
    return [
        {"information_family": "price_momentum", "hypotheses": "0",
         "unique_experiments": "59",
         "evidence_classification": "TESTED_INCONCLUSIVE",
         "required_missing_data": ""},
        {"information_family": "value", "hypotheses": "3",
         "unique_experiments": "0", "evidence_classification": "NOT_YET_TESTED",
         "required_missing_data": ""},
        {"information_family": "macro_and_regime", "hypotheses": "3",
         "unique_experiments": "0", "evidence_classification": "NOT_YET_TESTED",
         "required_missing_data": ""},
        {"information_family": "earnings_surprise", "hypotheses": "14",
         "unique_experiments": "1", "evidence_classification": "REJECT_FAMILY",
         "required_missing_data": ""},
    ]


def test_reopen_new_data_when_template_unlocked():
    out = hb.compute_reopen_candidates(
        _cov_rows(), unlocked_templates={"price_momentum_rank"},
        data_version="dv1", prior_data_versions=set(), max_candidates=3)
    by_fam = {r["information_family"]: r for r in out}
    assert by_fam["price_momentum"]["disposition"] == bc.REOPEN_NEW_DATA
    # value maps to a locked template -> still held.
    assert by_fam["value"]["disposition"] == bc.REOPEN_STILL_HELD
    # macro maps to no template -> unsupported.
    assert by_fam["macro_and_regime"]["disposition"] == \
        bc.REOPEN_REJECT_UNSUPPORTED
    # conclusively rejected family is never reopened.
    assert "earnings_surprise" not in by_fam


def test_reopen_exact_duplicate_rejected():
    out = hb.compute_reopen_candidates(
        _cov_rows(), unlocked_templates={"price_momentum_rank"},
        data_version="dvX", prior_data_versions={"dvX"}, max_candidates=3)
    by_fam = {r["information_family"]: r for r in out}
    assert by_fam["price_momentum"]["disposition"] == \
        bc.REOPEN_REJECT_DUPLICATE


def test_reopen_capped_at_three():
    rows = [{"information_family": fam, "hypotheses": "1",
             "unique_experiments": "5",
             "evidence_classification": "TESTED_INCONCLUSIVE",
             "required_missing_data": ""}
            for fam in ("price_momentum", "sector_exposure",
                        "short_term_reversal", "long_term_reversal")]
    unlocked = {"price_momentum_rank", "sector_relative_ranking"}
    out = hb.compute_reopen_candidates(
        rows, unlocked_templates=unlocked, data_version="dv",
        prior_data_versions=set(), max_candidates=3)
    new_data = [r for r in out if r["disposition"] == bc.REOPEN_NEW_DATA]
    assert len(new_data) <= 3


def test_reopened_intake_only_for_new_data():
    cand = [{"disposition": bc.REOPEN_NEW_DATA, "hypothesis_id": "hyp_reopen_x",
             "information_family": "price_momentum",
             "statement": "momentum", "prior_classification": "TESTED_INCONCLUSIVE"},
            {"disposition": bc.REOPEN_STILL_HELD, "hypothesis_id": "hyp_reopen_y",
             "information_family": "value", "statement": "value"}]
    intake = hb.reopened_intake_dicts(cand)
    assert len(intake) == 1
    assert intake[0]["hypothesis_id"] == "hyp_reopen_x"
    assert intake[0]["grounding"] == "GROUNDED"


# --------------------------------------------------------------------------- #
# Integration tests over the module-scoped backfill.
# --------------------------------------------------------------------------- #
def test_backfill_terminal_and_records(built):
    r = built["result"]
    assert r["terminal"] in (bc.READY, bc.PARTIAL)
    assert r["records_written"] > 0
    assert r["ledgers_unchanged"] is True


def test_price_momentum_unlocked(built):
    assert "price_momentum_rank" in built["result"]["templates_unlocked"]
    assert built["result"]["readiness"]["price_momentum"]["usable"] is True


def test_survivorship_universe_keeps_all_current(built):
    # cap 40 == full 40 here, so nothing dropped; current all present.
    r = built["result"]
    assert r["universe_size"] >= 34
    assert r["universe_dropped"] == 0


def test_delisted_bounded_drop_reported():
    # A smaller cap forces a bounded drop that keeps every current member.
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    cfg = _base_cfg(tmp, cap=20)
    symbols = ["SYM%02d" % i for i in range(40)]
    current = symbols[:15]
    adapter = _FakeNorgate(symbols=symbols, current=current, n_days=40)
    r = hb.run_backfill(cfg, mode="backfill", as_of="2026-07-29",
                        now_iso="2026-07-29T00:00:00+00:00", adapter=adapter,
                        ledger_fingerprint=lambda: {})
    assert r["universe_size"] == 20
    assert r["universe_dropped"] == 20  # reported, not silent
    # every current member kept (no current-constituent substitution).
    sel = set()
    for row in hb._read_jsonl(Path(r["run_dir"]) / "reopened_hypotheses.jsonl"):
        pass
    # verify current kept via coverage tickers >= current count
    prices = next(x for x in r["coverage_after"] if x["family"] == "prices")
    assert prices["unique_tickers"] >= len(current)


def test_point_in_time_audit_no_fabrication(built):
    audit = {a["family"]: a for a in built["result"]["pit_audit"]}
    prices = audit["prices"]
    assert prices["has_availability_timestamps"] is False
    assert prices["null_available_at_count"] == prices["record_count"]
    assert prices["period_end_substituted_for_availability"] is False
    assert prices["verdict"] == "OK_NULL_BY_DESIGN"


def test_coverage_before_after_reconcile(built):
    before = {r["family"]: r for r in built["result"]["coverage_before"]}
    after = {r["family"]: r for r in built["result"]["coverage_after"]}
    assert after["prices"]["record_count"] >= before["prices"]["record_count"]
    delta = {d["family"]: d for d in built["result"]["coverage_delta"]}
    assert delta["prices"]["record_count_added"] == \
        after["prices"]["record_count"] - before["prices"]["record_count"]


def test_priority2_gaps_recorded(built):
    gaps = {(g["provider"], g["family"]) for g in built["result"]["data_gaps"]}
    assert ("eodhd", "fundamentals") in gaps
    assert built["result"]["entitlements"]  # entitlement rows recorded


def test_required_output_files_and_manifest(built):
    run_dir = Path(built["result"]["run_dir"])
    for name in bc.REQUIRED_RUN_FILES:
        assert (run_dir / name).exists(), "missing %s" % name
    manifest = json.loads((run_dir / "run_manifest.json").read_text("utf-8"))
    for name, want in manifest["file_hashes"].items():
        assert hb._sha256_file(run_dir / name) == want


def test_reopen_price_momentum_new_data(built):
    reopened = hb._read_jsonl(Path(built["result"]["run_dir"]) /
                              "reopened_hypotheses.jsonl")
    by_fam = {r["information_family"]: r for r in reopened}
    assert by_fam["price_momentum"]["disposition"] == bc.REOPEN_NEW_DATA
    intake = hb._read_jsonl(Path(built["cfg"]["reopened_hypotheses_output"]))
    assert any(i["hypothesis_id"] == "hyp_reopen_price_momentum" for i in intake)


def test_supplier_hypothesis_stays_held(built):
    act = json.loads((Path(built["result"]["run_dir"]) /
                      "experiment_activation.json").read_text("utf-8"))
    assert "DATA_HOLD" in act["supplier_hypothesis_status"]


def test_idempotent_replay(built):
    cfg = built["cfg"]
    symbols = built["symbols"]
    adapter = _FakeNorgate(symbols=symbols, current=built["current"],
                           n_days=900)
    again = hb.run_backfill(cfg, mode="backfill", as_of="2026-07-29",
                            now_iso="2026-07-29T00:00:00+00:00", adapter=adapter,
                            ledger_fingerprint=lambda: {})
    assert again.get("idempotent_replay") is True
    assert again["run_id"] == built["result"]["run_id"]


def test_dry_run_writes_nothing_to_ingestion(tmp_path):
    cfg = _base_cfg(tmp_path, cap=10)
    symbols = ["SYM%02d" % i for i in range(10)]
    adapter = _FakeNorgate(symbols=symbols, current=symbols, n_days=40)
    r = hb.run_backfill(cfg, mode="dry-run", as_of="2026-07-29",
                        now_iso="2026-07-29T00:00:00+00:00", adapter=adapter,
                        ledger_fingerprint=lambda: {})
    assert r["terminal"] == bc.DRY_RUN
    norm = Path(cfg["ingestion_root"]) / "normalized"
    assert not norm.exists() or not any(norm.rglob("*.jsonl"))


def test_duplicate_normalization_prevented():
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    w = hb.PartitionWriter(tmp, "run1")
    rec = build_normalized_record(
        record_type=RT_MARKET_BAR, source_id="norgate_local",
        source_native_id="X|2020-01-02", raw_object_id=None,
        retrieved_at="2026", observed_at="2020-01-02", effective_at="2020-01-02",
        available_at=None, ticker="X", payload={"Date": "2020-01-02", "Close": 1.0},
        entity_mapping_confidence=EM_MATCHED_EXACT, provenance="x")
    w.write([rec])
    w.write([rec])  # same record id again
    assert w.written == 1 and w.duplicate_prevented == 1


def test_verify_writes_nothing(built):
    cfg = built["cfg"]
    root = Path(cfg["backfill_root"])
    before = {str(p): _sha(p) for p in sorted(root.rglob("*")) if p.is_file()}
    res = hb.verify_backfill(cfg, ledger_fingerprint=lambda: {})
    assert res["terminal"] == bc.VERIFIED
    after = {str(p): _sha(p) for p in sorted(root.rglob("*")) if p.is_file()}
    assert before == after


def test_verify_detects_tamper(built):
    cfg = built["cfg"]
    run_dir = Path(built["result"]["run_dir"])
    target = run_dir / "coverage_after.csv"
    original = target.read_text(encoding="utf-8")
    try:
        target.write_text(original + "\n# tampered\n", encoding="utf-8")
        res = hb.verify_backfill(cfg, ledger_fingerprint=lambda: {})
        assert res["terminal"] == bc.BLOCKED
    finally:
        target.write_text(original, encoding="utf-8")


def test_ledger_mutation_blocks():
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    cfg = _base_cfg(tmp, cap=10)
    symbols = ["SYM%02d" % i for i in range(10)]
    adapter = _FakeNorgate(symbols=symbols, current=symbols, n_days=40)
    calls = {"n": 0}
    ledger_file = Path(cfg["operational_ledger_roots"][0]) / "book.json"

    def fp():
        calls["n"] += 1
        if calls["n"] > 1:
            ledger_file.write_text('{"nav": 999}', encoding="utf-8")
        return {str(ledger_file): _sha(ledger_file)}

    r = hb.run_backfill(cfg, mode="backfill", as_of="2026-07-29",
                        now_iso="2026-07-29T00:00:00+00:00", adapter=adapter,
                        ledger_fingerprint=fp)
    assert r["terminal"] == bc.BLOCKED
    assert r["status"] == "LEDGER_MUTATION_DETECTED"


def test_norgate_unavailable_blocks(tmp_path):
    cfg = _base_cfg(tmp_path, cap=10)
    adapter = _FakeNorgate(symbols=[], current=[], n_days=0, available=False)
    r = hb.run_backfill(cfg, mode="backfill", as_of="2026-07-29",
                        now_iso="2026-07-29T00:00:00+00:00", adapter=adapter,
                        ledger_fingerprint=lambda: {})
    assert r["terminal"] == bc.BLOCKED


# --------------------------------------------------------------------------- #
# Stage 4 report integration.
# --------------------------------------------------------------------------- #
def test_report_model_and_render(built):
    model = hb.backfill_report_model(built["result"])
    assert model["run_id"] == built["result"]["run_id"]
    assert "price_momentum_rank" in model["templates_unlocked"]
    full = {"cycle_label": "Manual", "cycle_date": "2026-07-29",
            "generated_at": "2026-07-29T00:00:00+00:00",
            "historical_readiness": model, "badges": [], "kpis": {},
            "research": {}, "paper_book": {}, "news_rss": {}, "llm": {},
            "evidence": {}, "material_events": []}
    html = rr.render_html(full)
    text = rr.render_text(full)
    # Stage 7.2: Stage 6 backfill internals no longer render in the executive
    # email (they moved to the API/UI observatory). The compact brief still
    # renders, stays dialog-free, and never leaks the raw run id into the email.
    assert "1. Bottom line" in html
    assert "HISTORICAL DATA & EXPERIMENT READINESS" not in text
    assert built["result"]["run_id"] not in html
    assert "alert(" not in html and "confirm(" not in html
    manifest = rr.report_manifest(full, html, text)
    assert manifest["historical_readiness"]["run_id"] == built["result"]["run_id"]


def test_latest_report_model_from_disk(built):
    model = hb.latest_report_model({"backfill_root": built["cfg"]["backfill_root"]})
    assert model is not None
    assert model["run_id"] == built["result"]["run_id"]


# --------------------------------------------------------------------------- #
# Stage 5 activation over the real backfilled ingestion tree (real experiment).
# --------------------------------------------------------------------------- #
def test_stage5_activation_runs_real_experiment(built):
    tmp = built["tmp"]
    cfg5 = {
        "stage": "5",
        "experiments_root": str(tmp / "experiments5"),
        "stage1_registry_root": str(tmp / "s5reg"),
        "stage2_ingestion_root": built["cfg"]["ingestion_root"],
        "stage3_director_root": str(tmp / "s5dir"),
        "stage3_5_news_rss_root": str(tmp / "s5news"),
        "operational_ledger_roots": [],
        "bounds": {"max_new_hypotheses": 3, "max_specs_per_hypothesis": 2,
                   "max_experiments": 6, "max_runtime_seconds": 1800,
                   "max_workers": 2, "max_symbols": 800, "max_files": 12000},
        "gates": {"min_periods": 12, "min_observations": 50, "min_universe": 20},
        "templates_enabled": list(ec.SUPPORTED_TEMPLATES),
        "cost_bps": [10, 25, 50],
    }
    # Minimal source packages so verify_source_packages passes.
    for name in ("s5reg", "s5dir"):
        (tmp / name / "runs" / "r").mkdir(parents=True, exist_ok=True)
    (tmp / "ingestion_run").mkdir(parents=True, exist_ok=True)
    pkgs = {"stage1": {"run_id": "r", "run_dir": "runs/r",
                       "champion_model": "fundamental_momentum_50_50_v1"},
            "stage2": {"run_id": "r", "run_dir": str(tmp / "ingestion_run")},
            "stage3": {"run_id": "r", "run_dir": "runs/r"},
            "stage3_5": {"run_id": "n"},
            "champion_model": "fundamental_momentum_50_50_v1"}
    reopened = hb._read_jsonl(Path(built["cfg"]["reopened_hypotheses_output"]))
    assert reopened, "expected reopened intake for Stage 5"
    res = ef.run_stage5_cycle(
        cfg5, as_of="s6-test", hypotheses=reopened, source_packages=pkgs,
        now_iso="2026-07-29T00:00:00+00:00", ledger_fingerprint=lambda: {})
    # A real experiment ran (not a pure data hold): price momentum produced a
    # decision. The outcome (KEEP/REJECT_*) is whatever the synthetic data says.
    assert res["counts"]["experiments_completed"] >= 1
    assert res["terminal"] in (ec.READY, ec.PARTIAL)


def test_stage5_supplier_hypothesis_still_held(built):
    tmp = built["tmp"]
    cfg5 = {
        "stage": "5", "experiments_root": str(tmp / "experiments5b"),
        "stage1_registry_root": str(tmp / "s5reg"),
        "stage2_ingestion_root": built["cfg"]["ingestion_root"],
        "stage3_director_root": str(tmp / "s5dir"),
        "stage3_5_news_rss_root": str(tmp / "s5news"),
        "operational_ledger_roots": [],
        "bounds": {"max_new_hypotheses": 3, "max_specs_per_hypothesis": 2,
                   "max_experiments": 6, "max_runtime_seconds": 1800,
                   "max_workers": 2, "max_symbols": 800, "max_files": 12000},
        "gates": {}, "templates_enabled": list(ec.SUPPORTED_TEMPLATES),
        "cost_bps": [10, 25, 50]}
    pkgs = {"stage1": {"run_id": "r", "run_dir": "runs/r"},
            "stage2": {"run_id": "r", "run_dir": str(tmp / "ingestion_run")},
            "stage3": {"run_id": "r", "run_dir": "runs/r"},
            "stage3_5": {"run_id": "n"}, "champion_model": "x"}
    supplier = {"hypothesis_id": "hyp_supplier-surprise-readthrough",
                "title": "supplier", "text": "supplier read-through consensus",
                "information_family": "earnings_surprise",
                "required_fields": ["supplier-customer relationship mapping"],
                "grounding": "GROUNDED",
                "queue_status": "READY_FOR_DETERMINISTIC_DESIGN_REVIEW"}
    res = ef.run_stage5_cycle(
        cfg5, as_of="s6-supplier", hypotheses=[supplier], source_packages=pkgs,
        now_iso="2026-07-29T00:00:00+00:00", ledger_fingerprint=lambda: {})
    gap_reasons = {g.get("gap_reason") for g in res["data_gaps"]}
    assert ec.DATA_HOLD_UNSUPPORTED_TEMPLATE in gap_reasons
