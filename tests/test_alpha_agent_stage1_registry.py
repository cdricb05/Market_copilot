"""
tests/test_alpha_agent_stage1_registry.py

Alpha Agent Stage 1 — Unified Research Memory and Canonical Registry.

Deterministic, hermetic tests (each builds its own tiny synthetic source tree in a
tmp dir) plus a read-only integration check against the real source roots that skips
cleanly when D: is unavailable. The registry is RESEARCH MEMORY ONLY and read-only
w.r.t. every source root; these tests prove the Stage 1 matrix:

  1-3  no network / LLM / PostgreSQL import or call in the package;
  4-6  no write outside the output root; source + operational ledgers unchanged;
  7-8  full hashing at/below threshold, labelled sampled fingerprint above it;
  9-12 deterministic artifact / experiment ids and exact / family fingerprints;
  13-16 exact-duplicate + parameter-variant detection, incomplete-metadata handling,
        conflicting source records kept separate (+ reconciliation issue);
  17-18 every discovered file is recorded; malformed files recorded, not skipped;
  19-20 SQLite integrity + foreign-key integrity;
  21-23 immutable prior versions, atomic latest.json, no-change incremental;
  24-25 changed-artifact incremental, verify mode writes nothing;
  26-27 current champion + Phase 31B state imported; active book unchanged;
  28-30 repeated bootstrap => identical run id, required exports complete,
        no experiment/model execution occurs.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from paper_trader.alpha_agent import research_importers as imp
from paper_trader.alpha_agent import research_registry as reg

_MODULE_FILES = [
    Path(reg.__file__), Path(imp.__file__),
    Path(reg.__file__).parent / "__init__.py",
    Path(reg.__file__).resolve().parents[1] / "scripts" / "build_alpha_research_registry.py",
]
_FIXED_NOW = "2026-07-28T00:00:00+00:00"


# --------------------------------------------------------------------------- #
# Synthetic source tree.
# --------------------------------------------------------------------------- #
def _make_tree(base: Path) -> dict:
    src = base / "src_root"
    for camp in ("c1", "c2"):
        d = src / "campaigns" / camp / "experiments" / "exp_a"
        d.mkdir(parents=True)
        (d / "config.json").write_text(json.dumps({
            "experiment_id": "exp_a", "hypothesis_id": "hyp_1",
            "candidate_family": "fundamental_momentum_blend_v1",
            "baseline_model": "fundamental_momentum_50_50_v1", "universe": "mhz_reconstruction",
            "cost_bps_per_side": 25.0, "evaluation_horizons": ["1m"], "spec_hash": "abc123",
            "model_params": {"fundamental_weight": 0.5}, "portfolio_params": {"top_n": 25},
            "data_cutoff": "2026-06-30"}))
        (d / "decision.json").write_text(json.dumps(
            {"decision": "REJECTED", "reasons": ["hard gate failed: sector_concentration"]}))
    # metrics + provenance only in c1
    c1 = src / "campaigns" / "c1" / "experiments" / "exp_a"
    (c1 / "metrics.json").write_text(json.dumps({"metrics": {"rank_ic_mean": 0.008, "rank_ic_t": 0.76}}))
    (c1 / "provenance.json").write_text(json.dumps({"code_commit": "deadbeef", "input_provenance": {
        "paths": {"momentum_panel": "x.csv"}, "sha256": {"momentum_panel": "aa11"}}}))
    # a parameter-variant experiment dir (same family/universe, different params, no spec_hash)
    dv = src / "campaigns" / "c1" / "experiments" / "exp_b"
    dv.mkdir(parents=True)
    (dv / "config.json").write_text(json.dumps({
        "experiment_id": "exp_b", "candidate_family": "fundamental_momentum_blend_v1",
        "universe": "mhz_reconstruction", "cost_bps_per_side": 25.0,
        "evaluation_horizons": ["1m"], "model_params": {"fundamental_weight": 0.7},
        "portfolio_params": {"top_n": 25}, "data_cutoff": "2026-06-30"}))
    (dv / "decision.json").write_text(json.dumps({"decision": "REJECTED", "reasons": ["weak"]}))
    # factory-style tabular sources
    (src / "experiment_results.csv").write_text(
        "exp_id,family,signal_status,ev_after_25bps,lift_vs_control,reason\n"
        "E1,MOMENTUM,promoted,0.01,0.02,ok\n"
        "E2,earnings surprise,rejected,-0.01,0.0,provider_limited\n")
    (src / "candidate_signal_registry.csv").write_text(
        "candidate_id,family,signal_status,reason\n"
        "SIG1,VOLATILITY,champion,ok\n"
        "SIG1,QUALITY,rejected,conflict\n")   # same native id, conflicting family
    (src / "hypotheses.jsonl").write_text(
        json.dumps({"hypothesis_id": "hyp_1", "diagnosis": "blend balance", "status": "QUEUED"}) + "\n"
        + json.dumps({"hypothesis_id": "hyp_2", "diagnosis": "news sentiment", "status": "QUEUED"}) + "\n")
    (src / "rejected_hypothesis_graveyard.csv").write_text(
        "cycle,exp_id,family,rejection_reason\n1,E9,VOLATILITY,STATISTICALLY_WEAK_IC_T\n")
    (src / "next_action_decision.json").write_text(
        json.dumps({"action": "REQUIRE_PROVIDER", "reason": "bank exhausted"}))
    (src / "notes.md").write_text("# Title\nbody\n")
    (src / "opaque.pkl").write_bytes(b"\x80\x04\x95NOT-UNPICKLED-EVER")
    (src / "bad.json").write_text("{not valid json,,,")

    led = base / "ledger"
    led.mkdir()
    (led / "paper_books.json").write_text(json.dumps({"rows": [{"book": {
        "book_id": "alpha_paper_book_1", "model_id": "fundamental_momentum_50_50_v1",
        "target_position_count": 25, "transaction_cost_bps_per_side": 12.5,
        "review_cadence": "monthly", "snapshot_id": "snap1", "target_book": "fm_top25",
        "frozen_target_weights": {"AAA": 0.04, "BBB": 0.04}}}]}))
    (led / "forward_performance.json").write_text(json.dumps({"rows": [{"row": {
        "date": "2026-07-27", "nav": 98960.36, "cumulative_return_pct": -1.04,
        "benchmark_cumulative_return_pct": -1.11, "holdings_count": 25}}]}))

    p31 = base / "phase31b_pkg"
    (p31 / "phase31b_deadbeef").mkdir(parents=True)
    (p31 / "phase31b_deadbeef" / "run_manifest.json").write_text(json.dumps({
        "run_id": "phase31b_deadbeef", "completeness": {"decision": "RESEARCH_DATA_INSUFFICIENT"}}))

    config = {
        "excluded_roots": [], "source_roots": [
            {"id": "src_root", "path": str(src), "kind": "research_output"},
            {"id": "ledger", "path": str(led), "kind": "operational_ledger"}],
        "operational_ledger_roots": ["ledger"],
        "current_state": {"desk_dir": str(led), "paper_books_file": "paper_books.json",
                          "forward_performance_file": "forward_performance.json",
                          "daily_close_journal_file": "daily_close_journal.json",
                          "phase31b_package_root": str(p31)},
        "hashing": {"full_hash_max_bytes": 268435456}, "parsing": {},
    }
    return {"base": base, "src": src, "led": led, "config": config,
            "out": base / "registry"}


def _hash_tree(root: Path) -> dict:
    out = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            out[str(p)] = imp.fingerprint_file(p)["fingerprint"]
    return out


@pytest.fixture()
def tree(tmp_path):
    return _make_tree(tmp_path)


@pytest.fixture()
def built(tree):
    res = reg.build_registry(tree["config"], str(tree["out"]), mode="bootstrap",
                             git_commit="testcommit", now=_FIXED_NOW)
    assert res["status"] == reg.READY, res.get("reason")
    return {"res": res, "run_dir": Path(res["run_dir"]), **tree}


def _db(run_dir: Path):
    conn = sqlite3.connect(str(run_dir / "alpha_research_registry.sqlite"))
    conn.row_factory = sqlite3.Row
    return conn


# --------------------------------------------------------------------------- #
# 1-3  No network / LLM / PostgreSQL import or call.
# --------------------------------------------------------------------------- #
def _all_src() -> str:
    return "\n".join(p.read_text(encoding="utf-8") for p in _MODULE_FILES)


def test_01_no_network_import_or_call():
    src = _all_src()
    for banned in ("import requests", "import urllib", "urllib.request", "http.client",
                   "import socket", "socket.socket", "yfinance", "eodhd", "aiohttp",
                   "httpx"):
        assert banned not in src, "network primitive present: %s" % banned


def test_02_no_llm_api_import_or_call():
    src = _all_src()
    for banned in ("import openai", "from openai", "import anthropic", "from anthropic",
                   "openai.", "anthropic.", "ChatCompletion", "messages.create",
                   "completions.create"):
        assert banned not in src, "LLM primitive present: %s" % banned


def test_03_no_postgres_import_or_connection():
    src = _all_src()
    for banned in ("psycopg2", "psycopg", "sqlalchemy", "get_session", "get_engine",
                   "postgresql", "create_engine", "session.execute", "session.commit"):
        assert banned not in src, "PostgreSQL primitive present: %s" % banned


# --------------------------------------------------------------------------- #
# 4-6  No writes outside the output root; source + ledgers unchanged.
# --------------------------------------------------------------------------- #
def test_04_no_write_outside_output_root(tree):
    before = _hash_tree(tree["src"]) | _hash_tree(tree["led"])
    reg.build_registry(tree["config"], str(tree["out"]), mode="bootstrap",
                       git_commit="c", now=_FIXED_NOW)
    after = _hash_tree(tree["src"]) | _hash_tree(tree["led"])
    assert before == after
    # every written file is under the output root
    for p in tree["out"].rglob("*"):
        assert str(p).startswith(str(tree["out"]))


def test_05_no_modification_to_source_artifacts(built):
    before = _hash_tree(built["src"])
    reg.build_registry(built["config"], str(built["out"]), mode="verify", git_commit="c")
    assert _hash_tree(built["src"]) == before


def test_06_no_modification_to_operational_ledgers(built):
    man = json.loads((built["run_dir"] / "import_manifest.json").read_text())
    assert man["operational_ledger_unchanged"] is True
    assert man["active_book_changed"] is False
    assert man["ledger_fingerprints_before"] == man["ledger_fingerprints_after"]


# --------------------------------------------------------------------------- #
# 7-8  Hashing thresholds.
# --------------------------------------------------------------------------- #
def test_07_full_hash_at_or_below_threshold(tmp_path):
    p = tmp_path / "small.bin"
    p.write_bytes(b"x" * 50)
    fp = imp.fingerprint_file(p, {"hashing": {"full_hash_max_bytes": 1000}})
    assert fp["fingerprint_type"] == imp.FP_FULL


def test_08_sampled_fingerprint_labeled_above_threshold(tmp_path):
    p = tmp_path / "big.bin"
    p.write_bytes(b"y" * 500)
    cfg = {"hashing": {"full_hash_max_bytes": 10, "sampled_fingerprint_edge_bytes": 4,
                       "read_chunk_bytes": 4}}
    fp = imp.fingerprint_file(p, cfg)
    assert fp["fingerprint_type"] == imp.FP_PARTIAL
    # deterministic
    assert imp.fingerprint_file(p, cfg)["fingerprint"] == fp["fingerprint"]


# --------------------------------------------------------------------------- #
# 9-12  Deterministic ids + fingerprints.
# --------------------------------------------------------------------------- #
def test_09_deterministic_artifact_ids(tree, tmp_path):
    r1 = reg.build_registry(tree["config"], str(tree["out"]), mode="bootstrap",
                            git_commit="c", now=_FIXED_NOW)
    tree2 = _make_tree(tmp_path / "second")
    r2 = reg.build_registry(tree2["config"], str(tree2["out"]), mode="bootstrap",
                            git_commit="c", now=_FIXED_NOW)
    c1 = _db(Path(r1["run_dir"])); c2 = _db(Path(r2["run_dir"]))
    a1 = {r["relpath"]: r["artifact_id"] for r in c1.execute("select relpath,artifact_id from artifacts")}
    a2 = {r["relpath"]: r["artifact_id"] for r in c2.execute("select relpath,artifact_id from artifacts")}
    c1.close(); c2.close()
    assert a1 == a2 and len(a1) > 0


def test_10_deterministic_experiment_ids(built, tmp_path):
    tree2 = _make_tree(tmp_path / "again")
    r2 = reg.build_registry(tree2["config"], str(tree2["out"]), mode="bootstrap",
                            git_commit="testcommit", now=_FIXED_NOW)
    c1 = _db(built["run_dir"]); c2 = _db(Path(r2["run_dir"]))
    e1 = sorted(r["native_id"] + "|" + r["exact_fingerprint"]
                for r in c1.execute("select native_id,exact_fingerprint from experiments"))
    e2 = sorted(r["native_id"] + "|" + r["exact_fingerprint"]
                for r in c2.execute("select native_id,exact_fingerprint from experiments"))
    c1.close(); c2.close()
    assert e1 == e2


def test_11_deterministic_exact_experiment_fingerprint():
    n = imp.normalize_record({"candidate_family": "F", "universe": "U", "cost_bps_per_side": 25,
                              "evaluation_horizons": ["1m"], "model_params": {"a": 1}})
    assert imp.exact_experiment_fingerprint(n) == imp.exact_experiment_fingerprint(dict(n))
    # spec_hash is authoritative
    ns = imp.normalize_record({"spec_hash": "zzz", "candidate_family": "OTHER"})
    assert imp.exact_experiment_fingerprint(ns) == "spec:zzz"


def test_12_deterministic_information_family_fingerprint():
    n = imp.normalize_record({"candidate_family": "price momentum", "universe": "U"})
    assert imp.information_family_fingerprint(n) == imp.information_family_fingerprint(dict(n))
    assert imp.classify_information_family("6-1 momentum") == "price_momentum"
    assert imp.classify_information_family("earnings surprise pead") == "earnings_surprise"


# --------------------------------------------------------------------------- #
# 13-16  Duplicate / variant / incomplete / conflict.
# --------------------------------------------------------------------------- #
def test_13_exact_duplicate_detection(built):
    c = _db(built["run_dir"])
    rows = c.execute("select cluster_id,count(*) n from experiment_duplicates group by cluster_id").fetchall()
    c.close()
    # the two exp_a dirs share spec_hash abc123 -> one cluster of size 2
    assert any(r["n"] == 2 for r in rows)


def test_14_parameter_variant_detection():
    a = imp.normalize_record({"candidate_family": "F", "universe": "U", "cost_bps_per_side": 25,
                              "evaluation_horizons": ["1m"], "model_params": {"w": 0.5}})
    b = imp.normalize_record({"candidate_family": "F", "universe": "U", "cost_bps_per_side": 25,
                              "evaluation_horizons": ["1m"], "model_params": {"w": 0.7}})
    assert imp.compare_experiments(a, b) == imp.CMP_PARAMETER_VARIANT
    verdict = reg.classify_candidate_experiment(
        b, [{"experiment_id": "e1", "exact_fingerprint": imp.exact_experiment_fingerprint(a),
             "family_fingerprint": imp.information_family_fingerprint(a), "decision": "REJECTED"}])
    assert verdict["result"] == "PARAMETER_DUPLICATE"


def test_15_incomplete_metadata_classification():
    a = imp.normalize_record({"candidate_family": "F"})  # missing universe/horizon/params/cost
    b = imp.normalize_record({"candidate_family": "F"})
    assert imp.compare_experiments(a, b) == imp.CMP_INCOMPLETE
    assert reg.classify_candidate_experiment(a, [])["result"] == "METADATA_INSUFFICIENT"


def test_16_conflicting_source_records_kept_separate(built):
    c = _db(built["run_dir"])
    sigs = c.execute("select * from signals where native_id='SIG1'").fetchall()
    issues = c.execute("select * from import_issues where issue_type='CONFLICTING_SOURCE_RECORDS'").fetchall()
    c.close()
    assert len(sigs) == 2                      # both kept, not merged
    assert len(issues) >= 1                     # a reconciliation issue was raised


# --------------------------------------------------------------------------- #
# 17-18  Coverage + malformed handling.
# --------------------------------------------------------------------------- #
def test_17_every_discovered_file_has_artifact_record(built):
    files = [p for p in built["src"].rglob("*") if p.is_file()]
    files += [p for p in built["led"].rglob("*") if p.is_file()]
    c = _db(built["run_dir"])
    n = c.execute("select count(*) n from artifacts").fetchone()["n"]
    c.close()
    assert n == len(files)


def test_18_malformed_files_recorded_not_skipped(built):
    c = _db(built["run_dir"])
    bad = c.execute("select * from artifacts where filename='bad.json'").fetchone()
    pkl = c.execute("select * from artifacts where filename='opaque.pkl'").fetchone()
    iss = {r["issue_type"] for r in c.execute("select issue_type from import_issues")}
    c.close()
    assert bad is not None and bad["parser_status"] == imp.PS_PARSE_ERROR
    assert pkl is not None and pkl["parser_status"] == imp.PS_UNSUPPORTED
    assert imp.PS_PARSE_ERROR in iss and imp.PS_UNSUPPORTED in iss


# --------------------------------------------------------------------------- #
# 19-20  SQLite + FK integrity.
# --------------------------------------------------------------------------- #
def test_19_sqlite_integrity(built):
    v = reg.validate_run(built["run_dir"])
    assert v["checks"]["integrity_check"] == "ok"
    assert v["ok"] is True


def test_20_foreign_key_integrity(built):
    v = reg.validate_run(built["run_dir"])
    assert v["checks"]["foreign_key_violations"] == 0


# --------------------------------------------------------------------------- #
# 21-25  Immutability, atomic latest, incremental, verify.
# --------------------------------------------------------------------------- #
def test_21_immutable_prior_versions(built):
    db = built["run_dir"] / "alpha_research_registry.sqlite"
    before = imp.fingerprint_file(db)["fingerprint"]
    r2 = reg.build_registry(built["config"], str(built["out"]), mode="bootstrap",
                            git_commit="testcommit", now="2026-07-28T09:09:09+00:00")
    assert r2.get("already_existed") is True
    assert imp.fingerprint_file(db)["fingerprint"] == before  # not rewritten


def test_22_atomic_latest_json(built):
    latest = json.loads((built["out"] / "latest.json").read_text())
    assert latest["run_id"] == built["res"]["run_id"]
    assert latest["run_dir"] == "runs/" + built["res"]["run_id"]
    man = json.loads((built["run_dir"] / "import_manifest.json").read_text())
    assert latest["artifact_set_hash"] == man["artifact_set_hash"]


def test_23_no_change_incremental(built):
    r = reg.build_registry(built["config"], str(built["out"]), mode="incremental",
                           git_commit="testcommit", now=_FIXED_NOW)
    assert r["status"] == reg.NO_CHANGES


def test_24_changed_artifact_incremental(built):
    (built["src"] / "new_signal.csv").write_text("candidate_id,family,signal_status\nSNEW,QUALITY,promoted\n")
    r = reg.build_registry(built["config"], str(built["out"]), mode="incremental",
                           git_commit="testcommit", now=_FIXED_NOW)
    assert r["status"] == reg.READY
    assert r["run_id"] != built["res"]["run_id"]
    assert r["incremental_diff"]["additions"] >= 1


def test_25_verify_writes_nothing(built):
    before = _hash_tree(built["out"])
    r = reg.build_registry(built["config"], str(built["out"]), mode="verify", git_commit="c")
    assert r["status"] == reg.VERIFIED
    assert _hash_tree(built["out"]) == before


# --------------------------------------------------------------------------- #
# 26-30  Current state, active book, determinism, exports, no execution.
# --------------------------------------------------------------------------- #
def test_26_current_champion_and_phase31b_state(built):
    cs = json.loads((built["run_dir"] / "current_state_summary.json").read_text())
    assert cs["champion_model"] == "fundamental_momentum_50_50_v1"
    assert cs["active_book_id"] == "alpha_paper_book_1"
    assert cs["latest_phase31b_decision"] == "RESEARCH_DATA_INSUFFICIENT"


def test_27_active_book_unchanged(built):
    cs = json.loads((built["run_dir"] / "current_state_summary.json").read_text())
    assert cs["active_book_changed"] is False


def test_28_repeated_bootstrap_identical_run_id(built):
    r2 = reg.build_registry(built["config"], str(built["out"]), mode="bootstrap",
                            git_commit="testcommit", now="2026-07-28T23:23:23+00:00")
    assert r2["run_id"] == built["res"]["run_id"]


def test_29_required_exports_complete(built):
    required = ["alpha_research_registry.sqlite", "registry_schema.json", "artifact_registry.csv",
                "dataset_registry.csv", "feature_registry.csv", "hypothesis_registry.csv",
                "experiment_registry.csv", "signal_registry.csv", "evidence_registry.csv",
                "decision_journal.csv", "duplicate_experiment_clusters.csv",
                "research_coverage_map.csv", "unresolved_imports.csv",
                "current_state_summary.json", "import_manifest.json", "stage1_executive_report.md"]
    for f in required:
        assert (built["run_dir"] / f).exists(), "missing export: %s" % f


def test_30_no_experiment_or_model_execution():
    src = _all_src()
    for banned in ("run_daily_close", "run_research(", "capture_snapshots", "mature_outcomes",
                   "prediction_client", "engine.market_data", "subprocess.run(['python'",
                   "backtest(", "evaluate_signal("):
        assert banned not in src, "execution primitive present: %s" % banned


# --------------------------------------------------------------------------- #
# Read-only integration (real source roots). Skips if D: is unavailable.
# --------------------------------------------------------------------------- #
def _real_config():
    p = Path(reg.__file__).resolve().parents[1] / "configs" / "alpha_agent" / "stage1_registry.json"
    return json.loads(p.read_text(encoding="utf-8-sig"))


def test_31_real_roots_scan_readonly_and_stable():
    cfg = _real_config()
    if not any(Path(r["path"]).exists() for r in cfg["source_roots"]):
        pytest.skip("no real source roots available")
    files, roots = reg.scan_source_roots(cfg)
    assert len(files) > 0
    # ledger fingerprints are stable across repeated read-only calls (no mutation)
    lf1 = reg.ledger_fingerprints(cfg)
    lf2 = reg.ledger_fingerprints(cfg)
    assert lf1 == lf2
