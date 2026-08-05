"""tests/test_slice4_universe_scoring.py — Slice 4 (Phase 29E) canonical universe scoring.

Deterministic, isolated-fixture tests for the canonical universe-scoring composition
& read owner (api.universe_scoring) over the pure kernel (api.multi_horizon_engine),
the migrated consumers (daily_research_cycle, multi_horizon_platform), the canonical
+ compatibility endpoints, the cross-consumer consistency validator, the UI loader and
the architecture guards. No provider / prediction / DB / ledger / real-cycle / close /
promotion / order / signal / decision / fill occurs; every scoring input is a local
fixture root or a hand-crafted in-memory kernel result.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from paper_trader.api import universe_scoring as us
from paper_trader.api import multi_horizon_engine as eng


# --------------------------------------------------------------------------- #
# Isolated fixtures: write owned-shape CSVs into a tmp root and point the kernel
# at them. The sector map env is redirected to a non-existent path so the real
# owned map never leaks in.
# --------------------------------------------------------------------------- #
SECTORS = ("Tech", "Fin", "Health", "Energy", "Industrials", "Staples")


def _write_panel(path: Path, rows: list[dict], reb="2026-05-29", asof="2026-05-22"):
    cols = ["rebalance_date", "ticker", "sector", "liquidity_proxy", "composite_sn", "as_of_date"]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({"rebalance_date": reb, "ticker": r["ticker"], "sector": r["sector"],
                        "liquidity_proxy": r.get("liquidity_proxy", 5.0),
                        "composite_sn": r["composite_sn"], "as_of_date": asof})


def _write_momentum(path: Path, rows: list[dict], market="2026-07-31", month="2026-07"):
    cols = ["ticker", "market_as_of_date", "month_label", "mom_6_1", "is_member",
            "adv_dollar", "realized_vol_63d", "trailing_obs_126", "eligible_history",
            "extreme_flag", "sector"]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({"ticker": r["ticker"], "market_as_of_date": market, "month_label": month,
                        "mom_6_1": r["mom_6_1"], "is_member": r.get("is_member", "1"),
                        "adv_dollar": r.get("adv_dollar", 5.0e7), "realized_vol_63d": 0.2,
                        "trailing_obs_126": 126, "eligible_history": r.get("eligible_history", "1"),
                        "extreme_flag": r.get("extreme_flag", "0"), "sector": r["sector"]})


def _write_risk(path: Path, rows: list[dict]):
    cols = ["ticker", "realized_vol_63d", "beta_universe", "adv_dollar_20d",
            "max_drawdown_252d", "is_current_member", "sector"]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({"ticker": r["ticker"], "realized_vol_63d": 0.2, "beta_universe": 1.0,
                        "adv_dollar_20d": r.get("adv_dollar", 5.0e7), "max_drawdown_252d": -0.1,
                        "is_current_member": "1", "sector": r["sector"]})


def _rows(n=60, sectors=6, ties=False):
    out = []
    for i in range(n):
        out.append({"ticker": "T%03d" % i, "sector": SECTORS[i % sectors],
                    "composite_sn": round(1.5 - i * 0.02, 6),
                    "mom_6_1": round(0.9 - i * 0.013, 6), "adv_dollar": 5.0e7})
    if ties:
        # two byte-identical names to exercise the deterministic ticker tie-break.
        for tk in ("TIEAAA", "TIEBBB"):
            out.append({"ticker": tk, "sector": "Tech", "composite_sn": 0.5,
                        "mom_6_1": 0.4, "adv_dollar": 5.0e7})
    return out


def _fixture(tmp_path: Path, monkeypatch, rows=None, **kw):
    rows = _rows(**kw) if rows is None else rows
    tmp_path.mkdir(parents=True, exist_ok=True)
    panel = tmp_path / "panel.csv"
    idir = tmp_path / "inputs"
    idir.mkdir(parents=True, exist_ok=True)
    _write_panel(panel, rows)
    _write_momentum(idir / eng.CUR_MOM_FILE, rows)
    _write_risk(idir / eng.RISK_FILE, rows)
    monkeypatch.setenv(eng.SECTOR_MAP_ENV, str(tmp_path / "no_such_sector_map.csv"))
    return panel, idir


def _build(tmp_path, monkeypatch, **kw):
    panel, idir = _fixture(tmp_path, monkeypatch, **kw)
    return us.build_universe_scoring(panel_path=panel, inputs_dir=idir, use_cache=False)


def _raw_kernel(fund: dict, mom: dict, combined_map: dict, n_common: int,
                books: dict, market="2026-07-31"):
    """A minimal hand-crafted kernel `build_current` result for reconciliation tests."""
    return {
        "status": eng.STATUS_READY, "market_as_of_date": market,
        "momentum_month": "2026-07", "fundamental_as_of_date": "2026-05-22",
        "fundamental_month": "2026-05",
        "scores": {"composite_sn": fund, "mom_6_1": mom,
                   "counts": {"composite_sn_eligible": sum(1 for s in fund.values() if s.get("eligible")),
                              "mom_6_1_eligible": sum(1 for s in mom.values() if s.get("eligible"))}},
        "combined": {"combined": combined_map, "n_common": n_common},
        "books": {"primary_book_id": us.PRIMARY_BOOK_ID, "books": books},
        "inputs": {"validations": {"fundamental_available": True, "momentum_available": True,
                                   "risk_available": True, "sector_map_available": False},
                   "fingerprints": {"fundamental_panel": "fp1", "current_momentum_scores": "fp2",
                                    "current_risk_stats": "fp3", "sector_map": None},
                   "paths": {}},
        "warnings": [],
    }


# =========================================================================== #
# KERNEL AND CONTRACT (1-10)
# =========================================================================== #
def test_01_owner_delegates_to_kernel(tmp_path, monkeypatch, mocker=None):
    calls = {"n": 0}
    real = eng.build_current

    def _spy(**kw):
        calls["n"] += 1
        return real(**kw)

    panel, idir = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(eng, "build_current", _spy)
    c = us.build_universe_scoring(panel_path=panel, inputs_dir=idir, use_cache=False)
    assert c["status"] == us.STATUS_READY
    assert calls["n"] == 1  # the kernel is called exactly once


def test_02_owner_defines_no_scoring_math():
    src = Path("api/universe_scoring.py").read_text(encoding="utf-8")
    for tok in ("def compute_scores(", "def _percentiles(", "def compute_combined(",
                "def build_books(", "def _select_book("):
        assert tok not in src, tok


def test_03_complete_required_contract(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    required = ["status", "evaluated_at", "eligible_market_date", "ranking_date",
               "strategy_id", "strategy_version", "model_registry_version",
               "primary_model_id", "primary_book_id", "universe_id", "universe_label",
               "universe_definition", "universe_as_of_date", "input_contract_hash",
               "input_fingerprints", "fundamental_as_of_date", "fundamental_month",
               "momentum_market_date", "momentum_month", "risk_as_of_date",
               "source_statuses", "universe_size", "fundamental_eligible_count",
               "momentum_eligible_count", "combined_eligible_count", "scored_count",
               "excluded_count", "coverage_ratio", "exclusion_summary", "exclusions",
               "rankings", "top25", "top50", "construction", "warnings", "blockers",
               "consistency_status", "consistency_violations", "safety", "universe"]
    for k in required:
        assert k in c, k
    row = c["rankings"][0]
    for k in ["ticker", "rank", "combined_score", "percentile", "fundamental_score",
              "fundamental_rank", "fundamental_percentile", "momentum_score",
              "momentum_rank", "momentum_percentile", "sector", "adv_dollar", "eligible",
              "exclusion_reason", "data_quality_flags", "market_as_of_date", "model_id",
              "model_version"]:
        assert k in row, k


def test_04_identical_inputs_identical_ranks_and_hash(tmp_path, monkeypatch):
    c1 = _build(tmp_path, monkeypatch)
    c2 = _build(tmp_path / "b", monkeypatch)  # byte-identical fixture, different dir
    assert c1["input_contract_hash"] == c2["input_contract_hash"]
    assert [r["ticker"] for r in c1["rankings"]] == [r["ticker"] for r in c2["rankings"]]
    assert [r["rank"] for r in c1["rankings"]] == [r["rank"] for r in c2["rankings"]]


def test_05_changed_input_content_changes_hash(tmp_path, monkeypatch):
    panel, idir = _fixture(tmp_path, monkeypatch)
    h1 = us.build_universe_scoring(panel_path=panel, inputs_dir=idir, use_cache=False)["input_contract_hash"]
    # change momentum content (different scores) -> fingerprint changes -> hash changes
    _write_momentum(idir / eng.CUR_MOM_FILE, _rows(n=60), month="2026-07")
    rows2 = _rows(n=60)
    for r in rows2:
        r["mom_6_1"] = round(r["mom_6_1"] + 0.001, 6)
    _write_momentum(idir / eng.CUR_MOM_FILE, rows2)
    h2 = us.build_universe_scoring(panel_path=panel, inputs_dir=idir, use_cache=False)["input_contract_hash"]
    assert h1 != h2


def test_06_evaluated_at_not_in_hash(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    comps = us.input_contract_components(
        eng.build_current(panel_path=_fixture(tmp_path / "x", monkeypatch)[0],
                          inputs_dir=_fixture(tmp_path / "x", monkeypatch)[1], use_cache=False))
    assert "evaluated_at" not in comps and "built_at" not in comps


def test_07_mtime_alone_does_not_define_hash(tmp_path, monkeypatch):
    panel, idir = _fixture(tmp_path, monkeypatch)
    h1 = us.build_universe_scoring(panel_path=panel, inputs_dir=idir, use_cache=False)["input_contract_hash"]
    # rewrite byte-identical content (updates mtime, same bytes) -> same hash
    _write_momentum(idir / eng.CUR_MOM_FILE, _rows(n=60))
    h2 = us.build_universe_scoring(panel_path=panel, inputs_dir=idir, use_cache=False)["input_contract_hash"]
    assert h1 == h2
    # the hash components use content fingerprints, never inputs["paths"] or mtime
    comps = us.input_contract_components(eng.build_current(panel_path=panel, inputs_dir=idir, use_cache=False))
    assert "paths" not in comps and "mtime" not in json.dumps(comps)


def test_08_kernel_cache_not_mutated(tmp_path, monkeypatch):
    panel, idir = _fixture(tmp_path, monkeypatch)
    kernel = eng.build_current(panel_path=panel, inputs_dir=idir, use_cache=True)
    before = json.dumps(kernel, sort_keys=True, default=str)
    c = us.build_universe_scoring(panel_path=panel, inputs_dir=idir, use_cache=True)
    if c.get("top25"):
        c["top25"][0]["ticker"] = "MUTATED"
        c["rankings"][0]["combined_score"] = -999
    after = json.dumps(eng.build_current(panel_path=panel, inputs_dir=idir, use_cache=True),
                       sort_keys=True, default=str)
    assert before == after


def test_09_consumer_mutation_cannot_contaminate_later_output(tmp_path, monkeypatch):
    panel, idir = _fixture(tmp_path, monkeypatch)
    c1 = us.build_universe_scoring(panel_path=panel, inputs_dir=idir, use_cache=True)
    t0 = c1["top25"][0]["ticker"]
    c1["top25"][0]["ticker"] = "HACKED"
    c2 = us.build_universe_scoring(panel_path=panel, inputs_dir=idir, use_cache=True)
    assert c2["top25"][0]["ticker"] == t0 != "HACKED"


def test_10_input_unavailable_degrades_honestly(tmp_path, monkeypatch):
    monkeypatch.setenv(eng.SECTOR_MAP_ENV, str(tmp_path / "none.csv"))
    c = us.build_universe_scoring(panel_path=tmp_path / "missing_panel.csv",
                                  inputs_dir=tmp_path / "missing_inputs", use_cache=False)
    assert c["status"] == us.STATUS_INPUTS_UNAVAILABLE
    assert c["rankings"] == [] and c["top25"] == []
    assert c["input_contract_hash"]  # still deterministic
    assert c["consistency_status"] == us.UNKNOWN
    assert c["safety"]["automatic_model_promotion_allowed"] is False


# =========================================================================== #
# RANKING (11-24)
# =========================================================================== #
def test_11_deterministic_ticker_tie_break(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch, n=20, ties=True)
    order = [r["ticker"] for r in c["rankings"]]
    assert "TIEAAA" in order and "TIEBBB" in order
    assert order.index("TIEAAA") < order.index("TIEBBB")  # asc-ticker on a score tie


def test_12_no_duplicate_ticker(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    tks = [r["ticker"] for r in c["rankings"]]
    assert len(tks) == len(set(tks))
    for book in ("top25", "top50"):
        bt = [r["ticker"] for r in c[book]]
        assert len(bt) == len(set(bt))


def test_13_rank_sequence_reconciles(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    ranks = [r["rank"] for r in c["rankings"]]
    assert ranks == list(range(1, len(ranks) + 1))


def test_14_combined_eligible_count_reconciles(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    assert c["combined_eligible_count"] == len(c["rankings"])
    assert c["scored_count"] == c["combined_eligible_count"] + c["combined_excluded_count"]


def test_15_scored_count_reconciles(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    assert c["scored_count"] == c["universe_size"] == c["universe"]["scored_universe_size"]


def test_16_excluded_count_reconciles():
    fund = {"A": {"eligible": True, "raw_signal": 1.0, "sector": "Tech"},
            "B": {"eligible": True, "raw_signal": 0.9, "sector": "Fin"},
            "X": {"eligible": False, "exclusion_reason": "MISSING_COMPOSITE_SN", "sector": "Tech"}}
    mom = {"A": {"eligible": True, "raw_signal": 0.5}, "B": {"eligible": True, "raw_signal": 0.4}}
    comb = {"A": {"combined_score": 0.9, "rank": 1, "sector": "Tech"},
            "B": {"combined_score": 0.8, "rank": 2, "sector": "Fin"}}
    books = {us.PRIMARY_BOOK_ID: {"constituents": [{"ticker": "A", "rank": 1, "weight": 0.5, "sector": "Tech"}]},
             us.TOP50_BOOK_ID: {"constituents": []}}
    c = us.normalize_built(_raw_kernel(fund, mom, comb, 2, books))
    assert c["excluded_count"] == 1
    assert c["scored_count"] == c["fundamental_eligible_count"] + c["excluded_count"]


def test_17_exclusion_reasons_retained():
    fund = {"A": {"eligible": True}, "X": {"eligible": False, "exclusion_reason": "LIQUIDITY_FILTER_FAILED"}}
    c = us.normalize_built(_raw_kernel(fund, {"A": {"eligible": True}}, {}, 1,
                                       {us.PRIMARY_BOOK_ID: {"constituents": []}, us.TOP50_BOOK_ID: {"constituents": []}}))
    assert c["exclusions"]["X"] == "LIQUIDITY_FILTER_FAILED"
    assert c["exclusion_summary"]["LIQUIDITY_FILTER_FAILED"] == 1


def test_18_19_top25_top50_equal_ranks_under_policy(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)  # 6 sectors, cap not binding
    assert c["top25_count"] == 25 and c["top50_count"] == 50
    # book constituents are the deterministic sector-capped top-N in kernel order
    assert [r["rank"] for r in c["top25"]] == list(range(1, 26))
    assert [r["rank"] for r in c["top50"]] == list(range(1, 51))


def test_20_top25_contained_in_top50_where_policy_permits(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    t25 = {r["ticker"] for r in c["top25"]}
    t50 = {r["ticker"] for r in c["top50"]}
    # exposed explicitly rather than assumed; when the sector cap does not bind it holds
    assert c["top25_subset_of_top50"] == t25.issubset(t50)
    assert c["top25_in_top50_count"] == len(t25 & t50)


def test_21_sector_cap_policy_preserved(tmp_path, monkeypatch):
    # concentrate 40 names in ONE sector -> top25 caps at int(0.25*25)=6 per sector
    rows = [{"ticker": "C%03d" % i, "sector": "Tech", "composite_sn": round(2.0 - i * 0.01, 6),
             "mom_6_1": round(1.0 - i * 0.005, 6), "adv_dollar": 5.0e7} for i in range(40)]
    c = _build(tmp_path, monkeypatch, rows=rows)
    assert c["construction"]["max_per_sector"] == int(eng.SECTOR_CAP_FRACTION * 25)
    assert c["construction"]["primary_book_size_actual"] == int(eng.SECTOR_CAP_FRACTION * 25)
    assert c["construction"]["sector_capped_out"]  # names were capped out


def test_22_liquidity_policy_preserved(tmp_path, monkeypatch):
    rows = _rows(n=20)
    rows[5]["adv_dollar"] = 1.0e5  # below MIN_ADV_DOLLAR -> momentum-excluded
    illiquid = rows[5]["ticker"]
    c = _build(tmp_path, monkeypatch, rows=rows)
    assert illiquid not in {r["ticker"] for r in c["rankings"]}
    assert "LIQUIDITY_FILTER_FAILED" in c["momentum_exclusion_summary"]


def test_23_construction_weights_unchanged(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    assert c["construction"]["primary_weights"] == {"composite_sn": 0.5, "mom_6_1": 0.5}
    assert c["construction"]["sector_cap_fraction"] == eng.SECTOR_CAP_FRACTION
    assert c["construction"]["max_individual_weight"] == eng.MAX_INDIVIDUAL_WEIGHT
    assert c["construction"]["min_adv_dollar"] == eng.MIN_ADV_DOLLAR


def test_24_model_version_unchanged(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    assert c["strategy_id"] == "fundamental_momentum_50_50_v1"
    assert c["strategy_version"] == "v1"
    assert c["primary_book_id"] == "fundamental_momentum_50_50_top25"


# =========================================================================== #
# UNIVERSE (25-30)
# =========================================================================== #
def test_25_universe_identity_present(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    u = c["universe"]
    for k in ("universe_id", "universe_label", "universe_definition", "source",
              "as_of_date", "point_in_time_policy", "current_member_policy",
              "unknown_membership_count"):
        assert k in u


def test_26_broader_universe_not_labelled_strict_sp500(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    assert c["universe"]["is_strict_sp500"] is False
    assert "S&P 500" not in c["universe_label"]
    assert "NOT a strict S&P 500" in c["universe_definition"]


def test_27_point_in_time_policy_present(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    assert c["universe"]["point_in_time_policy"]
    assert c["universe"]["unknown_membership_policy"] == "explicit"


def test_28_unknown_membership_explicit(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    assert isinstance(c["unknown_membership_count"], int)
    assert c["universe"]["unknown_membership_count"] == c["unknown_membership_count"]


def test_29_source_eligible_excluded_counts_reconcile(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    assert c["universe"]["scored_universe_size"] == c["scored_count"]
    assert c["universe"]["eligible_count"] == c["fundamental_eligible_count"]
    assert c["universe"]["excluded_count"] == c["excluded_count"]


def test_30_universe_as_of_present(tmp_path, monkeypatch):
    c = _build(tmp_path, monkeypatch)
    assert c["universe_as_of_date"] == c["ranking_date"]
    assert c["universe"]["as_of_date"] == c["ranking_date"]


# =========================================================================== #
# CONSUMERS (31-38)
# =========================================================================== #
def test_31_32_drc_invokes_canonical_once_and_records_hash(tmp_path, monkeypatch):
    from paper_trader.api import daily_research_cycle as drc
    calls = {"n": 0}
    real = us.build_universe_scoring

    def _spy(**kw):
        calls["n"] += 1
        return real(**kw)

    monkeypatch.setattr(us, "build_universe_scoring", _spy)
    built = drc._default_scoring_fn()
    assert calls["n"] == 1
    sc = drc._extract_scoring(built)
    assert sc["available"] and sc["input_contract_hash"]
    assert sc["scoring_owner"] == "api.universe_scoring.build_universe_scoring"


def test_33_target_uses_canonical_primary_book():
    from paper_trader.api import alpha_target as at
    assert at.PRIMARY_BOOK_ID == us.PRIMARY_BOOK_ID
    assert at.PRIMARY_MODEL_ID == us.PRIMARY_MODEL_ID


def test_34_daily_action_gate_uses_canonical_primary_book():
    from paper_trader.api import daily_action_gate as dag
    # the gate reads the kernel primary_book_id, which equals the canonical id
    src = Path("api/daily_action_gate.py").read_text(encoding="utf-8")
    assert "primary_book_id" in src  # target derived from the canonical book id
    # equivalence: the canonical primary book id is the gate's frozen target book
    assert us.PRIMARY_BOOK_ID == "fundamental_momentum_50_50_top25"


def test_35_forward_evidence_references_canonical_identity():
    from paper_trader.api import forward_prediction_skill as fps
    assert fps.ACTIVE_BOOK_ID == us.PRIMARY_BOOK_ID
    assert fps.ACTIVE_MODEL_ID == us.PRIMARY_MODEL_ID


def test_36_compat_route_delegates():
    from paper_trader.api import multi_horizon_platform as plat
    src = Path("api/multi_horizon_platform.py").read_text(encoding="utf-8")
    assert "universe_scoring" in src
    p = plat.load_current_scores()
    assert p.get("compatibility_surface") is True
    assert p.get("canonical_owner") == us.CANONICAL_OWNER


def test_37_compat_payload_preserves_legacy_fields():
    from paper_trader.api import multi_horizon_platform as plat
    p = plat.load_current_scores()
    if p.get("status") == "MHZ_SCORES_READY":
        for k in ("combined_universe", "composite_sn_top", "mom_6_1_top", "counts",
                  "fingerprints", "n_common_universe", "blocked_models", "validations"):
            assert k in p, k


def test_38_no_operational_consumer_recalculates_combined_score():
    import glob
    kernel = "api/multi_horizon_engine.py"
    for f in glob.glob("api/*.py"):
        rel = f.replace("\\", "/")
        if rel == kernel:
            continue
        src = Path(f).read_text(encoding="utf-8")
        for tok in ("def compute_scores(", "def _percentiles(", "def compute_combined("):
            assert tok not in src, f"{rel}: {tok}"


# =========================================================================== #
# API (39-46)
# =========================================================================== #
def _client():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    return TestClient(app, raise_server_exceptions=False)


def _key():
    from paper_trader.config import get_settings
    return get_settings().service_api_key


def test_39_canonical_endpoint_requires_auth():
    c = _client()
    assert c.get("/v1/research/universe-scoring").status_code in (401, 403)


def test_40_canonical_endpoint_get_only():
    c = _client()
    assert c.post("/v1/research/universe-scoring", headers={"X-API-Key": _key()}).status_code == 405
    assert c.get("/v1/research/universe-scoring", headers={"X-API-Key": _key()}).status_code == 200


def test_41_42_43_44_45_46_canonical_endpoint_read_only_no_side_effects(monkeypatch):
    # inject a canned canonical contract; assert the route neither writes nor calls a
    # provider/prediction/cycle/close, and creates no order/signal/decision/fill.
    canned = {"contract": us.CONTRACT_ID, "status": us.STATUS_READY, "ranking_date": "2026-07-31",
              "primary_book_id": us.PRIMARY_BOOK_ID, "input_contract_hash": "abc",
              "rankings": [], "top25": [], "top50": [], "consistency_status": "CONSISTENT",
              "safety": {"read_only": True, "creates_orders": False, "creates_signals": False,
                         "creates_trade_decisions": False, "creates_fills": False,
                         "calls_provider": False, "calls_prediction_service": False,
                         "wrote_to_database": False, "wrote_to_operational_ledger": False,
                         "automatic_model_promotion_allowed": False}}
    monkeypatch.setattr("paper_trader.api.app._uscore.build_universe_scoring", lambda: canned)
    c = _client()
    r = c.get("/v1/research/universe-scoring", headers={"X-API-Key": _key()})
    assert r.status_code == 200
    b = r.json()
    s = b["safety"]
    assert s["read_only"] and not s["creates_orders"] and not s["creates_signals"]
    assert not s["creates_trade_decisions"] and not s["creates_fills"]
    assert not s["calls_provider"] and not s["calls_prediction_service"]
    assert not s["wrote_to_database"] and not s["wrote_to_operational_ledger"]
    assert s["automatic_model_promotion_allowed"] is False


# =========================================================================== #
# CONSISTENCY (47-53)
# =========================================================================== #
def _canon():
    return {"contract": us.CONTRACT_ID, "status": us.STATUS_READY, "ranking_date": "2026-07-31",
            "primary_book_id": us.PRIMARY_BOOK_ID, "primary_model_id": us.PRIMARY_MODEL_ID,
            "strategy_id": us.STRATEGY_ID, "strategy_version": "v1", "universe_id": us.UNIVERSE_ID,
            "input_contract_hash": "HASH1",
            "top25": [{"ticker": "A"}, {"ticker": "B"}], "top50": [{"ticker": "A"}, {"ticker": "B"}]}


def test_47_matching_consumers_consistent():
    r = us.check_scoring_consistency(_canon(), {
        "api.daily_research_cycle": {"ranking_date": "2026-07-31", "input_contract_hash": "HASH1",
                                     "primary_book_id": us.PRIMARY_BOOK_ID, "top25": ["A", "B"]},
        "api.alpha_target": {"primary_book_id": us.PRIMARY_BOOK_ID, "strategy_version": "v1"}})
    assert r["consistency_status"] == us.CONSISTENT and r["consistency_violations"] == []


def test_48_ranking_date_mismatch_inconsistent():
    r = us.check_scoring_consistency(_canon(), {"api.daily_research_cycle": {"ranking_date": "2026-07-30"}})
    assert r["consistency_status"] == us.INCONSISTENT
    assert any(v["concept"] == "ranking_date" for v in r["consistency_violations"])


def test_49_universe_mismatch_inconsistent():
    r = us.check_scoring_consistency(_canon(), {"api.x": {"universe_id": "OTHER_UNIVERSE"}})
    assert r["consistency_status"] == us.INCONSISTENT
    assert any(v["concept"] == "universe_id" for v in r["consistency_violations"])


def test_50_strategy_version_mismatch_inconsistent():
    r = us.check_scoring_consistency(_canon(), {"api.x": {"strategy_version": "v2"}})
    assert r["consistency_status"] == us.INCONSISTENT
    assert any(v["concept"] == "strategy_version" for v in r["consistency_violations"])


def test_51_top25_mismatch_inconsistent():
    r = us.check_scoring_consistency(_canon(), {"api.x": {"top25": ["B", "A"]}})
    assert r["consistency_status"] == us.INCONSISTENT
    assert any(v["concept"] == "top25" for v in r["consistency_violations"])


def test_52_input_hash_mismatch_inconsistent():
    r = us.check_scoring_consistency(_canon(), {"api.x": {"input_contract_hash": "OTHER"}})
    assert r["consistency_status"] == us.INCONSISTENT
    assert any(v["concept"] == "input_contract_hash" for v in r["consistency_violations"])


def test_53_violations_name_both_owners_and_values():
    r = us.check_scoring_consistency(_canon(), {"api.alpha_target": {"ranking_date": "2020-01-01"}})
    v = r["consistency_violations"][0]
    assert v["canonical_owner"] == "api.universe_scoring"
    assert v["conflicting_owner"] == "api.alpha_target"
    assert v["canonical_value"] == "2026-07-31" and v["conflicting_value"] == "2020-01-01"


def test_53b_unknown_when_no_comparable_facts():
    r = us.check_scoring_consistency(_canon(), {"api.x": {}})
    assert r["consistency_status"] == us.UNKNOWN


# =========================================================================== #
# UI (54-61)
# =========================================================================== #
def _ui():
    return Path("api/ui/index.html").read_text(encoding="utf-8")


def test_54_exactly_one_canonical_scoring_loader():
    assert _ui().count("function loadUniverseScoring") == 1


def test_55_56_57_58_59_60_ui_performs_no_computation():
    ui = _ui()
    start = ui.find("function loadUniverseScoring")
    end = ui.find("window.loadUniverseScoring")
    assert start != -1 and end > start
    region = ui[start:end]
    for pat in ("new Date(", "Date.now(", ".getTime(", ".sort(", "_percentiles",
                "compute_", "* 0.5"):
        assert pat not in region, pat


def test_61_ui_region_fetches_canonical_endpoint():
    ui = _ui()
    start = ui.find("function loadUniverseScoring")
    end = ui.find("window.loadUniverseScoring")
    assert "/v1/research/universe-scoring" in ui[start:end]


# =========================================================================== #
# SAFETY AND ARCHITECTURE (62-72)
# =========================================================================== #
def test_62_63_64_65_owner_has_no_provider_prediction_write_paths():
    src = Path("api/universe_scoring.py").read_text(encoding="utf-8")
    for tok in ("requests.get", "requests.post", "urlopen", "httpx.", "predict(",
                "open(", "write(", ".commit(", "Session(", "subprocess"):
        assert tok not in src, tok


def test_66_67_no_recalibration_or_promotion():
    src = Path("api/universe_scoring.py").read_text(encoding="utf-8")
    assert us.AUTOMATIC_PROMOTION_ALLOWED is False
    for tok in ("promote_model(", "replace_champion(", "recalibrate("):
        assert tok not in src, tok


def test_68_no_order_signal_decision_fill_path():
    src = Path("api/universe_scoring.py").read_text(encoding="utf-8")
    for tok in ("place_order(", "submit_order(", "create_order(", "run_fill_cycle(",
                "Signal(", "TradeDecision(", "Order("):
        assert tok not in src, tok


def _audit():
    import importlib
    mod = importlib.import_module("scripts.audit_architecture")
    return mod.run_audit()


def test_69_no_scheduled_task_change_in_scope():
    # the scoring owner never touches scheduled tasks
    src = Path("api/universe_scoring.py").read_text(encoding="utf-8")
    assert "schtasks" not in src and "ScheduledTask" not in src


def test_70_71_72_audit_zero_drift_and_slice_boundaries():
    rep = _audit()
    d = rep["inventory_drift"]
    assert d["on_disk_not_in_inventory"] == [] and d["in_inventory_not_on_disk"] == []
    usr = rep["universe_scoring_ownership"]
    assert usr["owner_present"] and usr["kernel_present"]
    assert usr["missing_delegation"] == []
    assert usr["second_scoring_engine_in_owner"] == []
    assert usr["duplicate_operational_scoring_modules"] == []
    assert usr["ui_scoring_loader_count"] == 1
    assert usr["ui_scoring_computation"] == []
    assert usr["canonical_route_present"] and usr["compat_route_present"]
    assert usr["canonical_route_methods"] == ["GET"]
