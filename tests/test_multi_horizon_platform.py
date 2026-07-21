"""
tests/test_multi_horizon_platform.py — Phase 25 multi-horizon paper alpha platform (Track A).

Fully offline: synthetic owned-style fundamental panel / momentum / risk / sector-map CSVs are wired
via env seams and the append-only ledger is redirected to a tmp dir.  Covers the versioned model
contract, exact composite_sn / mom_6_1 pass-through (no re-optimization), PIT / membership /
staleness rules, score normalization, the fixed 50/50 combined model over the common eligible
universe, the six deterministic books (sector cap, tie-breaking, equal weight), cadence + review-due
logic, the daily operating state, the recommendation engine + reason codes, the blocked reversal
model, the inactive fast sleeve, historical reconstruction + transaction costs + no look-ahead,
the append-only / idempotent / preview-no-write snapshot ledger, the API safety fields and auth,
runtime caching, and the UI static contract (badges, sections, no native dialogs, no order button).
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api import alpha_target as at
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_history as hist
from paper_trader.api import multi_horizon_ledger as ledger
from paper_trader.api import multi_horizon_platform as plat
from paper_trader.api import multi_horizon_registry as mreg
from paper_trader.api.app import app
from paper_trader.config import get_settings

_KEY = "mhz-test-key"
_AUTH = {"X-API-Key": _KEY}
_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


# --------------------------------------------------------------------------- #
# Synthetic owned-style fixtures
# --------------------------------------------------------------------------- #
_TICKERS = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"]
_SECTORS = {"AAA": "Tech", "BBB": "Tech", "CCC": "Health", "DDD": "Health",
            "EEE": "Energy", "FFF": "Energy", "GGG": "Staples", "HHH": "Staples"}


def _write_fund_panel(path: Path, month="2026-05", extra_months=("2026-02",)) -> None:
    """Frozen-panel-style CSV: latest month + earlier months, composite_sn descending by ticker order."""
    cols = ["as_of_date", "rebalance_date", "ticker", "sector", "liquidity_proxy",
            "composite_sn", "forward_63d_return", "has_forward_return"]
    rows = []
    for m in list(extra_months) + [month]:
        for i, tk in enumerate(_TICKERS):
            rows.append({
                "as_of_date": "2026-06-30", "rebalance_date": f"{m}-22", "ticker": tk,
                "sector": _SECTORS[tk] if tk != "HHH" else "Unknown",   # HHH resolved via sector map
                "liquidity_proxy": 1e6 * (i + 1),
                "composite_sn": round(2.0 - 0.4 * i + (0.05 if m == month else 0.0), 4),
                "forward_63d_return": round(0.05 - 0.01 * i, 4), "has_forward_return": "True"})
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def _write_mom_current(path: Path, market_date="2026-07-17", month="2026-07") -> None:
    cols = ["ticker", "mom_6_1", "is_member", "adv_dollar", "realized_vol_63d", "trailing_obs_126",
            "eligible_history", "extreme_flag", "sector", "market_as_of_date", "month_label"]
    rows = []
    for i, tk in enumerate(_TICKERS):
        rows.append({"ticker": tk, "mom_6_1": round(0.8 - 0.2 * i, 4), "is_member": 1,
                     "adv_dollar": 5e7 * (i + 1), "realized_vol_63d": 0.3, "trailing_obs_126": 126,
                     "eligible_history": 1, "extreme_flag": 0, "sector": "Unknown",
                     "market_as_of_date": market_date, "month_label": month})
    # extra rows exercising the eligibility gates
    rows.append({"ticker": "XLOW", "mom_6_1": 0.9, "is_member": 1, "adv_dollar": 1e6,
                 "realized_vol_63d": 0.3, "trailing_obs_126": 126, "eligible_history": 1,
                 "extreme_flag": 0, "sector": "Tech", "market_as_of_date": market_date, "month_label": month})
    rows.append({"ticker": "XEXT", "mom_6_1": 9.9, "is_member": 1, "adv_dollar": 9e8,
                 "realized_vol_63d": 0.3, "trailing_obs_126": 126, "eligible_history": 1,
                 "extreme_flag": 1, "sector": "Tech", "market_as_of_date": market_date, "month_label": month})
    rows.append({"ticker": "XNON", "mom_6_1": 0.5, "is_member": 0, "adv_dollar": 9e8,
                 "realized_vol_63d": 0.3, "trailing_obs_126": 126, "eligible_history": 1,
                 "extreme_flag": 0, "sector": "Tech", "market_as_of_date": market_date, "month_label": month})
    rows.append({"ticker": "XHIS", "mom_6_1": 0.4, "is_member": 1, "adv_dollar": 9e8,
                 "realized_vol_63d": 0.3, "trailing_obs_126": 60, "eligible_history": 0,
                 "extreme_flag": 0, "sector": "Tech", "market_as_of_date": market_date, "month_label": month})
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def _write_risk(path: Path) -> None:
    cols = ["ticker", "realized_vol_63d", "beta_universe", "adv_dollar_20d", "max_drawdown_252d",
            "is_current_member", "last_price_date", "sector"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for tk in _TICKERS:
            w.writerow({"ticker": tk, "realized_vol_63d": 0.25, "beta_universe": 1.0,
                        "adv_dollar_20d": 5e8, "max_drawdown_252d": -0.1, "is_current_member": 1,
                        "last_price_date": "2026-07-17", "sector": _SECTORS[tk]})


def _write_mom_monthly(path: Path, months=("2026-03", "2026-04", "2026-05")) -> None:
    cols = ["month", "market_date", "ticker", "mom_6_1", "fwd_1m_return", "is_member",
            "adv_dollar", "realized_vol_63d", "eligible_history", "sector"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for m in months:
            for i, tk in enumerate(_TICKERS):
                w.writerow({"month": m, "market_date": f"{m}-28", "ticker": tk,
                            "mom_6_1": round(0.8 - 0.2 * i, 4), "fwd_1m_return": round(0.03 - 0.005 * i, 4),
                            "is_member": 1, "adv_dollar": 5e8, "realized_vol_63d": 0.3,
                            "eligible_history": 1, "sector": _SECTORS[tk]})


def _write_sector_map(path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["ticker", "original_sector", "repaired_sector"])
        w.writeheader()
        w.writerow({"ticker": "HHH", "original_sector": "Unknown", "repaired_sector": "Staples"})
        for tk in _TICKERS[:-1]:
            w.writerow({"ticker": tk, "original_sector": "Unknown", "repaired_sector": _SECTORS[tk]})


@pytest.fixture
def env(monkeypatch, tmp_path):
    panel = tmp_path / "panel.csv"
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    led = tmp_path / "ledger"
    smap = tmp_path / "sector_map.csv"
    _write_fund_panel(panel)
    _write_mom_current(inputs / eng.CUR_MOM_FILE)
    _write_risk(inputs / eng.RISK_FILE)
    _write_mom_monthly(inputs / eng.MONTHLY_PANEL_FILE)
    _write_sector_map(smap)
    monkeypatch.setenv(eng.PANEL_ENV, str(panel))
    monkeypatch.setenv(eng.INPUTS_ENV, str(inputs))
    monkeypatch.setenv(eng.SECTOR_MAP_ENV, str(smap))
    monkeypatch.setenv(ledger.LEDGER_DIR_ENV, str(led))
    monkeypatch.setenv(plat.FAST_SPEC_ENV, str(tmp_path / "no_fast_spec.json"))
    # Phase 27A.2: pin the freshness-gate clock to the fixtures' market date
    # (Fri 2026-07-17 after the close) and scale the complete-target contract to
    # this synthetic 8-name world so the endpoint confirm gate evaluates the
    # same way regardless of the real run date.
    from datetime import datetime, timezone
    monkeypatch.setattr(at, "_now_override",
                        datetime(2026, 7, 17, 21, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(at, "REQUIRED_TARGET_COUNT", 8)
    monkeypatch.setattr(at, "_VALUATION_LOADER", lambda: {"current_mark": {}})
    eng.clear_cache()
    plat.clear_caches()
    yield {"panel": panel, "inputs": inputs, "ledger": led, "tmp": tmp_path}
    eng.clear_cache()
    plat.clear_caches()


@pytest.fixture
def client(env, monkeypatch) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL",
                       "postgresql+psycopg2://unused:unused@localhost:5432/unused")
    get_settings.cache_clear()
    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------- #
# A1 — versioned model + sleeve contract
# --------------------------------------------------------------------------- #
class TestModelContract:
    def test_registry_has_all_required_models(self):
        ids = {m["model_id"] for m in mreg.model_registry()}
        assert {"composite_sn", "mom_6_1", "lowvol_12m_sn", "ivol_spy_sn",
                "short_reversal_close_to_close", "fundamental_momentum_50_50_v1"} <= ids

    def test_contract_fields_complete(self):
        required = ["model_id", "model_version", "display_name", "family", "formula",
                    "required_inputs", "universe", "pit_rules", "observation_frequency",
                    "signal_horizon", "expected_holding_period", "rebalance_frequency",
                    "evaluation_frequency", "next_manual_review", "transaction_cost_assumption",
                    "eligibility_filters", "validation_evidence", "source_phase",
                    "correlation_cluster", "deployment_status", "actionability",
                    "safety_classification", "reproducibility_fingerprint"]
        for m in mreg.model_registry():
            for f in required:
                assert f in m, (m["model_id"], f)

    def test_statuses_are_the_phase25_vocabulary(self):
        for m in mreg.model_registry():
            assert m["deployment_status"] in mreg.ALL_STATUSES

    def test_reversal_blocked_from_recommendations(self):
        rev = mreg.model_by_id("short_reversal_close_to_close")
        assert rev["deployment_status"] == mreg.STATUS_INFO_ONLY
        assert rev["blocked_from_recommendations"] is True
        assert "short_reversal_close_to_close" not in mreg.recommendation_eligible_model_ids()

    def test_diagnostics_not_recommendation_eligible(self):
        elig = mreg.recommendation_eligible_model_ids()
        assert "lowvol_12m_sn" not in elig and "ivol_spy_sn" not in elig
        assert set(elig) == {"composite_sn", "mom_6_1", "fundamental_momentum_50_50_v1"}

    def test_sleeves_cadences_and_primary(self):
        sl = {s["sleeve_id"]: s for s in mreg.sleeve_registry()}
        assert sl["fundamental"]["cadence"] == "quarterly"
        assert sl["momentum"]["cadence"] == "monthly"
        assert sl["combined"]["cadence"] == "monthly" and sl["combined"]["is_primary"]
        assert sl["defensive_risk_overlay"]["action_generation_enabled"] is False
        assert sl["fast"]["action_generation_enabled"] is False
        assert sl["fast"]["fast_status"] == mreg.NO_VALIDATED_FAST_ALPHA

    def test_safety_block_fields(self):
        sb = mreg.safety_block()
        assert sb["paper_only"] is True and sb["orders_enabled"] is False
        assert sb["automation_enabled"] is False and sb["champion_replaced"] is False
        assert sb["validated_fast_alpha_available"] is False
        assert sb["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"


# --------------------------------------------------------------------------- #
# A2/A3 — input pipeline + exact score pass-through
# --------------------------------------------------------------------------- #
class TestScores:
    def test_exact_composite_sn_pass_through(self, env):
        cur = eng.build_current(use_cache=False)
        assert cur["status"] == eng.STATUS_READY
        # composite_sn raw values are exactly the panel values for the latest month (no re-optimization)
        assert cur["scores"]["composite_sn"]["AAA"]["raw_signal"] == pytest.approx(2.05)
        assert cur["scores"]["composite_sn"]["HHH"]["raw_signal"] == pytest.approx(2.05 - 0.4 * 7)

    def test_exact_mom_6_1_pass_through(self, env):
        cur = eng.build_current(use_cache=False)
        assert cur["scores"]["mom_6_1"]["AAA"]["raw_signal"] == pytest.approx(0.8)
        assert cur["scores"]["mom_6_1"]["DDD"]["raw_signal"] == pytest.approx(0.2)

    def test_latest_month_only_no_lookahead(self, env):
        # the extra earlier month (2026-02) must NOT leak into the current cross-section
        cur = eng.build_current(use_cache=False)
        assert cur["fundamental_month"] == "2026-05"
        assert cur["fundamental_as_of_date"] == "2026-05-22"

    def test_membership_and_history_and_extreme_gates(self, env):
        cur = eng.build_current(use_cache=False)
        mom = cur["scores"]["mom_6_1"]
        assert mom["XNON"]["eligible"] is False and mom["XNON"]["exclusion_reason"] == "NOT_CURRENT_MEMBER"
        assert mom["XHIS"]["eligible"] is False and mom["XHIS"]["exclusion_reason"] == "MOMENTUM_HISTORY_INSUFFICIENT"
        assert mom["XEXT"]["eligible"] is False and mom["XEXT"]["exclusion_reason"] == "DATA_QUALITY_BLOCK"
        assert mom["XLOW"]["eligible"] is False and mom["XLOW"]["exclusion_reason"] == "LIQUIDITY_FILTER_FAILED"

    def test_score_normalization_percentiles(self, env):
        cur = eng.build_current(use_cache=False)
        mom = cur["scores"]["mom_6_1"]
        assert mom["AAA"]["percentile"] == pytest.approx(1.0)   # best
        assert mom["HHH"]["percentile"] == pytest.approx(0.0)   # worst of the 8 eligible
        assert mom["AAA"]["rank"] == 1

    def test_stale_fundamental_rule(self):
        assert eng._is_fundamental_stale("2026-01", "2026-07-17") is True     # 6 months
        assert eng._is_fundamental_stale("2026-05", "2026-07-17") is False    # 2 months

    def test_sector_map_fills_unknown(self, env):
        cur = eng.build_current(use_cache=False)
        assert cur["scores"]["composite_sn"]["HHH"]["sector"] == "Staples"


# --------------------------------------------------------------------------- #
# A4 — fixed 50/50 combined over the common eligible universe
# --------------------------------------------------------------------------- #
class TestCombined:
    def test_fixed_primary_weights(self):
        assert eng.PRIMARY_WEIGHTS == {"composite_sn": 0.5, "mom_6_1": 0.5}
        assert set(eng.SENSITIVITY_VIEWS) == {"fund30_mom70", "fund70_mom30"}

    def test_common_universe_only(self, env):
        cur = eng.build_current(use_cache=False)
        common = set(cur["combined"]["common_universe"])
        assert common == set(_TICKERS)          # only names eligible in BOTH legs
        assert "XLOW" not in common and "XNON" not in common

    def test_combined_score_is_50_50_rank_blend(self, env):
        cur = eng.build_current(use_cache=False)
        c = cur["combined"]["combined"]["AAA"]
        assert c["combined_score"] == pytest.approx(0.5 * c["fund_percentile"] + 0.5 * c["mom_percentile"])
        assert c["component_contributions"]["composite_sn"] == pytest.approx(0.5 * c["fund_percentile"])

    def test_sensitivity_views_present_but_separate(self, env):
        cur = eng.build_current(use_cache=False)
        c = cur["combined"]["combined"]["AAA"]
        assert set(c["sensitivity_ranks"]) == {"fund30_mom70", "fund70_mom30"}


# --------------------------------------------------------------------------- #
# A5 — six books, sector cap, deterministic tie-breaking
# --------------------------------------------------------------------------- #
class TestBooks:
    def test_six_books_exist(self, env):
        cur = eng.build_current(use_cache=False)
        assert set(cur["books"]["books"]) == {
            "composite_sn_top25", "composite_sn_top50", "mom_6_1_top25", "mom_6_1_top50",
            "fundamental_momentum_50_50_top25", "fundamental_momentum_50_50_top50"}
        assert cur["books"]["primary_book_id"] == "fundamental_momentum_50_50_top25"

    def test_equal_weight_and_max_weight(self, env):
        cur = eng.build_current(use_cache=False)
        bk = cur["books"]["books"]["fundamental_momentum_50_50_top25"]
        n = bk["size_actual"]
        # equal weight, hard-capped at the max individual weight; a thin synthetic book (n=8 here)
        # caps at 10% and reports the remainder as unallocated (implied cash).
        assert bk["equal_weight"] == pytest.approx(min(1.0 / n, eng.MAX_INDIVIDUAL_WEIGHT))
        assert bk["equal_weight"] <= eng.MAX_INDIVIDUAL_WEIGHT + 1e-9
        assert bk["unallocated_weight"] == pytest.approx(max(0.0, 1.0 - bk["equal_weight"] * n), abs=1e-6)

    def test_sector_cap(self):
        rows = [{"ticker": f"T{i:02d}", "score": 10 - i, "sector": "Tech", "adv_dollar": 1e9}
                for i in range(10)]
        rows += [{"ticker": f"U{i:02d}", "score": 5 - i, "sector": "Health", "adv_dollar": 1e9}
                 for i in range(10)]
        book = eng._select_book(rows, 8)
        max_per = max(1, int(eng.SECTOR_CAP_FRACTION * 8))
        by_sec = {}
        for c in book["constituents"]:
            by_sec[c["sector"]] = by_sec.get(c["sector"], 0) + 1
        assert all(v <= max_per for v in by_sec.values())

    def test_deterministic_tie_breaking(self):
        vals = {"ZZZ": 1.0, "AAA": 1.0, "MMM": 1.0}
        pct, ranks, _z = eng._percentiles(vals)
        assert ranks["AAA"] == 1 and ranks["MMM"] == 2 and ranks["ZZZ"] == 3  # ticker asc on ties


# --------------------------------------------------------------------------- #
# A6 — cadence + operating state
# --------------------------------------------------------------------------- #
class TestOperatingState:
    def test_next_review_dates(self):
        assert eng.next_review_date("monthly", "2026-07-17") == "2026-08-01"
        assert eng.next_review_date("monthly", "2026-12-17") == "2027-01-01"
        assert eng.next_review_date("quarterly", "2026-07-17") == "2026-10-01"
        assert eng.next_review_date("quarterly", "2026-11-02") == "2027-01-01"

    def test_first_run_requires_manual_confirmation(self, env):
        cur = eng.build_current(use_cache=False)
        st = eng.compute_operating_state(cur, prior=None)
        assert st["operating_state"] == eng.STATE_MANUAL_CONFIRMATION_REQUIRED
        assert st["portfolio_review_due"] is True

    def test_confirmed_current_period_gives_risk_refresh_only(self, env):
        cur = eng.build_current(use_cache=False)
        prior = {sid: {"period": ("2026-05" if sid == "fundamental" else "2026-07"),
                       "constituents_top25": [], "confirmed_at": "x"}
                 for sid in ("fundamental", "momentum", "combined")}
        st = eng.compute_operating_state(cur, prior=prior)
        assert st["operating_state"] == eng.STATE_RISK_REFRESH_ONLY
        assert st["no_change_required"] is True

    def test_data_blocked_when_inputs_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv(eng.PANEL_ENV, str(tmp_path / "missing.csv"))
        monkeypatch.setenv(eng.INPUTS_ENV, str(tmp_path))
        eng.clear_cache()
        cur = eng.build_current(use_cache=False)
        st = eng.compute_operating_state(cur, prior=None)
        assert st["operating_state"] == eng.STATE_DATA_BLOCKED

    def test_fast_sleeve_inactive_without_validation(self, env):
        cur = eng.build_current(use_cache=False)
        st = eng.compute_operating_state(cur, prior=None, validated_fast_alpha_available=False)
        fast = [s for s in st["sleeves"] if s["sleeve_id"] == "fast"][0]
        assert fast["action_generation_enabled"] is False
        assert fast["fast_status"] == mreg.NO_VALIDATED_FAST_ALPHA
        assert fast["current_actionability"] == mreg.ACT_INACTIVE

    def test_defensive_sleeve_is_risk_only(self, env):
        cur = eng.build_current(use_cache=False)
        st = eng.compute_operating_state(cur, prior=None)
        d = [s for s in st["sleeves"] if s["sleeve_id"] == "defensive_risk_overlay"][0]
        assert d["risk_only_refresh"] is True and d["action_generation_enabled"] is False


# --------------------------------------------------------------------------- #
# A7 — recommendations + reason codes + buffers
# --------------------------------------------------------------------------- #
class TestRecommendations:
    def test_first_run_all_buy_candidates(self, env):
        cur = eng.build_current(use_cache=False)
        rec = eng.compute_recommendations(cur, prior=None, sleeve_id="combined", size=25)
        assert rec["counts"] == {"BUY_CANDIDATE": len(rec["recommendations"])}
        assert all("ENTERED_TOP_25" in r["reason_codes"] for r in rec["recommendations"])
        assert rec["estimated_turnover"] == 1.0

    def test_no_plain_buy_or_sell(self, env):
        cur = eng.build_current(use_cache=False)
        rec = eng.compute_recommendations(cur, prior=None, sleeve_id="combined", size=25)
        for r in rec["recommendations"]:
            assert r["recommendation"] in ("BUY_CANDIDATE", "HOLD", "REDUCE_CANDIDATE",
                                           "EXIT_CANDIDATE", "WAIT")

    def test_matching_prior_gives_holds(self, env):
        cur = eng.build_current(use_cache=False)
        tgt = [c["ticker"] for c in cur["books"]["books"]["fundamental_momentum_50_50_top25"]["constituents"]]
        prior = {"combined": {"period": cur["momentum_month"], "constituents_top25": tgt,
                              "confirmed_at": "x"}}
        rec = eng.compute_recommendations(cur, prior=prior, sleeve_id="combined", size=25)
        assert set(rec["counts"]) == {"HOLD"}
        assert rec["estimated_turnover"] == 0.0

    def test_exit_buffer(self, env):
        cur = eng.build_current(use_cache=False)
        # size=4 with a 1-per-sector cap -> target book is AAA/CCC/EEE/GGG (one per sector).
        # BBB ranks 2nd overall (within the exit-buffer rank ceil(4*1.2)=5) but is sector-capped
        # out of the target -> a prior holding of BBB stays HOLD (within buffer).
        # HHH ranks 8th (outside the buffer) -> a prior holding of HHH becomes EXIT_CANDIDATE.
        size = 4
        inside_buffer, outside_buffer = "BBB", "HHH"
        prior = {"combined": {"period": "2026-01",  # stale -> review due
                              "constituents_top4": [inside_buffer, outside_buffer],
                              "confirmed_at": "x"}}
        rec = eng.compute_recommendations(cur, prior=prior, sleeve_id="combined", size=size)
        by_tk = {r["ticker"]: r for r in rec["recommendations"]}
        assert by_tk[inside_buffer]["recommendation"] == "HOLD"
        assert any(c.startswith("WITHIN_EXIT_BUFFER") for c in by_tk[inside_buffer]["reason_codes"])
        assert by_tk[outside_buffer]["recommendation"] == "EXIT_CANDIDATE"
        assert "FELL_BELOW_EXIT_BUFFER" in by_tk[outside_buffer]["reason_codes"]

    def test_review_not_due_gives_hold_wait(self, env):
        cur = eng.build_current(use_cache=False)
        tgt = [c["ticker"] for c in cur["books"]["books"]["fundamental_momentum_50_50_top25"]["constituents"]]
        prior = {"combined": {"period": cur["momentum_month"], "constituents_top25": tgt,
                              "confirmed_at": "x"}}
        rec = eng.compute_recommendations(cur, prior=prior, sleeve_id="combined", size=25,
                                          review_due=False)
        for r in rec["recommendations"]:
            assert r["recommendation"] in ("HOLD", "WAIT")
            assert "REVIEW_NOT_DUE" in r["reason_codes"]

    def test_component_reason_codes(self, env):
        cur = eng.build_current(use_cache=False)
        rec = eng.compute_recommendations(cur, prior=None, sleeve_id="combined", size=25)
        top = rec["recommendations"][0]
        assert "BOTH_ALPHA_LEGS_POSITIVE" in top["reason_codes"]

    def test_estimated_cost_uses_25bps(self, env):
        cur = eng.build_current(use_cache=False)
        rec = eng.compute_recommendations(cur, prior=None, sleeve_id="combined", size=25)
        assert rec["estimated_transaction_cost"] == pytest.approx(2 * 0.0025 * rec["estimated_turnover"])

    def test_blocked_model_notice(self):
        notice = eng.blocked_model_notice()["short_reversal_close_to_close"]
        assert notice["recommendations_generated"] is False
        assert "FAST_MODEL_NOT_VALIDATED" in notice["reason_codes"]
        assert "INFORMATION_ONLY_SIGNAL" in notice["reason_codes"]


# --------------------------------------------------------------------------- #
# A8 — historical reconstruction, costs, no look-ahead
# --------------------------------------------------------------------------- #
class TestHistory:
    def test_book_series_net_is_gross_minus_cost_times_turnover(self):
        secs = {"A": "Tech", "B": "Health", "C": "Energy", "D": "Staples"}
        monthly = {m: {tk: {"composite_sn": 2.0 - i, "sector": secs[tk], "fwd63": 0.02,
                            "has_fwd": True}
                       for i, tk in enumerate(["A", "B", "C", "D"])}
                   for m in ("2025-01", "2025-02")}
        s = hist._book_series(monthly, "composite_sn", "fwd63", 2, ["2025-01", "2025-02"])
        assert len(s) == 2
        assert s[0]["turnover"] == 1.0 and s[0]["established"] is True
        assert s[1]["turnover"] == 0.0
        assert s[0]["net"] == pytest.approx(s[0]["gross"] - hist.COST25 * 1.0)
        assert s[1]["net"] == pytest.approx(s[1]["gross"])

    def test_carry_forward_no_lookahead(self):
        fund_monthly = {"2025-03": {"A": {"composite_sn": 1.0, "sector": "Tech", "fwd63": 0.0,
                                          "has_fwd": True}}}
        cf = hist.build_fund_carryforward(fund_monthly, ["2025-01", "2025-03", "2025-05", "2025-09"])
        assert "A" not in cf["2025-01"]           # future fundamental never visible earlier
        assert cf["2025-03"]["A"]["composite_sn"] == 1.0
        assert cf["2025-05"]["A"]["composite_sn"] == 1.0   # carried forward within a quarter
        assert "A" not in cf["2025-09"]           # expired after max_stale_months

    def test_full_history_runs_on_synthetic_inputs(self, env):
        h = hist.build_history()
        assert h["status"] == "MHZ_HISTORY_READY"
        assert h["cost_assumption_bps"] == 25
        assert "fundamental_momentum_50_50_top25" in h["books"]
        assert "reconciliation" in h and "combined_lift" in h


# --------------------------------------------------------------------------- #
# A9 — append-only ledger: preview no-write, idempotent confirm, dedicated store
# --------------------------------------------------------------------------- #
class TestLedger:
    def test_preview_writes_nothing(self, env):
        out = ledger.preview_snapshot()
        assert out["status"] == ledger.STATUS_PREVIEW
        assert out["performed_write"] is False
        assert not (env["ledger"] / ledger.SNAPSHOTS_FILE).exists()

    def test_confirm_requires_token(self, env):
        out = ledger.confirm_snapshot(confirm=None)
        assert out["status"] == ledger.STATUS_CONFIRM_REQUIRED
        assert out["performed_write"] is False
        out2 = ledger.confirm_snapshot(confirm="WRONG_TOKEN")
        assert out2["status"] == ledger.STATUS_CONFIRM_REQUIRED

    def test_confirm_appends_and_is_idempotent(self, env):
        out = ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
        assert out["status"] == ledger.STATUS_CONFIRMED and out["performed_write"] is True
        dup = ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
        assert dup["status"] == ledger.STATUS_SKIPPED_DUPLICATE
        assert dup["performed_write"] is False
        snaps = ledger.list_snapshots()
        assert snaps["n_snapshots"] == 1 and snaps["n_confirmed"] == 1

    def test_append_only_never_rewrites(self, env, monkeypatch):
        ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
        first = ledger.list_snapshots()["snapshots"][0]
        # change the momentum month -> a new period -> a second (different) snapshot appends
        _write_mom_current(env["inputs"] / eng.CUR_MOM_FILE, market_date="2026-08-14", month="2026-08")
        eng.clear_cache()
        out2 = ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
        assert out2["status"] == ledger.STATUS_CONFIRMED
        snaps = ledger.list_snapshots()
        assert snaps["n_snapshots"] == 2
        assert snaps["snapshots"][0]["snapshot_id"] == first["snapshot_id"]  # original untouched

    def test_confirm_writes_only_the_dedicated_ledger(self, env):
        ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
        files = {p.name for p in env["ledger"].iterdir()}
        assert files == {ledger.SNAPSHOTS_FILE}

    def test_snapshot_payload_shape(self, env):
        out = ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
        s = out["snapshot"]
        for f in ("snapshot_id", "calculation_timestamp", "market_as_of_date", "model_versions",
                  "input_fingerprints", "sleeves", "primary_book_id", "risks",
                  "confirmation_status", "immutable", "creation_record"):
            assert f in s
        assert s["immutable"] is True
        assert s["safety"]["no_orders"] is True

    def test_confirmed_snapshot_becomes_prior(self, env):
        ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
        prior = ledger.latest_confirmed_by_sleeve()
        assert "combined" in prior and prior["combined"]["period"] == "2026-07"


# --------------------------------------------------------------------------- #
# A10 — API endpoints: auth, safety fields, read-only, caching
# --------------------------------------------------------------------------- #
_GET_ROUTES = [
    "/v1/research/alpha-models", "/v1/research/alpha-sleeves",
    "/v1/research/alpha-operating-state", "/v1/research/current-alpha-scores",
    "/v1/research/current-alpha-books", "/v1/research/current-alpha-recommendations",
    "/v1/research/alpha-book-comparison", "/v1/research/alpha-paper-history",
    "/v1/research/alpha-paper-snapshots",
]


class TestEndpoints:
    def test_auth_required_on_all_routes(self, client):
        for path in _GET_ROUTES:
            assert client.get(path).status_code in (401, 403), path
        assert client.post("/v1/research/alpha-paper-snapshots/preview").status_code in (401, 403)
        assert client.post("/v1/research/alpha-paper-snapshots/confirm").status_code in (401, 403)

    def test_all_gets_return_safety_fields(self, client):
        for path in _GET_ROUTES:
            body = client.get(path, headers=_AUTH).json()
            assert body["paper_only"] is True, path
            assert body["orders_enabled"] is False, path
            assert body["automation_enabled"] is False, path
            assert body["champion_replaced"] is False, path
            assert body["validated_fast_alpha_available"] is False, path

    def test_models_route_registers_reversal_blocked(self, client):
        body = client.get("/v1/research/alpha-models", headers=_AUTH).json()
        rev = [m for m in body["models"] if m["model_id"] == "short_reversal_close_to_close"][0]
        assert rev["deployment_status"] == "INFORMATION_ONLY_NOT_TRADABLE"
        assert rev["blocked_from_recommendations"] is True

    def test_operating_state_route(self, client):
        body = client.get("/v1/research/alpha-operating-state", headers=_AUTH).json()
        assert body["operating_state"] in (
            "NO_REVIEW_DUE", "RISK_REFRESH_ONLY", "FUNDAMENTAL_REVIEW_DUE", "MOMENTUM_REVIEW_DUE",
            "COMBINED_REVIEW_DUE", "FAST_REVIEW_DUE", "DATA_BLOCKED", "MANUAL_CONFIRMATION_REQUIRED")

    def test_preview_then_confirm_flow(self, client, env):
        pv = client.post("/v1/research/alpha-paper-snapshots/preview", headers=_AUTH).json()
        assert pv["status"] == "MHZ_SNAPSHOT_PREVIEW" and pv["performed_write"] is False
        assert not (env["ledger"] / ledger.SNAPSHOTS_FILE).exists()
        bad = client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                          json={"confirm": "WRONG"})
        assert bad.status_code == 400
        ok = client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                         json={"confirm": ledger.CONFIRM_TOKEN}).json()
        assert ok["status"] == "MHZ_SNAPSHOT_CONFIRMED" and ok["performed_write"] is True
        one = client.get(f"/v1/research/alpha-paper-snapshots/{ok['snapshot_id']}",
                         headers=_AUTH).json()
        assert one["status"] == "MHZ_SNAPSHOT_READY"
        assert one["snapshot"]["snapshot_id"] == ok["snapshot_id"]

    def test_missing_inputs_degrade_not_500(self, client, monkeypatch, tmp_path):
        monkeypatch.setenv(eng.PANEL_ENV, str(tmp_path / "missing.csv"))
        eng.clear_cache()
        plat.clear_caches()
        r = client.get("/v1/research/current-alpha-scores", headers=_AUTH)
        assert r.status_code == 200
        assert r.json()["status"] == eng.STATUS_INPUTS_UNAVAILABLE

    def test_runtime_caching(self, env):
        a = eng.build_current()
        b = eng.build_current()
        assert a is b     # mtime-keyed in-process cache

    def test_get_routes_write_nothing(self, client, env):
        for path in _GET_ROUTES:
            client.get(path, headers=_AUTH)
        assert not (env["ledger"] / ledger.SNAPSHOTS_FILE).exists()


# --------------------------------------------------------------------------- #
# Source-safety: no order / signal / trade-decision / DB writes in the modules
# --------------------------------------------------------------------------- #
class TestSourceSafety:
    def _sources(self):
        base = Path(__file__).resolve().parents[1] / "api"
        return "\n".join((base / f).read_text(encoding="utf-8") for f in (
            "multi_horizon_registry.py", "multi_horizon_engine.py", "multi_horizon_ledger.py",
            "multi_horizon_history.py", "multi_horizon_platform.py"))

    def test_no_db_or_trading_workflow_imports(self):
        # code-level patterns only (docstrings legitimately SAY "no broker" etc.)
        src = self._sources()
        for bad in ("from paper_trader.db", "import sqlalchemy", "Session(", "session.add",
                    "session.commit", "Order(", "Trade(", "Signal(", "TradeDecision(",
                    "create_order", "submit_order", "Broker("):
            assert bad not in src, bad

    def test_no_network_calls(self):
        src = self._sources()
        for bad in ("requests.", "urllib.request", "httpx", "socket."):
            assert bad not in src, bad

    def test_no_credentials(self):
        src = self._sources()
        for bad in ("api_key", "API_KEY", "password", "secret"):
            # the modules never read or embed credentials
            assert bad.lower() not in src.lower() or bad == "api_key" and "api_key" not in src.lower(), bad


# --------------------------------------------------------------------------- #
# A11 — UI static contract
# --------------------------------------------------------------------------- #
class TestUiStatic:
    def _html(self):
        return _UI.read_text(encoding="utf-8")

    def test_page_and_nav_present(self):
        html = self._html()
        assert "MULTI-HORIZON ALPHA PORTFOLIO" in html
        assert 'id="tab-multi-horizon"' in html
        assert 'data-route="multi-horizon"' in html
        assert "'multi-horizon': 'multi-horizon'" in html

    def test_required_sections(self):
        html = self._html()
        for sec in ("Model Sleeves", "Primary Recommendations", "Book Comparison",
                    "Performance", "Daily Monitoring", "Manual Snapshot"):
            assert sec in html, sec
        for el in ("mhz-sleeves", "mhz-recs", "mhz-books", "mhz-perf", "mhz-monitoring",
                   "mhz-confirm-box", "mhz-audit"):
            assert f'id="{el}"' in html, el

    def test_safety_badges_visible(self):
        html = self._html()
        for b in ("PAPER ONLY", "ORDERS DISABLED", "AUTOMATION OFF", "MANUAL REVIEW",
                  "NO LIVE PROMOTION"):
            assert b in html, b

    def test_fast_status_shown(self):
        html = self._html()
        assert "NO_VALIDATED_FAST_ALPHA" in html
        assert 'id="mhz-fast-badge"' in html

    def test_no_native_dialogs(self):
        html = self._html()
        assert len(re.findall(r"(?<![A-Za-z0-9_])alert\s*\(", html)) == 0
        assert len(re.findall(r"(?<![A-Za-z0-9_])confirm\s*\(", html)) == 0
        assert len(re.findall(r"(?<![A-Za-z0-9_])prompt\s*\(", html)) == 0

    def test_styled_in_page_confirmation(self):
        html = self._html()
        assert "mhzConfirmSnapshot" in html and "mhzCancelConfirm" in html
        assert "Confirm Paper Snapshot" in html and "Preview Snapshot" in html

    def test_no_order_creation_controls(self):
        html = self._html()
        mhz = html[html.index('id="tab-multi-horizon"'):html.index('id="tab-audit-advanced"')]
        assert "Create Order" not in mhz
        assert "Submit Order" not in mhz
        assert "Connect to Load" not in mhz

    def test_loader_functions_defined(self):
        html = self._html()
        for fn in ("loadMultiHorizon", "renderMhzSleeves", "renderMhzRecommendations",
                   "renderMhzBooks", "renderMhzPerformance", "renderMhzMonitoring",
                   "mhzPreviewSnapshot"):
            assert fn in html, fn
