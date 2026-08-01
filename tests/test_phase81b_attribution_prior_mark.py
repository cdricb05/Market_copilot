"""
tests/test_phase81b_attribution_prior_mark.py

STAGE 8.1B — DAILY ATTRIBUTION PRIOR-MARK CONSISTENCY REPAIR.

ROOT CAUSE proven by the live July-31 close: the desk mark store (desk_marks.json)
is a provider CACHE replaced whole on every sync, so a July-31 dividend re-adjustment
of VLO silently shifted the already-completed July-30 close from $311.71 to $310.51.
The immutable July-30 NAV row still embedded $311.71, but attribution recomputed BOTH
legs off the re-adjusted cache — over-stating VLO's contribution by 12 x $1.20 = $14.40
and breaking reconciliation (residual -$14.40, reconciles=false).

THE FIX: attribution reads prior/current per-position prices from the IMMUTABLE,
first-write-wins completed-close ledger (forward_prediction_prices.json), which records
each completed close once and never restates it. The desk cache is only an explicit,
FLAGGED per-(ticker,date) fallback for dates the ledger predates.

These deterministic, fully-offline tests prove the ten required behaviours (WS4):
  1. attribution uses the exact prior completed desk mark (the immutable ledger);
  2. a later provider refresh cannot replace a historical prior mark;
  3. duplicate date prices resolve to a deterministic canonical mark identity;
  4. adjusted/unadjusted sources cannot be silently mixed (flagged + diagnostic);
  5. the VLO-style $1.20 x 12 = $14.40 discrepancy is detected;
  6. position contributions reconcile to the NAV movement;
  7. the existing prior-day (July-30 style) attribution remains unchanged;
  8. a read-only attribution call mutates no operational records;
  9. the Telegram /attribution surface returns the reconciled canonical result;
 10. missing / inconsistent marks produce a SPECIFIC diagnostic, never a silent
     reconciles=false.

A miniature world reproduces the live drift exactly: VLO qty 12, prior close
311.71 (true) vs 310.51 (re-adjusted cache) -> 12 x 1.20 = $14.40.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from paper_trader.api import daily_close as dc
from paper_trader.api import forward_evidence as fe
from paper_trader.api import forward_prediction_skill as fps
from alpha_agent import telegram_control as tc


# --------------------------------------------------------------------------- #
# Miniature world — VLO qty 12 (Energy) + AAA qty 10 (Tech), cash 100 static.
#   invested = 10*AAA + 12*VLO ; nav = cash + invested
#   07-29: 10*49  + 12*300.16 + 100 = 4191.92
#   07-30: 10*50  + 12*311.71 + 100 = 4340.52   (VLO true completed close 311.71)
#   07-31: 10*52  + 12*312.90 + 100 = 4374.80
# NAV move 07-30->07-31 = 34.28  (AAA 20.00 + VLO 14.28)
# --------------------------------------------------------------------------- #
_LEDGER = {  # immutable first-write-wins completed-close ledger (TRUE closes)
    "AAA": [["2026-07-29", 49.0], ["2026-07-30", 50.0], ["2026-07-31", 52.0]],
    "VLO": [["2026-07-29", 300.16], ["2026-07-30", 311.71], ["2026-07-31", 312.90]],
    "SPY": [["2026-07-29", 738.0], ["2026-07-30", 741.69], ["2026-07-31", 747.03]],
}
# The desk cache AFTER a July-31 dividend re-adjustment of VLO: every VLO close
# strictly before the ex-date is scaled down (311.71 -> 310.51 = -$1.20); the
# July-31 close and every other ticker are unchanged.
_CACHE_READJUSTED = {
    "AAA": [["2026-07-29", 49.0], ["2026-07-30", 50.0], ["2026-07-31", 52.0]],
    "VLO": [["2026-07-29", 298.96], ["2026-07-30", 310.51], ["2026-07-31", 312.90]],
    "SPY": [["2026-07-29", 738.0], ["2026-07-30", 741.69], ["2026-07-31", 747.03]],
}
_PERF = [
    {"date": "2026-07-29", "nav": 4191.92, "cash": 100.0, "invested": 4091.92,
     "cumulative_return_pct": 0.0, "benchmark_cumulative_return_pct": 0.0,
     "drawdown_pct": 0.0, "transaction_cost": 0.0},
    {"date": "2026-07-30", "nav": 4340.52, "cash": 100.0, "invested": 4240.52,
     "cumulative_return_pct": 3.545, "benchmark_cumulative_return_pct": 0.5,
     "drawdown_pct": 0.0, "transaction_cost": 0.0},
    {"date": "2026-07-31", "nav": 4374.80, "cash": 100.0, "invested": 4274.80,
     "cumulative_return_pct": 4.363, "benchmark_cumulative_return_pct": 1.0,
     "drawdown_pct": 0.0, "transaction_cost": 0.0},
]
_HOLDS = [
    {"ticker": "VLO", "quantity": 12, "sector": "Energy", "average_cost": 300.0,
     "current_weight": 0.86},
    {"ticker": "AAA", "quantity": 10, "sector": "Tech", "average_cost": 49.0,
     "current_weight": 0.14},
]


def _store(series):
    return lambda desk_dir=None: {"series": {k: [list(x) for x in v]
                                             for k, v in series.items()}}


def _perf_loader(rows=_PERF):
    return lambda desk_dir=None: {"rows": [dict(r) for r in rows],
                                  "summary": {"total_transaction_cost": 0.0}}


def _ops(holds=_HOLDS, starting=4191.92):
    return {"operational_book": {"book_id": "alpha_paper_book_1",
                                 "starting_capital": starting, "holdings_detail": holds},
            "canonical_state": {"holdings_detail": holds}}


def _attr_ledger(*, market_date=None, ledger=_LEDGER, rows=_PERF, holds=_HOLDS):
    """Ledger injected as the authoritative mark source (cache disabled)."""
    return fe.build_daily_attribution(
        market_date=market_date, perf_loader=_perf_loader(rows),
        marks_loader=_store(ledger), ops=_ops(holds))


def _attr_primary_with_cache(monkeypatch, ledger, cache, *, market_date=None):
    """Immutable ledger PRIMARY + desk-cache FALLBACK (the live resolution path)."""
    monkeypatch.setattr(fe, "_MARKS_LOADER", _store(cache))
    return fe.build_daily_attribution(
        market_date=market_date, perf_loader=_perf_loader(),
        mark_ledger_loader=_store(ledger), ops=_ops())


def _vlo(attr):
    return next(h for h in attr["holdings"] if h["ticker"] == "VLO")


def _write_price_store(tmp, series):
    (Path(tmp) / "forward_prediction_prices.json").write_text(
        json.dumps({"schema_version": 1,
                    "kind": "prediction_price_store_first_write_wins",
                    "series": series, "updated_at": None}), encoding="utf-8")


def _fingerprint_dir(tmp):
    out = {}
    for p in sorted(Path(tmp).rglob("*")):
        if p.is_file():
            out[str(p.relative_to(tmp))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


# =========================================================================== #
# WS4 — the ten required behaviours.
# =========================================================================== #
class TestPriorMarkConsistency:
    def test_1_uses_exact_prior_completed_mark(self):
        v = _vlo(_attr_ledger())                          # 07-31 vs 07-30
        assert v["prior_price"] == 311.71
        assert v["prior_mark_date"] == "2026-07-30"
        assert v["prior_mark_source"] == fe.MARK_SOURCE_LEDGER
        assert v["pnl_contribution"] == pytest.approx(14.28, abs=1e-6)

    def test_2a_later_refresh_cannot_replace_prior_mark(self, monkeypatch):
        # The re-adjusted cache holds VLO 07-30 = 310.51; the immutable ledger wins.
        a = _attr_primary_with_cache(monkeypatch, _LEDGER, _CACHE_READJUSTED)
        v = _vlo(a)
        assert v["prior_price"] == 311.71                 # ledger, NOT cache 310.51
        assert v["prior_mark_source"] == fe.MARK_SOURCE_LEDGER
        assert a["reconciliation"]["reconciles"] is True

    def test_2b_price_store_is_first_write_wins(self, tmp_path):
        fps._merge_prices(tmp_path, {"VLO": [["2026-07-30", 311.71]]})
        # a later re-adjusting refresh tries to overwrite 07-30 and add 07-31
        merge = fps._merge_prices(
            tmp_path, {"VLO": [["2026-07-30", 310.51], ["2026-07-31", 312.90]]})
        got = dict(fps.read_price_store(tmp_path)["series"]["VLO"])
        assert got["2026-07-30"] == 311.71                # never overwritten
        assert got["2026-07-31"] == 312.90                # new date accepted
        assert merge["prices_already_recorded"] == 1 and merge["prices_added"] == 1

    def test_3_duplicate_date_deterministic_identity(self):
        dup = dict(_LEDGER)
        dup["VLO"] = [["2026-07-29", 300.16], ["2026-07-30", 311.71],
                      ["2026-07-30", 999.0], ["2026-07-31", 312.90]]
        a1, a2 = _attr_ledger(ledger=dup), _attr_ledger(ledger=dup)
        assert _vlo(a1)["prior_price"] == 311.71          # first-written wins
        assert _vlo(a1)["prior_price"] == _vlo(a2)["prior_price"]  # stable / deterministic

    def test_4_adjusted_unadjusted_cannot_be_mixed(self, monkeypatch):
        # ledger has VLO current only; prior 07-30 must fall back to the cache ->
        # the two legs come from different vintages and are explicitly flagged.
        ledger_partial = {"AAA": _LEDGER["AAA"], "SPY": _LEDGER["SPY"],
                          "VLO": [["2026-07-31", 312.90]]}
        a = _attr_primary_with_cache(monkeypatch, ledger_partial, _CACHE_READJUSTED)
        v = _vlo(a)
        assert v["current_mark_source"] == fe.MARK_SOURCE_LEDGER
        assert v["prior_mark_source"] == fe.MARK_SOURCE_CACHE_FALLBACK
        assert a["mark_source"]["mixed_source_tickers"] == ["VLO"]
        assert a["reconciliation"]["reconciles"] is False
        assert a["reconciliation"]["diagnostic"].startswith("MARK_SOURCE_INCONSISTENT")

    def test_5_vlo_1_20_times_12_discrepancy_detected(self):
        good = _attr_ledger()
        assert good["reconciliation"]["reconciles"] is True
        assert good["reconciliation"]["residual"] == pytest.approx(0.0, abs=1e-6)
        # forcing the re-adjusted cache as the mark source reproduces the -$14.40.
        bad = fe.build_daily_attribution(
            perf_loader=_perf_loader(), marks_loader=_store(_CACHE_READJUSTED), ops=_ops())
        assert bad["reconciliation"]["reconciles"] is False
        assert bad["reconciliation"]["residual"] == pytest.approx(-14.40, abs=1e-6)
        assert bad["reconciliation"]["diagnostic"].startswith(
            "MARK_LEDGER_DOES_NOT_REPRODUCE_NAV")
        # the discrepancy is exactly 12 shares x $1.20.
        assert (_vlo(bad)["pnl_contribution"] - _vlo(good)["pnl_contribution"]) \
            == pytest.approx(14.40, abs=1e-6)

    def test_6_contributions_reconcile_to_nav_move(self):
        a = _attr_ledger()
        r = a["reconciliation"]
        assert r["position_contribution_sum"] == pytest.approx(34.28, abs=1e-6)
        assert r["market_movement"] == pytest.approx(34.28, abs=1e-6)
        assert r["residual"] == pytest.approx(0.0, abs=1e-6)
        assert r["reconciles"] is True
        assert r["cash_contribution"] == pytest.approx(0.0, abs=1e-9)
        assert r["execution_cost_charged_today"] == pytest.approx(0.0, abs=1e-9)
        assert a["portfolio"]["daily_pnl"] == pytest.approx(34.28, abs=1e-6)

    def test_7_prior_day_attribution_unchanged(self):
        a30 = _attr_ledger(market_date="2026-07-30")      # 07-29 -> 07-30
        assert a30["reconciliation"]["reconciles"] is True
        assert _vlo(a30)["pnl_contribution"] == pytest.approx(138.60, abs=1e-6)
        hist = fe.build_attribution_history(
            perf_loader=_perf_loader(), marks_loader=_store(_LEDGER), ops=_ops())
        assert hist["count"] == 2 and all(r["reconciles"] for r in hist["rows"])

    def test_8_read_only_call_mutates_nothing(self, tmp_path):
        _write_price_store(tmp_path, _LEDGER)
        before = _fingerprint_dir(tmp_path)
        fe.build_daily_attribution(desk_dir=tmp_path, perf_loader=_perf_loader(), ops=_ops())
        fe.build_attribution_history(desk_dir=tmp_path, perf_loader=_perf_loader(), ops=_ops())
        assert _fingerprint_dir(tmp_path) == before

    def test_9_telegram_attribution_reconciled(self):
        attr = _attr_ledger()
        providers = tc.build_operational_providers(attribution_loader=lambda: attr)
        text = providers["attribution"]()
        assert "DAILY ATTRIBUTION" in text and "VLO" in text
        assert text.splitlines()[-1].endswith("OK")       # reconciliation OK, not CHECK
        assert "residual" in text and "+$14.28" in text    # corrected VLO, not 28.68
        assert "+$28.68" not in text

    def test_10_missing_marks_specific_diagnostic(self):
        # VLO absent from the mark source on both dates, but the NAV move includes it.
        ledger_no_vlo = {"AAA": _LEDGER["AAA"], "SPY": _LEDGER["SPY"]}
        a = fe.build_daily_attribution(
            perf_loader=_perf_loader(), marks_loader=_store(ledger_no_vlo), ops=_ops())
        assert a["status"] == fe.ATTRIB_COVERAGE_INCOMPLETE
        assert a["coverage"]["missing_tickers"] == ["VLO"]
        assert a["reconciliation"]["reconciles"] is False
        diag = a["reconciliation"]["diagnostic"]
        assert diag is not None and diag.startswith("MARKS_MISSING") and "VLO" in diag


# =========================================================================== #
# The daily-close attribution block resolves marks from the same immutable ledger.
# =========================================================================== #
class TestDailyCloseAttributionBlock:
    def test_daily_close_block_uses_immutable_ledger(self, tmp_path):
        _write_price_store(tmp_path, _LEDGER)
        block = dc._attribution_block(perf=_perf_loader()(), ops=_ops(), desk_dir=tmp_path)
        assert block["available"] is True and block["reconciles"] is True
        assert block["reconciliation_residual"] == pytest.approx(0.0, abs=1e-2)
        v = next(p for p in block["position_contributions"] if p["ticker"] == "VLO")
        assert v["price_prev"] == 311.71
        assert v["prior_mark_source"] == fe.MARK_SOURCE_LEDGER
        assert v["pnl_contribution"] == pytest.approx(14.28, abs=1e-2)

    def test_daily_close_block_no_mutation(self, tmp_path):
        _write_price_store(tmp_path, _LEDGER)
        before = _fingerprint_dir(tmp_path)
        dc._attribution_block(perf=_perf_loader()(), ops=_ops(), desk_dir=tmp_path)
        assert _fingerprint_dir(tmp_path) == before
