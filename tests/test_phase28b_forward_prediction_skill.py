"""
tests/test_phase28b_forward_prediction_skill.py

Phase 28B — FORWARD PREDICTION SKILL, SHADOW BOOKS AND OUTCOME MATURATION.

Fully offline: a hand-computed 8-ticker world with deterministic geometric price
paths aligned to the prediction ranks (higher score -> higher return, so the
Spearman rank IC is exactly +1), a fake frozen-model cross-section, an injected
downloader and tmp desk dirs. Proves the Part K matrix:

  * SNAPSHOT CAPTURE (1-10): first-close TRUE_FORWARD capture, no retroactive
    backfill, same-date idempotency, active + shadow books, explicit
    unavailability, identity / point-in-time provenance, per-ticker ranks and
    weights, tamper-evident immutability.
  * OUTCOME MATURATION (11-20): exact eligible-close horizons (weekends never
    count), 1/5-close maturation, 20/63-close pending, explicit symbol /
    benchmark unavailability, idempotent reprocessing, original predictions
    never modified.
  * METRICS (21-30): rank IC, top-minus-bottom spread, cost-adjusted shadow
    return, SPY excess, turnover, membership stability, drawdown, insufficient-
    sample gating, no annualisation, TRUE_FORWARD-only inputs.
  * SAFETY (31-38): no holdings / cash / order / broker / automation / weight /
    promotion changes; no secret exposure.
  * API / UI (39-48): controlled empty state, stable safety fields, snapshot
    filtering, research-only shadow labelling, pending/insufficient horizon
    wording, composition-vs-refresh date wording, JS null-guards, no native
    dialogs, no blank buttons, Phase 28A evidence intact.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from paper_trader.api import daily_action_gate as dag
from paper_trader.api import daily_close as dc
from paper_trader.api import forward_evidence as fe
from paper_trader.api import forward_prediction_skill as fps
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import paper_trading_desk as desk

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, client, env,  # noqa: F401
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

# --------------------------------------------------------------------------- #
# The deterministic offline world.
#
#   8 tickers, scores 8..1 (AAA best .. HHH worst). Sectors alternate.
#   Price path: p(t, i) = base_t * (1 + 0.001*score_t)^i over 15 real NYSE-style
#   sessions (weekends structurally absent). Higher score -> higher return at
#   every horizon, so the Spearman rank IC is exactly +1.
#   SPY: 400 * 1.002^i (daily return exactly +0.2%).
#   Fake books: top25 book holds the top-4 names, top50 holds all 8.
# --------------------------------------------------------------------------- #
_T8 = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"]
_SECTORS = {t: ("Tech" if i % 2 == 0 else "Energy") for i, t in enumerate(_T8)}

#: 15 consecutive weekday sessions (2026-03-07/08, 14/15, 21/22 are weekends).
_SESS = ["2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06",
         "2026-03-09", "2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13",
         "2026-03-16", "2026-03-17", "2026-03-18", "2026-03-19", "2026-03-20"]
_D1 = _SESS[4]   # 2026-03-06 (a Friday) — the first snapshot date
_D2 = _SESS[5]   # 2026-03-09 (the following Monday) — the next eligible close


def _default_scores():
    return {t: float(8 - i) for i, t in enumerate(_T8)}


_RATE = {t: 0.001 * (8 - i) for i, t in enumerate(_T8)}


def _px(t, i):
    base = 100.0 + 10 * _T8.index(t)
    return round(base * ((1 + _RATE[t]) ** i), 6)


def _spy(i):
    return round(400.0 * (1.002 ** i), 6)


def _table(through=len(_SESS) - 1, drop=()):
    tbl = {t: [{"date": _SESS[i], "adjusted_close": _px(t, i)}
               for i in range(through + 1)] for t in _T8 if t not in drop}
    if "SPY" not in drop:
        tbl["SPY"] = [{"date": _SESS[i], "adjusted_close": _spy(i)}
                      for i in range(through + 1)]
    return tbl


def _dl(table):
    def get(symbol, _start):
        s = str(symbol).upper()
        return table.get(s, table.get(s.split(".")[0], []))
    return get


def _book(model_id, members, size_target):
    w = round(1.0 / len(members), 6) if members else 0.0
    expo: dict = {}
    for t in members:
        expo[_SECTORS[t]] = round(expo.get(_SECTORS[t], 0.0) + w, 6)
    return {"model_id": model_id, "size_target": size_target,
            "size_actual": len(members), "equal_weight": w,
            "unallocated_weight": round(max(0.0, 1 - w * len(members)), 6),
            "max_individual_weight_cap": 0.5, "sector_cap_fraction": 0.25,
            "max_per_sector": max(1, int(0.25 * size_target)),
            "sector_exposure": expo,
            "constituents": [{"ticker": t, "weight": w, "rank": i + 1,
                              "score": None, "sector": _SECTORS[t]}
                             for i, t in enumerate(members)],
            "sector_capped_out": []}


def _cur(md, scores=None, drop_books=()):
    scores = scores or _default_scores()
    pct, ranks, z = eng._percentiles({t: scores[t] for t in _T8})
    order = sorted(_T8, key=lambda t: (-scores[t], t))

    def leg(model):
        return {t: {"ticker": t, "model_id": model, "model_version": "v1",
                    "raw_signal": scores[t], "normalized_score": round(z[t], 6),
                    "percentile": round(pct[t], 6), "rank": ranks[t],
                    "eligible": True, "exclusion_reason": None,
                    "data_quality_flags": [], "sector": _SECTORS[t]} for t in _T8}
    combined = {t: {"ticker": t, "model_id": "fundamental_momentum_50_50_v1",
                    "combined_score": round(pct[t], 6),
                    "percentile": round(pct[t], 6), "rank": ranks[t],
                    "sector": _SECTORS[t]} for t in _T8}
    books = {}
    for fam, mid in (("composite_sn", "composite_sn"), ("mom_6_1", "mom_6_1"),
                     ("fundamental_momentum_50_50", "fundamental_momentum_50_50_v1")):
        books[fam + "_top25"] = _book(mid, order[:4], 25)
        books[fam + "_top50"] = _book(mid, order[:8], 50)
    for b in drop_books:
        books.pop(b, None)
    return {"status": eng.STATUS_READY, "market_as_of_date": md,
            "fundamental_as_of_date": "2026-02-27", "fundamental_month": "2026-02",
            "momentum_month": md[:7],
            "inputs": {"validations": {"fundamental_names": 8, "momentum_names": 8,
                                       "momentum_eligible": 8, "risk_names": 8,
                                       "sector_map_names": 8,
                                       "fundamental_sector_coverage": 1.0}},
            "scores": {"composite_sn": leg("composite_sn"),
                       "mom_6_1": leg("mom_6_1")},
            "combined": {"combined": combined, "common_universe": list(_T8),
                         "n_common": 8},
            "books": {"books": books,
                      "primary_book_id": "fundamental_momentum_50_50_top25"},
            "warnings": []}


def _ops():
    hd = [{"ticker": t, "quantity": 10, "sector": _SECTORS[t], "average_cost": 100.0,
           "current_weight": 0.25, "cost_basis": 1000.0}
          for t in ("AAA", "BBB", "CCC", "DDD")]
    return {"operational_book": {"book_id": "alpha_paper_book_1", "initialized": True,
                                 "starting_capital": 100000.0, "holdings_detail": hd,
                                 "holdings": {r["ticker"]: 10 for r in hd}},
            "canonical_state": {"holdings_detail": hd, "lifecycle_stage": "FILLED",
                                "pending_order_count": 0, "fill_count": 4,
                                "nav": 100000.0, "cash": 100.0, "holdings_count": 4,
                                "valuation_date": _D1, "desk_mark_date": _D1}}


def _cap(tmp, i, cur=None, table=None, **kw):
    return fps.capture_snapshots(market_date=_SESS[i], desk_dir=tmp,
                                 current=cur if cur is not None else _cur(_SESS[i]),
                                 ops=_ops(), downloader=_dl(table or _table()), **kw)


def _extend(tmp, idxs, drop=()):
    series = {t: [[_SESS[i], _px(t, i)] for i in idxs] for t in _T8 if t not in drop}
    if "SPY" not in drop:
        series["SPY"] = [[_SESS[i], _spy(i)] for i in idxs]
    fps._merge_prices(tmp, series)


def _ledger_rows(tmp, fname):
    return desk._read_ledger(desk._desk_dir(tmp), fname)


# =========================================================================== #
# SNAPSHOT CAPTURE (Part K 1-10)
# =========================================================================== #
class TestSnapshotCapture:
    def test_first_close_captures_true_forward_snapshots(self, tmp_path):  # (1)
        out = _cap(tmp_path, 4)
        assert out["status"] == "SNAPSHOTS_CAPTURED"
        assert out["snapshots_created"] == 6
        assert out["snapshots_expected"] == 6
        assert out["forward_evidence_type"] == fps.TRUE_FORWARD
        rows = _ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)
        kinds = [r["kind"] for r in rows]
        assert kinds.count(fps.KIND_CROSS_SECTION) == 3
        assert kinds.count(fps.KIND_BOOK_SNAPSHOT) == 6
        assert all(r["forward_evidence_type"] == fps.TRUE_FORWARD for r in rows)

    def test_historical_dates_are_not_backfilled(self, tmp_path):  # (2)
        _cap(tmp_path, 5)
        before = len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        out = _cap(tmp_path, 4)  # an EARLIER date after a newer snapshot exists
        assert out["status"] == "SNAPSHOTS_UNAVAILABLE"
        assert out["snapshots_created"] == 0
        assert all("NO_RETROACTIVE_TRUE_FORWARD" in r
                   for r in out["unavailable_reasons"].values())
        assert len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)) == before

    def test_same_date_rerun_is_idempotent(self, tmp_path):  # (3)
        _cap(tmp_path, 4)
        before = len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        out = _cap(tmp_path, 4)
        assert out["snapshots_created"] == 0
        assert out["snapshots_already_present"] == 6
        assert out["idempotent"] is True
        assert len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)) == before

    def test_active_model_snapshot_is_captured(self, tmp_path):  # (4)
        _cap(tmp_path, 4)
        snaps = fps._book_snapshots(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        active = [s for s in snaps if s["book_id"] == fps.ACTIVE_BOOK_ID]
        assert len(active) == 1
        assert active[0]["book_role"] == "ACTIVE"
        assert active[0]["model_id"] == fps.ACTIVE_MODEL_ID

    def test_available_shadow_models_are_captured(self, tmp_path):  # (5)
        _cap(tmp_path, 4)
        snaps = fps._book_snapshots(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        shadows = {s["book_id"] for s in snaps if s["book_role"] == "SHADOW"}
        assert shadows == {"fundamental_momentum_50_50_top50", "mom_6_1_top25",
                           "mom_6_1_top50", "composite_sn_top25", "composite_sn_top50"}

    def test_unavailable_shadow_models_are_explicit(self, tmp_path):  # (6)
        cur = _cur(_D1, drop_books=("mom_6_1_top25", "mom_6_1_top50"))
        out = _cap(tmp_path, 4, cur=cur)
        assert out["snapshots_created"] == 4
        assert out["snapshots_unavailable"] == 2
        assert set(out["unavailable_reasons"]) == {"mom_6_1_top25", "mom_6_1_top50"}
        assert all("BOOK_UNAVAILABLE" in r
                   for r in out["unavailable_reasons"].values())

    def test_model_date_mismatch_never_fabricates(self, tmp_path):  # (1b/A)
        out = _cap(tmp_path, 4, cur=_cur(_SESS[3]))  # model inputs one day behind
        assert out["status"] == "SNAPSHOTS_UNAVAILABLE"
        assert out["snapshots_created"] == 0
        assert all("MODEL_DATE_MISMATCH" in r
                   for r in out["unavailable_reasons"].values())
        assert _ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE) == []

    def test_snapshot_preserves_identity(self, tmp_path):  # (7)
        _cap(tmp_path, 4)
        s = fps._book_snapshots(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE),
                                fps.ACTIVE_BOOK_ID)[0]
        assert s["snapshot_id"] == "fps_%s_%s" % (fps.ACTIVE_BOOK_ID, _D1)
        assert s["market_date"] == _D1
        assert s["model_id"] == fps.ACTIVE_MODEL_ID
        assert s["model_version"] == "v1"
        assert s["book_size"] == {"target": 25, "actual": 4}
        assert s["rebalance_cadence"] == "monthly"
        assert "12.5 bps" in s["transaction_cost_assumption"]
        assert s["created_at"]

    def test_snapshot_preserves_point_in_time_dates(self, tmp_path):  # (8)
        _cap(tmp_path, 4)
        s = fps._book_snapshots(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE),
                                fps.ACTIVE_BOOK_ID)[0]
        assert s["price_data_through"] == _D1
        assert s["fundamental_data_as_of"] == "2026-02-27"
        assert s["benchmark_date"] == _D1
        assert s["universe_as_of"] == _D1[:7]
        assert s["input_coverage"]["momentum_names"] == 8
        assert s["eligible_universe_count"] == 8

    def test_snapshot_contains_ranks_and_weights(self, tmp_path):  # (9)
        _cap(tmp_path, 4)
        rows = _ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)
        cs = fps._cross_sections(rows, "mom_6_1")[0]
        byt = {r["ticker"]: r for r in fps._cs_rows_as_dicts(cs)}
        assert byt["AAA"]["rank"] == 1 and byt["HHH"]["rank"] == 8
        assert byt["AAA"]["percentile"] == 1.0 and byt["HHH"]["percentile"] == 0.0
        assert byt["AAA"]["in_top25"] == 1 and byt["HHH"]["in_top25"] == 0
        assert byt["HHH"]["in_top50"] == 1
        assert byt["AAA"]["held_operational"] == 1 and byt["EEE"]["held_operational"] == 0
        s = fps._book_snapshots(rows, "mom_6_1_top25")[0]
        assert s["membership"] == ["AAA", "BBB", "CCC", "DDD"]
        assert s["target_weights"]["AAA"] == pytest.approx(0.25)
        assert s["equal_weight"] == pytest.approx(0.25)

    def test_snapshot_mutation_is_detectable(self, tmp_path):  # (10)
        _cap(tmp_path, 4)
        assert fps.verify_prediction_ledgers(tmp_path)["all_intact"] is True
        path = desk._desk_dir(tmp_path) / fps.SNAPSHOT_LEDGER_FILE
        obj = json.loads(path.read_text(encoding="utf-8"))
        for r in obj["rows"]:
            if r["kind"] == fps.KIND_BOOK_SNAPSHOT:
                r["membership"] = ["TAMPERED"]
                break
        path.write_text(json.dumps(obj), encoding="utf-8")
        assert fps.verify_prediction_ledgers(tmp_path)["all_intact"] is False


# =========================================================================== #
# OUTCOME MATURATION (Part K 11-20)
# =========================================================================== #
class TestOutcomeMaturation:
    def test_one_close_does_not_mature_early(self, tmp_path):  # (11)
        _cap(tmp_path, 4)
        out = fps.mature_outcomes(desk_dir=tmp_path)
        assert out["outcomes_newly_matured"] == 0
        assert any(p["detail"] == fps.OUT_NOT_ENOUGH_CLOSES
                   and p["horizon"] == 1 for p in out["pending_outcomes"])
        assert _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE) == []

    def test_one_close_matures_on_next_eligible_close(self, tmp_path):  # (12)
        _cap(tmp_path, 4)
        _extend(tmp_path, [5])
        out = fps.mature_outcomes(desk_dir=tmp_path)
        rows = _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE)
        assert out["outcomes_newly_matured"] == 3          # one per model at h=1
        assert all(r["horizon"] == 1 and r["maturity_market_date"] == _D2
                   for r in rows)
        m = next(r for r in rows if r["model_id"] == "mom_6_1")
        assert m["status"] == fps.OUT_MATURED
        # AAA daily return is exactly +0.8%; benchmark exactly +0.2%.
        member = {x[0]: x for x in m["member_outcomes"]}
        assert member["AAA"][3] == pytest.approx(0.8, abs=1e-3)
        assert m["benchmark"]["return_pct"] == pytest.approx(0.2, abs=1e-3)

    def test_weekend_days_are_not_eligible_closes(self, tmp_path):  # (14)
        # Snapshot on Friday 2026-03-06; the ONE-close maturity is Monday
        # 2026-03-09 — Saturday/Sunday are structurally absent from the calendar.
        _cap(tmp_path, 4)
        _extend(tmp_path, [5])
        fps.mature_outcomes(desk_dir=tmp_path)
        row = _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE)[0]
        assert row["market_date"] == "2026-03-06"
        assert row["maturity_market_date"] == "2026-03-09"

    def test_five_close_matures_on_fifth_close(self, tmp_path):  # (13)
        _cap(tmp_path, 4)
        _extend(tmp_path, [5, 6, 7, 8])
        fps.mature_outcomes(desk_dir=tmp_path)
        assert not any(r["horizon"] == 5
                       for r in _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE))
        _extend(tmp_path, [9])
        fps.mature_outcomes(desk_dir=tmp_path)
        h5 = [r for r in _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE)
              if r["horizon"] == 5]
        assert len(h5) == 3
        assert all(r["maturity_market_date"] == _SESS[9] for r in h5)

    def test_twenty_and_sixtythree_close_remain_pending(self, tmp_path):  # (15, 16)
        _cap(tmp_path, 4)
        _extend(tmp_path, list(range(5, 15)))
        out = fps.mature_outcomes(desk_dir=tmp_path)
        pend = {(p["model_id"], p["horizon"]) for p in out["pending_outcomes"]}
        assert ("mom_6_1", 20) in pend and ("mom_6_1", 63) in pend
        assert not any(r["horizon"] in (20, 63)
                       for r in _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE))

    def test_missing_symbol_coverage_is_explicit(self, tmp_path):  # (17)
        _cap(tmp_path, 4)
        _extend(tmp_path, [5], drop=("HHH",))       # HHH has no maturity price
        fps.mature_outcomes(desk_dir=tmp_path)
        m = next(r for r in _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE)
                 if r["model_id"] == "mom_6_1")
        assert m["status"] == fps.OUT_COVERAGE_INCOMPLETE   # 7/8 priced < 0.95
        assert m["coverage"]["priced"] == 7 and m["coverage"]["total"] == 8
        assert "HHH" in m["coverage"]["missing_tickers"]
        member = {x[0]: x for x in m["member_outcomes"]}
        assert member["HHH"][5] == fps.OUT_SYMBOL_UNAVAILABLE

    def test_missing_benchmark_is_explicit(self, tmp_path):  # (18)
        # SPY has no completed close on the snapshot date; after the bounded
        # self-heal grace the row freezes as BENCHMARK_UNAVAILABLE (metrics None).
        _cap(tmp_path, 4, table=_table(drop=("SPY",)))
        _extend(tmp_path, list(range(5, 11)), drop=())   # SPY starts at _SESS[5]
        fps.mature_outcomes(desk_dir=tmp_path)
        h1 = [r for r in _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE)
              if r["horizon"] == 1]
        assert h1 and all(r["status"] == fps.OUT_BENCHMARK_UNAVAILABLE for r in h1)
        assert all(r["metrics"] is None for r in h1)

    def test_outcome_processing_is_idempotent(self, tmp_path):  # (19)
        _cap(tmp_path, 4)
        _extend(tmp_path, [5])
        fps.mature_outcomes(desk_dir=tmp_path)
        n = len(_ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE))
        out = fps.mature_outcomes(desk_dir=tmp_path)
        assert out["outcomes_newly_matured"] == 0
        assert len(_ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE)) == n

    def test_original_prediction_values_never_change(self, tmp_path):  # (20)
        _cap(tmp_path, 4)
        before = copy.deepcopy(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        _extend(tmp_path, [5])
        fps.mature_outcomes(desk_dir=tmp_path)
        _cap(tmp_path, 5)                                # a second capture day
        after = _ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)
        assert after[:len(before)] == before             # byte-identical prefix
        assert fps.verify_prediction_ledgers(tmp_path)["all_intact"] is True

    def test_prices_are_first_write_wins(self, tmp_path):  # (C rules)
        _cap(tmp_path, 4)
        p0 = fps._price_exact(fps.read_price_store(tmp_path)["series"], "AAA", _D1)
        fps._merge_prices(tmp_path, {"AAA": [[_D1, 999.0]]})
        p1 = fps._price_exact(fps.read_price_store(tmp_path)["series"], "AAA", _D1)
        assert p1 == p0 != 999.0


# =========================================================================== #
# METRICS (Part K 21-30)
# =========================================================================== #
class TestMetrics:
    def _matured(self, tmp):
        _cap(tmp, 4)
        _extend(tmp, [5])
        fps.mature_outcomes(desk_dir=tmp)
        return _ledger_rows(tmp, fps.OUTCOME_LEDGER_FILE)

    def test_rank_ic_is_correct(self, tmp_path):  # (21)
        m = next(r for r in self._matured(tmp_path) if r["model_id"] == "mom_6_1")
        assert m["metrics"]["rank_ic_spearman"] == pytest.approx(1.0)
        assert m["metrics"]["n_ic_names"] == 8

    def test_rank_ic_sign_flips_when_anti_aligned(self, tmp_path):  # (21b)
        inv = {t: float(i + 1) for i, t in enumerate(_T8)}  # AAA worst .. HHH best
        _cap(tmp_path, 4, cur=_cur(_D1, scores=inv))
        _extend(tmp_path, [5])
        fps.mature_outcomes(desk_dir=tmp_path)
        m = next(r for r in _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE)
                 if r["model_id"] == "mom_6_1")
        assert m["metrics"]["rank_ic_spearman"] == pytest.approx(-1.0)

    def test_top_minus_bottom_spread_is_correct(self, tmp_path):  # (22)
        m = next(r for r in self._matured(tmp_path) if r["model_id"] == "mom_6_1")
        # top decile = AAA (+0.8%); bottom decile = HHH (+0.1%) -> spread 0.7pp
        assert m["metrics"]["top_decile_return_pct"] == pytest.approx(0.8, abs=1e-3)
        assert m["metrics"]["bottom_decile_return_pct"] == pytest.approx(0.1, abs=1e-3)
        assert m["metrics"]["top_minus_bottom_pp"] == pytest.approx(0.7, abs=1e-3)
        # top-25 avg = mean(.8,.7,.6,.5); top-50 avg = mean over all 8
        assert m["metrics"]["top25_avg_return_pct"] == pytest.approx(0.65, abs=1e-3)
        assert m["metrics"]["top50_avg_return_pct"] == pytest.approx(0.45, abs=1e-3)
        assert m["metrics"]["top25_excess_pp"] == pytest.approx(0.45, abs=1e-3)

    def test_cost_adjusted_return_is_correct(self, tmp_path):  # (23)
        _cap(tmp_path, 4)
        _extend(tmp_path, [5, 6])
        sim = fps.build_shadow_portfolio("mom_6_1_top25", desk_dir=tmp_path)
        # entry at _SESS[5]: cost 100000 * 0.00125 = 125 -> nav 99875; one daily
        # return = mean(.8,.7,.6,.5)% = 0.65% -> nav 99875*1.0065 = 100524.19
        assert sim["observations"] == 1
        assert sim["estimated_transaction_cost"] == pytest.approx(125.0, abs=0.01)
        assert sim["gross_return_pct"] == pytest.approx(0.65, abs=1e-2)
        assert sim["net_return_pct"] == pytest.approx(
            (99875 * 1.0065 / 100000 - 1) * 100, abs=1e-2)

    def test_spy_excess_return_is_correct(self, tmp_path):  # (24)
        _cap(tmp_path, 4)
        _extend(tmp_path, [5, 6])
        sim = fps.build_shadow_portfolio("mom_6_1_top25", desk_dir=tmp_path)
        assert sim["spy_return_pct"] == pytest.approx(0.2, abs=1e-2)
        assert sim["excess_return_pp"] == pytest.approx(
            sim["net_return_pct"] - sim["spy_return_pct"], abs=1e-6)

    def test_turnover_and_membership_stability(self, tmp_path):  # (25, 26)
        _cap(tmp_path, 4)
        s2 = _default_scores()
        s2["EEE"] = 5.5                                   # EEE displaces DDD
        _cap(tmp_path, 5, cur=_cur(_D2, scores=s2))
        snaps = fps._book_snapshots(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE),
                                    "mom_6_1_top25")
        assert snaps[0]["membership"] == ["AAA", "BBB", "CCC", "DDD"]
        assert snaps[1]["membership"] == ["AAA", "BBB", "CCC", "EEE"]
        assert snaps[1]["expected_turnover_vs_previous"] == pytest.approx(0.25)
        assert snaps[1]["membership_stability_vs_previous"] == pytest.approx(0.75)
        assert snaps[1]["previous_snapshot_id"] == snaps[0]["snapshot_id"]
        _extend(tmp_path, [6, 7])
        sim = fps.build_shadow_portfolio("mom_6_1_top25", desk_dir=tmp_path)
        # rebalance at _SESS[6] (entry close of the 2nd snapshot): churn (1+1)/4
        assert sim["cumulative_turnover"] == pytest.approx(0.5)
        assert sim["membership_stability_avg"] == pytest.approx(0.75)

    def test_drawdown_is_correct(self, tmp_path):  # (27)
        _cap(tmp_path, 4, table=_table(through=4))
        # Custom member path (identical for the 4 members, so the portfolio
        # return equals the member return). Entry is AT the _SESS[5] close (no
        # hindsight: the p4 -> p5 move is never earned), then -3% and +2% days:
        #   p5 = p4*1.01, p6 = p5*0.97 (trough), p7 = p6*1.02.
        series = {}
        for t in ("AAA", "BBB", "CCC", "DDD"):
            p4 = _px(t, 4)
            series[t] = [[_SESS[5], p4 * 1.01], [_SESS[6], p4 * 1.01 * 0.97],
                         [_SESS[7], p4 * 1.01 * 0.97 * 1.02]]
        series["SPY"] = [[_SESS[i], _spy(i)] for i in (5, 6, 7)]
        fps._merge_prices(tmp_path, series)
        sim = fps.build_shadow_portfolio("mom_6_1_top25", desk_dir=tmp_path)
        # NAV: 100000 (peak) -> entry cost 125 -> 99875 -> *0.97 = 96878.75
        # (trough) -> *1.02. Max DD = 96878.75/100000 - 1 = -3.1213% — the entry
        # cost is part of net performance and is included in the drawdown.
        assert sim["max_drawdown_pct"] == pytest.approx(-3.1213, abs=1e-3)
        assert sim["net_return_pct"] == pytest.approx(
            (99875 * 0.97 * 1.02 / 100000 - 1) * 100, abs=1e-3)

    def test_insufficient_sample_blocks_overinterpretation(self, tmp_path):  # (28)
        self._matured(tmp_path)
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        cell = next(c for c in payload["prediction_skill"]
                    if c["model_id"] == fps.ACTIVE_MODEL_ID
                    and c["horizon_eligible_closes"] == 1)
        assert cell["evidence_state"] == fps.EV_INSUFFICIENT
        assert "Pipeline verification only" in cell["interpretation"]
        h63 = next(c for c in payload["prediction_skill"]
                   if c["model_id"] == fps.ACTIVE_MODEL_ID
                   and c["horizon_eligible_closes"] == 63)
        assert h63["evidence_state"] == fps.EV_OUTCOMES_PENDING

    def test_no_annualization_on_insufficient_sample(self, tmp_path):  # (29)
        _cap(tmp_path, 4)
        _extend(tmp_path, [5, 6, 7])
        sim = fps.build_shadow_portfolio("mom_6_1_top25", desk_dir=tmp_path)
        assert sim["observations"] == 2                  # far below the floor of 20
        assert sim["daily_volatility_annualized_pct"] is None
        assert "withheld" in (sim["volatility_warning"] or "")
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        assert all(c["annualized"] is False for c in payload["prediction_skill"])

    def test_historical_reconstruction_excluded_from_true_forward(self, tmp_path):  # (30)
        self._matured(tmp_path)
        rows = _ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)
        outs = _ledger_rows(tmp_path, fps.OUTCOME_LEDGER_FILE)
        assert all(r["forward_evidence_type"] == fps.TRUE_FORWARD for r in rows)
        assert all(r["forward_evidence_type"] == fps.TRUE_FORWARD for r in outs)
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        assert all(p["evidence_type"] == fps.TRUE_FORWARD
                   for p in payload["portfolio_comparison"])
        assert "HISTORICAL_RECONSTRUCTION" not in json.dumps(
            payload["prediction_skill"])


# =========================================================================== #
# SAFETY (Part K 31-38)
# =========================================================================== #
class TestSafety:
    def _world(self, tmp):
        ops_before = _ops()
        frozen = copy.deepcopy(ops_before)
        fps.capture_snapshots(market_date=_D1, desk_dir=tmp, current=_cur(_D1),
                              ops=ops_before, downloader=_dl(_table()))
        _extend(tmp, [5])
        fps.mature_outcomes(desk_dir=tmp)
        payload = fps.load_prediction_skill(desk_dir=tmp)
        return ops_before, frozen, payload

    def test_no_operational_mutation(self, tmp_path):  # (31, 32, 33)
        ops_before, frozen, payload = self._world(tmp_path)
        assert ops_before == frozen                      # holdings + cash untouched
        sdir = desk._desk_dir(tmp_path)
        for f in (desk.BOOKS_FILE, desk.ORDERS_FILE, desk.FILLS_FILE,
                  desk.PERFORMANCE_FILE):
            assert not (sdir / f).exists()               # no order/fill/book writes
        assert payload["creates_orders"] is False

    def test_only_prediction_stores_are_written(self, tmp_path):  # (31b)
        self._world(tmp_path)
        names = {p.name for p in desk._desk_dir(tmp_path).iterdir()}
        assert names == {fps.SNAPSHOT_LEDGER_FILE, fps.OUTCOME_LEDGER_FILE,
                         fps.PRICE_STORE_FILE}

    def test_safety_contract_fields(self, tmp_path):  # (34, 35, 36, 37)
        _ops_b, _f, payload = self._world(tmp_path)
        assert payload["read_only"] is True
        assert payload["broker_execution"] is False
        assert payload["automation_enabled"] is False
        assert payload["changes_operational_model"] is False
        assert payload["changes_model_weights"] is False
        assert payload["promotes_challenger"] is False
        assert payload["retires_model"] is False
        gates = payload["evidence_gates"]
        assert "NEVER promote" in gates["note"]

    def test_no_secret_exposure(self, tmp_path):  # (38)
        _ops_b, _f, payload = self._world(tmp_path)
        dump = json.dumps(payload).lower()
        for banned in ("api_key", "api-key", "authorization", "secret",
                       "password", "local-dev-key", "eodhd_api"):
            assert banned not in dump
        snaps = fps.load_prediction_snapshots(desk_dir=tmp_path)
        dump2 = json.dumps(snaps).lower()
        for banned in ("api_key", "authorization", "secret", "password"):
            assert banned not in dump2


# =========================================================================== #
# DAILY-CLOSE INTEGRATION (Part B contract)
# =========================================================================== #
def _ok_refresh(md):
    def _fn(**_kw):
        return {"status": desk.S_OK, "performed_write": True,
                "resulting_desk_mark_date": md,
                "latest_completed_market_date": md,
                "settlement": {"n_filled": 0}, "performance": {"n_appended": 1}}
    return _fn


def _hold_gate(*_a, **_k):
    return {"outcome": dag.OUTCOME_NO_ACTION_TODAY, "proposed_change_count": 0,
            "data_ready": True, "checks_summary": {"line": "ok"},
            "warnings": []}


def _capture_seam():
    def _fn(*, market_date, desk_dir=None, current=None, downloader=None, ops=None):
        return fps.capture_for_daily_close(
            market_date=market_date, desk_dir=desk_dir,
            current=_cur(market_date), downloader=_dl(_table()),
            ops=ops or _ops())
    return _fn


def _run_close(tmp, *, capture_fn=None, today=_D2):
    return dc.run_daily_close(
        confirm=dc.EXECUTE_CONFIRMATION, today=today, desk_dir=tmp,
        operational_loader=(lambda _t: _ops()), gate_loader=_hold_gate,
        refresh_fn=_ok_refresh(_D1), prediction_capture_fn=capture_fn)


class TestDailyCloseIntegration:
    def test_fresh_close_captures_and_reports_contract(self, tmp_path):  # (B)
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        fpc = out["forward_prediction_capture"]
        assert fpc["status"] == "SNAPSHOTS_CAPTURED"
        assert fpc["market_date"] == _D1
        assert fpc["snapshots_expected"] == 6
        assert fpc["snapshots_created"] == 6
        assert fpc["snapshots_already_present"] == 0
        assert fpc["snapshots_unavailable"] == 0
        assert fpc["unavailable_reasons"] == {}
        assert fpc["outcomes_newly_matured"] == 0
        assert fpc["idempotent"] is True
        assert fpc["performed_write"] is True
        assert fpc["created_orders"] is False
        assert fpc["changed_operational_model"] is False
        assert len(fps._book_snapshots(
            _ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))) == 6

    def test_rerun_does_not_duplicate_anything(self, tmp_path):
        _run_close(tmp_path, capture_fn=_capture_seam())
        snap_n = len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        out2 = _run_close(tmp_path, capture_fn=_capture_seam())
        assert out2["close_status"] == dc.ALREADY_PROCESSED
        fpc = out2["forward_prediction_capture"]
        assert fpc["status"] == "SNAPSHOTS_PRESENT"
        assert fpc["snapshots_already_present"] == 6
        assert fpc["performed_write"] is False
        assert len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)) == snap_n

    def test_offline_seam_without_capture_fn_writes_nothing(self, tmp_path):
        out = _run_close(tmp_path, capture_fn=None)
        fpc = out["forward_prediction_capture"]
        assert fpc["status"] == "CAPTURE_INACTIVE_OFFLINE_SEAM"
        assert not (desk._desk_dir(tmp_path) / fps.SNAPSHOT_LEDGER_FILE).exists()

    def test_get_daily_close_reports_presence_read_only(self, tmp_path):
        _run_close(tmp_path, capture_fn=_capture_seam())
        out = dc.load_daily_close(today=_D2, desk_dir=tmp_path,
                                  operational=_ops(), gate=_hold_gate())
        fpc = out["forward_prediction_capture"]
        assert fpc["status"] == "SNAPSHOTS_PRESENT"
        assert fpc["read_only_presence_check"] is True
        assert fpc["performed_write"] is False


# =========================================================================== #
# API (Part K 39-41, 48) — reuses the Phase 27A offline client harness.
# =========================================================================== #
class TestApiContract:
    def test_empty_state_is_valid_and_controlled(self, tmp_path):  # (39)
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        assert payload["status"] == fps.EV_NO_SNAPSHOTS
        assert payload["snapshot_count"] == 0
        assert payload["latest_snapshot_date"] is None
        assert payload["matured_outcome_count"] == 0
        assert isinstance(payload["prediction_skill"], list)
        assert all(c["evidence_state"] == fps.EV_NO_SNAPSHOTS
                   for c in payload["prediction_skill"])
        assert all(p["status"] == fps.EV_NO_SNAPSHOTS
                   for p in payload["portfolio_comparison"])

    def test_prediction_skill_route_stable_safety(self, client):  # (40)
        r = client.get("/v1/evidence/prediction-skill", headers=_AUTH)
        assert r.status_code == 200
        d = r.json()
        for key, val in (("read_only", True), ("creates_orders", False),
                         ("broker_execution", False), ("automation_enabled", False),
                         ("changes_operational_model", False),
                         ("promotes_challenger", False)):
            assert d[key] is val, key
        assert d["horizons"] == [1, 5, 20, 63]
        r2 = client.get("/v1/evidence/prediction-skill")
        assert r2.status_code in (401, 403)

    def test_snapshot_filtering_works(self, tmp_path):  # (41)
        _cap(tmp_path, 4)
        _cap(tmp_path, 5)
        all_ = fps.load_prediction_snapshots(desk_dir=tmp_path)
        assert all_["count"] == 12
        by_book = fps.load_prediction_snapshots(book_id="mom_6_1_top25",
                                                desk_dir=tmp_path)
        assert by_book["count"] == 2
        assert all(s["book_id"] == "mom_6_1_top25" for s in by_book["snapshots"])
        by_model = fps.load_prediction_snapshots(model_id="composite_sn",
                                                 desk_dir=tmp_path)
        assert by_model["count"] == 4
        by_date = fps.load_prediction_snapshots(market_date=_D1, desk_dir=tmp_path)
        assert by_date["count"] == 6
        lim = fps.load_prediction_snapshots(limit=3, desk_dir=tmp_path)
        assert lim["count"] == 3

    def test_snapshots_route_smoke(self, client):
        r = client.get("/v1/evidence/prediction-skill/snapshots", headers=_AUTH)
        assert r.status_code == 200
        d = r.json()
        assert d["immutable"] is True and d["append_only"] is True

    def test_phase28a_forward_evidence_still_works(self, client):  # (48)
        r = client.get("/v1/evidence/forward", headers=_AUTH)
        assert r.status_code == 200
        d = r.json()
        assert d["status"] == "FORWARD_EVIDENCE_READY"
        assert "active_vs_shadow" in d and "rolling_evidence" in d
        # Phase 28B: the concise prediction-skill summary is embedded.
        ps = d.get("prediction_skill")
        assert ps is not None
        assert ps["active_book_id"] == fps.ACTIVE_BOOK_ID
        assert ps["detail_route"] == "/v1/evidence/prediction-skill"

    def test_forward_evidence_summary_offline(self, tmp_path):
        _cap(tmp_path, 4)
        s = fps.prediction_skill_summary(desk_dir=tmp_path)
        assert s["evidence_state"] == fps.EV_OUTCOMES_PENDING
        assert s["snapshot_count"] == 6
        assert s["latest_snapshot_date"] == _D1
        assert s["forward_evidence_type"] == fps.TRUE_FORWARD


# =========================================================================== #
# UI STATIC + WORDING (Part K 42-47, 44)
# =========================================================================== #
@pytest.fixture(scope="module")
def ui_html() -> str:
    return _UI.read_text(encoding="utf-8")


class TestUiStatic:
    def test_research_ui_labels_shadow_books(self, ui_html):  # (42)
        assert 'id="fps-panel"' in ui_html
        assert "Forward Prediction Skill" in ui_html
        assert "TRUE FORWARD ONLY" in ui_html
        assert "RESEARCH SHADOW BOOK &mdash; NOT EXECUTED HOLDINGS" in ui_html
        assert "RESEARCH SHADOW &mdash; NOT EXECUTED" in ui_html
        assert "ACTIVE MODEL &middot; TRUE FORWARD" in ui_html

    def test_horizon_cards_show_pending_insufficient_states(self, ui_html):  # (43)
        assert 'id="fps-horizons"' in ui_html
        assert "Insufficient sample" in ui_html
        assert "no model conclusion" in ui_html
        assert "No forward snapshots yet" in ui_html
        assert "never backfilled" in ui_html
        assert "eligible completed closes, never calendar days" in ui_html

    def test_wording_composition_vs_refresh(self, ui_html):  # (44)
        assert "Target composition date" in ui_html
        assert "Latest price/score refresh" in ui_html
        assert "Model target calculated as of" not in ui_html

    def test_gate_headline_anchored_to_completed_close(self):  # (44b)
        h = dag._PRESENTATION[dag.OUTCOME_NO_ACTION_TODAY]["headline"]
        assert h.startswith("NO PORTFOLIO CHANGE REQUIRED FROM THE LATEST "
                            "COMPLETED CLOSE")
        assert "TODAY" not in h

    def test_js_null_guards_present(self, ui_html):  # (45)
        assert "function renderPredictionSkill(d)" in ui_html
        assert "if (!d) { panel.style.display = 'none'; return; }" in ui_html
        assert "(d.prediction_skill || [])" in ui_html
        assert "(d.portfolio_comparison || [])" in ui_html
        assert "d.research_flags || []" in ui_html

    def test_no_native_dialogs(self, ui_html):  # (46)
        for pat in ("alert(", "confirm(", "prompt("):
            assert pat not in ui_html, pat

    def test_no_blank_action_buttons(self, ui_html):  # (47)
        assert 'onclick="loadPredictionSkill()"' in ui_html
        i = ui_html.index('onclick="loadPredictionSkill()"')
        assert "Refresh" in ui_html[i:i + 200]
        # The Portfolio compact link routes to Research & Audit, never a blank CTA.
        assert 'id="fe-fps-link"' in ui_html
        assert "view in Research" in ui_html

    def test_gates_note_never_promotes(self, ui_html):
        assert "never promote or retire a model" in ui_html
