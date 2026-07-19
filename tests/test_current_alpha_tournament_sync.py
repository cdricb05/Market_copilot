"""
tests/test_current_alpha_tournament_sync.py — Phase 19 tournament data sync (unit).

Fully offline: synthetic champion / challenger packages plus a FAKE downloader (or the JSON
fixture seam) mean no research runner, no network, no EODHD key and no database are ever used.

Covers: the frozen union is correct + deduplicated; the downloader receives only union tickers
+ SPY, each requested from the signal date; preview performs no provider call and no write; a
committing sync requires the explicit confirmation token; successful per-ticker data is retained
during a partial failure; no synthetic price for a missing ticker; no cross-book price
substitution; the four books stay frozen (no rerank / rebalance); a single common financial
calendar; the STALE / ALIGNED / PARTIAL_COVERAGE alignment states; same-date idempotency; the
safety block (no DB / positions / orders / signals / decisions / fills / prediction / champion
replacement / live promotion); and no EODHD key leakage into the store or payload.
"""
from __future__ import annotations

import json
from pathlib import Path

from paper_trader.api.current_alpha_tournament_sync import (
    run_current_alpha_tournament_sync,
    build_alignment_block,
    resolve_frozen_union,
    load_synced_tournament,
    SYNC_CONFIRM_TOKEN,
    SYNC_PREVIEW,
    SYNC_COMPLETE,
    SYNC_PARTIAL,
    SYNC_NO_NEW,
    SYNC_CONFIRM_REQUIRED,
    SYNC_UNAVAILABLE,
    ALIGN_ALIGNED,
    ALIGN_STALE,
    ALIGN_PARTIAL,
    _SYNC_STATE_FILE,
    _SYNC_DATA_FILE,
    _SYNC_PRICES_FILE,
)

SIGNAL = "2026-05-22"
DATES = ["2026-05-22", "2026-05-26", "2026-05-27", "2026-05-28", "2026-05-29", "2026-06-01"]

# Frozen membership (Top25 rows are a subset of Top50; challenger shares C1/C2 with champion).
CHAMP_T25 = ["C1", "C2", "C3", "C4", "C5", "C6"]
CHAMP_T50 = CHAMP_T25 + ["C7", "C8", "C9", "C10"]
CHALL_T25 = ["C1", "C2", "X1", "X2", "X3", "X4"]
CHALL_T50 = CHALL_T25 + ["X5", "X6", "X7", "X8"]
UNION = sorted(set(CHAMP_T50) | set(CHALL_T50))  # 18 tickers


def _bars(base: float):
    return [{"date": d, "adjusted_close": round(base * (1 + 0.01 * j), 4)} for j, d in enumerate(DATES)]


def _price_table(drop=()):
    """clean_symbol -> bars. ``drop`` removes a ticker (simulates a provider miss)."""
    table = {}
    for i, tk in enumerate(UNION):
        if tk in drop:
            table[tk] = []
            continue
        table[tk] = _bars(100.0 + i * 7.0)
    table["SPY"] = [{"date": d, "adjusted_close": round(400.0 * (1 + 0.002 * j), 4)}
                    for j, d in enumerate(DATES)]
    return table


def _entry_at_signal(tk: str) -> float:
    i = UNION.index(tk)
    return round(100.0 + i * 7.0, 4)  # == _bars(base)[0] (j=0)


def _write_champion_pkg(base: Path):
    base.mkdir(parents=True, exist_ok=True)
    (base / "phase13a_current_champion_alpha_paper_test_package.json").write_text(
        json.dumps({"signal_date": SIGNAL, "alpha_name": "composite_sn"}), encoding="utf-8")
    for size, names in ((25, CHAMP_T25), (50, CHAMP_T50)):
        lines = ["ticker,sector,signal_composite_sn,entry_price,entry_reference_date,target_weight"]
        for r, tk in enumerate(names):
            lines.append("%s,Technology,%.4f,%.4f,%s,0.02" % (tk, 9.0 - r * 0.1, _entry_at_signal(tk), SIGNAL))
        (base / ("current_alpha_paper_portfolio_top%d.csv" % size)).write_text(
            "\n".join(lines) + "\n", encoding="utf-8")


def _write_challenger_pkg(base: Path):
    base.mkdir(parents=True, exist_ok=True)
    (base / "phase17b_sector_repaired_challenger_package.json").write_text(
        json.dumps({"signal_date": SIGNAL, "alpha_name": "composite_sn_repaired"}), encoding="utf-8")
    for size, names in ((25, CHALL_T25), (50, CHALL_T50)):
        lines = ["ticker,repaired_sector,signal_composite_sn_repaired,target_weight"]
        for r, tk in enumerate(names):
            lines.append("%s,Industrials,%.4f,0.02" % (tk, 8.0 - r * 0.1))
        (base / ("challenger_paper_portfolio_top%d.csv" % size)).write_text(
            "\n".join(lines) + "\n", encoding="utf-8")


def _pkgs(tmp_path: Path):
    champ = tmp_path / "champ_pkg"
    chall = tmp_path / "chall_pkg"
    _write_champion_pkg(champ)
    _write_challenger_pkg(chall)
    return champ, chall


def _recording_downloader(table, calls):
    def _dl(symbol, start):
        calls.append((symbol, start))
        return table.get(symbol, [])
    return _dl


def _sync(tmp_path, *, commit, confirm=None, drop=(), calls=None, table=None, tdir=None):
    champ, chall = _pkgs(tmp_path)
    tdir = tdir or (tmp_path / "store")
    table = table if table is not None else _price_table(drop=drop)
    calls = calls if calls is not None else []
    return run_current_alpha_tournament_sync(
        commit=commit, confirm=confirm,
        downloader=_recording_downloader(table, calls),
        champion_pkg_dir=champ, challenger_pkg_dir=chall,
        tournament_dir=tdir, system_mark_dir=tmp_path / "no_system_mark",
        today="2026-07-19",
    ), tdir, calls


# --------------------------------------------------------------------------- #
def test_frozen_union_is_correct_and_deduplicated(tmp_path):
    champ, chall = _pkgs(tmp_path)
    members_by_key, union, spy = resolve_frozen_union(champ, chall)
    assert union == UNION
    assert len(union) == len(set(union))  # deduplicated
    assert spy == ["SPY"]
    assert [m["ticker"] for m in members_by_key["champion_top25"]] == CHAMP_T25
    assert [m["ticker"] for m in members_by_key["challenger_top50"]] == CHALL_T50


def test_preview_performs_no_provider_call_and_no_write(tmp_path):
    calls = []
    res, tdir, calls = _sync(tmp_path, commit=False, calls=calls)
    assert res["status"] == SYNC_PREVIEW
    assert res["performed_provider_call"] is False
    assert res["wrote_store"] is False
    assert calls == []  # downloader never invoked in preview
    assert res["union"]["union_size"] == len(UNION)
    assert res["union"]["expected_provider_calls"] == len(UNION) + 1
    assert not (tdir / _SYNC_STATE_FILE).exists()
    assert not (tdir / _SYNC_DATA_FILE).exists()


def test_commit_requires_explicit_confirmation(tmp_path):
    calls = []
    res, tdir, calls = _sync(tmp_path, commit=True, confirm="WRONG", calls=calls)
    assert res["status"] == SYNC_CONFIRM_REQUIRED
    assert res["wrote_store"] is False
    assert calls == []  # no provider call without confirmation
    assert not (tdir / _SYNC_STATE_FILE).exists()


def test_downloader_receives_only_union_plus_spy_from_signal_date(tmp_path):
    calls = []
    res, tdir, calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN, calls=calls)
    assert res["status"] == SYNC_COMPLETE
    requested = [c[0] for c in calls]
    assert sorted(requested) == sorted(UNION + ["SPY"])       # exactly the union + SPY
    assert all(start == SIGNAL for _sym, start in calls)      # signal-date history requested


def test_commit_full_coverage_writes_only_local_store(tmp_path):
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN)
    assert res["status"] == SYNC_COMPLETE
    assert res["latest_tournament_common_mark"] == DATES[-1]
    assert res["n_tickers_ok"] == len(UNION) + 1 and res["n_tickers_failed"] == 0
    cov = res["reconstructed"]["coverage"]
    assert cov["champion_top25"]["coverage_pct"] == 100.0
    assert cov["challenger_top50"]["coverage_pct"] == 100.0
    # only the dedicated local store files are written
    assert (tdir / _SYNC_STATE_FILE).exists()
    assert (tdir / _SYNC_DATA_FILE).exists()
    assert (tdir / _SYNC_PRICES_FILE).exists()
    assert res["wrote_to_database"] is False


def test_partial_failure_retains_successful_tickers(tmp_path):
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN, drop=("X8",))
    assert res["status"] == SYNC_PARTIAL
    assert "X8" in res["failed_tickers"]
    assert res["n_tickers_ok"] == len(UNION)  # every other ticker + SPY retained
    # X8 only lives in challenger_top50 -> that book is short one, the rest stay fully covered
    cov = res["reconstructed"]["coverage"]
    assert cov["challenger_top50"]["covered"] == len(CHALL_T50) - 1
    assert cov["champion_top25"]["coverage_pct"] == 100.0


def test_missing_ticker_is_uncovered_not_synthetic(tmp_path):
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN, drop=("X1",))
    miss = res["reconstructed"]["missing_tickers_by_book"]
    # X1 is in both challenger books; it is reported missing, never given a fabricated price
    assert "X1" in miss["challenger_top25"]
    assert "X1" in miss["challenger_top50"]
    # champion books never hold X1 -> unaffected and never borrow another ticker's price
    assert "X1" not in miss["champion_top25"]
    assert res["reconstructed"]["coverage"]["champion_top25"]["coverage_pct"] == 100.0


def test_no_cross_book_price_substitution(tmp_path):
    # C1 present, X1 dropped. C1 is shared by champion + challenger; X1 only in challenger.
    # A missing X1 must NOT be filled from C1 (or any other) — covered sets stay exact.
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN, drop=("X1",))
    cov = res["reconstructed"]["coverage"]
    assert cov["challenger_top25"]["covered"] == len(CHALL_T25) - 1
    assert cov["champion_top25"]["covered"] == len(CHAMP_T25)  # C1 still covered on its own series


def test_books_stay_frozen_no_rerank_or_rebalance(tmp_path):
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN)
    synced = load_synced_tournament(tdir)
    bs = synced["book_summaries"]
    assert bs["champion_top25"]["n_members"] == len(CHAMP_T25)
    assert bs["challenger_top50"]["n_members"] == len(CHALL_T50)
    assert synced["book_isolation"]["all_isolated"] is True


def test_single_common_financial_calendar(tmp_path):
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN)
    bs = res["reconstructed"]["book_summaries"]
    starts = {b["start_date"] for b in bs.values()}
    ends = {b["end_date"] for b in bs.values()}
    assert starts == {DATES[0]} and ends == {DATES[-1]}  # same-date across all four books


def test_reproduces_frozen_entry_prices(tmp_path):
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN)
    repro = res["reconstructed"]["reproduction"]
    assert repro["entries_resolved"] is True
    assert repro["max_abs_error"] == 0.0  # fixture entry == price at signal


def test_same_date_idempotency(tmp_path):
    champ, chall = _pkgs(tmp_path)
    tdir = tmp_path / "store"
    table = _price_table()
    kw = dict(champion_pkg_dir=champ, challenger_pkg_dir=chall, tournament_dir=tdir,
              system_mark_dir=tmp_path / "nope", today="2026-07-19")
    first = run_current_alpha_tournament_sync(commit=True, confirm=SYNC_CONFIRM_TOKEN,
                                              downloader=_recording_downloader(table, []), **kw)
    assert first["status"] == SYNC_COMPLETE
    second = run_current_alpha_tournament_sync(commit=True, confirm=SYNC_CONFIRM_TOKEN,
                                               downloader=_recording_downloader(table, []), **kw)
    assert second["status"] == SYNC_NO_NEW
    assert second["wrote_store"] is False


def test_unavailable_when_packages_missing(tmp_path):
    res = run_current_alpha_tournament_sync(
        commit=True, confirm=SYNC_CONFIRM_TOKEN,
        downloader=_recording_downloader({}, []),
        champion_pkg_dir=tmp_path / "nope_c", challenger_pkg_dir=tmp_path / "nope_x",
        tournament_dir=tmp_path / "store", system_mark_dir=tmp_path / "nope_s")
    assert res["status"] == SYNC_UNAVAILABLE
    assert res["wrote_store"] is False


def test_safety_block_no_db_no_orders_no_prediction(tmp_path):
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN)
    for k in ("creates_orders", "creates_signals", "creates_trade_decisions", "creates_fills",
              "wrote_to_database", "champion_replaced", "promotes_to_live", "is_automation",
              "calls_prediction_service", "reranked", "rebalanced"):
        assert res[k] is False, k
    assert res["order_action_all"] == "NO_ORDER"
    assert res["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"


def test_no_eodhd_key_leak_in_store_or_payload(tmp_path, monkeypatch):
    monkeypatch.setenv("EODHD_API_KEY", "SECRET-KEY-DO-NOT-LEAK-123")
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN)
    blob = json.dumps(res)
    assert "SECRET-KEY-DO-NOT-LEAK-123" not in blob
    for f in (_SYNC_STATE_FILE, _SYNC_DATA_FILE, _SYNC_PRICES_FILE):
        assert "SECRET-KEY-DO-NOT-LEAK-123" not in (tdir / f).read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Alignment states (STALE / ALIGNED / PARTIAL_COVERAGE)
# --------------------------------------------------------------------------- #
def _static(latest, cov_pct=100.0):
    def bk(k, size, cov, total):
        return {"book_key": k, "covered_count": cov, "total_count": total,
                "coverage_pct": round(100.0 * cov / total, 2)}
    return {
        "horizon_progress": {"latest_common_owned_eod_date": latest},
        "calendar": {"end_date": latest},
        "book_isolation": {"all_isolated": True},
        "book_summaries": {
            "champion_top25": bk("champion_top25", 25, int(round(cov_pct / 100.0 * 25)), 25),
            "challenger_top25": bk("challenger_top25", 25, int(round(cov_pct / 100.0 * 25)), 25),
            "champion_top50": bk("champion_top50", 50, int(round(cov_pct / 100.0 * 50)), 50),
            "challenger_top50": bk("challenger_top50", 50, int(round(cov_pct / 100.0 * 50)), 50),
        },
    }


def _system_mark_dir(tmp_path, mark):
    d = tmp_path / "sysmark"
    (d / "latest").mkdir(parents=True, exist_ok=True)
    (d / "latest" / "refresh_manifest.json").write_text(
        json.dumps({"mark_date": mark, "blocked": False}), encoding="utf-8")
    return d


def test_alignment_stale_when_tournament_behind_system(tmp_path):
    sysd = _system_mark_dir(tmp_path, "2026-07-17")
    al = build_alignment_block(_static("2026-06-26", cov_pct=56.0),
                               tournament_dir=tmp_path / "empty", system_mark_dir=sysd)
    assert al["tournament_alignment"] == ALIGN_STALE
    assert al["latest_system_market_mark"] == "2026-07-17"
    assert al["latest_tournament_common_mark"] == "2026-06-26"
    assert al["mark_date_delta"] == 21
    assert al["is_stale"] is True


def test_alignment_partial_when_dates_equal_but_coverage_low(tmp_path):
    sysd = _system_mark_dir(tmp_path, "2026-06-26")
    al = build_alignment_block(_static("2026-06-26", cov_pct=56.0),
                               tournament_dir=tmp_path / "empty", system_mark_dir=sysd)
    assert al["tournament_alignment"] == ALIGN_PARTIAL
    assert al["mark_date_delta"] == 0


def test_alignment_aligned_after_full_sync(tmp_path):
    # a real synced store advanced to the system mark with 100% coverage
    res, tdir, _calls = _sync(tmp_path, commit=True, confirm=SYNC_CONFIRM_TOKEN)
    sysd = _system_mark_dir(tmp_path, DATES[-1])
    al = build_alignment_block(None, tournament_dir=tdir, system_mark_dir=sysd)
    assert al["tournament_alignment"] == ALIGN_ALIGNED
    assert al["latest_tournament_common_mark"] == DATES[-1]
    assert al["mark_date_delta"] == 0
    assert al["unresolved_ticker_count"] == 0
