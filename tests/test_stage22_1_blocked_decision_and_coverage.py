r"""Stage 22.1 — BLOCKED PORTFOLIO DECISION + HOLDING-ANALYTICS COVERAGE.

The two live defects observed on the real 2026-08-14 session, and the invariants that
make each of them unrepresentable.

DEFECT 1 — HOLDING-ANALYTICS COVERAGE
    The Daily Close and the Daily Research Cycle both completed. The Holding
    Opportunity-Cost assessment covered all 25 real holdings but only 15 of them had
    complete required analytics, because the point-in-time trailing prices were read
    ONLY from the frozen Phase-7I research CSV — 301 yfinance names ending 2026-06-22.
    Ten real holdings (AIZ DVA DVN DXCM EXPE FANG HST LH LYV XYZ) are not in those 301
    names at all, so return_20d / volatility_60d / dollar volume were absent for them,
    ``required_data_complete`` was 15/25 = 0.60, and the canonical reassessment
    correctly refused with INSUFFICIENT_HOLDING_DATA_COMPLETENESS.

    The owned bars were never missing. ``api.alpha_target.run_refresh`` — the owned-data
    refresh the Daily Close already composes — downloads ~380 trading days of owned
    daily OHLCV for EVERY current-universe name each session, computed four scalars from
    them and discarded the bars. Stage 22.1 persists the bounded trailing window it
    already had, and ``api.price_panel`` composes it over the frozen artifact.

DEFECT 2 — WORKFLOW SEMANTICS
    With the reassessment BLOCKED_DATA the workflow reported DAILY_CYCLE_COMPLETE /
    MONITOR_PORTFOLIO / "No portfolio change requires review", while the SAME payload
    reported SYSTEM_BLOCKER / BLOCKED_DATA / blocks_portfolio_action. "No proposal
    because BLOCKED" was presented as "no change because the economic gate cleared".

Everything here is offline, hermetic and read-only with respect to production: no
provider call, no prediction service, no live store, no order, no close, no research
run, no promotion. Every write goes to a tmp_path.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

from paper_trader.api import alpha_target as at
from paper_trader.api import price_panel as pp
from paper_trader.api import workflow_state as ws
from paper_trader.engine import holding_opportunity_cost as hoc_kernel
from paper_trader.engine import normal_cycle as nc
from paper_trader.engine import portfolio_reassessment as prs_kernel

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

ELIG = "2026-08-14"
#: The exact ten live holdings whose owned trailing analytics were absent.
LIVE_GAP_NAMES = ["AIZ", "DVA", "DVN", "DXCM", "EXPE", "FANG", "HST", "LH", "LYV", "XYZ"]
#: Fifteen live holdings the frozen research artifact DID cover.
LIVE_COVERED_NAMES = ["ABNB", "ALAB", "AMD", "ANET", "CAT", "CVS", "DDOG", "EOG",
                      "FTNT", "GWW", "ITW", "KEYS", "MNST", "SPG", "VLO"]


# =========================================================================== #
# Deterministic owned-bar helpers (no provider, no network).
# =========================================================================== #
def _sessions(n: int, through: str = ELIG) -> list[str]:
    """``n`` ascending pseudo-session dates ending at ``through`` (weekdays only)."""
    from datetime import date, timedelta
    d = date.fromisoformat(through)
    out: list[str] = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return sorted(out)


def _bars(ticker: str, n: int, through: str = ELIG) -> list[dict]:
    """An owned-EODHD-shaped EOD payload with a deterministic, strictly positive path."""
    seed = sum(ord(c) for c in ticker)
    rows = []
    for i, d in enumerate(_sessions(n, through)):
        px = 50.0 + (seed % 17) + 0.25 * ((i * (seed % 7 + 3)) % 23)
        rows.append({"date": d, "adjusted_close": round(px, 4), "close": round(px, 4),
                     "volume": 1_000_000 + (seed % 13) * 10_000})
    return rows


def _downloader(table: dict):
    def _get(symbol: str, _start: str):
        return table.get(symbol, [])
    return _get


def _panel_rows(path) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _seed_inputs(d: Path, tickers, market_as_of="2026-08-13") -> Path:
    """Seed a HERMETIC copy of the owned model-input store this refresh owns."""
    d = Path(d)
    d.mkdir(parents=True, exist_ok=True)
    with open(d / "current_momentum_scores.csv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=at._MOM_FIELDS)
        w.writeheader()
        for i, tk in enumerate(sorted(tickers)):
            w.writerow({"ticker": tk, "mom_6_1": 0.1 + i / 100.0, "is_member": 1,
                        "adv_dollar": 1.0e8, "realized_vol_63d": 0.25,
                        "trailing_obs_126": 126, "eligible_history": 1,
                        "extreme_flag": 0, "sector": "Tech",
                        "market_as_of_date": market_as_of, "month_label": "2026-08"})
    with open(d / "current_risk_stats.csv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["ticker", "realized_vol_63d", "beta_universe",
                                           "adv_dollar_20d", "max_drawdown_252d",
                                           "is_current_member", "last_price_date",
                                           "sector"])
        w.writeheader()
        for tk in sorted(tickers):
            w.writerow({"ticker": tk, "realized_vol_63d": 0.25, "beta_universe": 1.0,
                        "adv_dollar_20d": 1.0e8, "max_drawdown_252d": -0.2,
                        "is_current_member": 1, "last_price_date": market_as_of,
                        "sector": "Tech"})
    return d


@pytest.fixture()
def inputs_dir(tmp_path):
    """The hermetic owned model-input store, seeded with the live 25 holdings."""
    return _seed_inputs(tmp_path / "_inputs", LIVE_GAP_NAMES + LIVE_COVERED_NAMES)


def _refresh(inputs_dir, *, bars_per_ticker=140, tickers=None, through=ELIG,
             monkeypatch=None):
    """Run the REAL owned-data refresh against a hermetic store + injected bars."""
    names = tickers if tickers is not None else (LIVE_GAP_NAMES + LIVE_COVERED_NAMES)
    table = {tk: _bars(tk, bars_per_ticker, through) for tk in names}
    return at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN,
                          downloader=_downloader(table), inputs_dir=inputs_dir,
                          completed_through=through)


# =========================================================================== #
# A. THE ROOT CAUSE, STATED AS A TEST (1-4)
# =========================================================================== #
def test_01_the_frozen_research_panel_declares_that_it_may_omit_names(tmp_path):
    """The frozen artifact is a RESEARCH panel and says so: it documents a survivorship
    caveat and its own end date. Those are exactly the two properties that make it an
    incomplete OPERATIONAL source, so the composition below is required, not optional."""
    p = tmp_path / "f.csv"
    with open(p, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "ticker", "adjusted_close", "benchmark_close", "volume"])
        w.writerow(["2026-06-22", "AMD", "100.0", "400.0", "1000"])
    man = pp.load_price_panel(p)["manifest"]
    assert "delisted names absent" in man["survivorship_caveat"]
    assert man["date_end"] == "2026-06-22" and man["n_tickers"] == 1
    assert pp.FROZEN_SOURCE_KEY == "FROZEN_RESEARCH_PANEL"
    assert pp.OWNED_SOURCE_KEY == "OWNED_CURRENT_PANEL"


def test_02_required_data_complete_needs_rank_score_return20d_and_vol60d():
    """The four inputs whose absence produced the live 15/25. This pins the definition
    so a later change cannot quietly DROP one of them to clear the gate."""
    src = (REPO / "engine" / "holding_opportunity_cost.py").read_text(encoding="utf-8")
    assert ("required_data_complete = (current_rank is not None and current_score is not None"
            in src)
    assert "and return_20d is not None and vol_60d is not None)" in src


def test_03_the_completeness_floor_is_still_080():
    """The repair must never be a lowered threshold."""
    assert prs_kernel.default_policy()["min_holdings_data_complete_fraction"] == 0.80


def test_04_a_holding_absent_from_the_panel_is_reported_uncovered_by_name(tmp_path):
    panel = {"series": {"AMD": {"dates": _sessions(140), "adj": [10.0] * 140,
                                "ret": [None] * 140, "dollar_vol": [1.0] * 140,
                                "bench": [None] * 140, "bret": [None] * 140}},
             "manifest": {"series_source": {"AMD": pp.FROZEN_SOURCE_KEY}}}
    cov = pp.holding_coverage(panel=panel, tickers=["AMD", "AIZ", "XYZ"], as_of=ELIG)
    by = {r["ticker"]: r for r in cov["per_ticker"]}
    assert by["AMD"]["covered"] is True and by["AMD"]["bars_through_as_of"] == 140
    assert by["AIZ"]["covered"] is False
    assert by["AIZ"]["reason"] == "NO_OWNED_PRICE_HISTORY"
    assert by["XYZ"]["covered"] is False
    assert cov["uncovered_tickers"] == ["AIZ", "XYZ"]
    assert cov["tickers_covered"] == 1 and cov["coverage_fraction"] == round(1 / 3, 6)


# =========================================================================== #
# B. THE ACQUISITION REPAIR — the owner that already had the bars persists them (5-13)
# =========================================================================== #
def test_05_the_refresh_persists_the_trailing_window_it_already_fetched(inputs_dir):
    r = _refresh(inputs_dir)
    assert r["status"] == at.R_REFRESHED
    path = at.owned_panel_path(inputs_dir)
    assert path.exists()
    assert str(path) in r["artifacts_written"]
    assert r["counts"]["trailing_panel_tickers"] == 25
    assert r["counts"]["trailing_panel_rows"] == 25 * 140


def test_06_the_panel_is_written_into_the_store_this_owner_already_owns(inputs_dir):
    _refresh(inputs_dir)
    p = at.owned_panel_path(inputs_dir)
    assert p.parent == Path(inputs_dir)
    assert p.name == at.OWNED_PANEL_FILE
    # Beside the two CSVs this owner already writes — not a new store elsewhere.
    assert (Path(inputs_dir) / "current_momentum_scores.csv").exists()
    assert (Path(inputs_dir) / "current_risk_stats.csv").exists()


def test_07_the_persisted_panel_is_point_in_time(inputs_dir):
    """No bar may describe a session after the resolved market date."""
    _refresh(inputs_dir, through=ELIG, bars_per_ticker=140)
    rows = _panel_rows(at.owned_panel_path(inputs_dir))
    assert rows and all(r["date"] <= ELIG for r in rows)


def test_08_future_bars_supplied_by_a_provider_are_refused(inputs_dir):
    """A provider that returns a bar for a session that has not completed must not be
    able to put it in the owned window."""
    table = {tk: _bars(tk, 140, "2026-08-21")
             for tk in (LIVE_GAP_NAMES + LIVE_COVERED_NAMES)}
    at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN, downloader=_downloader(table),
                   inputs_dir=inputs_dir, completed_through=ELIG)
    rows = _panel_rows(at.owned_panel_path(inputs_dir))
    assert rows and max(r["date"] for r in rows) == ELIG


def test_09_the_write_is_idempotent(inputs_dir, tmp_path):
    _refresh(inputs_dir)
    first = at.owned_panel_path(inputs_dir).read_bytes()
    # A second identical fetch into a fresh store seeded the same way is byte-identical.
    other = _seed_inputs(tmp_path / "_inputs2", LIVE_GAP_NAMES + LIVE_COVERED_NAMES)
    _refresh(other)
    assert at.owned_panel_path(other).read_bytes() == first


def test_10_short_history_is_named_by_ticker_and_never_padded(inputs_dir):
    """A name whose owned window cannot support the 60-close windows is REPORTED, and
    its bars are written honestly short — never forward-filled to look complete."""
    names = LIVE_GAP_NAMES + LIVE_COVERED_NAMES
    table = {tk: _bars(tk, (20 if tk in ("AIZ", "XYZ") else 140)) for tk in names}
    r = at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN, downloader=_downloader(table),
                       inputs_dir=inputs_dir, completed_through=ELIG)
    assert r["trailing_panel_short_history_count"] == 2
    assert r["trailing_panel_short_history_tickers"] == ["AIZ", "XYZ"]
    per: dict[str, int] = {}
    for row in _panel_rows(at.owned_panel_path(inputs_dir)):
        per[row["ticker"]] = per.get(row["ticker"], 0) + 1
    assert per["AIZ"] == 20 and per["AMD"] == 140


def test_11_no_dollar_volume_is_invented_when_the_provider_supplies_none(inputs_dir):
    names = LIVE_GAP_NAMES + LIVE_COVERED_NAMES
    table = {}
    for tk in names:
        rows = _bars(tk, 140)
        if tk == "AIZ":
            for row in rows:
                row.pop("volume")
        table[tk] = rows
    at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN, downloader=_downloader(table),
                   inputs_dir=inputs_dir, completed_through=ELIG)
    rows = _panel_rows(at.owned_panel_path(inputs_dir))
    aiz = [r for r in rows if r["ticker"] == "AIZ"]
    assert aiz and all(r["dollar_volume"] == "" for r in aiz)
    amd = [r for r in rows if r["ticker"] == "AMD"]
    assert amd and all(float(r["dollar_volume"]) > 0 for r in amd)
    # ...and the reader reports it as unavailable rather than zero.
    panel = pp.load_owned_current_panel(at.owned_panel_path(inputs_dir))
    assert all(v is None for v in panel["series"]["AIZ"]["dollar_vol"])


def test_12_the_refresh_creates_no_order_and_confirms_no_snapshot(inputs_dir):
    r = _refresh(inputs_dir)
    for k in ("orders_created", "signals_created", "trade_decisions_created",
              "fills_created", "alpha_book_initialized", "snapshot_confirmed",
              "model_formulas_changed", "model_weights_changed",
              "prediction_service_called"):
        assert r[k] is False, k
    assert r["historical_evidence_modified"] is False


def test_13_build_owned_panel_rows_is_pure_and_deterministic():
    series = {"BBB": [("2026-08-13", 2.0, 2.0, 10.0)],
              "AAA": [("2026-08-12", 1.0, 1.0, 5.0), ("2026-08-13", 1.5, 1.5, 6.0)]}
    rows1, short1 = at.build_owned_panel_rows(series)
    rows2, short2 = at.build_owned_panel_rows(series)
    assert rows1 == rows2 and short1 == short2
    assert [r["ticker"] for r in rows1] == ["AAA", "AAA", "BBB"]
    assert [r["date"] for r in rows1[:2]] == ["2026-08-12", "2026-08-13"]
    assert sorted(short1) == ["AAA", "BBB"]        # both far below the 61-bar minimum


# =========================================================================== #
# C. THE READ SEAM — one owner, one basis per series (14-20)
# =========================================================================== #
@pytest.fixture()
def frozen_csv(tmp_path):
    """A frozen research panel covering only the 15 names, ending BEFORE the session."""
    p = tmp_path / "frozen.csv"
    with open(p, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "ticker", "adjusted_close", "benchmark_close", "volume",
                    "dollar_volume"])
        for tk in LIVE_COVERED_NAMES + ["SPY"]:
            for i, d in enumerate(_sessions(200, "2026-06-22")):
                w.writerow([d, tk, round(20.0 + i * 0.1, 4), 400.0, 1000, 20000.0])
    return p


def test_14_the_operational_panel_covers_every_holding_after_the_repair(
        inputs_dir, frozen_csv):
    _refresh(inputs_dir)
    panel = pp.load_operational_price_panel(
        frozen_path=frozen_csv, owned_path=at.owned_panel_path(inputs_dir))
    cov = pp.holding_coverage(panel=panel,
                              tickers=LIVE_GAP_NAMES + LIVE_COVERED_NAMES, as_of=ELIG)
    assert cov["uncovered_tickers"] == []
    assert cov["tickers_covered"] == 25 and cov["coverage_fraction"] == 1.0


def test_15_without_the_owned_window_the_ten_names_stay_uncovered(frozen_csv, tmp_path):
    """The pre-repair world, reproduced exactly: the frozen artifact alone leaves the
    ten live names with no history at all."""
    panel = pp.load_operational_price_panel(frozen_path=frozen_csv,
                                            owned_path=tmp_path / "absent.csv")
    assert panel["manifest"]["owned_current_available"] is False
    cov = pp.holding_coverage(panel=panel,
                              tickers=LIVE_GAP_NAMES + LIVE_COVERED_NAMES, as_of=ELIG)
    assert sorted(cov["uncovered_tickers"]) == sorted(LIVE_GAP_NAMES)
    assert cov["tickers_covered"] == 15
    assert round(cov["coverage_fraction"], 2) == 0.60


def test_16_one_adjustment_basis_per_series_never_a_splice(inputs_dir, frozen_csv):
    """A ticker present in BOTH sources is served entirely from the owned window; the
    two bases are never concatenated, so no return is manufactured at a join."""
    _refresh(inputs_dir)
    panel = pp.load_operational_price_panel(
        frozen_path=frozen_csv, owned_path=at.owned_panel_path(inputs_dir))
    src = panel["manifest"]["series_source"]
    assert src["AMD"] == pp.OWNED_SOURCE_KEY          # in both -> owned wins WHOLE
    owned_only = pp.load_owned_current_panel(at.owned_panel_path(inputs_dir))
    frozen_only = pp.load_price_panel(frozen_csv)
    # The two sources genuinely OVERLAP in time, so a splice was possible and was not
    # taken: the composed series is exactly the owned one, never owned+frozen.
    assert set(frozen_only["series"]["AMD"]["dates"]) & set(
        owned_only["series"]["AMD"]["dates"])
    assert panel["series"]["AMD"] == owned_only["series"]["AMD"]
    assert len(panel["series"]["AMD"]["dates"]) == 140
    assert len(panel["series"]["AMD"]["dates"]) < (
        len(frozen_only["series"]["AMD"]["dates"]) + 140)


def test_17_a_ticker_only_in_the_frozen_artifact_still_resolves(tmp_path, frozen_csv):
    """The owned window covers only the ten gap names here; the fifteen the frozen
    artifact carries must still resolve, from the frozen artifact."""
    idir = _seed_inputs(tmp_path / "_inputs_gaps", LIVE_GAP_NAMES)
    _refresh(idir, tickers=LIVE_GAP_NAMES)
    panel = pp.load_operational_price_panel(
        frozen_path=frozen_csv, owned_path=at.owned_panel_path(idir))
    assert panel["manifest"]["series_source"]["AMD"] == pp.FROZEN_SOURCE_KEY
    assert panel["manifest"]["series_source"]["AIZ"] == pp.OWNED_SOURCE_KEY
    assert panel["manifest"]["owned_current_tickers"] == 10


def test_18_the_research_panel_reader_is_unchanged(frozen_csv, inputs_dir):
    """``load_price_panel`` must keep serving the frozen research artifact verbatim, so
    no backtest silently changes basis because the operational repair landed."""
    _refresh(inputs_dir)
    research = pp.load_price_panel(frozen_csv)
    assert sorted(research["series"]) == sorted(LIVE_COVERED_NAMES + ["SPY"])
    assert "composition" not in research["manifest"]
    assert research["manifest"]["date_end"] == "2026-06-22"


def test_19_the_operational_loaders_all_read_the_composed_panel():
    for mod in ("api/holding_opportunity_cost.py", "api/reallocation_proposal.py",
                "api/app.py"):
        src = (REPO / mod).read_text(encoding="utf-8")
        assert "load_operational_price_panel()" in src, mod
        assert "pp.load_price_panel()" not in src, mod
        assert "_pp.load_price_panel()" not in src, mod


def test_20_the_panel_owner_is_still_exactly_one_module():
    """No second trailing-price READER may appear: the owned window has exactly one."""
    hits = []
    for p in (REPO / "api").rglob("*.py"):
        src = p.read_text(encoding="utf-8", errors="ignore")
        if "load_owned_current_panel" in src and p.name != "price_panel.py":
            hits.append(p.name)
    assert hits == []


# =========================================================================== #
# D. END TO END: the coverage repair clears the gate honestly (21-24)
# =========================================================================== #
def _positions(tickers):
    return [{"ticker": tk, "sector": "Tech", "quantity": 100,
             "current_weight": 1.0 / len(tickers), "market_value": 4000.0,
             "price": 40.0} for tk in tickers]


def _universe(tickers):
    return [{"ticker": tk, "rank": i + 1, "percentile": 0.9 - i / 100.0,
             "combined_score": 0.9 - i / 100.0, "sector": "Tech", "eligible": True,
             "adv_dollar": 1.0e8} for i, tk in enumerate(tickers)]


def _assess(panel):
    from paper_trader.api import holding_opportunity_cost as hoc
    held = LIVE_GAP_NAMES + LIVE_COVERED_NAMES
    ic = hoc.build_input_contract(
        portfolio_state={"dates": {"eligible_market_date": ELIG},
                         "active_book": {"book_id": "b1"},
                         "capital": {"nav": 100000.0, "cash": 0.0},
                         "positions": _positions(held), "state_hash": "h"},
        scoring={"rankings": _universe(held), "output_hash": "u", "ranking_date": ELIG},
        price_panel=panel, previous_ranking_state="UNAVAILABLE")
    return hoc.run_assessment(input_contract=ic)["assessment"]


def test_21_before_the_repair_the_assessment_is_15_of_25(frozen_csv, tmp_path):
    panel = pp.load_operational_price_panel(frozen_path=frozen_csv,
                                            owned_path=tmp_path / "absent.csv")
    a = _assess(panel)
    dq = a["data_quality"]
    assert dq["holdings_evaluated"] == 25 and dq["holdings_data_complete"] == 15
    incomplete = sorted(r["ticker"] for r in a["holding_reviews"]
                        if not r["required_data_complete"])
    assert incomplete == sorted(LIVE_GAP_NAMES)


def test_22_the_blocked_reassessment_is_reproduced_exactly(frozen_csv, tmp_path):
    a = _assess(pp.load_operational_price_panel(frozen_path=frozen_csv,
                                                owned_path=tmp_path / "absent.csv"))
    from paper_trader.api import portfolio_reassessment as prs
    res = prs.run_reassessment(input_contract=prs.build_input_contract(
        portfolio_state={"dates": {"eligible_market_date": ELIG},
                         "active_book": {"book_id": "b1", "initialized": True,
                                         "holdings_count": 25},
                         "capital": {"nav": 100000.0, "cash": 0.0}, "state_hash": "h"},
        scoring={"output_hash": "u"}, hoc_assessment=a,
        freshness={"eligible_market_date": ELIG, "source_freshness": []},
        policy=prs.resolve_policy()))["reassessment"]
    assert res["reassessment_state"] == prs_kernel.STATE_BLOCKED_DATA
    codes = [b["code"] if isinstance(b, dict) else b for b in res["decision"]["blockers"]]
    assert "INSUFFICIENT_HOLDING_DATA_COMPLETENESS" in codes
    for k in ("expected_net_improvement", "expected_one_way_turnover",
              "expected_transaction_cost_usd"):
        assert res["decision"][k] is None, k
    assert res["decision"]["proposal_required"] is False


def test_23_after_the_repair_all_25_holdings_are_complete(inputs_dir, frozen_csv):
    _refresh(inputs_dir)
    a = _assess(pp.load_operational_price_panel(
        frozen_path=frozen_csv, owned_path=at.owned_panel_path(inputs_dir)))
    dq = a["data_quality"]
    assert dq["holdings_evaluated"] == 25 and dq["holdings_data_complete"] == 25
    assert all(r["required_data_complete"] for r in a["holding_reviews"])
    assert all(r["return_20d"] is not None and r["volatility_60d"] is not None
               for r in a["holding_reviews"])
    assert all(r["liquidity_state"] != hoc_kernel.LIQ_UNAVAILABLE
               for r in a["holding_reviews"])


def test_24_a_name_with_genuinely_short_owned_history_stays_incomplete(
        inputs_dir, frozen_csv):
    """The repair must not manufacture completeness. A holding the provider can only
    supply 30 bars for is still honestly incomplete."""
    names = LIVE_GAP_NAMES + LIVE_COVERED_NAMES
    table = {tk: _bars(tk, (30 if tk == "AIZ" else 140)) for tk in names}
    at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN, downloader=_downloader(table),
                   inputs_dir=inputs_dir, completed_through=ELIG)
    a = _assess(pp.load_operational_price_panel(
        frozen_path=frozen_csv, owned_path=at.owned_panel_path(inputs_dir)))
    by = {r["ticker"]: r for r in a["holding_reviews"]}
    assert by["AIZ"]["required_data_complete"] is False
    assert by["AIZ"]["volatility_60d"] is None
    assert by["AMD"]["required_data_complete"] is True
    assert a["data_quality"]["holdings_data_complete"] == 24


# =========================================================================== #
# E. DEFECT 2 — the blocked decision may never read as "no change" (25-36)
# =========================================================================== #
def test_25_blocked_data_suspends_the_cycle():
    assert ws.reassessment_blocks_cycle(
        reassessment_state="BLOCKED_DATA",
        blockers=["INSUFFICIENT_HOLDING_DATA_COMPLETENESS"]) is True


def test_26_unresolvable_blocked_evidence_suspends_the_cycle():
    assert ws.reassessment_blocks_cycle(
        reassessment_state="BLOCKED_EVIDENCE",
        blockers=[{"code": "SOMETHING_THE_NEXT_CYCLE_WILL_NOT_FIX"}]) is True


def test_27_expected_stale_evidence_does_not_suspend_the_cycle():
    """The Stage-22 Workstream-B distinction is reused, not re-derived: an assessment
    whose every named cause is superseded by the next cycle is not a blocker."""
    assert ws.reassessment_blocks_cycle(
        reassessment_state="BLOCKED_EVIDENCE",
        blockers=["STALE_CORPORATE_ACTION_EVIDENCE"]) is False
    assert ws.reassessment_blocks_cycle(
        reassessment_state="STALE_CORPORATE_ACTION_REVIEW_REQUIRED",
        blockers=["PORTFOLIO_STATE_CHANGED_SINCE_ASSESSMENT"]) is False


def test_28_a_healthy_reassessment_never_suspends_the_cycle():
    for state in ("CURRENT_NO_CHANGE", "CHANGE_CANDIDATE", "PROPOSAL_READY",
                  "NOT_READY", "NOT_RUN", "MANUAL_REVIEW_REQUIRED", "UNAVAILABLE"):
        assert ws.reassessment_blocks_cycle(reassessment_state=state,
                                            blockers=[]) is False, state


def test_29_the_gate_and_the_evidence_classifier_can_never_disagree():
    """Whenever the cycle is suspended the classifier must call it a SYSTEM BLOCKER,
    and whenever it is not suspended a blocked state must be EXPECTED stale."""
    cases = [("BLOCKED_DATA", ["INSUFFICIENT_HOLDING_DATA_COMPLETENESS"]),
             ("BLOCKED_EVIDENCE", ["STALE_CORPORATE_ACTION_EVIDENCE"]),
             ("BLOCKED_EVIDENCE", ["UNKNOWN_CAUSE"]),
             ("BLOCKED_EVIDENCE", []),
             ("STALE_CORPORATE_ACTION_REVIEW_REQUIRED", ["ASSESSMENT_ELIGIBLE_DATE_MISMATCH"])]
    for state, blockers in cases:
        suspends = ws.reassessment_blocks_cycle(reassessment_state=state,
                                                blockers=blockers)
        overall = ws.RESEARCH_CYCLE_BLOCKED if suspends else ws.DAILY_CYCLE_COMPLETE
        ec = ws.build_evidence_classification(reassessment_state=state, overall=overall,
                                              blockers=blockers)
        expect = ws.EVIDENCE_SYSTEM_BLOCKER if suspends else ws.EVIDENCE_EXPECTED_STALE
        assert ec["classification"] == expect, (state, blockers)
        # Fail-closed semantics are untouched in BOTH directions.
        assert ec["blocks_portfolio_action"] is True


def test_30_the_priority_policy_refuses_to_complete_a_blocked_session():
    kw = dict(inconsistent=False, session_status="AFTER_SESSION_CLOSE",
              has_confirmed_eligible=True, eligible_session_closed=True,
              owned_data_lag=False, research_current=True, assessment_status="CURRENT",
              manual_review_required=False, evidence_gap=False, cycle_complete=True,
              hoc_current=True)
    assert ws._decide_overall(**kw) == ws.DAILY_CYCLE_COMPLETE
    assert ws._decide_overall(**kw, reassessment_blocked=True) == ws.RESEARCH_CYCLE_BLOCKED


def test_31_an_evidence_gap_cannot_outrank_a_blocked_reassessment():
    kw = dict(inconsistent=False, session_status="AFTER_SESSION_CLOSE",
              has_confirmed_eligible=True, eligible_session_closed=True,
              owned_data_lag=False, research_current=True, assessment_status="CURRENT",
              evidence_gap=True, cycle_complete=True, hoc_current=True,
              reassessment_blocked=True)
    assert ws._decide_overall(manual_review_required=False, **kw) == ws.RESEARCH_CYCLE_BLOCKED
    # ...nor may the LEGACY membership-comparison review gate, which has no decision
    # authority at all.
    assert ws._decide_overall(manual_review_required=True, **kw) == ws.RESEARCH_CYCLE_BLOCKED


def test_32_an_earlier_named_cause_still_outranks_the_blocked_reassessment():
    """Ordering is preserved: an unconfirmed session, an in-flight or input-blocked
    cycle, an unclosed session and a due research cycle each name an EARLIER fix."""
    base = dict(inconsistent=False, session_status="AFTER_SESSION_CLOSE",
                has_confirmed_eligible=True, eligible_session_closed=True,
                owned_data_lag=False, research_current=True,
                assessment_status="CURRENT", manual_review_required=False,
                evidence_gap=False, cycle_complete=True, hoc_current=True,
                reassessment_blocked=True)
    assert ws._decide_overall(**{**base, "owned_data_lag": True}) == ws.WAITING_FOR_OWNED_DATA
    assert ws._decide_overall(**{**base, "cycle_running": True}) == ws.RESEARCH_CYCLE_RUNNING
    assert ws._decide_overall(**{**base, "eligible_session_closed": False}) \
        == ws.READY_FOR_DAILY_CLOSE
    assert ws._decide_overall(**{**base, "research_current": False}) \
        == ws.RESEARCH_CYCLE_REQUIRED
    assert ws._decide_overall(**{**base, "inconsistent": True}) == ws.INCONSISTENT_STATE


def test_33_the_blocked_decision_names_its_cause_and_points_back_at_the_cycle():
    a = ws._primary_action(ws.RESEARCH_CYCLE_BLOCKED, {
        "eligible_date": ELIG, "reassessment_blocked": True,
        "reassessment_blocker_codes": ["INSUFFICIENT_HOLDING_DATA_COMPLETENESS"]})
    assert a["action_code"] == ws.ACTION_RESOLVE_RESEARCH_BLOCKER
    assert "INSUFFICIENT_HOLDING_DATA_COMPLETENESS" in a["explanation"]
    assert "Daily Research Cycle" in a["explanation"]
    assert a["execution_available"] is False
    assert a["severity"] == ws.SEV_BLOCKED
    assert "no change" in a["explanation"].lower()
    # The monthly-emitter blocker keeps its own wording.
    b = ws._primary_action(ws.RESEARCH_CYCLE_BLOCKED, {"eligible_date": ELIG})
    assert "monthly momentum input" in b["explanation"]


def test_34_the_suspended_cycle_offers_no_mutation_and_no_rebalance():
    v = nc.build_cycle_view(overall=ws.RESEARCH_CYCLE_BLOCKED, eligible_market_date=ELIG,
                            latest_completed_close_date=ELIG,
                            blockers=["INSUFFICIENT_HOLDING_DATA_COMPLETENESS"],
                            completed_stages=[nc.STAGE_DAILY_CLOSE,
                                              nc.STAGE_DAILY_RESEARCH_CYCLE])
    assert v["current_stage"] == nc.STAGE_RECOVERY
    assert v["in_recovery"] is True
    assert v["executable_stages"] == [] and v["executable_stage_count"] == 0
    assert v["action_required"] is True and v["no_action_required"] is False
    gates = v["stage_gates"]
    assert all(not g["execution_allowed"] for g in gates.values())
    assert all(not g["review_required"] for g in gates.values())
    assert gates[nc.STAGE_CONTROLLED_REBALANCE]["execution_allowed"] is False
    assert v["creates_orders"] is False and v["automatic_execution"] is False
    assert v["blockers"] == ["INSUFFICIENT_HOLDING_DATA_COMPLETENESS"]


def test_35_the_suspended_cycle_never_says_no_change_requires_review():
    v = nc.build_cycle_view(overall=ws.RESEARCH_CYCLE_BLOCKED, eligible_market_date=ELIG,
                            latest_completed_close_date=ELIG,
                            completed_stages=[nc.STAGE_DAILY_CLOSE,
                                              nc.STAGE_DAILY_RESEARCH_CYCLE])
    decision = v["stage_gates"][nc.STAGE_PORTFOLIO_DECISION]["passive_status"]
    assert "No portfolio change requires review" not in decision
    assert "reached no verdict" in decision
    assert "Daily Research Cycle" in decision


def test_36_suspending_the_cycle_preserves_the_work_that_completed():
    """The valid Daily Close and the completed signal refresh are still reported as
    done — a suspended cycle must not retroactively blank recorded work."""
    v = nc.build_cycle_view(overall=ws.RESEARCH_CYCLE_BLOCKED, eligible_market_date=ELIG,
                            latest_completed_close_date=ELIG,
                            completed_stages=[nc.STAGE_DAILY_CLOSE,
                                              nc.STAGE_DAILY_RESEARCH_CYCLE])
    by = {s["stage"]: s["status"] for s in v["stages"]}
    assert by[nc.STAGE_DAILY_CLOSE] == nc.ST_DONE
    assert by[nc.STAGE_DAILY_RESEARCH_CYCLE] == nc.ST_DONE
    assert by[nc.STAGE_CONTROLLED_REBALANCE] == nc.ST_UPCOMING
    assert "Daily Close complete for %s" % ELIG in \
        v["stage_gates"][nc.STAGE_DAILY_CLOSE]["passive_status"]
    assert "Daily Research Cycle complete for %s" % ELIG in \
        v["stage_gates"][nc.STAGE_DAILY_RESEARCH_CYCLE]["passive_status"]
    assert v["completed_stages"] == sorted([nc.STAGE_DAILY_CLOSE,
                                            nc.STAGE_DAILY_RESEARCH_CYCLE])


# =========================================================================== #
# F. THE LIVE-SHAPED COMPOSITION, END TO END (37-42)
# =========================================================================== #
@pytest.fixture(scope="module")
def fx():
    import stage20_ui_fixtures as _fx
    return _fx


@pytest.fixture(scope="module")
def live_shaped(fx):
    return fx.compose("scenario_10_reassessment_data_blocked")


def test_37_the_live_shaped_scenario_exists(fx):
    assert "scenario_10_reassessment_data_blocked" in fx.SCENARIO_KEYS


def test_38_the_scenario_reproduces_the_live_evidence(live_shaped):
    panels = live_shaped["panels"]
    hocp = panels["holding_opportunity_cost"]
    assert hocp["state"] == "DEGRADED"
    dq = hocp["data_quality"]
    assert dq["holdings_evaluated"] == 25 and dq["holdings_data_complete"] == 15
    incomplete = sorted(r["ticker"] for r in hocp["holding_reviews"]
                        if not r["required_data_complete"])
    assert incomplete == sorted(LIVE_GAP_NAMES)


def test_39_the_scenario_reassessment_is_blocked_with_the_named_cause(live_shaped):
    prsp = live_shaped["panels"]["portfolio_reassessment"]
    assert prsp["state"] == "BLOCKED_DATA"
    blob = json.dumps(prsp)
    assert "INSUFFICIENT_HOLDING_DATA_COMPLETENESS" in blob


def test_40_the_scenario_workflow_is_not_complete_and_not_monitor(live_shaped):
    wf = live_shaped["panels"]["workflow_state"]
    assert wf["overall_state"] == ws.RESEARCH_CYCLE_BLOCKED
    assert wf["overall_state"] != ws.DAILY_CYCLE_COMPLETE
    primary = wf["primary_action"]
    assert primary["action_code"] != ws.ACTION_MONITOR
    assert primary["action_code"] == ws.ACTION_RESOLVE_RESEARCH_BLOCKER
    assert "INSUFFICIENT_HOLDING_DATA_COMPLETENESS" in primary["explanation"]
    codes = [b.get("code") for b in (wf.get("blockers") or []) if isinstance(b, dict)]
    assert "INSUFFICIENT_HOLDING_DATA_COMPLETENESS" in codes


def test_41_the_scenario_offers_no_mutation_and_no_proposal(live_shaped):
    wf = live_shaped["panels"]["workflow_state"]
    cyc = wf["normal_cycle"]
    assert cyc["current_stage"] == nc.STAGE_RECOVERY
    assert cyc["executable_stages"] == []
    assert live_shaped["consistency"]["mutation_action_count"] == 0
    rp = live_shaped["panels"]["reallocation_proposal"]
    assert rp["state"] in ("NOT_RUN", "UNAVAILABLE")
    assert rp.get("proposal_hash") is None
    assert rp.get("approvable", False) is False
    assert rp.get("executable", False) is False


def test_42_every_scenario_including_the_live_shaped_one_stays_consistent(fx):
    failures, lines = fx.check()
    assert failures == 0, "\n".join(lines)
    assert len(fx.SCENARIO_KEYS) >= 11
