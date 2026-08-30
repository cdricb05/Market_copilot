r"""Release 50 - MULTI-ASSET OPERATIONAL CAPITAL MANAGER.

Every scenario the release specification names is exercised here, hermetically
(temp desk / decision / plan / evidence roots, a JSON reference-data fixture, an
injected mark downloader; no live endpoint, no operational ledger, no research root):

  A. only equities qualify               -> no fake non-equity allocation; equity behaviour preserved
  B. futures become eligible             -> they enter the ONE frontier and can receive target capital
  C. rates + equities eligible           -> a cross-asset target can carry rates
  D. research-only crypto challenger     -> zero operational capital
  E. non-USD instrument                  -> correct USD NAV (EUR contract, owned FX conversion)
  F. futures quantity / multiplier / notional / collateral correct
  G. cost model                          -> implemented (declared per-class policy), never deferred
  H. accounting that cannot be defined   -> stays ineligible (a sleeve with no owned mark)
  I. cross-asset constraint breached     -> the optimiser solves around it (reshape, never a freeze)
  J. non-equity opportunities weak       -> zero allocation is valid
  K. current equity portfolio best       -> HOLD_CURRENT_BOOK
  L. multi-asset target superior         -> PROPOSAL_READY
  M. no feasible trustworthy target      -> TRUE_BLOCKER
  N. manual approval absent              -> zero orders
  O. second confirmation absent          -> zero fills
  P. paper execution                     -> correct holdings / cash / NAV / collateral
  Q. replay                              -> no duplicate transition
  R. decision snapshot                   -> Today + Portfolio share NAV / date / decision identity
  S. R46                                 -> never reachable from an operational owner
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from paper_trader.api import capital_pool as cp
from paper_trader.api import cross_asset_risk as xr_api
from paper_trader.api import decision_snapshot as ds
from paper_trader.api import investability_registry as ir
from paper_trader.api import market_reference_data as mrd
from paper_trader.api import opportunity_frontier as of_api
from paper_trader.api import operator_presentation as op
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_decision_outcome as pdo
from paper_trader.api import rebalance_execution as rb
from paper_trader.api import reallocation_proposal as arp
from paper_trader.engine import constrained_reallocation as CR
from paper_trader.engine import cross_asset_risk as XR
from paper_trader.engine import instrument_contract as IC
from paper_trader.engine import opportunity_frontier as OF
from paper_trader.engine import portfolio_decision_outcome as PDOK
from paper_trader.engine import reallocation_proposal as RP
from paper_trader.engine import zero_base_allocator as ZB

REPO = Path(__file__).resolve().parents[1]

# --------------------------------------------------------------------------- #
# Hermetic reference-data fixture (replaces the vendor entirely)
# --------------------------------------------------------------------------- #
_DATES = ["2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31"]


def _series(base: float, step: float, vol: float = 1000.0):
    return [[d, round(base + i * step, 6), vol] for i, d in enumerate(_DATES)]


FIXTURE = {
    "databases": ["Continuous Futures", "Forex Spot"],
    "futures_symbols": ["&ZN", "&MES", "&FDAX", "&BTC", "&ES", "&CL", "&VX", "&6E"],
    "fx_symbols": ["EURUSD", "USDJPY", "GBPUSD"],
    "metadata": {
        "&CL": {"market_name": "Crude Oil", "point_value": 1000.0, "initial_margin": 5720.0,
                "currency": "USD", "tick_size": 0.01, "exchange": "NYMEX"},
        "&VX": {"market_name": "CBOE Volatility Index", "point_value": 1000.0, "initial_margin": 8800.0,
                "currency": "USD", "tick_size": 0.05, "exchange": "CBOE"},
        "&6E": {"market_name": "Euro FX", "point_value": 125000.0, "initial_margin": 2970.0,
                "currency": "USD", "tick_size": 0.00005, "exchange": "CME"},
        "&ZN": {"market_name": "10-Year U.S. T-Note", "point_value": 1000.0, "initial_margin": 2063.0,
                "currency": "USD", "tick_size": 0.015625, "exchange": "CBOT"},
        "&MES": {"market_name": "Micro E-mini S&P 500", "point_value": 5.0, "initial_margin": 1606.0,
                 "currency": "USD", "tick_size": 0.25, "exchange": "CME"},
        "&ES": {"market_name": "E-mini S&P 500", "point_value": 50.0, "initial_margin": 16060.0,
                "currency": "USD", "tick_size": 0.25, "exchange": "CME"},
        "&FDAX": {"market_name": "DAX", "point_value": 25.0, "initial_margin": 44602.0,
                  "currency": "EUR", "tick_size": 0.5, "exchange": "Eurex"},
        "&BTC": {"market_name": "Bitcoin", "point_value": 5.0, "initial_margin": 79880.0,
                 "currency": "USD", "tick_size": 5.0, "exchange": "CME"},
    },
    "closes": {
        "&CL": _series(70.0, 0.5, 300_000),
        "&VX": _series(16.0, 0.2, 100_000),
        "&6E": _series(1.15, 0.001, 200_000),
        "&ZN": _series(108.0, 0.25, 3_000_000),
        "&MES": _series(6400.0, 10.0, 800_000),
        "&ES": _series(6400.0, 10.0, 1_500_000),
        "&FDAX": _series(26000.0, 100.0, 20_000),
        "&BTC": _series(60000.0, 500.0, 10_000),
        "EURUSD": _series(1.15, 0.001, None),
        "USDJPY": _series(150.0, 0.5, None),
        "GBPUSD": _series(1.30, 0.001, None),
    },
}


@pytest.fixture()
def refdata(tmp_path, monkeypatch):
    p = tmp_path / "refdata_fixture.json"
    p.write_text(json.dumps(FIXTURE), encoding="utf-8")
    monkeypatch.setenv(mrd.FIXTURE_ENV, str(p))
    mrd.reset_cache()
    yield p
    mrd.reset_cache()


@pytest.fixture()
def stores(tmp_path, monkeypatch, refdata):
    """Every operational store root redirected to a temp root; production untouched."""
    roots = {
        "PAPER_TRADER_DESK_DIR": tmp_path / "desk",
        "PAPER_TRADER_CORPORATE_ACTIONS_DIR": tmp_path / "corporate_actions",
        "PAPER_TRADER_PORTFOLIO_DECISION_DIR": tmp_path / "decisions",
        "PAPER_TRADER_REBALANCE_PLAN_DIR": tmp_path / "plans",
        "PAPER_TRADER_PORTFOLIO_DECISION_OUTCOME_DIR": tmp_path / "outcomes",
        "PAPER_TRADER_REALLOC_DIR": tmp_path / "realloc",
        "PAPER_TRADER_HOC_DIR": tmp_path / "hoc",
        "PAPER_TRADER_REASSESSMENT_DIR": tmp_path / "reassess",
        "PAPER_TRADER_DRC_DIR": tmp_path / "drc",
        "PAPER_TRADER_MHZ_LEDGER_DIR": tmp_path / "mhz",
    }
    for k, v in roots.items():
        v.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv(k, str(v))
    monkeypatch.setenv("PAPER_TRADER_ACCEPTANCE_MODE", "1")
    ds.reset()
    return {k: v for k, v in roots.items()}


def _downloader(symbol: str, _start: str):
    """EODHD-shaped bars for every symbol the tests price (equities + fixture)."""
    eq = {"AAA": 100.0, "BBB": 50.0, "SPY": 500.0}
    if symbol in eq:
        return [{"date": d, "adjusted_close": round(eq[symbol] * (1 + 0.01 * i), 4)}
                for i, d in enumerate(_DATES)]
    rows = FIXTURE["closes"].get(symbol) or []
    return [{"date": r[0], "close": r[1]} for r in rows]


def _make_book(desk_dir: Path, *, initial_capital: float, fills: list) -> dict:
    book = {"book_id": "alpha_paper_book_1", "book_number": 1, "display_name": "Alpha Paper Book #1",
            "creation_date": "2026-08-20", "created_at": "2026-08-20T00:00:00Z",
            "model_id": "fundamental_momentum_50_50_v1", "model_version": "v1",
            "execution_model": "NEXT_CLOSE", "transaction_cost_bps_per_side": 12.5,
            "transaction_cost_bps_round_trip": 25.0, "review_cadence": "monthly",
            "initial_capital": float(initial_capital), "currency": "USD_PAPER",
            "frozen_target_weights": {}, "snapshot_id": "snap_test", "snapshot_market_date": "2026-08-24",
            "benchmark": "SPY", "status": "OPEN", "immutable_record": True}
    desk._append_ledger(desk_dir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
    if fills:
        desk._append_ledger(desk_dir, desk.FILLS_FILE, [{"event": "PAPER_FILL", "fill": f} for f in fills])
    return book


def _equity_fill(tk, qty, px, date="2026-08-24"):
    gross = qty * px
    cost = gross * desk.COST_RATE_PER_SIDE
    return {"fill_id": "fill_%s" % tk, "order_id": "ord_%s" % tk, "book_id": "alpha_paper_book_1",
            "ticker": tk, "side": desk.SIDE_BUY, "quantity": qty, "fill_date": date,
            "fill_price": px, "gross_value": round(gross, 2), "transaction_cost": round(cost, 4),
            "cost_bps_per_side": 12.5, "net_cash_delta": round(-(gross + cost), 4),
            "execution_model": "NEXT_CLOSE", "price_source": "OWNED_EODHD_ADJUSTED_CLOSE_AS_RECORDED",
            "immutable": True}


# =========================================================================== #
# 1. THE POSITION CONTRACT (F, E)
# =========================================================================== #
class TestPositionContract:

    def test_01_equity_row_without_instrument_block_is_the_pre_r50_contract(self):
        d = IC.descriptor_from_row({"ticker": "AAPL", "quantity": 10, "fill_price": 100.0})
        assert d["instrument_type"] == IC.IT_CASH_EQUITY and d["multiplier"] == 1.0
        assert d["currency"] == "USD" and d["execution_convention"] == "NEXT_CLOSE"
        pos = IC.value_position(d, quantity=10, mark=110.0, cost_basis_usd=1000.0, nav=10000.0)
        assert pos["market_value_usd"] == 1100.0 and pos["notional_usd"] == 1100.0
        assert pos["collateral_usd"] == 0.0 and pos["unrealized_pnl_usd"] == 100.0
        assert pos["exposure_weight"] == 0.11 and pos["capital_usage_weight"] == 0.11

    def test_02_future_is_not_valued_like_a_cash_equity(self):
        d = IC.describe("&ZN", asset_class=IC.AC_RATES_FUTURES, instrument_type=IC.IT_FUTURE,
                        sleeve_id="s", multiplier=1000.0, initial_margin_per_unit=2063.0)
        pos = IC.value_position(d, quantity=2, mark=108.5, entry_mark=108.0, nav=1_000_000.0)
        assert pos["notional_usd"] == 217000.0                # 2 x 108.5 x 1000
        assert pos["unrealized_pnl_usd"] == 1000.0            # 2 x 0.5 x 1000
        assert pos["market_value_usd"] == 1000.0              # NAV contribution = variation
        assert pos["collateral_usd"] == 4126.0 and pos["capital_usage_usd"] == 4126.0
        assert pos["exposure_weight"] == 0.217 and pos["valuation_basis"] == "VARIATION_MARGIN_UNREALISED"
        assert pos["collateral_semantics"] == IC.COLLATERAL_SEMANTICS[IC.IT_FUTURE]

    def test_03_non_usd_future_converts_through_fx(self):
        d = IC.describe("&FDAX", asset_class=IC.AC_INTL_EQUITY_INDEX_FUTURES,
                        instrument_type=IC.IT_FUTURE, sleeve_id="s", currency="EUR",
                        multiplier=25.0, initial_margin_per_unit=44602.0)
        pos = IC.value_position(d, quantity=1, mark=26000.0, entry_mark=25900.0, fx_to_usd=1.15)
        assert pos["notional_usd"] == round(26000 * 25 * 1.15, 2)
        assert pos["unrealized_pnl_usd"] == round(100 * 25 * 1.15, 2)
        assert pos["collateral_usd"] == round(44602 * 1.15, 2)
        gap = IC.value_position(d, quantity=1, mark=26000.0, entry_mark=25900.0, fx_to_usd=None)
        assert gap["market_value_usd"] is None and "FX_UNAVAILABLE" in gap["valuation_gaps"]

    def test_04_fill_cash_semantics_by_instrument_type(self):
        eq = IC.equity_descriptor("AAA")
        buy = IC.fill_cash_delta(eq, side_is_buy=True, units=10, price=100.0, cost_rate=0.00125)
        assert buy["net_cash_delta"] == -(1000.0 + 1.25)
        fut = IC.describe("&MES", asset_class=IC.AC_EQUITY_INDEX_FUTURES, instrument_type=IC.IT_FUTURE,
                          sleeve_id="s", multiplier=5.0, initial_margin_per_unit=1606.0)
        o = IC.fill_cash_delta(fut, side_is_buy=True, units=3, price=6400.0)
        assert o["notional_usd"] == 96000.0 and o["net_cash_delta"] == -o["transaction_cost"]
        c = IC.fill_cash_delta(fut, side_is_buy=False, units=3, price=6450.0, entry_reference_price=6400.0)
        assert c["realized_pnl"] == 750.0 and c["net_cash_delta"] == 750.0 - c["transaction_cost"]

    def test_05_units_for_notional_and_entry_replay(self):
        fut = IC.describe("&MES", asset_class=IC.AC_EQUITY_INDEX_FUTURES, instrument_type=IC.IT_FUTURE,
                          sleeve_id="s", multiplier=5.0, initial_margin_per_unit=1606.0)
        assert IC.units_for_notional(fut, 100_000.0, 6400.0) == 3
        blk = IC.instrument_block(fut)
        fills = [{"book_id": "b", "ticker": "&MES", "side": desk.SIDE_BUY, "quantity": 2,
                  "fill_price": 6400.0, "fill_date": "2026-08-25", "instrument": blk},
                 {"book_id": "b", "ticker": "&MES", "side": desk.SIDE_BUY, "quantity": 2,
                  "fill_price": 6500.0, "fill_date": "2026-08-26", "instrument": blk},
                 {"book_id": "b", "ticker": "&MES", "side": desk.SIDE_SELL, "quantity": 1,
                  "fill_price": 6600.0, "fill_date": "2026-08-27", "instrument": blk}]
        e = IC.replay_entry_marks(fills, book_id="b")
        assert e["&MES"]["quantity"] == 3 and e["&MES"]["average_entry_price"] == 6450.0

    def test_06_cost_policy_declared_once_and_equity_rate_is_the_desk_rate(self):
        assert IC.COST_BPS_PER_SIDE_BY_CLASS[IC.AC_US_EQUITY] == desk.COST_BPS_PER_SIDE
        assert IC.COST_POLICY_VERSION == "multi_asset_cost_policy.v1"
        assert IC.COST_BPS_PER_SIDE_BY_CLASS[IC.AC_RATES_FUTURES] < IC.COST_BPS_PER_SIDE_BY_CLASS[IC.AC_US_EQUITY]
        assert IC.HOLDING_COST_BPS_ANNUAL_BY_CLASS[IC.AC_VOLATILITY_FUTURES] > 100

    def test_07_allocation_by_group_never_renders_a_cosmetic_zero_row(self):
        out = IC.allocation_by_group({"AAA": 0.6, "BBB": 0.3}, {"AAA": "US_EQUITY", "BBB": "US_EQUITY"})
        assert out == {"US_EQUITY": 0.9, "CASH": 0.1}
        assert "FX_SPOT" not in out and "RATES_FUTURES" not in out


# =========================================================================== #
# 2. OWNED REFERENCE DATA + MARKS (E, F)
# =========================================================================== #
class TestReferenceData:

    def test_10_classification_by_owned_convention(self, refdata):
        assert mrd.classify_symbol("&ZN")["asset_class"] == IC.AC_RATES_FUTURES
        assert mrd.classify_symbol("EURUSD")["instrument_type"] == IC.IT_FX_SPOT
        assert mrd.classify_symbol("AAPL")["instrument_type"] == IC.IT_CASH_EQUITY
        assert mrd.is_owned_non_equity_symbol("&MES") and not mrd.is_owned_non_equity_symbol("SPY")

    def test_11_descriptor_from_owned_metadata(self, refdata):
        d = mrd.descriptor_for("&FDAX", sleeve_id="s")
        assert d["currency"] == "EUR" and d["multiplier"] == 25.0 and d["initial_margin_per_unit"] == 44602.0
        assert mrd.descriptor_for("AAPL") is None

    def test_12_fx_to_usd_direction_and_gap(self, refdata):
        eur = mrd.fx_to_usd("EUR", as_of="2026-08-26")
        assert eur["state"] == "OK" and eur["direction"] == "MULTIPLY" and abs(eur["fx_to_usd"] - 1.152) < 1e-9
        jpy = mrd.fx_to_usd("JPY", as_of="2026-08-26")
        assert jpy["direction"] == "DIVIDE" and abs(jpy["fx_to_usd"] - 1.0 / 151.0) < 1e-12
        assert mrd.fx_to_usd("USD")["fx_to_usd"] == 1.0
        assert mrd.fx_to_usd("ZAR")["state"] == "PAIR_NOT_OWNED"

    def test_13_mark_downloader_is_the_desk_bar_shape(self, refdata):
        bars = mrd.mark_downloader("&ZN", "2026-08-01")
        assert bars and set(bars[0]) == {"date", "close"}
        assert mrd.average_daily_volume("&ZN", as_of="2026-08-28") == 3_000_000

    def test_14_desk_routes_non_equity_marks_and_fx_pairs(self, stores):
        out = desk.sync_marks(tickers=["AAA", "&FDAX"], start="2026-08-20", downloader=_downloader,
                              today="2026-09-01", completed_through="2026-08-31")
        marks = desk.read_marks()
        assert "&FDAX" in marks["series"] and "EURUSD" in marks["series"] and "AAA" in marks["series"]
        assert marks["sources"]["&FDAX"] == "INJECTED"
        assert out["synced"]


# =========================================================================== #
# 3. THE ONE NAV / CAPITAL POOL (E, F, P)
# =========================================================================== #
class TestNavAndCapitalPool:

    def test_20_equity_only_book_values_exactly_as_before(self, stores):
        sdir = desk._desk_dir(None)
        book = _make_book(sdir, initial_capital=100_000.0,
                          fills=[_equity_fill("AAA", 100, 100.0), _equity_fill("BBB", 200, 50.0)])
        desk.sync_marks(tickers=["AAA", "BBB"], start="2026-08-20", downloader=_downloader,
                        today="2026-09-01", completed_through="2026-08-28")
        nav = desk.book_nav(book, desk._fills(sdir), desk.read_marks())
        cash = 100_000 - (10_000 + 12.5) - (10_000 + 12.5)
        assert nav["cash"] == round(cash, 2)
        assert nav["invested"] == round(100 * 104.0 + 200 * 52.0, 2)
        assert nav["nav"] == round(cash + 100 * 104.0 + 200 * 52.0, 2)
        assert nav["collateral"] == 0.0 and nav["free_cash"] == nav["cash"]
        assert all(p["instrument_type"] == IC.IT_CASH_EQUITY for p in nav["positions"])

    def test_21_futures_position_inside_the_one_nav_replay(self, stores):
        sdir = desk._desk_dir(None)
        fut = mrd.descriptor_for("&MES", sleeve_id="sleeve_equity_index_futures")
        blk = IC.instrument_block(fut)
        econ = IC.fill_cash_delta(fut, side_is_buy=True, units=3, price=6400.0)
        fut_fill = {"fill_id": "fill_mes", "order_id": "ord_mes", "book_id": "alpha_paper_book_1",
                    "ticker": "&MES", "side": desk.SIDE_BUY, "quantity": 3, "fill_date": "2026-08-24",
                    "fill_price": 6400.0, "gross_value": round(econ["gross_value"], 2),
                    "transaction_cost": round(econ["transaction_cost"], 4), "cost_bps_per_side": 2.0,
                    "net_cash_delta": round(econ["net_cash_delta"], 4), "instrument": blk,
                    "execution_model": "NEXT_CLOSE", "immutable": True}
        book = _make_book(sdir, initial_capital=1_000_000.0,
                          fills=[_equity_fill("AAA", 1000, 100.0), fut_fill])
        desk.sync_marks(tickers=["AAA", "&MES"], start="2026-08-20", downloader=_downloader,
                        today="2026-09-01", completed_through="2026-08-28")
        nav = desk.book_nav(book, desk._fills(sdir), desk.read_marks())
        mes = next(p for p in nav["positions"] if p["instrument_id"] == "&MES")
        # mark on 2026-08-28 = 6440 -> unrealised = 3 x 40 x 5
        assert mes["unrealized_pnl_usd"] == 600.0 and mes["market_value_usd"] == 600.0
        assert mes["notional_usd"] == 3 * 6440 * 5 and mes["collateral_usd"] == 3 * 1606.0
        assert nav["collateral"] == 4818.0 and nav["free_cash"] == round(nav["cash"] - 4818.0, 2)
        cash = 1_000_000 - (100_000 + 125.0) - econ["transaction_cost"]
        assert nav["cash"] == round(cash, 2)
        assert nav["nav"] == round(cash + 1000 * 104.0 + 600.0, 2)      # NAV = cash + MV + variation
        assert nav["non_equity_position_count"] == 1

    def test_22_capital_pool_composes_the_one_nav(self):
        eq = IC.value_position(IC.equity_descriptor("AAA"), quantity=100, mark=104.0, nav=200_000)
        fut = IC.value_position(IC.describe("&ZN", asset_class=IC.AC_RATES_FUTURES,
                                            instrument_type=IC.IT_FUTURE, sleeve_id="sleeve_rates_futures",
                                            multiplier=1000.0, initial_margin_per_unit=2063.0),
                                quantity=1, mark=108.5, entry_mark=108.0, nav=200_000)
        pool = cp.build_capital_pool(book_id="b", valuation_date="2026-08-28", nav=200_000.0,
                                     cash=189_100.0, starting_capital=200_000.0,
                                     positions=[eq, fut], valuation_contract={"collateral": 2063.0})
        assert pool["one_capital_pool"] and pool["collateral"] == 2063.0
        assert pool["available_capital"] == 189_100.0 - 2063.0
        assert set(pool["asset_class_exposure"]) == {IC.AC_US_EQUITY, IC.AC_RATES_FUTURES}
        assert pool["currency_exposure"] == {"USD": round(0.052 + 0.5425, 6)}
        assert "CASH" in pool["allocation"] and IC.AC_FX_SPOT not in pool["allocation"]


# =========================================================================== #
# 4. THE REGISTRY (A, D, H) - eligibility is derived, never declared
# =========================================================================== #
class TestRegistry:

    def test_30_production_registry_only_equities_and_cash_are_eligible(self, refdata):
        reg = ir.load_investability_registry(probe=True, as_of="2026-08-28", nav=99_000.0)
        assert reg["capital_eligible_sleeve_ids"] == [IC.DEFAULT_EQUITY_SLEEVE, IC.CASH_SLEEVE]
        assert reg["non_equity_eligible_sleeve_ids"] == []
        for row in reg["capital_ineligible"]:
            assert row["reason"] == ir.R_NO_APPROVED_SIGNAL
            assert "MODEL_APPROVED_FOR_OPERATION" in row["missing_capabilities"]
            # every PLUMBING capability holds; only the signal / approval side is missing
            assert set(row["missing_capabilities"]) <= {"MODEL_APPROVED_FOR_OPERATION", "SIGNAL_AVAILABLE",
                                                        "LIQUIDITY_SUPPORTED"}
        assert reg["promotion_governance"]["automatic_promotion"] is False

    def test_31_every_non_equity_sleeve_has_every_plumbing_capability(self, refdata):
        reg = ir.load_investability_registry(probe=True, as_of="2026-08-28")
        for s in reg["sleeves"]:
            if s["asset_class"] in (IC.AC_US_EQUITY, IC.AC_CASH):
                continue
            caps = s["capabilities"]
            for c in ("USD_VALUATION_SUPPORTED", "RISK_SUPPORTED", "COST_SUPPORTED",
                      "POSITION_ACCOUNTING_SUPPORTED", "PAPER_EXECUTION_SUPPORTED",
                      "RECONCILIATION_SUPPORTED", "CAPACITY_SUPPORTED"):
                assert caps[c], (s["sleeve_id"], c)
            assert s["r50_activation_attempt"]["implementation_attempted"] if "implementation_attempted" in s["r50_activation_attempt"] else s["r50_activation_attempt"]["implemented_in_r50"]

    def test_32_research_only_crypto_gets_zero_operational_capital(self, refdata):
        reg = ir.load_investability_registry(probe=True, as_of="2026-08-28")
        crypto = ir.sleeve_map(reg)["sleeve_crypto_futures"]
        assert crypto["capital_eligible"] is False
        assert crypto["model_approval_state"] == ir.RESEARCH_ONLY
        assert "BELOW_CASH" in crypto["approval_evidence"]["verdict"]
        assert ir.eligible_non_equity_instruments(reg, nav=1e7) == []

    def test_33_injected_approval_makes_a_sleeve_eligible_only_in_a_hermetic_process(self, refdata):
        appr = {"sleeve_rates_futures": {"model_approval_state": ir.APPROVED,
                                         "approval_evidence": {"state": "INJECTED"},
                                         "signal_scores": {"&ZN": 0.9}}}
        reg = ir.load_investability_registry(approvals=appr, probe=True, as_of="2026-08-28")
        assert "sleeve_rates_futures" in reg["non_equity_eligible_sleeve_ids"]
        assert reg["approvals_injected"] == ["sleeve_rates_futures"]
        inst = ir.eligible_non_equity_instruments(reg, nav=5_000_000.0, as_of="2026-08-28")
        zn = next(i for i in inst if i["instrument_id"] == "&ZN")
        assert zn["executable_at_nav"] and zn["opportunity_score"] == 0.9
        small = ir.eligible_non_equity_instruments(reg, nav=99_000.0, as_of="2026-08-28")
        assert next(i for i in small if i["instrument_id"] == "&ZN")["executability_reason"] == \
            "UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV"

    def test_34_a_sleeve_whose_data_is_unreadable_stays_ineligible(self, tmp_path, monkeypatch):
        p = tmp_path / "empty.json"
        p.write_text(json.dumps({"metadata": {}, "closes": {}, "fx_symbols": [], "futures_symbols": []}))
        monkeypatch.setenv(mrd.FIXTURE_ENV, str(p))
        mrd.reset_cache()
        appr = {"sleeve_rates_futures": {"model_approval_state": ir.APPROVED, "signal_scores": {"&ZN": 0.9}}}
        reg = ir.load_investability_registry(approvals=appr, probe=True)
        rates = ir.sleeve_map(reg)["sleeve_rates_futures"]
        assert rates["capital_eligible"] is False
        assert rates["capital_ineligible_reason"] in (ir.R_MARK_UNAVAILABLE, ir.R_DATA_UNAVAILABLE)
        mrd.reset_cache()

    def test_35_registry_has_no_promotion_path_and_no_research_import(self):
        src = (REPO / "api" / "investability_registry.py").read_text(encoding="utf-8")
        assert "alpha_agent" not in src and "def promote" not in src
        assert '"this_module_can_promote": False' in src
        assert ir.activation_attempts()[2]["remaining_blocker"] == ir.R_NO_APPROVED_SIGNAL


# =========================================================================== #
# 5. RISK + FRONTIER (B, C, J)
# =========================================================================== #
def _returns(n=60, seed=1):
    import random
    rnd = random.Random(seed)
    dates = ["2026-%02d-%02d" % (1 + i // 28, 1 + i % 28) for i in range(n)]
    a = [rnd.gauss(0.0004, 0.01) for _ in range(n)]
    b = [rnd.gauss(0.0002, 0.006) for _ in range(n)]
    c = [-0.3 * x + rnd.gauss(0.0, 0.004) for x in a]     # negatively related
    return {"dates": dates, "series": {"AAA": a, "BBB": b, "&ZN": c}}


class TestRiskAndFrontier:

    def test_40_risk_state_on_exposure_weights_with_one_covariance(self):
        pos = [{"instrument_id": "AAA", "exposure_weight": 0.5, "asset_class": "US_EQUITY",
                "sleeve_id": "eq", "currency": "USD", "notional_usd": 50.0, "collateral_usd": 0.0,
                "capital_usage_usd": 50.0, "instrument_type": "CASH_EQUITY"},
               {"instrument_id": "&ZN", "exposure_weight": 0.3, "asset_class": "RATES_FUTURES",
                "sleeve_id": "rates", "currency": "USD", "notional_usd": 30.0, "collateral_usd": 0.6,
                "capital_usage_usd": 0.6, "instrument_type": "FUTURE"}]
        rs = XR.build_risk_state(positions=pos, aligned_returns=_returns(), nav=100.0, cash=49.4,
                                 drawdown={"owner": "api.paper_trading_desk.current_drawdown",
                                           "current_drawdown_pct": -1.0})
        assert rs["state"] == XR.STATE_AVAILABLE
        assert abs(sum(rs["risk_contribution"].values()) - 1.0) < 1e-6
        assert rs["asset_class_exposure"] == {"RATES_FUTURES": 0.3, "US_EQUITY": 0.5}
        assert rs["gross_exposure"] == 0.8 and rs["net_exposure"] == 0.8
        assert rs["drawdown"]["current_drawdown_pct"] == -1.0 and rs["drawdown_owner"].endswith("current_drawdown")
        assert rs["approximations"]["cash"].startswith("RISKLESS")

    def test_41_diversification_effect_is_advisory(self):
        w = {"AAA": 0.6}
        risk = XR.portfolio_risk(weights=w, aligned_returns=_returns(), policy=XR.default_policy())
        eff = XR.diversification_effect(risk=risk, candidate="&ZN", aligned_returns=_returns(),
                                        weights=w, delta_weight=0.2, policy=XR.default_policy())
        assert eff["advisory_only"] is True and eff["diversifies"] is True

    def test_42_frontier_lists_every_opportunity_with_a_score_basis(self):
        inst = [{"instrument_id": "&ZN", "sleeve_id": "sleeve_rates_futures", "asset_class": "RATES_FUTURES",
                 "asset_class_label": "Rates Futures", "instrument_type": "FUTURE", "currency": "USD",
                 "opportunity_score": 0.9, "unit_notional_usd": 108000.0, "executable_at_nav": True,
                 "capital_usage_ratio": 0.019, "cost_bps_per_side": 1.75, "holding_cost_bps_annual": 6.0,
                 "average_daily_volume_units": 3e6, "multiplier": 1000.0, "initial_margin_per_unit": 2063.0}]
        fr = OF.build_frontier(eligible_market_date="2026-08-28", nav=5e6,
                               equity_rankings=[{"ticker": "AAA", "percentile": 0.8, "adv_dollar": 1e9,
                                                 "rank": 1, "sector": "Tech", "eligible": True}],
                               equity_sleeve_eligible=True, non_equity_instruments=inst, positions=[])
        rows = {r["instrument_id"]: r for r in fr["rows"]}
        assert rows["&ZN"]["score_basis"] == OF.SB_SLEEVE_RANK and rows["AAA"]["score_basis"] == OF.SB_EQUITY_PERCENTILE
        assert rows["USD_CASH"]["score_basis"] == OF.SB_CASH
        assert rows["&ZN"]["expected_return_state"] == OF.ER_NOT_CALIBRATED
        assert fr["eligible_non_equity_count"] == 1 and fr["forced_diversification"] is False
        cand = OF.candidate_rows_for_proposal(fr)
        assert cand[0]["ticker"] == "&ZN" and cand[0]["percentile"] == 0.9 and cand[0]["frontier_row"]

    def test_43_zero_signal_instrument_is_never_eligible(self):
        inst = [{"instrument_id": "&ZN", "sleeve_id": "s", "asset_class": "RATES_FUTURES",
                 "instrument_type": "FUTURE", "opportunity_score": None, "unit_notional_usd": 108000.0,
                 "executable_at_nav": True}]
        fr = OF.build_frontier(eligible_market_date="d", nav=5e6, equity_rankings=[],
                               equity_sleeve_eligible=True, non_equity_instruments=inst, positions=[])
        zn = next(r for r in fr["rows"] if r["instrument_id"] == "&ZN")
        assert zn["eligible"] is False and zn["eligibility_reason"] == OF.E_NO_SCORE
        assert OF.candidate_rows_for_proposal(fr) == []

    def test_44_rank_normalise_within_sleeve(self):
        assert OF.rank_normalise({"a": 3, "b": 1, "c": 2}) == {"b": 1 / 3, "c": 2 / 3, "a": 1.0}


# =========================================================================== #
# 6. THE CONSTRAINT OWNER + ZERO-BASE (A, I, J)
# =========================================================================== #
def _cand(tk, *, sector="Tech", score=0.5, rank=1, adv=5e8, **inst):
    return dict({"ticker": tk, "sector": sector, "score": score, "rank": rank, "adv_dollar": adv}, **inst)


_ZN = dict(asset_class="RATES_FUTURES", sleeve_id="sleeve_rates_futures", instrument_type="FUTURE",
           currency="USD", capital_usage_ratio=0.02, cost_bps_per_side=1.75)
_FDAX = dict(asset_class="INTERNATIONAL_EQUITY_INDEX_FUTURES", sleeve_id="sleeve_intl", instrument_type="FUTURE",
             currency="EUR", capital_usage_ratio=0.07, cost_bps_per_side=2.5)


class TestConstraintOwner:

    def test_50_equity_only_inputs_solve_exactly_as_before(self):
        cands = [_cand("A%d" % i, score=0.9 - i * 0.05, rank=i + 1, sector="S%d" % i) for i in range(8)]
        ideal = {c["ticker"]: 0.12 for c in cands}
        pol = dict(CR.default_policy(), target_position_count=12, min_position_weight=0.01,
                   max_one_way_turnover=1.0)
        a = CR.solve_feasible_target(current_weight={}, ideal_weight=ideal, candidates=cands, nav=1e6, policy=pol)
        cands2 = [dict(c, asset_class="US_EQUITY", sleeve_id=IC.DEFAULT_EQUITY_SLEEVE, currency="USD",
                       instrument_type="CASH_EQUITY") for c in cands]
        b = CR.solve_feasible_target(current_weight={}, ideal_weight=ideal, candidates=cands2, nav=1e6, policy=pol)
        assert a["best_feasible_target"] == b["best_feasible_target"]
        assert a["best_feasible_allocation_by_asset_class"] == {"US_EQUITY": 0.8, "CASH": 0.2}

    def test_51_asset_class_cap_reshapes_never_freezes(self):
        cands = [_cand("A1", score=0.9), _cand("A2", score=0.8, sector="Health"),
                 _cand("&ZN", score=0.95, sector="Rates Futures", unit_notional_usd=108000.0, **_ZN),
                 _cand("&ZF", score=0.94, sector="Rates Futures", unit_notional_usd=106000.0, **_ZN),
                 _cand("&ZB", score=0.93, sector="Rates Futures", unit_notional_usd=110000.0, **_ZN),
                 _cand("&ZT", score=0.92, sector="Rates Futures", unit_notional_usd=205000.0, **_ZN)]
        ideal = {"&ZN": 0.10, "&ZF": 0.10, "&ZB": 0.10, "&ZT": 0.10, "A1": 0.10, "A2": 0.10}
        # The asset-class cap is set BELOW the sector cap (the class label doubles as
        # the sector for non-equity rows), so the class limit is the binding one.
        pol = dict(CR.default_policy(), target_position_count=12, min_position_weight=0.01,
                   asset_class_weight_caps={"US_EQUITY": 1.0, "CASH": 1.0, "DEFAULT_NON_EQUITY": 0.15})
        sol = CR.solve_feasible_target(current_weight={}, ideal_weight=ideal, candidates=cands,
                                       nav=5e6, policy=pol)
        assert sol["feasible"]
        assert sol["best_feasible_allocation_by_asset_class"]["RATES_FUTURES"] <= 0.15 + 1e-9
        assert CR.C_ASSET_CLASS_CAP in sol["constraints_that_reshaped"]
        assert sol["verification"]["valid"] and not sol["blockers"]
        assert sol["cross_asset"]["forced_diversification"] is False

    def test_52_currency_and_collateral_caps_reshape(self):
        cands = [_cand("A1", score=0.9), _cand("&FDAX", score=0.99, sector="Intl", unit_notional_usd=750000.0, **_FDAX),
                 _cand("&FESX", score=0.98, sector="Intl", unit_notional_usd=60000.0, **_FDAX)]
        ideal = {"&FDAX": 0.10, "&FESX": 0.10, "A1": 0.10}
        pol = dict(CR.default_policy(), target_position_count=12, min_position_weight=0.01,
                   non_usd_currency_cap=0.12, collateral_cap_fraction=0.01, sleeve_weight_caps={"DEFAULT_NON_EQUITY": 1.0},
                   asset_class_weight_caps={"US_EQUITY": 1.0, "DEFAULT_NON_EQUITY": 1.0})
        sol = CR.solve_feasible_target(current_weight={}, ideal_weight=ideal, candidates=cands, nav=1e7, policy=pol)
        g = sol["cross_asset"]["group_weights"]
        assert g["non_usd"] <= 0.12 + 1e-9 and g["collateral"] <= 0.01 + 1e-9
        assert {CR.C_CURRENCY_CAP, CR.C_COLLATERAL_CAP} & set(sol["constraints_that_reshaped"])

    def test_53_unit_granularity_skips_a_contract_too_big_for_the_book(self):
        cands = [_cand("A1", score=0.5), _cand("&ZN", score=0.99, sector="Rates Futures",
                                                unit_notional_usd=108000.0, **_ZN)]
        caps, binding = CR.name_caps(candidates=cands, nav=99_000.0, policy=CR.default_policy())
        assert caps["&ZN"] == 0.0 and binding["&ZN"] == CR.C_UNIT_GRANULARITY
        caps5, _ = CR.name_caps(candidates=cands, nav=5e6, policy=CR.default_policy())
        assert caps5["&ZN"] == 0.10

    def test_54_switching_economics_prices_each_instrument_at_its_own_rate(self):
        cands = [_cand("A1", score=0.5), _cand("&ZN", score=0.9, sector="Rates Futures", **_ZN)]
        sw = CR.switching_economics(current_weight={"A1": 0.5}, target_weight={"A1": 0.4, "&ZN": 0.1},
                                    candidates=cands, nav=1e6)
        assert sw["per_instrument_cost_rates_applied"] is True
        assert sw["estimated_transaction_cost"] == round((0.1 * 0.00125 + 0.1 * 0.000175) * 1e6, 2)
        assert sw["allocation_after_by_asset_class"]["RATES_FUTURES"] == 0.1
        sw0 = CR.switching_economics(current_weight={"A1": 0.5}, target_weight={"A1": 0.4},
                                     candidates=[_cand("A1", score=0.5)], nav=1e6)
        assert sw0["per_instrument_cost_rates_applied"] is False

    def test_55_zero_base_allocator_respects_cross_asset_caps_and_is_unchanged_for_equities(self):
        cands = [_cand("A1", score=0.5), _cand("A2", score=0.5, sector="Health")]
        ic_base = {"eligible_market_date": "2026-08-28", "active_book_id": "b", "nav": 1e6,
                   "candidates": cands, "mu": {"A1": 0.02, "A2": 0.015}, "sigma_forecast": {"A1": 0.01, "A2": 0.01},
                   "downside": {}, "aligned_returns": {"dates": [], "series": {}},
                   "forecast_model_spec_hash": "h", "current_weights": {}}
        base = ZB.build_allocation(input_contract=ic_base)
        cands_fut = cands + [dict(_cand("&ZN", score=0.9, sector="Rates Futures", **_ZN), unit_notional_usd=108000.0)]
        ic_fut = dict(ic_base, candidates=cands_fut, mu={"A1": 0.02, "A2": 0.015, "&ZN": 0.05},
                      sigma_forecast={"A1": 0.01, "A2": 0.01, "&ZN": 0.01})
        multi = ZB.build_allocation(input_contract=ic_fut, policy={"asset_class_weight_caps": {
            "US_EQUITY": 1.0, "CASH": 1.0, "DEFAULT_NON_EQUITY": 0.05}})
        zb = multi["zero_base_target"]
        assert zb["allocation_by_asset_class"].get("RATES_FUTURES", 0.0) <= 0.05 + 1e-9
        assert zb["constraints"]["valid"]
        assert base["zero_base_target"]["allocation_by_asset_class"] == {"US_EQUITY": 0.2, "CASH": 0.8}


# =========================================================================== #
# 7. THE PROPOSAL (A, B, C, J, K, L, M)
# =========================================================================== #
def _hoc_review(tk, rec="HOLD", score=0.5):
    return {"ticker": tk, "recommendation": rec, "current_rank": 1, "current_score": score,
            "signal_strength": score, "strongest_replacement_ticker": None, "replacement_rank": None,
            "replacement_score": None, "gross_score_improvement": None, "net_improvement": None,
            "switching_cost_usd": None, "deterioration_state": "STABLE", "drawdown_60d": -0.05,
            "volatility_60d": 0.2, "liquidity_state": "LIQUID", "risk_contribution_pct": None}


def _proposal_contract(*, nav=1e6, positions, universe_rows, hoc_reviews):
    return {"schema_version": RP.INPUT_SCHEMA_VERSION, "eligible_market_date": "2026-08-28",
            "active_book_id": "b", "nav": nav, "cash": nav * (1 - sum(p["current_weight"] for p in positions)),
            "portfolio_state_hash": "ps", "universe_scoring_hash": "us", "hoc_assessment_hash": "hoc",
            "hoc_assessment_state": "READY", "hoc_available": True, "hoc_data_gaps": [],
            "positions": positions, "hoc_reviews": hoc_reviews, "universe_rows": universe_rows,
            "aligned_returns": {"dates": [], "series": {}}}


def _urow(tk, rank, pct, sector="Tech", **inst):
    return dict({"ticker": tk, "rank": rank, "percentile": pct, "sector": sector, "adv_dollar": 5e8,
                 "eligible": True}, **inst)


class TestProposal:

    def _equity_world(self):
        positions = [{"ticker": "A%d" % i, "sector": "S%d" % (i % 5), "quantity": 100,
                      "current_weight": 0.04, "market_value": 40_000.0, "price": 400.0} for i in range(25)]
        rows = [_urow("A%d" % i, i + 1, 1 - i / 60.0, sector="S%d" % (i % 5)) for i in range(25)]
        rows += [_urow("N%d" % i, 26 + i, 1 - (25 + i) / 60.0, sector="S%d" % (i % 5)) for i in range(25)]
        return positions, rows

    def test_60_scenario_A_only_equities_qualify_no_fake_non_equity_allocation(self):
        positions, rows = self._equity_world()
        ic = _proposal_contract(positions=positions, universe_rows=rows,
                                hoc_reviews=[_hoc_review(p["ticker"]) for p in positions])
        out = RP.build_proposal(input_contract=ic, policy=arp.resolve_policy())
        assert out["proposal_state"] in (RP.STATE_READY, RP.STATE_DEGRADED)
        assert out["portfolio"]["proposed_allocation_by_asset_class"] == {"US_EQUITY": 1.0}
        assert out["portfolio"]["non_equity_position_count_in_target"] == 0
        assert out["portfolio"]["forced_diversification"] is False
        assert out["outcome"] == CR.OUTCOME_HOLD_CURRENT_BOOK       # K: the equity book is best
        assert all(a["instrument_type"] == "CASH_EQUITY" for a in out["allocations"])

    def test_61_scenario_B_C_L_eligible_futures_enter_the_frontier_and_receive_capital(self):
        positions, rows = self._equity_world()
        # five HOC EXITs free five slots; five frontier rows outrank every equity
        # candidate by normalised score, and the switch clears the frozen hurdle.
        exits = {"A20", "A21", "A22", "A23", "A24"}
        reviews = [_hoc_review(p["ticker"], rec=("EXIT" if p["ticker"] in exits else "HOLD")) for p in positions]
        fut = [_urow("&Z%d" % i, 1 + i, 0.99 - i * 0.001, sector="Rates Futures", frontier_row=True,
                     unit_notional_usd=108000.0, multiplier=1000.0, initial_margin_per_unit=2063.0,
                     score_basis=OF.SB_SLEEVE_RANK, **_ZN) for i in range(5)]
        ic = _proposal_contract(positions=positions, universe_rows=rows + fut, hoc_reviews=reviews)
        out = RP.build_proposal(input_contract=ic, policy=arp.resolve_policy())
        zn_rows = [a for a in out["allocations"] if a["ticker"].startswith("&Z")]
        assert zn_rows and all(a["action"] == RP.ACT_ADD and a["proposed_weight"] > 0 for a in zn_rows)
        assert all(a["instrument_type"] == "FUTURE" and a["asset_class"] == "RATES_FUTURES" for a in zn_rows)
        assert out["portfolio"]["proposed_allocation_by_asset_class"]["RATES_FUTURES"] == \
            round(sum(a["proposed_weight"] for a in zn_rows), 6)
        assert "RATES_FUTURES" in out["portfolio"]["asset_classes_in_target"]
        assert out["turnover"]["per_instrument_cost_rates_applied"] is True
        assert out["switching_economics"]["score_improvement_net_of_cost"] >= 0.05
        assert out["outcome"] == CR.OUTCOME_PROPOSAL_READY                     # L
        assert out["approvable"]

    def test_62_scenario_J_weak_non_equity_opportunity_receives_zero(self):
        positions, rows = self._equity_world()
        reviews = [_hoc_review(p["ticker"], rec=("EXIT" if p["ticker"] == "A24" else "HOLD")) for p in positions]
        weak = _urow("&ZN", 1, 0.05, sector="Rates Futures", frontier_row=True, unit_notional_usd=108000.0, **_ZN)
        ic = _proposal_contract(positions=positions, universe_rows=rows + [weak], hoc_reviews=reviews)
        out = RP.build_proposal(input_contract=ic, policy=arp.resolve_policy())
        assert all(a["ticker"] != "&ZN" or a["proposed_weight"] == 0 for a in out["allocations"])
        assert "RATES_FUTURES" not in out["portfolio"]["proposed_allocation_by_asset_class"]

    def test_63_scenario_I_cross_asset_cap_breach_routes_to_the_repair_kernel(self):
        positions, rows = self._equity_world()
        reviews = [_hoc_review(p["ticker"], rec=("EXIT" if p["ticker"] in ("A20", "A21", "A22", "A23", "A24") else "HOLD"))
                   for p in positions]
        fut = [_urow("&Z%d" % i, 1 + i, 0.99 - i * 0.001, sector="Rates Futures", frontier_row=True,
                     unit_notional_usd=100000.0, **_ZN) for i in range(5)]
        pol = dict(arp.resolve_policy(), asset_class_weight_caps={"US_EQUITY": 1.0, "CASH": 1.0,
                                                                   "DEFAULT_NON_EQUITY": 0.10})
        ic = _proposal_contract(positions=positions, universe_rows=rows + fut, hoc_reviews=reviews)
        out = RP.build_proposal(input_contract=ic, policy=pol)
        assert out["proposal_state"] != RP.STATE_WITHHELD
        assert out["portfolio"]["proposed_allocation_by_asset_class"].get("RATES_FUTURES", 0.0) <= 0.10 + 1e-9
        assert out["constraint_reoptimization"]["applied"] is True
        assert CR.C_ASSET_CLASS_CAP in out["constraint_reoptimization"]["constraints_that_reshaped"]
        assert out["outcome"] in (CR.OUTCOME_PROPOSAL_READY, CR.OUTCOME_HOLD_CURRENT_BOOK)

    def test_64_scenario_M_no_trustworthy_target_is_a_true_blocker(self):
        v = CR.decide_outcome(solution={"feasible": True, "best_feasible_target": {"A": 0.1}},
                              economics={"clears_switching_hurdle": True},
                              true_blockers=[{"code": CR.B_STALE_MARKET_DATA}])
        assert v["outcome"] == CR.OUTCOME_TRUE_BLOCKER
        v2 = CR.decide_outcome(solution={"feasible": True, "best_feasible_target": {"A": 0.1}},
                               economics={"clears_switching_hurdle": True},
                               true_blockers=[{"code": CR.C_ASSET_CLASS_CAP}])
        assert v2["outcome"] == CR.OUTCOME_PROPOSAL_READY and v2["misclassified_blockers"] == [CR.C_ASSET_CLASS_CAP]

    def test_65_hoc_owner_is_scoped_to_the_equity_sleeve(self):
        from paper_trader.api import holding_opportunity_cost as hoc_api
        ps = {"positions": [{"ticker": "AAA", "portfolio_weight": 0.5, "quantity": 1},
                            {"ticker": "&ZN", "portfolio_weight": 0.1, "quantity": 1, "instrument_type": "FUTURE",
                             "sleeve_id": "sleeve_rates_futures", "asset_class": "RATES_FUTURES"}]}
        assert [p["ticker"] for p in hoc_api._positions_from_state(ps)] == ["AAA"]
        ex = hoc_api.excluded_non_equity_positions(ps)
        assert ex[0]["ticker"] == "&ZN" and ex[0]["reviewed_by"] == "engine.opportunity_frontier"

    def test_66_frontier_reviews_exit_an_ineligible_sleeves_position(self):
        fr = {"rows": [{"instrument_id": "&ZN", "eligible": False, "eligibility_reason": OF.E_NOT_ELIGIBLE_SLEEVE}]}
        pos = [{"instrument_id": "&ZN", "instrument_type": "FUTURE"}, {"instrument_id": "AAA", "instrument_type": "CASH_EQUITY"}]
        rv = of_api.frontier_reviews(fr, pos)
        assert len(rv) == 1 and rv[0]["recommendation"] == "EXIT" and rv[0]["ticker"] == "&ZN"

    def test_67_input_contract_admits_frontier_rows_and_binds_the_frontier_identity(self):
        ps = {"dates": {"eligible_market_date": "2026-08-28"}, "active_book": {"book_id": "b"},
              "capital": {"nav": 1e6, "cash": 5e5}, "positions": [], "state_hash": "h"}
        fr = {"frontier_hash": "fh", "eligible_non_equity_count": 1,
              "candidate_rows_for_proposal": [_urow("&ZN", 1, 0.9, frontier_row=True, **_ZN)],
              "non_equity_reviews": [], "registry_identity": {"capital_eligible_sleeve_ids": ["x"]}}
        ic = arp.build_input_contract(portfolio_state=ps, scoring={"rankings": [_urow("AAA", 1, 0.8)]},
                                      hoc_assessment={}, price_panel=None, frontier=fr)
        assert [r["ticker"] for r in ic["universe_rows"]] == ["AAA", "&ZN"]
        assert ic["frontier_hash"] == "fh" and ic["frontier_rows_admitted"] == ["&ZN"]
        ident = arp.proposal_identity(input_contract=ic, result={"proposal_hash": "p"})
        assert ident["frontier_hash"] == "fh"


# =========================================================================== #
# 8. GOVERNED PAPER EXECUTION (F, N, O, P, Q)
# =========================================================================== #
def _artifact_with_future(nav_weight=0.10):
    allocs = [{"ticker": "AAA", "sector": "Tech", "current_weight": 0.10, "proposed_weight": 0.10,
               "delta_weight": 0.0, "action": "RETAIN", "instrument_type": "CASH_EQUITY"},
              {"ticker": "&MES", "sector": "Equity Index Futures", "current_weight": 0.0,
               "proposed_weight": nav_weight, "delta_weight": nav_weight, "action": "ADD",
               "asset_class": "EQUITY_INDEX_FUTURES", "sleeve_id": "sleeve_equity_index_futures",
               "instrument_type": "FUTURE", "currency": "USD", "multiplier": 5.0,
               "initial_margin_per_unit": 1606.0, "cost_bps_per_side": 2.0,
               "execution_convention": "NEXT_SESSION_SETTLEMENT"}]
    proposal = {"proposal_state": "READY", "outcome": CR.OUTCOME_PROPOSAL_READY, "approvable": True,
                "allocations": allocs, "turnover": {"one_way_turnover": nav_weight / 2.0 + 0.0},
                "action_counts": {"RETAIN": 1, "ADD": 1, "EXIT": 0, "REDUCE": 0, "INCREASE": 0,
                                  "REPLACE_IN": 0, "REPLACE_OUT": 0},
                "reallocation_outcome": {"outcome": CR.OUTCOME_PROPOSAL_READY, "feasible_target_exists": True,
                                         "headline": "REALLOCATION PROPOSAL READY FOR REVIEW", "reason_codes": []},
                "switching_economics": {"switching_hurdle": 0.05, "clears_switching_hurdle": True},
                "constraint_reoptimization": {"applied": False, "constraints_that_reshaped": []},
                "portfolio": {"proposed_holding_count": 2}, "signal": {}, "risk": {}, "constraints": {},
                "complete_target_limits": {}, "policy": {}, "policy_version": RP.ALLOCATION_POLICY_VERSION,
                "schema_version": RP.SCHEMA_VERSION, "data_gaps": []}
    proposal["proposal_hash"] = RP.stable_hash(proposal)
    return {"proposal_id": "reap_2026-08-28_alpha_paper_book_1_%s" % proposal["proposal_hash"][:12],
            "identity": {"proposal_hash": proposal["proposal_hash"], "eligible_market_date": "2026-08-28",
                         "active_book_id": "alpha_paper_book_1", "portfolio_state_hash": "ps",
                         "corporate_actions_hash": None, "hoc_assessment_hash": "h",
                         "universe_scoring_hash": "u", "allocation_policy_version": RP.ALLOCATION_POLICY_VERSION},
            "input_contract": {}, "proposal": proposal}


@pytest.fixture()
def executed_world(stores):
    sdir = desk._desk_dir(None)
    book = _make_book(sdir, initial_capital=1_000_000.0, fills=[_equity_fill("AAA", 1000, 100.0)])
    desk.sync_marks(tickers=["AAA", "&MES"], start="2026-08-20", downloader=_downloader,
                    today="2026-08-29", completed_through="2026-08-28")
    art = _artifact_with_future()
    ps = {"active_book": {"book_id": "alpha_paper_book_1"}, "dates": {"eligible_market_date": "2026-08-28"},
          "capital": {"nav": 1_000_000.0}, "positions": []}
    return {"sdir": sdir, "book": book, "artifact": art, "ps": ps}


class TestGovernedExecution:

    def test_70_scenario_N_no_approval_means_zero_orders(self, executed_world):
        w = executed_world
        st = rb.load_rebalance_state(active_book_id="alpha_paper_book_1", eligible_market_date="2026-08-28",
                                     artifact=w["artifact"], decision_record=None, portfolio_state=w["ps"])
        assert st["rebalance_state"] == rb.RB_PROPOSAL_REVIEW_REQUIRED and st["order_plan"] is None
        res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, active_book_id="alpha_paper_book_1",
                                              eligible_market_date="2026-08-28", artifact=w["artifact"],
                                              decision_record=None, portfolio_state=w["ps"])
        assert res["status"] == rb.C_NOT_APPROVED and res["created_orders"] is False
        assert desk.load_orders()["n_orders"] == 0

    def _approve(self, w):
        summ = arp.load_proposal_summary(active_book_id="alpha_paper_book_1", eligible_market_date="2026-08-28",
                                         artifact=w["artifact"])
        rec = pdec.record_decision(decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
                                   artifact=w["artifact"], proposal_summary=summ)
        assert rec["recorded"], rec
        return rec["record"]

    def test_71_scenario_F_P_O_Q_futures_plan_execution_and_replay(self, executed_world):
        w = executed_world
        record = self._approve(w)
        st = rb.load_rebalance_state(active_book_id="alpha_paper_book_1", eligible_market_date="2026-08-28",
                                     artifact=w["artifact"], decision_record=record, portfolio_state=w["ps"])
        assert st["rebalance_state"] == rb.RB_PLAN_REVIEW_REQUIRED, st.get("message")
        plan = st["order_plan"]
        mes = next(o for o in plan["orders"] if o["ticker"] == "&MES")
        nav = plan["sizing_nav_basis"]
        unit = 6440.0 * 5.0                                     # mark on 2026-08-28 x point value
        assert mes["quantity"] == int((0.10 * nav) // unit) and mes["unit_type"] == "CONTRACTS"
        assert mes["gross_notional"] == round(mes["quantity"] * unit, 2)
        assert mes["collateral_change_usd"] == round(mes["quantity"] * 1606.0, 2)
        assert mes["cash_impact"] == round(-mes["estimated_transaction_cost"], 4)     # only the cost is cash
        assert mes["instrument"]["instrument_type"] == "FUTURE" and mes["execution_convention"] == "NEXT_SESSION_SETTLEMENT"
        assert plan["collateral_after"] == mes["collateral_change_usd"]
        assert "WHOLE_CONTRACTS" in plan["supported_execution_mechanics"]
        # O. no second confirmation -> nothing filled, nothing written
        assert desk.load_orders()["n_orders"] == 0 and desk.load_fills()["n_fills"] == 1
        # second confirmation -> orders SUBMITTED, no same-close fill (no hindsight)
        res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, active_book_id="alpha_paper_book_1",
                                              eligible_market_date="2026-08-28", artifact=w["artifact"],
                                              decision_record=record, portfolio_state=w["ps"], today="2026-08-28")
        n_plan = len(plan["orders"])          # the futures ADD, plus the Stage-19 name-cap trim of AAA
        assert res["status"] == rb.C_CREATED and res["n_orders_created"] == n_plan
        assert res["settlement"]["n_filled"] == 0
        assert res["decision_evidence"]["frozen"] and res["decision_evidence"]["record"]["instrument_meta"]["&MES"]["multiplier"] == 5.0
        # Q. replay -> zero duplicate orders, evidence reused
        again = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, active_book_id="alpha_paper_book_1",
                                                eligible_market_date="2026-08-28", artifact=w["artifact"],
                                                decision_record=record, portfolio_state=w["ps"], today="2026-08-28")
        assert again["status"] == rb.C_REUSED and desk.load_orders()["n_orders"] == n_plan
        # P. the next session settles at the instrument's own settlement, under futures semantics
        desk.sync_marks(tickers=["AAA", "&MES"], start="2026-08-20", downloader=_downloader,
                        today="2026-09-01", completed_through="2026-08-31")
        settle = desk.settle_due_orders(today="2026-09-01")
        assert settle["n_filled"] == n_plan
        fill = next(f for f in desk.load_fills()["fills"] if f["ticker"] == "&MES")
        assert fill["fill_date"] == "2026-08-31" and fill["fill_price"] == 6450.0
        assert fill["price_source"] == "OWNED_NORGATE_SETTLEMENT_AS_RECORDED"
        assert fill["net_cash_delta"] == round(-fill["transaction_cost"], 4)      # only the cost is cash
        assert fill["instrument"]["instrument_type"] == "FUTURE"
        nav_blk = desk.book_nav(w["book"], desk._fills(w["sdir"]), desk.read_marks())
        pos = next(p for p in nav_blk["positions"] if p["instrument_id"] == "&MES")
        assert pos["quantity"] == mes["quantity"] and pos["entry_mark"] == 6450.0
        assert pos["unrealized_pnl_usd"] == 0.0 and pos["collateral_usd"] == round(mes["quantity"] * 1606.0, 2)
        assert nav_blk["collateral"] == pos["collateral_usd"]
        assert nav_blk["free_cash"] == round(nav_blk["cash"] - nav_blk["collateral"], 2)
        # NAV = cash + sum(market value) where the future contributes its variation (0 at entry)
        assert abs(nav_blk["nav"] - (nav_blk["cash"] + sum(p["market_value_usd"] for p in nav_blk["positions"]))) < 0.01
        assert nav_blk["futures_notional"] == round(mes["quantity"] * 6450.0 * 5.0, 2)
        # replaying the settlement creates nothing
        assert desk.settle_due_orders(today="2026-09-01")["n_filled"] == 0
        assert desk.load_fills()["n_fills"] == 1 + n_plan

    def test_72_decision_evidence_measures_both_paths_on_usd_marks(self):
        rec = PDOK.freeze_decision_record(
            decision_id="d", frozen_at="t", eligible_market_date="2026-08-28", active_book_id="b",
            previous_portfolio={"AAA": 0.5}, proposed_target={"AAA": 0.4, "&FDAX": 0.1},
            executed_target={"AAA": 0.4, "&FDAX": 0.1},
            reference_prices={"AAA": 100.0, "&FDAX": 26400.0 * 1.154}, nav_at_decision=1e6,
            transaction_cost=100.0,
            instrument_meta={"&FDAX": {"instrument_type": "FUTURE", "currency": "EUR", "multiplier": 25.0,
                                       "fx_series_id": "EURUSD", "fx_direction": "MULTIPLY"}})
        assert rec["instrument_meta"]["&FDAX"]["fx_series_id"] == "EURUSD"
        marks = {"series": {"AAA": [["2026-08-28", 100.0], ["2026-08-31", 101.0]],
                            "&FDAX": [["2026-08-28", 26400.0], ["2026-08-31", 26500.0]],
                            "EURUSD": [["2026-08-28", 1.154], ["2026-08-31", 1.155]]}}
        hist = pdo._price_history(marks, {"AAA", "&FDAX"}, rec["instrument_meta"])
        assert hist["&FDAX"]["2026-08-31"] == 26500.0 * 1.155
        m = PDOK.measure_paths(record=rec, price_history=hist)
        assert m["state"] == PDOK.M_MEASURED and m["observation_count"] == 1
        # hold path: 0.5 x 1% ; executed: 0.4 x 1% + 0.1 x ((26500 x 1.155)/(26400 x 1.154) - 1)
        assert abs(m["hold_return"] - 0.005) < 1e-9

    def test_73_forward_evidence_contribution_is_multiplier_aware(self):
        from paper_trader.api import forward_evidence as fe
        assert fe._unit_scale({"multiplier": 5.0, "fx_to_usd": 1.0}) == 5.0
        assert fe._unit_scale({"instrument_type": "CASH_EQUITY"}) == 1.0


# =========================================================================== #
# 9. DRAWDOWN OWNERSHIP + DECISION SNAPSHOT (R)
# =========================================================================== #
class TestDrawdownAndSnapshot:

    def test_80_one_drawdown_owner(self):
        perf = {"current_rows": [{"date": "2026-08-27", "drawdown_pct": -1.0},
                                 {"date": "2026-08-28", "drawdown_pct": -2.5}],
                "current_summary": {"max_drawdown_pct": -2.5}, "summary": {"max_drawdown_pct": -2.4}}
        dd = desk.current_drawdown(performance=perf)
        assert dd["owner"] == "api.paper_trading_desk.current_drawdown"
        assert dd["current_drawdown_pct"] == -2.5 and dd["max_drawdown_pct"] == -2.5
        assert dd["historical_raw_max_drawdown_pct"] == -2.4
        from paper_trader.api import daily_close as dc
        fm = dc._forward_monitor_block(perf={"current_rows": [{"date": "2026-08-27", "nav": 100.0},
                                                             {"date": "2026-08-28", "nav": 97.5}],
                                             "current_summary": {"max_drawdown_pct": -2.5}},
                                       starting_capital=100.0)
        assert fm["max_drawdown_pct"] == -2.5 and fm["max_drawdown_owner"].endswith("current_drawdown")
        from paper_trader.api import portfolio_analytics as pa
        perfx = pa._build_performance({"rows": [{"date": "2026-08-28", "drawdown_pct": -9.0}],
                                       "current_rows": [{"date": "2026-08-28", "drawdown_pct": -2.5}],
                                       "current_summary": {"max_drawdown_pct": -2.5}})
        assert perfx["points"][0]["drawdown_pct"] == -2.5 and perfx["max_drawdown_pct"] == -2.5

    def test_81_snapshot_is_served_on_identity_match_and_regenerated_on_change(self, stores, monkeypatch):
        calls = {"n": 0}

        def fake_compose(identity):
            calls["n"] += 1
            return {"sections": {"presentation": {"portfolio_decision": {"state": "HOLD"}},
                                 "portfolio_state": {"capital": {"nav": 1.0}}},
                    "identity": identity, "warnings": []}
        monkeypatch.setattr(ds, "_compose", fake_compose)
        ds.reset()
        a = ds.load_decision_snapshot()
        b = ds.load_decision_snapshot()
        assert a["served_from"] == "REGENERATED_FROM_CANONICAL_OWNERS" and b["served_from"] == "SNAPSHOT_IDENTITY_MATCH"
        assert calls["n"] == 1
        # a decision-relevant store changes -> the identity differs -> regenerated
        desk._append_ledger(desk._desk_dir(None), desk.JOURNAL_FILE, [{"entry": {"text": "x"}}])
        c = ds.load_decision_snapshot()
        assert c["served_from"] == "REGENERATED_FROM_CANONICAL_OWNERS" and calls["n"] == 2
        assert ds.section("presentation")["decision_snapshot"]["identity_hash"] == c["identity"]["identity_hash"]
        assert ds.section("portfolio_state")["decision_snapshot"]["identity_hash"] == \
            ds.section("presentation")["decision_snapshot"]["identity_hash"]

    def test_82_snapshot_identity_is_stat_only_and_env_scoped(self, stores):
        ident = ds.snapshot_identity()
        assert "desk" in ident["store_fingerprints"] and ident["store_env"]["PAPER_TRADER_DESK_DIR"]
        assert ident["identity_hash"]

    def test_83_presentation_reads_the_capital_pool_allocation_verbatim(self):
        pres = op.build_operator_presentation(
            workflow={"status": "OK", "overall_state": "DAILY_CYCLE_COMPLETE",
                      "operational_state": {"nav": 100.0, "cash": 4.5},
                      "canonical_portfolio_decision": {"state": "NO_CHANGE"}},
            daily_close={"pnl": {}},
            capital_pool={"owner": cp.OWNER, "allocation": {"US_EQUITY": 0.955, "CASH": 0.045},
                          "allocation_labels": {"US_EQUITY": "US Equities", "CASH": "Cash"},
                          "collateral": 0.0, "available_capital": 4.5, "gross_exposure": 0.955,
                          "non_equity_position_count": 0})
        snap = pres["portfolio_snapshot"]
        assert snap["allocation_available"] and [a["label"] for a in snap["allocation"]] == ["US Equities", "Cash"]
        assert snap["asset_classes_present"] == ["US_EQUITY", "CASH"]
        assert pres["sources"]["capital_pool"]["owner"] == "api.capital_pool"
        assert pres["recomputes_nothing"] is True


# =========================================================================== #
# 10. SAFETY, ROUTES, UI STRUCTURE, RESEARCH SEPARATION (S)
# =========================================================================== #
class TestSafetyAndArchitecture:

    def test_90_no_r50_operational_owner_imports_research(self):
        for rel in ("engine/instrument_contract.py", "api/market_reference_data.py",
                    "api/investability_registry.py", "api/capital_pool.py", "engine/cross_asset_risk.py",
                    "api/cross_asset_risk.py", "engine/opportunity_frontier.py", "api/opportunity_frontier.py",
                    "api/decision_snapshot.py"):
            src = (REPO / rel).read_text(encoding="utf-8")
            assert "alpha_agent" not in src, rel

    def test_91_new_routes_are_get_only(self):
        app = (REPO / "api" / "app.py").read_text(encoding="utf-8")
        for r in ("/v1/operations/decision-snapshot", "/v1/operations/investability-registry",
                  "/v1/operations/capital-pool", "/v1/operations/cross-asset-risk",
                  "/v1/operations/opportunity-frontier"):
            assert app.count('"%s"' % r) == 1
        for s in ("presentation", "portfolio_state", "constrained", "rebalance", "workflow",
                  "daily_close", "operational"):
            assert ('_snap.section("%s")' % s) in app

    def test_92_ui_registry_card_is_an_audit_surface_and_the_r50_region_is_read_only(self):
        ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8", errors="replace")
        assert 'id="r50-investability-card"' in ui
        assert '#tab-portfolio-manager:not([data-pm-view="audit"]) > #r50-investability-card' in ui
        r0, r1 = ui.find("/* R50_REGION_START */"), ui.find("/* R50_REGION_END */")
        region = ui[r0:r1]
        assert r0 != -1 and r1 > r0
        for tok in ("fetch(", "alert(", "confirm(", "prompt(", "method: 'POST'"):
            assert tok not in region
        assert region.count("function loadInvestabilityRegistry(") == 1
        assert "s.allocation_available" in ui

    def test_93_manual_gates_and_execution_convention_unchanged_for_equities(self):
        assert pdec.CONFIRM_TOKEN == "CONFIRM_PORTFOLIO_REBALANCE_DECISION"
        assert rb.CONFIRM_TOKEN == "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN"
        assert IC.EXECUTION_CONVENTION_BY_TYPE[IC.IT_CASH_EQUITY] == desk.EXECUTION_MODEL_DEFAULT
        assert IC.SHORT_EXPOSURE_SUPPORTED is False

    def test_94_strict_architecture_audit_exits_zero(self):
        proc = subprocess.run([sys.executable, str(REPO / "scripts" / "audit_architecture.py"),
                               "--strict", "--json-only"], cwd=str(REPO), capture_output=True,
                              text=True, timeout=900)
        assert proc.returncode == 0, proc.stdout[-4000:]
        rep = json.loads(proc.stdout)
        r50 = rep["release50_multi_asset"]
        assert r50["nav_owners"] == ["api/paper_trading_desk.py"]
        assert r50["snapshot_business_reach"] == [] and r50["r46_reach"] == []
        assert r50["registry_no_promotion"] and r50["registry_eligibility_derived"]
