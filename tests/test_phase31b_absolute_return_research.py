"""
tests/test_phase31b_absolute_return_research.py

Phase 31B — ABSOLUTE-RETURN DIAGNOSIS AND RISK-CONTROLLED SHADOW CHALLENGERS.

The engine is research-only and READ ONLY with respect to Paper Trader. These
tests prove the Part M matrix:

  1  the live database path is never opened (read-only by construction);
  2  no insert/update/delete/truncate/migration/DDL is ever issued;
  3  the active Alpha Paper Book #1 ledgers are unchanged by a run;
  4  no order / fill / decision / performance file is written;
  5  accounting reconciliation passes on a deterministic fixture;
  6  attribution reconciles (and carries an explicit residual);
  7  no future price / future target is used;
  8  pending forward observations are excluded from matured results;
  9  signal-decay horizons are eligible completed closes, not calendar days;
  10 Variant A reproduces the control;
  11 Variant B is long-only, unlevered, and respects name/sector constraints;
  12 Variant C hedge direction and net-beta math are correct;
  13 beta sensitivity cases are fixed at 0.00 / 0.25 / 0.50;
  14 cost cases are fixed at 0 / 5 / 10 / 20 bps;
  15 the covariance fallback is deterministic;
  16 output is deterministic for identical inputs;
  17 existing active-book / daily-close code is not touched;
  18 no GCP / external network module is referenced or called;
  19 no model is promoted;
  20 the manifest captures the commit and configuration hashes.

The hermetic tests need no filesystem/DB. The read-only integration tests run
against the live desk store + owned inputs and skip cleanly if either is absent.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from paper_trader.engine import absolute_return_research as are

_REPO = Path(__file__).resolve().parents[1]
_CONFIG_PATH = _REPO / "configs" / "research" / "phase31b_absolute_return_shadow.json"
_FIXED_NOW = __import__("datetime").datetime(2026, 7, 28, 12, 0, 0,
                                            tzinfo=__import__("datetime").timezone.utc)


def _cfg() -> dict:
    return json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# Live-store availability gate (read-only integration tests).
# --------------------------------------------------------------------------- #
def _live_sources():
    try:
        S = are.load_sources(config=_cfg())
    except Exception:  # noqa: BLE001
        return None
    if not S.get("perf_rows"):
        return None
    return S


@pytest.fixture(scope="module")
def live_S():
    S = _live_sources()
    if S is None:
        pytest.skip("live desk store / owned inputs not available in this environment")
    return S


@pytest.fixture(scope="module")
def live_result(live_S):
    return are.run_research(output_root=str(_REPO), config=_cfg(), git_commit="TESTCOMMIT",
                            now=_FIXED_NOW, write=False)


# =========================================================================== #
# Pure numeric / construction helpers (hermetic).
# =========================================================================== #
class TestPureHelpers:
    def test_spearman_perfect_and_inverse(self):
        assert are._spearman([1, 2, 3, 4], [1, 2, 3, 4]) == pytest.approx(1.0)
        assert are._spearman([1, 2, 3, 4], [4, 3, 2, 1]) == pytest.approx(-1.0)
        assert are._spearman([1, 2], [1, 2]) is None            # < 3 names

    def test_max_drawdown(self):
        assert are._max_drawdown([100, 90, 95]) == pytest.approx(-0.10, abs=1e-9)
        assert are._max_drawdown([100]) is None

    def test_name_cap_enforced_and_normalized(self):
        # cap is satisfiable (3 names, 50% cap): 0.6 -> 0.5, excess spread to the rest
        w = are._apply_name_cap({"A": 0.6, "B": 0.3, "C": 0.1}, 0.5)
        assert abs(sum(w.values()) - 1.0) < 1e-9
        assert max(w.values()) <= 0.5 + 1e-9

    def test_annualization_guard_empty(self):
        assert are._annualize_return([]) is None


# =========================================================================== #
# 5. Accounting reconciliation on a deterministic fixture.
# =========================================================================== #
class TestAccountingFixture:
    def _S(self):
        return {
            "perf": {"summary": {}},
            "perf_rows": [
                {"date": "2026-01-02", "nav": 620.0, "cash": 100.0, "invested": 520.0,
                 "holdings": {"AAA": 10}, "daily_return_pct": None, "transaction_cost": 0.0},
                {"date": "2026-01-05", "nav": 640.0, "cash": 100.0, "invested": 540.0,
                 "holdings": {"AAA": 10}, "daily_return_pct": 3.225806, "transaction_cost": 0.0},
            ],
            "marks_series": {"AAA": [["2026-01-02", 52.0], ["2026-01-05", 54.0]]},
        }

    def test_reconciles_to_penny_and_bps(self):
        out = are.build_accounting_reconciliation(self._S(), _cfg())
        assert out["reconciliation"]["all_rows_reconcile"] is True
        assert out["reconciliation"]["control_reconciled"] is True
        assert out["reconciliation"]["max_abs_nav_residual_usd"] <= 0.01
        assert out["reconciliation"]["max_abs_return_residual_bps"] <= 1.0
        r2 = out["rows"][1]
        assert r2["reconstructed_nav"] == pytest.approx(640.0, abs=1e-6)
        assert r2["nav_reconciles"] is True and r2["return_reconciles"] is True

    def test_detects_a_mark_break(self):
        S = self._S()
        S["marks_series"]["AAA"][1][1] = 60.0          # wrong mark -> NAV won't reconcile
        out = are.build_accounting_reconciliation(S, _cfg())
        assert out["reconciliation"]["all_rows_reconcile"] is False


# =========================================================================== #
# 5 (durable as-of-close). Immutable frozen-close marks take precedence over the
#    revisable desk_marks cache; fallback is explicit and never fabricated.
# =========================================================================== #
class TestFrozenCloseMarkPrecedence:
    """As-of-close reconciliation source precedence (durable-reconciliation fix).

    The immutable first-write-wins price store (``S["price_series"]``, written by
    api.forward_prediction_skill and never overwritten per (ticker, date)) is the
    primary valuation source; the revisable ``desk_marks`` cache
    (``S["marks_series"]``) is an explicit, clearly-labelled fallback used ONLY
    when no frozen close exists. A frozen historical NAV must never be reconciled
    against a later-revised current mark when the original close mark is preserved.
    """
    FROZEN = "FROZEN_CLOSE_MARKS"
    FALLBACK = "HISTORICAL_MARK_FALLBACK"
    NONE = "NO_MARKS"

    def _S(self, *, frozen, marks, holdings_extra=None):
        """Two-date book (AAA x10, FANG x20, cash 100). The frozen close marks
        make both rows reconcile to the penny; ``marks`` is supplied separately so
        a later vendor revision can be injected into the fallback source only."""
        holdings = {"AAA": 10, "FANG": 20}
        if holdings_extra:
            holdings = {**holdings, **holdings_extra}
        return {
            "perf": {"summary": {}},
            "perf_rows": [
                {"date": "2026-01-02", "nav": 4400.0, "cash": 100.0, "invested": 4300.0,
                 "holdings": dict(holdings), "daily_return_pct": None, "transaction_cost": 0.0},
                {"date": "2026-01-05", "nav": 4430.0, "cash": 100.0, "invested": 4330.0,
                 "holdings": dict(holdings), "daily_return_pct": 0.681818, "transaction_cost": 0.0},
            ],
            "price_series": frozen,
            "marks_series": marks,
        }

    def _frozen_ok(self):
        # Correct immutable close marks: both rows reconcile exactly.
        return {"AAA": [["2026-01-02", 50.0], ["2026-01-05", 51.0]],
                "FANG": [["2026-01-02", 190.0], ["2026-01-05", 191.0]]}

    def _marks_revised(self):
        # desk_marks REVISES FANG 2026-01-02 up by $0.005/sh (x20 = +$0.10) --
        # exactly the live 2026-07-28 vendor adjusted-close drift.
        return {"AAA": [["2026-01-02", 50.0], ["2026-01-05", 51.0]],
                "FANG": [["2026-01-02", 190.005], ["2026-01-05", 191.0]]}

    # 1. A frozen row reconciles against its exact immutable marks even when the
    #    current desk_marks later revises that date.
    def test_frozen_marks_reconcile_despite_revised_desk_marks(self):
        S = self._S(frozen=self._frozen_ok(), marks=self._marks_revised())
        out = are.build_accounting_reconciliation(S, _cfg())
        rec = out["reconciliation"]
        assert rec["all_rows_reconcile"] is True
        assert rec["max_abs_nav_residual_usd"] <= 0.01
        assert rec["rows_from_frozen_close_marks"] == 2
        assert rec["rows_from_historical_fallback"] == 0
        assert rec["all_rows_from_frozen_close_marks"] is True
        for r in out["rows"]:
            assert r["mark_evidence"] == self.FROZEN
            assert r["fully_reproducible_from_frozen_close"] is True
            assert r["priced_from_historical_fallback"] == 0
        # the 2026-01-02 row used the frozen 190.0, not the revised 190.005
        assert out["rows"][0]["reconstructed_invested"] == pytest.approx(4300.0, abs=1e-6)

    # 2. The July-28-style $0.10 vendor revision breaks reconciliation only when
    #    the frozen store is absent; with it present the row is unaffected.
    def test_ten_cent_revision_breaks_only_when_frozen_absent(self):
        no_frozen = self._S(frozen={}, marks=self._marks_revised())
        bad = are.build_accounting_reconciliation(no_frozen, _cfg())
        assert bad["reconciliation"]["all_rows_reconcile"] is False
        assert bad["reconciliation"]["max_abs_nav_residual_usd"] == pytest.approx(0.10, abs=1e-6)
        assert bad["rows"][0]["mark_evidence"] == self.FALLBACK      # honestly labelled
        with_frozen = self._S(frozen=self._frozen_ok(), marks=self._marks_revised())
        good = are.build_accounting_reconciliation(with_frozen, _cfg())
        assert good["reconciliation"]["all_rows_reconcile"] is True

    # 3. The strict $0.01 NAV tolerance is unchanged: the fix moves the SOURCE,
    #    never the tolerance, so a frozen mark wrong by > $0.01 still fails.
    def test_strict_penny_tolerance_still_binds(self):
        ok = self._S(frozen=self._frozen_ok(), marks=self._frozen_ok())
        out = are.build_accounting_reconciliation(ok, _cfg())
        assert out["reconciliation"]["tolerance_nav_usd"] == pytest.approx(0.01)
        bad_frozen = self._frozen_ok()
        bad_frozen["AAA"][1][1] = 51.002            # 10 sh x $0.002 = $0.02 > $0.01
        Sb = self._S(frozen=bad_frozen, marks=bad_frozen)
        outb = are.build_accounting_reconciliation(Sb, _cfg())
        assert outb["reconciliation"]["all_rows_reconcile"] is False
        assert outb["reconciliation"]["max_abs_nav_residual_usd"] == pytest.approx(0.02, abs=1e-6)

    # 4. Current mutable historical marks cannot override exact frozen close marks.
    def test_revised_marks_cannot_override_frozen(self):
        marks = self._frozen_ok()
        marks["AAA"][0][1] = 999.0                  # absurd revision on the fallback source
        S = self._S(frozen=self._frozen_ok(), marks=marks)
        out = are.build_accounting_reconciliation(S, _cfg())
        assert out["rows"][0]["reconstructed_invested"] == pytest.approx(4300.0, abs=1e-6)
        assert out["rows"][0]["mark_evidence"] == self.FROZEN
        assert out["reconciliation"]["all_rows_reconcile"] is True

    # 5. Rows without a frozen close mark use an explicit, labelled fallback and
    #    are flagged not fully reproducible from immutable evidence.
    def test_missing_frozen_mark_uses_explicit_labelled_fallback(self):
        frozen = self._frozen_ok()
        del frozen["FANG"]                          # no frozen close for FANG at all
        marks = self._frozen_ok()                   # desk_marks still has a correct FANG close
        S = self._S(frozen=frozen, marks=marks)
        out = are.build_accounting_reconciliation(S, _cfg())
        rec = out["reconciliation"]
        assert rec["all_rows_reconcile"] is True                 # fallback value is correct
        assert rec["all_rows_from_frozen_close_marks"] is False
        assert rec["rows_from_historical_fallback"] == 2
        for r in out["rows"]:
            assert r["mark_evidence"] == self.FALLBACK
            assert r["fully_reproducible_from_frozen_close"] is False
            assert r["priced_from_frozen_close"] == 1            # AAA frozen
            assert r["priced_from_historical_fallback"] == 1     # FANG fell back

    # 6. Missing immutable evidence is reported honestly and never fabricated.
    def test_missing_marks_reported_never_fabricated(self):
        S = self._S(frozen=self._frozen_ok(), marks=self._frozen_ok(),
                    holdings_extra={"ZZZ": 5})
        out = are.build_accounting_reconciliation(S, _cfg())
        r0 = out["rows"][0]
        assert "ZZZ" in r0["missing_marks"].split(";")
        assert r0["priced_holdings"] == 2                        # AAA + FANG only
        assert r0["reconstructed_invested"] == pytest.approx(4300.0, abs=1e-6)  # ZZZ not invented
        # a row where EVERY holding is unmarked is NO_MARKS, not a fabricated frozen row
        S2 = {"perf": {"summary": {}},
              "perf_rows": [{"date": "2026-01-02", "nav": 100.0, "cash": 100.0,
                             "invested": 0.0, "holdings": {"ZZZ": 5},
                             "daily_return_pct": None, "transaction_cost": 0.0}],
              "price_series": {}, "marks_series": {}}
        out2 = are.build_accounting_reconciliation(S2, _cfg())
        assert out2["rows"][0]["mark_evidence"] == self.NONE
        assert out2["rows"][0]["priced_holdings"] == 0

    # 9. Every legacy accounting-row column is preserved (no downstream CSV break)
    #    and the new provenance columns are added.
    def test_row_schema_is_backward_compatible(self):
        S = self._S(frozen=self._frozen_ok(), marks=self._frozen_ok())
        out = are.build_accounting_reconciliation(S, _cfg())
        legacy = {"market_date", "starting_nav", "ending_nav", "cash", "recorded_invested",
                  "reconstructed_invested", "reconstructed_nav", "nav_residual_usd",
                  "gross_position_contribution", "execution_cost", "recorded_daily_return_pct",
                  "reconstructed_daily_return_pct", "return_residual_bps", "priced_holdings",
                  "missing_marks", "nav_reconciles", "return_reconciles"}
        added = {"priced_from_frozen_close", "priced_from_historical_fallback",
                 "mark_evidence", "fully_reproducible_from_frozen_close"}
        for r in out["rows"]:
            assert legacy.issubset(set(r.keys()))               # no column removed
            assert added.issubset(set(r.keys()))

    # 7/8. The reconciliation is a pure read: it mutates no desk file / ledger.
    def test_reconciliation_writes_no_desk_file_live(self, live_S):
        sdir = Path(live_S["desk_dir"])
        before = _hash_dir(sdir)
        are.build_accounting_reconciliation(live_S, _cfg())
        assert _hash_dir(sdir) == before

    # The live operational closes are all preserved in the immutable store, so
    # every live accounting row is frozen-quality and reconciles to the penny.
    def test_live_rows_reconcile_from_frozen_close_marks(self, live_S):
        out = are.build_accounting_reconciliation(live_S, _cfg())
        rec = out["reconciliation"]
        assert rec["all_rows_reconcile"] is True
        assert rec["rows_from_historical_fallback"] == 0
        assert rec["all_rows_from_frozen_close_marks"] is True
        assert rec["max_abs_nav_residual_usd"] <= 0.01


# =========================================================================== #
# 15. Covariance fallback is deterministic (diagonal when window too short).
# =========================================================================== #
class TestCovarianceFallback:
    def _S(self):
        return {
            "price_series": {"AAA": [["D1", 10.0], ["D2", 11.0], ["D3", 10.5]],
                             "BBB": [["D1", 20.0], ["D2", 21.0], ["D3", 22.0]],
                             "SPY": [["D1", 100.0], ["D2", 101.0], ["D3", 102.0]]},
            "marks_series": {},
            "risk_by_ticker": {"AAA": {"realized_vol_63d": 0.30},
                               "BBB": {"realized_vol_63d": 0.20}},
        }

    def test_diagonal_fallback_when_short(self):
        c1 = are.build_covariance(self._S(), ["AAA", "BBB"], _cfg())
        c2 = are.build_covariance(self._S(), ["AAA", "BBB"], _cfg())
        assert c1["estimator"] == "diagonal_fallback"
        assert c1["cov"][("AAA", "AAA")] == pytest.approx(0.09)
        assert c1["cov"][("AAA", "BBB")] == pytest.approx(0.0)   # no cross-term when diagonal
        assert c1["cov"] == c2["cov"]                            # deterministic


# =========================================================================== #
# 12/13/14. Variant C hedge algebra + fixed grids (hermetic, scale-independent).
# =========================================================================== #
class TestVariantMathHermetic:
    def _S4(self):
        # 4 names, 2 sectors, a 6-date price panel (5 returns) and owned vols/betas.
        px = {
            "AAA": [["D%d" % i, 100.0 * (1.01 ** i)] for i in range(6)],
            "BBB": [["D%d" % i, 50.0 * (1.00 ** i)] for i in range(6)],
            "CCC": [["D%d" % i, 30.0 * (0.99 ** i)] for i in range(6)],
            "DDD": [["D%d" % i, 80.0 * (1.005 ** i)] for i in range(6)],
            "SPY": [["D%d" % i, 400.0 * (1.002 ** i)] for i in range(6)],
        }
        return {
            "price_series": px, "marks_series": px,
            "perf_rows": [{"date": "D5", "nav": 100000.0, "cash": 0.0,
                           "holdings": {"AAA": 250, "BBB": 500, "CCC": 800, "DDD": 300},
                           "turnover_pct": 0.0, "transaction_cost": 0.0}],
            "snap_sector": {"AAA": "Tech", "BBB": "Tech", "CCC": "Energy", "DDD": "Energy"},
            "sector_by_ticker": {}, "snapshot_rows": [], "perf": {"summary": {}},
            "mom_by_ticker": {t: {"mom_6_1": v} for t, v in
                              (("AAA", 0.4), ("BBB", 0.2), ("CCC", 0.1), ("DDD", 0.3))},
            "risk_by_ticker": {t: {"realized_vol_63d": v, "beta_universe": b} for t, v, b in
                               (("AAA", 0.30, 1.5), ("BBB", 0.25, 1.1),
                                ("CCC", 0.40, 0.8), ("DDD", 0.20, 1.0))},
        }

    def test_variant_c_hedge_direction_and_net_beta(self):
        S = self._S4()
        vb = {"weights": {"AAA": 0.4, "BBB": 0.3}}          # gross 0.7, cash 0.3
        for target in (0.0, 0.25, 0.5):
            vc = are.construct_variant_c(S, _cfg(), vb, target)
            assert vc["available"] is True
            # net beta math: hedge = target - long_book_beta ; net = beta_b + hedge == target
            assert vc["estimated_net_beta"] == pytest.approx(target, abs=1e-6)
            beta_b = vc["long_book_beta"]                    # rounded to 4dp in the output
            assert vc["spy_hedge_weight"] == pytest.approx(target - beta_b, abs=1e-3)
            if beta_b > target:
                assert vc["spy_hedge_weight"] < 0            # short SPY to cut beta
            assert vc["no_single_stock_shorting"] is True

    def test_beta_sensitivity_and_cost_grids_are_fixed(self):
        shadow = are.build_shadow_variants(self._S4(), _cfg())
        assert set(shadow["variant_c"].keys()) == {"0.0", "0.25", "0.5"}
        costs = sorted({r["one_way_cost_bps"] for r in shadow["cost_sensitivity"]})
        assert costs == [0, 5, 10, 20]
        assert {r["one_way_cost_bps"] for r in shadow["cost_sensitivity"] if r["is_primary"]} == {10}

    def test_variant_a_reproduces_control(self):
        S = self._S4()
        shadow = are.build_shadow_variants(S, _cfg())
        a = shadow["definitions"]["variant_a"]["weights"]
        cur = are._current_weights(S)
        assert set(a) == set(cur)
        for t in cur:
            assert a[t] == pytest.approx(round(cur[t], 6), abs=1e-6)
        arow = next(r for r in shadow["variant_summary"] if r["variant"] == "A")
        assert arow["gross"] == pytest.approx(1.0, abs=1e-6)

    def test_apply_sector_cap_unit(self):
        # explicit benchmark so the 5pp cap is satisfiable
        S = {"snap_sector": {"A": "Tech", "B": "Tech", "C": "Energy"}, "sector_by_ticker": {}}
        w = {"A": 0.5, "B": 0.3, "C": 0.2}                  # Tech 0.8
        bench = {"Tech": 0.5, "Energy": 0.5}                # Tech cap 0.55
        capped, names = are._apply_sector_cap(S, w, bench, 0.05)
        tech = capped["A"] + capped["B"]
        assert tech <= 0.55 + 1e-6 and "Tech" in names
        assert abs(sum(capped.values()) - 1.0) < 1e-9


# =========================================================================== #
# 11. Variant B constraints on the live 25-name book (caps are satisfiable).
# =========================================================================== #
class TestVariantBLive:
    def test_variant_b_long_only_unlevered_and_name_capped(self, live_S):
        vb = are.construct_variant_b(live_S, _cfg())
        w = vb["weights"]
        assert all(v >= -1e-12 for v in w.values())         # long only
        assert vb["gross"] <= 1.0 + 1e-9                     # unlevered (cash allowed)
        assert max(w.values()) <= 0.05 + 1e-4                # 5% name cap (deterministic heuristic)
        assert vb["cash"] >= -1e-9                           # no leverage

    def test_variant_b_sector_active_deviation_capped(self, live_S):
        vb = are.construct_variant_b(live_S, _cfg())
        bench = are._eligible_universe_sector_weights(live_S)
        gross = sum(vb["weights"].values()) or 1.0
        sec = {}
        for t, v in vb["weights"].items():
            sec[are.sector_of(live_S, t)] = sec.get(are.sector_of(live_S, t), 0.0) + v / gross
        dev = _cfg()["shadow_variants"]["variant_b_risk_controlled_long_only"]["max_active_sector_deviation_pp"]
        for s, wsum in sec.items():
            assert wsum <= bench.get(s, 0.0) + dev + 1e-4

    def test_variant_b_name_cap_holds_after_final_scaling_live(self, live_S):
        # Regression for the Phase 31B name-cap breach: on the live 25-name book
        # the max position stayed at 0.05033778 (> 0.05) because the trailing
        # sector-cap redistribution re-inflated a name above the 5% cap. The
        # deterministic joint projection must keep it at/under the cap after every
        # later transformation (turnover blend, joint caps, volatility scaling).
        vb = are.construct_variant_b(live_S, _cfg())
        w = vb["weights"]
        name_cap = _cfg()["shadow_variants"]["variant_b_risk_controlled_long_only"]["max_individual_weight"]
        assert w, "live book produced no Variant B weights"
        assert max(w.values()) <= name_cap + 1e-9          # strict: no tolerance slack needed
        assert vb["gross"] <= 1.0 + 1e-12                   # unlevered
        assert vb["cash"] >= -1e-12                         # residual held as cash, never negative
        assert min(w.values()) >= -1e-12                    # long only
        assert abs(vb["gross"] + vb["cash"] - 1.0) < 1e-6   # gross + cash accounts for the whole book


# =========================================================================== #
# 11 (regression). Deterministic proofs that the joint name/sector projection
# never breaches the 5% name cap or a sector cap, and holds residual as cash
# instead of renormalizing concentration back above a cap. Hermetic — the
# projection needs only a sector map, so no live store / DB / network.
# =========================================================================== #
class TestVariantBNameCapProjection:
    # 9 sectors / 25 names, mirroring the live book's shape: a few single-name
    # sectors whose 5% name cap binds below their sector cap, so the book cannot
    # be fully invested and ~4% must be held as cash.
    _SECT = {
        "IT1": "IT", "IT2": "IT", "IT3": "IT", "IT4": "IT", "IT5": "IT", "IT6": "IT",
        "IND1": "Ind", "IND2": "Ind", "IND3": "Ind", "IND4": "Ind", "IND5": "Ind", "IND6": "Ind",
        "EN1": "En", "EN2": "En", "EN3": "En", "EN4": "En",
        "HC1": "HC", "HC2": "HC", "HC3": "HC",
        "CS1": "CS", "CS2": "CS",
        "MAT1": "Mat", "RE1": "RE", "COM1": "Com", "CD1": "CD",
    }
    _BENCH = {"IT": 0.15, "Ind": 0.15, "En": 0.06, "HC": 0.15, "CS": 0.05,
              "Mat": 0.07, "RE": 0.03, "Com": 0.04, "CD": 0.06}
    _NAME_CAP = 0.05
    _DEV = 0.05

    def _S(self):
        return {"snap_sector": dict(self._SECT), "sector_by_ticker": {}}

    def _skewed_weights(self):
        # a heavily concentrated pre-cap tilt: two IT names dominate, one big
        # single-name sector, the rest small — exactly the shape that made naive
        # sector redistribution push a name back above the cap.
        w = {t: 0.01 for t in self._SECT}
        w["IT1"] = 0.30
        w["IT2"] = 0.25
        w["IND1"] = 0.15
        w["EN1"] = 0.10
        w["CD1"] = 0.12
        s = sum(w.values())
        return {t: v / s for t, v in w.items()}

    def _sector_shares(self, S, w):
        gross = sum(w.values()) or 1.0
        sec = {}
        for t, v in w.items():
            sec[are.sector_of(S, t)] = sec.get(are.sector_of(S, t), 0.0) + v / gross
        return sec

    def test_25_name_input_never_exceeds_name_cap(self):
        # (1) live-style 25-name input cannot exceed the 5% cap.
        S = self._S()
        out = are._apply_joint_caps(S, self._skewed_weights(), self._BENCH,
                                    self._NAME_CAP, self._DEV)
        assert max(out.values()) <= self._NAME_CAP + 1e-12

    def test_multiple_initially_capped_names_stay_capped(self):
        # (2) several names start far above the cap; all end at/under it.
        S = self._S()
        w0 = self._skewed_weights()
        over = [t for t, v in w0.items() if v > self._NAME_CAP]
        assert len(over) >= 2                                # IT1, IT2, IND1, EN1, CD1 ...
        out = are._apply_joint_caps(S, w0, self._BENCH, self._NAME_CAP, self._DEV)
        for t in over:
            assert out[t] <= self._NAME_CAP + 1e-12
        # the dominant names remain pinned exactly at the cap (not zeroed out)
        assert out["IT1"] == pytest.approx(self._NAME_CAP, abs=1e-9)
        assert out["IT2"] == pytest.approx(self._NAME_CAP, abs=1e-9)

    def test_residual_is_held_as_cash_when_no_legal_recipient(self):
        # (3) with single-name sectors capped at 5%, the book cannot be fully
        # invested; the unplaceable remainder is cash (gross < 1), never forced
        # onto a name above its cap.
        S = self._S()
        out = are._apply_joint_caps(S, self._skewed_weights(), self._BENCH,
                                    self._NAME_CAP, self._DEV)
        gross = sum(out.values())
        assert gross < 1.0 - 1e-6                            # cash strictly held
        cash = 1.0 - gross
        assert cash > 0.0
        assert max(out.values()) <= self._NAME_CAP + 1e-12   # cap still intact with cash held

    def test_gross_never_exceeds_one_and_weights_nonnegative(self):
        # (4) gross <= 1.0  and  (5) all weights >= 0.
        S = self._S()
        out = are._apply_joint_caps(S, self._skewed_weights(), self._BENCH,
                                    self._NAME_CAP, self._DEV)
        assert sum(out.values()) <= 1.0 + 1e-12
        assert all(v >= 0.0 for v in out.values())

    def test_sector_limits_remain_satisfied(self):
        # (6) every sector's invested share stays within benchmark + deviation.
        S = self._S()
        out = are._apply_joint_caps(S, self._skewed_weights(), self._BENCH,
                                    self._NAME_CAP, self._DEV)
        for s, share in self._sector_shares(S, out).items():
            assert share <= self._BENCH.get(s, 0.0) + self._DEV + 1e-9

    def test_repeated_construction_is_identical(self):
        # (7) deterministic output for identical inputs.
        S = self._S()
        w0 = self._skewed_weights()
        a = are._apply_joint_caps(S, w0, self._BENCH, self._NAME_CAP, self._DEV)
        b = are._apply_joint_caps(S, dict(w0), self._BENCH, self._NAME_CAP, self._DEV)
        assert a == b

    def test_full_investment_when_caps_are_slack(self):
        # sanity: when the caps are easily satisfiable the projection invests the
        # whole book (no spurious cash) and still honors the name cap.
        S = self._S()
        loose_bench = {s: 0.50 for s in self._BENCH}         # 55% sector cap -> slack
        out = are._apply_joint_caps(S, self._skewed_weights(), loose_bench,
                                    self._NAME_CAP, self._DEV)
        assert sum(out.values()) == pytest.approx(1.0, abs=1e-9)
        assert max(out.values()) <= self._NAME_CAP + 1e-12

    def test_waterfill_respects_cap_and_conserves_mass(self):
        # the within-sector placer never exceeds the cap and places exactly the
        # requested mass when the cap allows it.
        alloc = are._waterfill(["A", "B", "C"], {"A": 100.0, "B": 1.0, "C": 1.0},
                               0.12, 0.05)
        assert max(alloc.values()) <= 0.05 + 1e-12
        assert sum(alloc.values()) == pytest.approx(0.12, abs=1e-9)
        assert alloc["A"] == pytest.approx(0.05, abs=1e-9)   # dominant name pinned at cap


# =========================================================================== #
# 1/2/3/4/17. Read-only: no DB connection, no desk mutation.
# =========================================================================== #
def _hash_dir(p: Path) -> dict:
    out = {}
    if not p.exists():
        return out
    for f in sorted(p.iterdir()):
        if f.is_file():
            out[f.name] = hashlib.sha256(f.read_bytes()).hexdigest()
    return out


class TestReadOnlyGuarantees:
    def test_run_opens_no_database_connection(self, live_S, monkeypatch):
        import paper_trader.db.session as sess

        def _boom(*a, **k):  # any attempt to build/use the engine must raise
            raise AssertionError("engine attempted to open a database connection")

        monkeypatch.setattr(sess, "get_engine", _boom, raising=True)
        monkeypatch.setattr(sess, "_build_engine", _boom, raising=True)
        out = are.run_research(output_root=str(_REPO), config=_cfg(),
                               git_commit="TESTCOMMIT", now=_FIXED_NOW, write=False)
        assert out["status"] == are.READY

    def test_run_write_false_mutates_no_desk_file(self, live_S):
        sdir = Path(live_S["desk_dir"])
        before = _hash_dir(sdir)
        are.run_research(output_root=str(_REPO), config=_cfg(), git_commit="X",
                         now=_FIXED_NOW, write=False)
        after = _hash_dir(sdir)
        assert before == after                                  # nothing in the store changed

    def test_run_writes_only_under_output_root(self, live_S, tmp_path):
        sdir = Path(live_S["desk_dir"])
        before = _hash_dir(sdir)
        out = are.run_research(output_root=str(tmp_path), config=_cfg(), git_commit="X",
                               now=_FIXED_NOW, write=True)
        assert out["status"] == are.READY
        run_dir = Path(out["run_dir"])
        assert run_dir.parent == tmp_path                       # written only under output root
        assert set(out["files_written"]) == set(are.OUTPUT_FILES)
        assert _hash_dir(sdir) == before                        # desk store untouched by the write

    def test_engine_source_has_no_write_or_network_calls(self):
        src = (Path(are.__file__)).read_text(encoding="utf-8")
        banned = ["engine.market_data", "market_data as", "yfinance", "requests.", "urllib",
                  "capture_snapshots", "mature_outcomes(", "run_refresh", "run_daily_close",
                  "get_session(", "get_dedicated_session(", "session.execute", "session.add(",
                  "session.commit(", "INSERT INTO", "DELETE FROM", "TRUNCATE", "CREATE TABLE"]
        present = [b for b in banned if b in src]
        assert present == [], "engine references write/network primitives: %s" % present


# =========================================================================== #
# 6/7/8/9. Attribution + signal-decay integrity (live, read-only).
# =========================================================================== #
class TestAttributionAndSignal:
    def test_accounting_and_attribution_reconcile_live(self, live_S):
        acc = are.build_accounting_reconciliation(live_S, _cfg())
        assert acc["reconciliation"]["all_rows_reconcile"] is True
        attr = are.build_return_attribution(live_S, _cfg())
        assert attr["reconciliation"]["attribution_available"] is True
        fac = attr["reconciliation"]["factor_decomposition"]
        assert fac["available"] is True
        assert "residual_pct" in fac                            # residual always explicit

    def test_no_future_snapshot_used(self, live_S):
        as_of = are.latest_completed_date(live_S)
        for r in live_S["snapshot_rows"]:
            md = r.get("market_date")
            if md:
                assert md <= as_of                              # never a future-dated snapshot

    def test_pending_excluded_from_matured(self, live_S):
        sd = are.build_signal_decay(live_S, _cfg())
        for row in sd["rows"]:
            if row["status"] == "PENDING_NOT_ENOUGH_CLOSES":
                assert row["rank_ic_spearman"] is None and row["matured"] is False
            if row["matured"]:
                assert row["rank_ic_spearman"] is not None
        # matured count never exceeds what the calendar supports
        assert sd["adequacy"]["matured_cells"] == sum(1 for r in sd["rows"] if r["matured"])

    def test_signal_horizons_are_eligible_closes(self, live_S):
        sd = are.build_signal_decay(live_S, _cfg())
        assert sd["adequacy"]["requested_horizons"] == [1, 5, 10, 20, 40]
        # with only 1 completed close after the latest maturable snapshot, h>=5 must pend
        pend_h = {r["horizon_eligible_closes"] for r in sd["rows"]
                  if r["status"] == "PENDING_NOT_ENOUGH_CLOSES"}
        assert {5, 10, 20, 40}.issubset(pend_h)


# =========================================================================== #
# 16/19/20. Determinism, no promotion, manifest hashes (live, read-only).
# =========================================================================== #
class TestDeterminismAndManifest:
    def test_output_is_deterministic(self, live_S):
        a = are.run_research(output_root=str(_REPO), config=_cfg(), git_commit="C",
                             now=_FIXED_NOW, write=False)
        b = are.run_research(output_root=str(_REPO), config=_cfg(), git_commit="C",
                             now=_FIXED_NOW, write=False)
        assert a["run_id"] == b["run_id"]
        assert json.dumps(a["manifest"], sort_keys=True, default=str) == \
               json.dumps(b["manifest"], sort_keys=True, default=str)
        assert a["payload"]["executive_recommendation"] == b["payload"]["executive_recommendation"]

    def test_no_model_promotion(self, live_result):
        g = live_result["payload"]["promotion_gates"]
        assert g["promotes_now"] is False
        assert g["active_book_unchanged"] is True
        assert g["decision"] in _cfg()["promotion_gates"]["allowed_decisions"]
        assert "promote now" not in json.dumps(g).lower().replace("promotes_now", "")

    def test_manifest_captures_commit_and_config_hash(self, live_result):
        m = live_result["manifest"]
        assert m["git_commit"] == "TESTCOMMIT"
        assert isinstance(m["config_hash"], str) and len(m["config_hash"]) == 64
        assert m["read_only"] is True and m["wrote_to_database"] is False
        assert m["database_connections_opened"] == 0
        assert m["external_market_data_calls"] == 0
        assert m["active_book_changed"] is False
        assert set(m["output_files"]) == set(are.OUTPUT_FILES)

    def test_run_id_changes_with_inputs(self, live_S):
        base = are.compute_run_id(git_commit="A", config_hash="X", as_of="2026-07-27",
                                  source_hashes={"f": "h1"})
        diff = are.compute_run_id(git_commit="A", config_hash="X", as_of="2026-07-27",
                                  source_hashes={"f": "h2"})
        assert base != diff and base.startswith("phase31b_")


# =========================================================================== #
# Immutable-run guard (never overwrite a prior run).
# =========================================================================== #
class TestImmutableRun:
    def test_second_write_is_blocked(self, live_S, tmp_path):
        first = are.run_research(output_root=str(tmp_path), config=_cfg(), git_commit="Z",
                                 now=_FIXED_NOW, write=True)
        assert first["status"] == are.READY
        second = are.run_research(output_root=str(tmp_path), config=_cfg(), git_commit="Z",
                                  now=_FIXED_NOW, write=True)
        assert second["status"] == are.BLOCKED
        assert "RUN_DIR_EXISTS_IMMUTABLE" in second["reason"]
