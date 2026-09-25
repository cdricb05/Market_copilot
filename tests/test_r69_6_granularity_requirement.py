"""R69.6 - the NAV a non-equity sleeve needs before it can hold ONE contract.

The multi-asset frontier had two independent walls and published only the first.
The capital-eligibility gate refuses a sleeve until its forward evidence matures,
which for the registered cadence challengers is 1.2-5.0 years away. BELOW that
gate sits an arithmetic wall nothing in the estate stated: an exchange-set
contract whose unit notional exceeds ``max_name_weight x NAV`` cannot be held at
all, and that is still true on the day the evidence finally arrives.

Because the granularity check ran only over sleeves that had ALREADY passed the
gate, the wall was unobservable until the moment it was too late to plan around.
These tests pin the repair: the requirement is computed for EVERY non-equity
sleeve regardless of gate state, it is arithmetic on figures already published,
and it admits nothing.

Hermetic: injected metadata/mark loaders, no provider call, no store, no network.
"""
from __future__ import annotations

import pytest

from paper_trader.api import investability_registry as ir
from paper_trader.api import opportunity_frontier as of
from paper_trader.engine import instrument_contract as ic


CAP = 0.10
NAV = 100_000.0
# One contract far too large for a $100k book at a 10% name cap ($10,000), and
# one that fits. 300_000 * 0.10 -> needs a $3.0m book; 80_000 -> needs $800k.
BIG, SMALL = 300_000.0, 8_000.0


def _registry(*, capital_eligible: bool) -> dict:
    return {"sleeves": [
        {"sleeve_id": "sleeve_fx_futures", "asset_class": "FX_FUTURES",
         "instrument_type": ic.IT_FUTURE, "instrument_ids": ["&BIG", "&SMALL"],
         "signal_scores": {"&BIG": 1.0, "&SMALL": 0.5},
         "capital_eligible": capital_eligible,
         "capital_ineligible_reason": None if capital_eligible else "GATE_NOT_PASSED",
         "capital_eligibility_gate": {"state": "GATE_PASSED" if capital_eligible
                                      else "GATE_NOT_PASSED",
                                      "remaining_codes": [] if capital_eligible
                                      else ["MIN_RAW_MATURED_OBSERVATIONS"]},
         "operational_signal_candidate": {"challenger_id": "CHALLENGER_X"}},
    ]}


@pytest.fixture
def loaders(monkeypatch):
    """Descriptors and marks for two synthetic futures, priced so that one unit
    notional is exactly the mark (multiplier 1)."""
    def _descriptor_for(sym, sleeve_id=None, metadata=None):
        return {"instrument_id": sym, "label": sym, "sleeve_id": sleeve_id,
                "asset_class": "FX_FUTURES", "asset_class_label": "FX futures",
                "instrument_type": ic.IT_FUTURE, "currency": "USD", "multiplier": 1.0}

    monkeypatch.setattr(ir.mrd, "descriptor_for", _descriptor_for)
    monkeypatch.setattr(ir.mrd, "fx_to_usd",
                        lambda cur, as_of=None: {"fx_to_usd": 1.0, "state": "OK"})
    monkeypatch.setattr(ir.mrd, "average_daily_volume", lambda sym, as_of=None: 5_000.0)
    monkeypatch.setattr(ic, "unit_notional_usd", lambda d, mark, fxv: mark * fxv)
    monkeypatch.setattr(ic, "capital_usage_ratio", lambda d, mark, fxv: 0.05)
    return dict(metadata_loader=lambda sym: {"state": "OK"},
                mark_loader=lambda sym: BIG if sym == "&BIG" else SMALL)


class TestTheRequirementIsPublished:

    def test_01_a_gated_sleeve_still_reports_what_nav_it_would_need(self, loaders):
        """The whole point: the wall is visible BEFORE the gate clears."""
        rows = ir.eligible_non_equity_instruments(
            _registry(capital_eligible=False), nav=NAV, max_name_weight=CAP,
            include_ineligible_sleeves=True, **loaders)
        assert len(rows) == 2
        big = next(r for r in rows if r["instrument_id"] == "&BIG")
        assert big["minimum_nav_for_one_unit_usd"] == pytest.approx(BIG / CAP)
        assert big["nav_multiple_required"] == pytest.approx(BIG / CAP / NAV, rel=1e-6)

    def test_02_the_default_still_admits_only_eligible_sleeves(self, loaders):
        """The flag is opt-in; no existing caller's universe widens."""
        assert ir.eligible_non_equity_instruments(
            _registry(capital_eligible=False), nav=NAV, max_name_weight=CAP,
            **loaders) == []

    def test_03_describing_a_gated_sleeve_admits_nothing(self, loaders):
        """A described row is not a fundable row, and says so on its face."""
        rows = ir.eligible_non_equity_instruments(
            _registry(capital_eligible=False), nav=NAV, max_name_weight=CAP,
            include_ineligible_sleeves=True, **loaders)
        assert all(r["sleeve_capital_eligible"] is False for r in rows)

    def test_04_the_minimum_is_exactly_where_executability_flips(self, loaders):
        """The published number is not an estimate: at one cent below it the unit
        is refused, and at the number itself it is executable."""
        need = BIG / CAP
        for nav, expected in ((need - 0.01, False), (need, True), (need * 2, True)):
            rows = ir.eligible_non_equity_instruments(
                _registry(capital_eligible=True), nav=nav, max_name_weight=CAP,
                **loaders)
            big = next(r for r in rows if r["instrument_id"] == "&BIG")
            assert big["executable_at_nav"] is expected, nav
            assert big["minimum_nav_for_one_unit_usd"] == pytest.approx(need)

    def test_05_the_name_cap_travels_with_the_verdict(self, loaders):
        """A refusal that names a cap must publish the cap it used."""
        rows = ir.eligible_non_equity_instruments(
            _registry(capital_eligible=True), nav=NAV, max_name_weight=CAP, **loaders)
        big = next(r for r in rows if r["instrument_id"] == "&BIG")
        assert big["name_cap_usd"] == pytest.approx(CAP * NAV)
        assert big["executability_reason"] == "UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV"

    def test_06_a_missing_mark_reports_no_requirement_rather_than_inventing_one(self, loaders):
        kw = dict(loaders); kw["mark_loader"] = lambda sym: None
        rows = ir.eligible_non_equity_instruments(
            _registry(capital_eligible=True), nav=NAV, max_name_weight=CAP, **kw)
        assert all(r["minimum_nav_for_one_unit_usd"] is None for r in rows)
        assert all(r["executability_reason"] == "MARK_OR_FX_UNAVAILABLE" for r in rows)


class TestTheLedgerCarriesIt:

    def _ledger(self, monkeypatch, loaders, *, capital_eligible):
        reg = _registry(capital_eligible=capital_eligible)
        real = ir.eligible_non_equity_instruments
        monkeypatch.setattr(
            of.ir, "eligible_non_equity_instruments",
            lambda registry, **kw: real(registry, **{**kw, **loaders}))
        frontier = {"nav": NAV, "policy": {"max_name_weight": CAP}, "rows": []}
        return {l["sleeve_id"]: l for l in of.non_equity_admission_ledger(reg, frontier)}

    def test_10_the_cheapest_contract_sets_the_sleeve_minimum(self, monkeypatch, loaders):
        """A sleeve is holdable as soon as its SMALLEST contract fits - not its
        largest, and not the one carrying the best score."""
        led = self._ledger(monkeypatch, loaders, capital_eligible=False)["sleeve_fx_futures"]
        assert led["cheapest_instrument_id"] == "&SMALL"
        assert led["minimum_nav_for_one_unit_usd"] == pytest.approx(SMALL / CAP)
        assert led["nav_multiple_required"] == pytest.approx(SMALL / CAP / NAV, rel=1e-6)

    def test_11_a_gated_sleeve_is_still_measured(self, monkeypatch, loaders):
        led = self._ledger(monkeypatch, loaders, capital_eligible=False)["sleeve_fx_futures"]
        assert led["capital_eligible"] is False
        assert led["gate_state"] == "GATE_NOT_PASSED"
        assert led["instruments_admitted"] == 0
        assert led["minimum_nav_for_one_unit_usd"] is not None

    def test_12_the_two_walls_stay_separately_reported(self, monkeypatch, loaders):
        """Evidence and granularity are different refusals with different remedies;
        neither may be folded into the other."""
        led = self._ledger(monkeypatch, loaders, capital_eligible=False)["sleeve_fx_futures"]
        assert "MIN_RAW_MATURED_OBSERVATIONS" in led["gate_remaining"]
        assert led["minimum_nav_for_one_unit_usd"] == pytest.approx(SMALL / CAP)


class TestSafety:

    def test_20_nothing_here_makes_anything_eligible(self, monkeypatch, loaders):
        """The repair publishes a number. It must never widen the fundable set."""
        reg = _registry(capital_eligible=False)
        assert ir.eligible_non_equity_instruments(
            reg, nav=NAV, max_name_weight=CAP, **loaders) == []
        described = ir.eligible_non_equity_instruments(
            reg, nav=NAV, max_name_weight=CAP, include_ineligible_sleeves=True, **loaders)
        assert described and not any(r["sleeve_capital_eligible"] for r in described)

    def test_21_no_threshold_moved(self, loaders):
        """max_name_weight is read, never written, and the executability rule is
        unchanged: unit notional <= cap x NAV."""
        rows = ir.eligible_non_equity_instruments(
            _registry(capital_eligible=True), nav=NAV, max_name_weight=CAP, **loaders)
        for r in rows:
            assert r["executable_at_nav"] is (r["unit_notional_usd"] <= CAP * NAV)
