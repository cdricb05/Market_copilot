r"""Release 62.3.2 - the entry-delay diagnostic that decides whether a LIVE OPRA
subscription is economically necessary, and the guards that keep it a
diagnostic.

WHAT THESE TESTS DEFEND AGAINST
    The diagnostic exists to inform a recurring purchase, which gives it two
    ways to go wrong.

    First, it could become a SECOND definition of the strategy - its own sign,
    its own z-score, its own horizon - and then the comparison would measure the
    re-typing rather than the delay. Tests 01-04 keep every frozen input
    imported from its owner and prove the baseline is literally
    ``options_surface.path``.

    Second, and worse, it could quietly become a challenger. A next-open variant
    that retained the edge would be a tempting thing to promote, and it has not
    earned anything: it runs on the sample the sign was read off. Tests 05-08
    prove it cannot register, cannot promote, cannot allocate, declares itself
    non-evidence, and that exactly two delayed boundaries exist so no delay can
    be searched.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from paper_trader.alpha_agent.alpha_recovery import entry_delay_diagnostic as ED
from paper_trader.alpha_agent.alpha_recovery import options_surface as OS
from paper_trader.alpha_agent.alpha_recovery import reversed_skew as RS

SRC = Path(ED.__file__).read_text(encoding="utf-8")
CODE = "\n".join(ln for ln in SRC.splitlines() if not ln.strip().startswith("#"))


# ------------------------------------------------- the rule is never restated
def test_01_every_frozen_input_is_imported_not_retyped():
    """A second spelling of the sign, the horizon or the lookback could drift
    away from the rule whose economics are being measured."""
    for forbidden in ("FROZEN_SIGN =", "ZSCORE_LOOKBACK =", "SELECTION_FRACTION =",
                      "HORIZON =", "COST_PRIMARY_BPS =", "PPY ="):
        assert forbidden not in CODE, "the diagnostic restates %r" % forbidden
    body_sign = RS.spec()["sign"]
    assert body_sign == RS.FROZEN_SIGN
    assert ED.COST_TREATMENT["ladder_bps_per_side"] == list(OS.COST_LADDER_BPS)


def test_02_the_baseline_is_the_frozen_path_itself():
    """The baseline must be computed by the owner, not reproduced here: a
    reproduced baseline could differ from the real one and no test would know."""
    assert ED.VARIANTS["frozen_same_day"]["book"] == "surface_close"
    assert "return OS.path(sp, cost_bps=cost_bps, surface=surface)" in CODE
    a = ED.variant_path("frozen_same_day", cost_bps=OS.COST_PRIMARY_BPS)
    b = OS.path(RS.spec(), cost_bps=OS.COST_PRIMARY_BPS, surface=OS.surface_path())
    assert np.array_equal(a["net"], b["net"], equal_nan=True)


def test_03_the_diagnostic_chooses_no_position_and_evaluates_no_rule():
    """It moves the MARK. The sign and the z-score stay with their owners."""
    for tok in ("def _z(", "def features(", "def spec(", "weights",
                "promote_model(", "activate_sleeve(", "allocate_capital(",
                "register_forward_challenger(", "adopt_prospective_freeze(",
                "create_order", "apply_fill"):
        assert tok not in CODE, "the diagnostic reaches %r" % tok
    # it may READ the owners' z-score, and that is the only route to a position
    assert "OS._z(f[sp[\"field\"]])" in CODE
    assert "float(sp[\"sign\"])" in CODE, "the sign must come from the frozen spec"


def test_04_exactly_two_delayed_boundaries_exist_and_both_are_the_next_session():
    """A delay that can be searched is a delay that was chosen."""
    delayed = {k: v for k, v in ED.VARIANTS.items() if v["lag"] != 0}
    assert set(delayed) == {"next_open", "next_close"}
    assert all(v["lag"] == 1 for v in delayed.values())
    assert all(v["needs_live_opra"] is False for v in delayed.values())
    baselines = {k for k, v in ED.VARIANTS.items() if v["lag"] == 0}
    assert baselines == {"frozen_same_day", "control_same_day"}
    assert all(ED.VARIANTS[k]["needs_live_opra"] is True for k in baselines)
    with pytest.raises(ValueError):
        ED.variant_path("next_open_plus_30m", cost_bps=1.0)


# ------------------------------------------------------ it is not a challenger
def test_05_it_declares_itself_not_evidence_and_not_a_challenger():
    body = ED.build(write=False)
    assert body["is_qualification_evidence"] is False
    assert body["is_true_forward"] is False
    assert body["creates_a_challenger"] is False
    assert body["challenger_unchanged"] is True
    assert "DISCOVERED POST-HOC" in body["why_not_evidence"]
    s = body["safety"]
    assert s["orders_created"] == 0 and s["fills_created"] == 0
    assert s["capital_allocated"] is False and s["promoted"] is False
    assert s["registered"] is False and s["purchased_usd"] == 0.0
    assert s["backfilled"] is False


def test_06_it_cannot_reach_a_registrar_an_owner_or_a_purchase():
    for tok in ("forward_challenger_registry", "prospective_decision",
                "freeze_decision", "declare_policy", "databento_acquisition",
                "import socket", "urlopen(", "api_key("):
        assert tok not in CODE, "the diagnostic reaches %r" % tok


def test_07_retention_is_reported_against_both_baselines():
    """Quoting only the flattering baseline would be a choice. The source
    control separates the delay from the change of price source."""
    body = ED.build(write=False)
    for v in ("next_open", "next_close"):
        r = body["alpha_retention"][v]
        assert r["vs_frozen_same_day"] is not None
        assert r["vs_source_control"] is not None
        assert r["needs_live_opra"] is False


def test_08_the_forward_arithmetic_uses_the_estates_own_floor():
    fe = ED.forward_evidence_arithmetic()
    assert fe["cadence_sessions"] == RS.FROZEN_HORIZON
    assert fe["min_effective_periods"] == ED.MIN_EFFECTIVE_PERIODS
    # 252 sessions / 5 per decision / 12 months
    assert fe["independent_decisions_per_month"] == pytest.approx(4.2, abs=0.01)
    assert fe["months_to_reach_the_floor"] == pytest.approx(
        ED.MIN_EFFECTIVE_PERIODS / fe["independent_decisions_per_month"], rel=1e-9)
    # an unverified price must never harden into a fact
    assert "monthly_usd_observed" not in fe
    assert "usd_to_reach_the_floor" not in fe
    priced = ED.forward_evidence_arithmetic(monthly_usd=199.0)
    assert priced["usd_per_year"] == pytest.approx(2388.0)


# --------------------------------------------------- the substantive economics
def test_09_a_delay_cannot_change_the_decision_count_so_turnover_is_unchanged():
    """The delayed variants must trade the SAME decisions, or a retention ratio
    would be comparing two different strategies."""
    book = ED.price_book()
    if book.get("state") != "OK":
        pytest.skip("the licensed Norgate book is not available here")
    base = ED.variant_path("control_same_day", cost_bps=1.0, book=book)
    for v in ("next_open", "next_close"):
        p = ED.variant_path(v, cost_bps=1.0, book=book)
        assert len(p["net"]) == len(base["net"])
        assert p["dates"] == base["dates"]
    assert ED.COST_TREATMENT["incremental_turnover"] == 0.0


def test_10_the_source_control_isolates_the_price_source_from_the_delay(tmp_path):
    """If the two same-day paths disagreed wildly, every retention number would
    be measuring a data discrepancy instead of an economic effect."""
    book = ED.price_book()
    if book.get("state") != "OK":
        pytest.skip("the licensed Norgate book is not available here")
    frozen = ED.measure_variant("frozen_same_day", book=book)
    ctrl = ED.measure_variant("control_same_day", book=book)
    a, b = ED._ann_net(frozen), ED._ann_net(ctrl)
    assert a is not None and b is not None
    assert np.sign(a) == np.sign(b)
    # the two price sources must agree to well within the effect being measured
    assert abs(b - a) < 0.05, ("the price sources disagree by %.2f%%/yr, which is "
                               "too large to attribute a delay effect" % ((b - a) * 100))
