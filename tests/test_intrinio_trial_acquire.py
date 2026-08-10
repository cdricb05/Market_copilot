"""Targeted tests for scripts/intrinio_trial_acquire.py pure helpers.

The acquisition CLI composes the RELEASED collector machinery (covered by
test_intrinio_trial_readiness.py); these tests pin the campaign-critical pure
logic: Norgate delisted-ticker normalization, Windows-safe filenames, the
near-term estimate window, and the vintage-dedupe grain.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO.parent))

_spec = importlib.util.spec_from_file_location(
    "intrinio_trial_acquire", _REPO / "scripts" / "intrinio_trial_acquire.py")
acq = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(acq)


def test_base_ticker_strips_delisted_suffix_only():
    assert acq.base_ticker("WFM-201708") == "WFM"
    assert acq.base_ticker("TWTR-202210") == "TWTR"
    assert acq.base_ticker("AAPL") == "AAPL"
    assert acq.base_ticker("BRK.B") == "BRK.B"
    # a non -YYYYMM dash suffix is NOT stripped
    assert acq.base_ticker("ABC-DEF") == "ABC-DEF"
    assert acq.base_ticker("") == ""


def test_safe_name_windows_safe():
    assert acq._safe_name("ngid:124589") == "ngid_124589"
    assert ":" not in acq._safe_name("a:b/c\\d")


def test_estimate_window_near_term_forward():
    w = acq._estimate_window("2026-08-10")
    assert w["as_of"] == "2026-08-10"
    # current quarter start minus ~one quarter -> covers the running quarter
    assert w["est_start"] <= "2026-04-01"
    assert w["est_end"] == "2028-12-31"


def test_fundamental_tags_are_bounded_registered_set():
    # exactly the concepts the pre-registered family + restatement audit use
    assert set(acq.FUNDAMENTAL_TAGS) == {
        "totalrevenue", "totalgrossprofit", "netincome", "totalassets",
        "totalequity", "netcashfromoperatingactivities",
        "purchaseofplantpropertyandequipment"}


def test_feed_subset_flags_cover_exactly_the_known_feeds():
    # Stage 13B: per-feed re-capture must never invent a feed name
    assert set(acq.ESTIMATE_UNIVERSE_FEEDS) == {"eps", "sales"}
    assert acq.ESTIMATE_UNIVERSE_FEEDS["eps"][0] == "eps_estimates_universe"
    assert acq.ESTIMATE_UNIVERSE_FEEDS["sales"][0] == "sales_estimates_universe"
    import inspect
    sig = inspect.signature(acq.run_zacks_snapshot)
    assert tuple(sig.parameters["feeds"].default) == tuple(acq.ZACKS_FEEDS)


def test_zacks_feeds_declare_pit_honesty():
    # estimates dedupe vintages (no vintage timestamp exists); surprises do not
    assert acq.ZACKS_FEEDS["eps_estimates"][2] is True
    assert acq.ZACKS_FEEDS["sales_estimates"][2] is True
    assert acq.ZACKS_FEEDS["eps_surprises"][2] is False
    assert acq.ZACKS_FEEDS["sales_surprises"][2] is False
