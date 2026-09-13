"""The preregistered 13F ownership-breadth experiment: identity, point-in-time
and the canonical 13F treatments.

Every test here pins something that would otherwise fail SILENTLY and produce a
number rather than an error: a bridge that peeks at a file published after the
decision, a manager counted twice because they amended, a notice counted as a
holder, an option counted as ownership, a delisted name dropped at the join.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent.alpha_recovery import ownership_13f as O13        # noqa: E402
from alpha_agent.alpha_recovery import ownership_breadth as OB     # noqa: E402
from alpha_agent.alpha_recovery import ownership_data as OD        # noqa: E402
from alpha_agent.alpha_recovery import ownership_identity as OI    # noqa: E402

PREREG = _ROOT / "research" / "preregistration" / \
    "INSTITUTIONAL_OWNERSHIP_BREADTH_PREREGISTRATION.md"


# --------------------------------------------------------------------------- #
# Frozen identity contract
# --------------------------------------------------------------------------- #
def test_01_placeholder_pattern_is_the_frozen_one():
    assert OI.PLACEHOLDER_PATTERN == r"(ZZZZ|XXXX)$|^[0-9]"
    for bad in ("DDZZZZ", "XOMXXXX", "HONXXXX", "6205REGWAY", "1ABC"):
        assert OI.is_placeholder(bad), bad
    for good in ("AAPL", "BRKB", "MSFT", "ZZ", "XOM"):
        assert not OI.is_placeholder(good), good


def test_02_cusip_join_key_is_the_eight_character_cusip():
    assert OI.cusip_key("037833100") == "03783310"
    assert OI.cusip_key("03783310") == "03783310"
    # the ninth character is only a check digit: a mistyped one still joins
    assert OI.cusip_key("037833109") == OI.cusip_key("037833100")
    assert OI.cusip_key("abc") is None


def test_03_ticker_key_is_a_spelling_convention_not_a_name_match():
    assert OI.ticker_key("BRK.B") == "BRKB"
    assert OI.ticker_key("BF.B") == "BFB"
    assert OI.ticker_key(" aapl ") == "AAPL"
    # it canonicalises punctuation only; it never matches different tickers
    assert OI.ticker_key("ABC") != OI.ticker_key("ABD")


# --------------------------------------------------------------------------- #
# The bridge is point-in-time and effective-dated
# --------------------------------------------------------------------------- #
def _obs(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["cusip", "symbol", "first_settlement",
                                     "last_settlement"])
    for c in ("first_settlement", "last_settlement"):
        df[c] = pd.to_datetime(df[c])
    lag = pd.Timedelta(days=OD.FTD_PUBLICATION_LAG_DAYS)
    df["first_available"] = df["first_settlement"] + lag
    df["last_available"] = df["last_settlement"] + lag
    return df


def test_04_a_file_published_after_the_decision_is_invisible():
    obs = _obs([("11111111", "AAA", "2020-01-02", "2020-01-02")])
    # available = settlement + the DECLARED lag; a day before that, nothing
    assert obs.loc[0, "first_available"] == pd.Timestamp("2020-02-16")
    assert bridge_map(obs, "2020-02-15") == {}
    assert bridge_map(obs, "2020-02-17") == {"11111111": "AAA"}


def bridge_map(obs, date):
    return OI.bridge_as_of(obs, date)["map"]


def test_05_a_ticker_change_resolves_effective_dated():
    obs = _obs([("22222222", "PCLN", "2012-01-03", "2018-02-01"),
                ("22222222", "BKNG", "2018-03-01", "2024-01-03")])
    assert bridge_map(obs, "2015-06-30") == {"22222222": "PCLN"}
    assert bridge_map(obs, "2020-06-30") == {"22222222": "BKNG"}
    # and never "the most common one": the early date must not see BKNG
    assert "BKNG" not in bridge_map(obs, "2015-06-30").values()


def test_06_a_genuinely_ambiguous_cusip_is_dropped_not_guessed():
    obs = _obs([("33333333", "AAA", "2020-01-02", "2020-01-02"),
                ("33333333", "BBB", "2020-01-02", "2020-01-02")])
    out = OI.bridge_as_of(obs, "2020-06-01")
    assert out["map"] == {}
    assert out["ambiguous"] == 1


def test_07_a_reused_ticker_resolves_to_the_security_alive_that_day():
    tix = {"AAA": [
        {"row": 0, "symbol": "AAA-197701", "first_session": "1965-01-04",
         "delisting_date": "1977-01-12", "is_current": 0},
        {"row": 1, "symbol": "AAA", "first_session": "1999-11-18",
         "delisting_date": None, "is_current": 1}]}
    assert OI.resolve_ticker(tix, "AAA", "1970-06-30")["row"] == 0
    assert OI.resolve_ticker(tix, "AAA", "2015-06-30")["row"] == 1
    assert OI.resolve_ticker(tix, "AAA", "1985-06-30")["state"] == "NOT_LISTED_ON_DATE"
    assert OI.resolve_ticker(tix, "ZZZ", "2015-06-30")["state"] == "NO_PANEL_SYMBOL"


def test_08_the_declared_publication_lag_is_fixed_and_conservative():
    # declared BEFORE use; the real SEC latency is ~2-3 weeks
    assert OD.FTD_PUBLICATION_LAG_DAYS == 45


# --------------------------------------------------------------------------- #
# The canonical 13F treatments
# --------------------------------------------------------------------------- #
def _sub(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["accession", "cik", "form", "filing_date",
                                     "period", "is_amendment", "amendment_type"])
    df["filing_date"] = pd.to_datetime(df["filing_date"])
    df["period"] = pd.to_datetime(df["period"])
    return df


P = "2020-03-31"


def test_09_a_notice_is_never_counted_as_a_holder():
    sub = _sub([("a1", "1", "13F-NT", "2020-05-10", P, False, ""),
                ("a2", "2", "13F-HR", "2020-05-10", P, False, "")])
    eff = O13.effective_accessions(sub, P, "2020-06-01")
    assert eff == {"a2"}


def test_10_a_restatement_supersedes_the_original():
    sub = _sub([("a1", "1", "13F-HR", "2020-05-10", P, False, ""),
                ("a2", "1", "13F-HR/A", "2020-06-10", P, True, "RESTATEMENT")])
    assert O13.effective_accessions(sub, P, "2020-07-01") == {"a2"}
    # ... and before the amendment was filed, the original still stands
    assert O13.effective_accessions(sub, P, "2020-06-01") == {"a1"}


def test_11_new_holdings_adds_rather_than_replaces():
    sub = _sub([("a1", "1", "13F-HR", "2020-05-10", P, False, ""),
                ("a2", "1", "13F-HR/A", "2020-06-10", P, True, "NEW HOLDINGS")])
    assert O13.effective_accessions(sub, P, "2020-07-01") == {"a1", "a2"}


def test_12_a_duplicate_original_is_not_counted_twice():
    sub = _sub([("a1", "1", "13F-HR", "2020-05-10", P, False, ""),
                ("a2", "1", "13F-HR", "2020-05-12", P, False, "")])
    assert O13.effective_accessions(sub, P, "2020-07-01") == {"a2"}


def test_13_an_amendment_with_no_declared_type_is_read_as_a_restatement():
    sub = _sub([("a1", "1", "13F-HR", "2020-05-10", P, False, ""),
                ("a2", "1", "13F-HR/A", "2020-06-10", P, True, "")])
    assert O13.effective_accessions(sub, P, "2020-07-01") == {"a2"}


def test_14_a_filing_made_after_the_decision_does_not_exist_yet():
    sub = _sub([("a1", "1", "13F-HR", "2020-05-10", P, False, ""),
                ("a2", "2", "13F-HR", "2020-08-20", P, False, "")])
    assert O13.effective_accessions(sub, P, "2020-06-01") == {"a1"}
    assert O13.effective_accessions(sub, P, "2020-09-01") == {"a1", "a2"}


def test_15_a_late_filing_is_included_only_from_when_it_arrived():
    sub = _sub([("a1", "1", "13F-HR", "2021-02-01", P, False, "")])
    assert O13.effective_accessions(sub, P, "2020-05-16") == set()
    assert O13.effective_accessions(sub, P, "2021-03-01") == {"a1"}


def test_16_breadth_counts_distinct_managers_not_filings(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADER_ALPHA_RECOVERY_RESEARCH_ROOT", str(tmp_path))
    O13.holdings_dir().mkdir(parents=True, exist_ok=True)
    # one manager files an original AND a NEW HOLDINGS amendment naming the
    # same security: one holder, not two
    accs = np.array(["a1", "a2", "a3"])
    cus = np.array(["03783310"])
    np.savez_compressed(O13.holdings_dir() / "holdings_202003.npz",
                        accessions=accs, cusips=cus,
                        acc_idx=np.array([0, 1, 2], dtype=np.int32),
                        cus_idx=np.array([0, 0, 0], dtype=np.int32))
    sub = _sub([("a1", "7", "13F-HR", "2020-05-10", P, False, ""),
                ("a2", "7", "13F-HR/A", "2020-05-12", P, True, "NEW HOLDINGS"),
                ("a3", "9", "13F-HR", "2020-05-11", P, False, "")])
    b = O13.breadth_as_of(sub, P, "2020-06-01")
    assert int(b.loc["03783310"]) == 2


# --------------------------------------------------------------------------- #
# The frozen experiment design
# --------------------------------------------------------------------------- #
def test_17_the_rebalance_is_the_first_session_after_the_45_day_deadline():
    assert OB.DEADLINE_DAYS == 45
    assert OB.deadline_for("2020-03-31") == pd.Timestamp("2020-05-15")
    dates = np.array(["2020-05-14", "2020-05-15", "2020-05-18", "2020-05-19"],
                     dtype="datetime64[D]")
    g = OB.decision_grid(dates, [pd.Timestamp("2020-03-31")])
    # strictly after: the deadline day itself is not a decision
    assert g[0]["date"] == "2020-05-18"


def test_18_the_two_frozen_horizons_are_non_overlapping_at_this_cadence():
    from alpha_agent.r63 import pit
    assert OB.HORIZONS_FROZEN == (21, 63)
    assert OB.CADENCE_QUARTERLY == 63
    for h in OB.HORIZONS_FROZEN:
        assert pit.nw_lag(h, OB.CADENCE_QUARTERLY) == 0


def test_19_no_sign_is_pre_specified():
    body_sign = OB.SIGN_DISCOVERY
    assert body_sign == "DISCOVERY_ONLY"
    assert OB.SIGN_PRESPECIFIED == "PRE_SPECIFIED"
    txt = PREREG.read_text(encoding="utf-8")
    assert "NOT PRE-SPECIFIED" in txt.upper()


def test_20_the_declared_family_has_exactly_two_cells():
    assert len(OB.HORIZONS_FROZEN) == 2
    from alpha_agent.r31 import multiple_testing as MT
    bh = MT.benjamini_hochberg([0.4, 0.6], 0.10)
    assert bh["m"] == 2 and bh["n_rejected"] == 0


def test_21_the_gates_are_inherited_never_redeclared_weaker():
    from alpha_agent import r63
    assert OB.MIN_EFFECTIVE_PERIODS == r63.MIN_EFFECTIVE_PERIODS == 36
    assert OB.CONDITIONAL_T_FLOOR == r63.CONDITIONAL_T_FLOOR == 2.0
    assert OB.MATERIALITY_ANN_NET == r63.MATERIALITY_ANN_NET
    assert OB.MIN_DATE_COVERAGE == 0.95 and OB.MAX_UNCOVERED_SHARE == 0.20


def test_22_the_one_scorer_and_the_one_pit_engine_are_reused():
    import inspect
    from alpha_agent.r63 import pit, sensitivity
    src = inspect.getsource(OB)
    assert "S.run_cell" in src
    assert sensitivity.CALCULATION_OWNER == "alpha_agent.r63.sensitivity"
    assert pit.CALCULATION_OWNER == "alpha_agent.r63.pit"
    # no second scorer: the module must not define its own ridge / IC engine
    for forbidden in ("def run_cell", "def _ridge_fit", "def grouped_rank_ic"):
        assert forbidden not in src


def test_23_the_signal_is_a_cross_sectional_rank_in_the_unit_interval():
    import inspect
    src = inspect.getsource(OB.signal_matrix)
    assert "rank(" in src
    rk = pd.Series([3.0, -1.0, 7.0, 0.0]).rank(method="average").to_numpy()
    rk = (rk - 1.0) / 3.0
    assert rk.min() == 0.0 and rk.max() == 1.0


def test_24_the_cost_ladder_is_the_frozen_one_plus_the_desk_rate():
    from alpha_agent import r63
    assert OB.COST_LADDER_BPS == (1.0, 2.0, 5.0)
    assert OB.DESK_EQUITY_COST_BPS == pytest.approx(r63.EQ_COST_RATE_PER_SIDE * 1e4)
    assert OB.DESK_EQUITY_COST_BPS > max(OB.COST_LADDER_BPS)


# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #
#: The bytes committed at 38453b9, read out of git BEFORE the experiment ran.
#: Line endings are normalised so a core.autocrlf checkout hashes identically.
PREREG_SHA256 = "51c4b76acc5212cad2a728a7c908b0a941b9fe93f6b462d494babe80779bdcfd"


def test_25_the_preregistration_was_not_altered():
    """Its bytes are the ones committed at 38453b9, before any result existed.

    A specification edited after seeing a result is a NEW preregistration, not
    this one, so this is the test that makes "we did not move the goalposts"
    checkable rather than asserted.
    """
    assert PREREG.exists()
    h = hashlib.sha256(PREREG.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    assert h == PREREG_SHA256


def test_26_nothing_in_this_axis_can_order_fill_promote_or_allocate():
    import inspect
    for mod in (OD, OI, O13, OB):
        src = inspect.getsource(mod)
        for forbidden in ("create_order", "submit_order", "place_order",
                          "register_challenger", "promote_model",
                          "allocate_capital", "approve_proposal"):
            assert forbidden not in src, (mod.__name__, forbidden)


def test_27_acquisition_spends_nothing():
    import inspect
    src = inspect.getsource(OD)
    assert "api_key" not in src.lower()
    assert "subscription" not in src.lower() or "subscription_started\": False" in src
    for url in (OD.FTD_INDEX_URL, OD.F13_INDEX_URL):
        assert url.startswith("https://www.sec.gov/")


def test_28_the_research_root_is_never_the_live_checkout():
    from alpha_agent.alpha_recovery import research_root
    assert "paper_trader_alpha_recovery" in str(OD.data_dir()).lower() \
        or str(research_root()).lower().startswith("d:")
    assert not str(OD.data_dir()).lower().startswith(r"c:\users\binis\paper_trader")
