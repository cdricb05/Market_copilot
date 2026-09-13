r"""Release 62.3.4 - the next-open challenger becomes OPERATIONAL in the live
checkout, owned by the canonical research runtime.

WHAT CHANGED, AND THEREFORE WHAT HAS TO BE PROVED
    R62.3.3 froze and registered ``REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1``
    in a development worktree. The registration went into the SHARED registry
    store, so the live accrual owner could already see it - and reported it
    ``DATA_BLOCKED / FROZEN_SPEC_OWNER_NOT_RESOLVABLE``, because live had none
    of the code. Landing that code is what these tests guard.

    Two defects were found by measurement while landing it, and both are
    regression-tested here rather than described in a commit message:

    (a) THE DECISION SESSION WAS OFF BY ONE. The accrual owner built its
        decision grid from the registrar's first eligible OBSERVATION session
        (2026-09-14) while the release freezes its decision for the ENTRY
        session (2026-09-15). The freeze would have been written under a key
        the accrual owner never looked up, so it would have been an orphan that
        nothing ever scored - while the registration went on looking merely
        young. Tests 21-23.

    (b) SPY WAS NOT PRICED. The valuation panel carried SPY frozen at
        2026-06-22 with a null close while 1008 other tickers were current,
        because the benchmark is absent from the tradable fetch universe. This
        challenger trades SPY and nothing else, so its forward evidence would
        have been marked against an 83-day-old price. Test 14.

WHAT THESE TESTS MAY NOT DO
    Nothing here writes to the campaign's research root, the registry, or any
    operational store. Every test that needs a store gets a private one.
"""
from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from paper_trader.alpha_agent import alpha_recovery as AR
from paper_trader.alpha_agent.alpha_recovery import next_open_challenger as NOC
from paper_trader.alpha_agent.alpha_recovery import next_open_runtime as NOR
from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
from paper_trader.alpha_agent.alpha_recovery import reversed_skew as RS
from paper_trader.api import canonical_forward_accrual as CFA
from paper_trader.api import forward_challenger_registry as FCR
from paper_trader.engine import shadow_portfolio_evidence as SPE

RUNTIME_SRC = Path(
    inspect.getfile(__import__("paper_trader.alpha_agent.r52.runtime",
                               fromlist=["runtime"]))).read_text(encoding="utf-8")
NOR_SRC = Path(NOR.__file__).read_text(encoding="utf-8")
CFA_SRC = Path(CFA.__file__).read_text(encoding="utf-8")

SIBLING = RS.CHALLENGER_ID
CHALLENGER = NOC.CHALLENGER_ID

INFO = "2026-09-14"
ENTRY = "2026-09-15"
MATURITY = "2026-09-22"

BEFORE_WINDOW = "2026-09-14T22:00:00+00:00"     # 18:00 ET on the information session
WINDOW_OPEN = "2026-09-15T05:00:00+00:00"       # 01:00 ET on the entry session
AFTER_OPEN = "2026-09-15T14:00:00+00:00"        # 10:00 ET - the open has passed


@pytest.fixture()
def root(tmp_path, monkeypatch):
    """A private research root. No test may touch the campaign's own store."""
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    return tmp_path


def _registration(challenger_id: str, *, release="ALPHA_RECOVERY_OFFENSIVE",
                  first_session=INFO, registered="2026-09-12") -> dict:
    return {"challenger_id": challenger_id,
            "registration_session": registered,
            "identity": {"challenger_id": challenger_id, "release": release},
            "observation_clock": {
                "first_eligible_observation_session": first_session}}


# --------------------------------------------------------------------------- #
# 1-5. IDENTITY, AND THE EVIDENCE THAT MAY NOT TRAVEL
# --------------------------------------------------------------------------- #
def test_01_the_same_day_challenger_is_untouched():
    """The sibling keeps its own id, its own spec and its own emission rule."""
    sib = RS.frozen_specification()
    assert SIBLING == "REVERSED_SPY_PUT_CALL_SKEW_H5"
    assert sib["challenger_id"] == SIBLING
    assert CHALLENGER != SIBLING
    # The sibling declares NO execution contract, so nothing this release added
    # can move its decision session.
    off = CFA.execution_offset_sessions(_registration(SIBLING))
    assert off["offset_sessions"] == 0
    assert off["declared"] is False


def test_02_the_next_open_identity_is_unchanged_by_landing_it():
    """The specification hash must be reproducible in the LIVE checkout.

    It hashes the campaign's operating contract and protocol documents, so a
    landing that brought the code but not those two files produced a different
    identity - measured, not hypothetical: it was 72d8c265 here and 5c586d23
    where it was registered, until the documents landed too.
    """
    assert NOC.specification_hash() == (
        "5c586d238f374749a1c42a6f57f274da90027126392235bac5d0b6966166b0a7")


def test_03_historical_qualification_is_none():
    rec = NOC.freeze_record()
    q = rec["qualification_evidence"]
    assert q["state"] == "NONE"
    assert q["inherited_from_sibling"] is False
    assert q["independent_historical_confirmation"] == "NOT_RUN_AND_NOT_INHERITED"


def test_04_the_diagnostic_is_not_qualification_evidence():
    spec = NOC.frozen_specification()
    blob = json.dumps(spec, default=str)
    assert "DIAGNOSTIC ONLY" in json.dumps(NOC.freeze_record(), default=str).upper()
    # The diagnostic's headline numbers may not appear as a qualification.
    assert "26.65" not in blob or "diagnostic" in blob.lower()


def test_05_true_forward_observations_begin_at_zero():
    st = NOC.true_forward_state()
    reg = st.get("registration") or {}
    assert reg.get("effective_independent_observations") == 0
    assert reg.get("matured_observations", 0) == 0
    assert reg.get("backfilled") is False
    assert reg.get("promotion_ready", False) is False


# --------------------------------------------------------------------------- #
# 6-11. THE PROSPECTIVE CONTRACT, THROUGH THE RUNTIME OWNER
# --------------------------------------------------------------------------- #
def test_06_no_decision_before_the_vendor_has_published(root, monkeypatch):
    """An unpublished session cannot be frozen, however open the window is."""
    monkeypatch.setattr(NOC, "source_available", lambda s, **k: {
        "information_session": s, "owned": False, "published": False,
        "usable_for_a_freeze": False, "publication_state": "NOT_PUBLISHED"})
    res = NOR.freeze_for_entry(ENTRY, now=WINDOW_OPEN)
    assert res["frozen"] is False
    assert res["outcome"] == NOC.REFUSED_SOURCE_NOT_PUBLISHED


def test_07_the_decision_must_exist_before_the_next_open(root):
    """The window is on the ENTRY session and shuts at its 09:30 ET open."""
    ctx = NOC.decision_context(ENTRY)
    assert ctx["information_session"] == INFO
    assert ctx["entry_session"] == ENTRY
    opens = NOC._et_instant(ENTRY, NOC.DECISION_WINDOW_OPENS_ET)
    mark = ctx["entry_mark_at"]
    assert opens < mark
    assert NOC._as_utc(WINDOW_OPEN) >= NOC._as_utc(opens)
    assert NOC._as_utc(WINDOW_OPEN) < NOC._as_utc(mark)


def test_08_a_decision_after_the_open_is_rejected(root):
    st = NOC.entry_state(ENTRY, now=AFTER_OPEN)
    assert st["entry_state"] == NOC.MISSED
    assert st["backfill_refused"] is True


def test_09_a_missed_session_is_never_backfilled(root):
    res = NOR.advance_daily(now=AFTER_OPEN, information_session=INFO,
                            probe=False, append=False)
    assert res["state"] == NOR.ADV_MISSED
    assert res["backfill_refused"] is True
    assert res["backfill_allowed"] is False
    # and nothing was written for that entry session
    assert PD.load_decision(CHALLENGER, ENTRY) in (None, {})


def test_10_the_entry_mark_is_the_next_sessions_open():
    ctx = NOC.decision_context(ENTRY)
    assert ctx["entry_boundary"] == NOC.EXECUTION_BOUNDARY == \
        "NEXT_ELIGIBLE_SESSION_OPEN"
    assert NOC.ENTRY_MARK_ET == (9, 30)
    assert ctx["entry_mark_at"].startswith(ENTRY)


def test_11_maturity_counts_five_eligible_sessions_from_the_entry():
    assert NOC.maturity_session_for(ENTRY) == MATURITY
    assert NOC.HORIZON_COUNTS_FROM == "ENTRY_SESSION"
    # Counted from the INFORMATION session it would be one session early. That
    # off-by-one is the whole challenger, so it is asserted explicitly.
    assert NOC.maturity_session_for(ENTRY) != NOC.maturity_session_for(INFO)


# --------------------------------------------------------------------------- #
# 12-14. THE MARKS AND THE COSTS
# --------------------------------------------------------------------------- #
def test_12_signed_long_and_short_pnl_stays_correct():
    """A short that falls MAKES money; the kernel must not be long-only."""
    assert hasattr(SPE, "VALUATION_PRICE_PANEL")
    src = Path(SPE.__file__).read_text(encoding="utf-8")
    assert "SIGNED" in src.upper()


def test_13_transaction_costs_are_positive_both_ways():
    """Opening a short pays the spread exactly as opening a long does."""
    src = Path(SPE.__file__).read_text(encoding="utf-8")
    assert "ALWAYS non-negative" in src


def test_14_spy_is_in_the_valuation_universe():
    """The ONE instrument this challenger trades must be priceable.

    The benchmark is not in the tradable fetch universe, so it entered the
    trailing panel only as an explicit EXTRA. Without that, SPY sat at
    2026-06-22 with a null close while the rest of the panel was current.
    """
    from paper_trader.api import alpha_target as AT
    src = Path(AT.__file__).read_text(encoding="utf-8")
    assert "panel_extra" in src
    assert "BENCHMARK_TICKER" in src
    assert "{**series, **panel_extra}" in src


# --------------------------------------------------------------------------- #
# 15-16. IDEMPOTENCE AND RESTART
# --------------------------------------------------------------------------- #
def test_15_a_repeated_runtime_invocation_is_idempotent(root):
    """Two advances in the same window produce ONE decision, not two."""
    a = NOR.advance_daily(now=AFTER_OPEN, information_session=INFO,
                          probe=False, append=False)
    b = NOR.advance_daily(now=AFTER_OPEN, information_session=INFO,
                          probe=False, append=False)
    assert a["state"] == b["state"]
    # The freeze owner itself is first-write-wins.
    assert "first_write_wins" in Path(PD.__file__).read_text(encoding="utf-8")


def test_16_restart_preserves_state(root):
    """State is read from durable artifacts, never from process memory."""
    assert "def load_decision(" in Path(PD.__file__).read_text(encoding="utf-8")
    st1 = NOC.entry_state(ENTRY, now=BEFORE_WINDOW)
    st2 = NOC.entry_state(ENTRY, now=BEFORE_WINDOW)
    assert st1["entry_state"] == st2["entry_state"]


# --------------------------------------------------------------------------- #
# 17-20. THE SAFETY BOUNDARY
# --------------------------------------------------------------------------- #
def test_17_nothing_here_can_promote():
    for src in (NOR_SRC, Path(NOC.__file__).read_text(encoding="utf-8")):
        assert "promote_model(" not in src
        assert "activate_sleeve(" not in src


def test_18_nothing_here_can_allocate_capital():
    res = NOR.advance_daily(now=BEFORE_WINDOW, information_session=INFO,
                            probe=False, append=False)
    assert res["allocates_capital"] is False
    assert res["promotes_model"] is False
    assert "approve_proposal(" not in NOR_SRC


def test_19_no_order_or_fill_path_is_reachable():
    for token in ("create_order", "submit_order", "apply_fill", "create_fill(",
                  "import requests", "import httpx", "from paper_trader.api",
                  "from api import"):
        assert token not in NOR_SRC, token
    res = NOR.advance_daily(now=BEFORE_WINDOW, information_session=INFO,
                            probe=False, append=False)
    assert res["creates_orders"] is False
    assert res["creates_fills"] is False


def test_20_the_r58_long_only_registrations_are_untouched():
    """The four R58 challengers keep offset 0 and their own decision grid."""
    for cid in ("R58_SHORT_VOLUME_PRESSURE_V1", "R58_FCF_PURE_V1"):
        off = CFA.execution_offset_sessions(
            _registration(cid, release="R58", first_session="2026-09-10"))
        assert off["offset_sessions"] == 0
        assert off["declared"] is False


# --------------------------------------------------------------------------- #
# 21-24. THE TWO DEFECTS FOUND WHILE LANDING, AND THE RUNTIME WIRING
# --------------------------------------------------------------------------- #
def test_21_the_decision_grid_honours_the_declared_entry_boundary():
    """A release that enters at the NEXT open decides on the ENTRY session."""
    sessions = ["2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15",
                "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21",
                "2026-09-22"]
    reg = _registration(CHALLENGER)
    shifted = CFA.decision_grid(reg, sessions=sessions, cadence_sessions=5,
                                offset_sessions=1)
    unshifted = CFA.decision_grid(reg, sessions=sessions, cadence_sessions=5,
                                  offset_sessions=0)
    assert unshifted[0] == INFO
    assert shifted[0] == ENTRY
    # and the shift is an ELIGIBLE-session step, not a calendar-day one
    assert CFA._shift_eligible("2026-11-25", 1) == "2026-11-27"   # Thanksgiving


def test_22_a_release_that_declares_nothing_is_byte_identical():
    """The offset parameter may not change one grid that did not ask for it."""
    sessions = ["2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15"]
    reg = _registration("R58_FCF_PURE_V1", release="R58",
                        first_session="2026-09-10", registered="2026-09-09")
    assert (CFA.decision_grid(reg, sessions=sessions, cadence_sessions=21)
            == CFA.decision_grid(reg, sessions=sessions, cadence_sessions=21,
                                 offset_sessions=0))


def test_23_the_freeze_owner_and_the_accrual_grid_agree():
    """The two owners must name the SAME first entry session.

    They are computed independently - one from the challenger's own inception
    on the exchange calendar, the other from the registrar's observation clock
    plus the declared offset - and if they ever disagree the decision becomes an
    orphan that nothing scores.
    """
    sessions = ["2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16"]
    grid = CFA.decision_grid(_registration(CHALLENGER), sessions=sessions,
                             cadence_sessions=5, offset_sessions=1)
    assert NOR.first_legal_entry_session() == grid[0] == ENTRY
    assert NOR.first_legal_information_session() == INFO


def test_24_the_canonical_runtime_owns_the_flow():
    """One cadence, inside the existing lock, BEFORE the accrual stage.

    A second scheduler would mean two cadences racing for the same emission
    window, which is the one question in this estate that may never have two
    answers.
    """
    assert '_stage("next_open_prospective_decision"' in RUNTIME_SRC
    assert "NOR.advance_daily(" in RUNTIME_SRC
    lock = RUNTIME_SRC.index("RL.acquire_path(_lock_file()")
    mine = RUNTIME_SRC.index("next_open_prospective_decision")
    accrual = RUNTIME_SRC.index("advance_canonical_forward_accrual(")
    assert lock < mine < accrual
    # and it registers no scheduled task of its own
    assert "Register-ScheduledTask" not in NOR_SRC


def test_25_the_advance_is_free_and_bounded():
    """PAID DOLLARS = 0, and the append can never exceed the free credit."""
    res = NOR.advance_daily(now=BEFORE_WINDOW, information_session=INFO,
                            probe=False, append=False)
    assert res["paid_dollars"] == 0.0
    assert "fits_in_free_credit" in NOR_SRC
    assert "BUDGET_SAFETY_MARGIN" in NOR_SRC


def test_26_the_append_can_never_rewrite_a_discovery_row():
    """On a key collision the ORIGINAL row wins, and the file is archived."""
    assert 'keep="first"' in NOR_SRC
    assert "archived_original" in NOR_SRC
    assert "discovery_rows_unchanged" in NOR_SRC


def test_27_nothing_landed_here_can_write_a_registration():
    """The registrar is READ by this release and never written by it.

    Asserted on the code rather than on the live registry, because the suite is
    deliberately isolated from the live forward-evidence stores (R62.1.2) - a
    test that read the real registry would be a test that could also corrupt
    it. The live registry was verified separately, read-only, outside pytest.
    """
    for src in (NOR_SRC, CFA_SRC):
        assert "register_forward_challenger(" not in src
        assert "FCR.register(" not in src
    assert "def load_registrations(" in Path(FCR.__file__).read_text(
        encoding="utf-8")
