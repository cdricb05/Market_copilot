r"""Release 76 - COLLECTING DATA AND DECIDING ARE DIFFERENT ACTS.

THE DEFECT THESE TESTS PIN
    ``next_open_runtime.advance_daily`` declares its own contract in its
    docstring::

        probe the vendor  ->  append the session  ->  freeze ONE decision

    The code did not honour it. The append sat BELOW the ``ALREADY_FROZEN`` and
    ``MISSED`` early returns, so the local OPRA panel was advanced only on a
    cycle whose entry boundary was still live.

    On a weekday that is almost never true. The vendor serves session ``t`` at
    ~09:27-09:29 ET on ``t+1``; the decision window for entry ``t+1`` shuts at
    its 09:30 ET open; the forfeiture lands about an hour later. Every cycle
    after that returned ``NEXT_OPEN_MISSED`` before reaching the append, so the
    session the vendor HAD just published was never collected. The panel fell a
    further session behind, and the next boundary was then unreachable for a
    reason that had nothing to do with the vendor.

    That is how ONE miss became NINE (2026-09-15 -> 2026-09-25), and why
    ``append_state`` read ``APPENDED`` exactly once in 400 retained cycles - on
    a Saturday, when no boundary was live to short-circuit it, catching up
    2026-09-21 -> 2026-09-25 in a single batch.

WHAT MAY NOT CHANGE, AND IS ASSERTED HERE
    The ENTRY contract. A window that shut without a decision is still MISSED
    permanently, still refuses every backfill, and still writes no decision.
    Acquisition became unconditional; deciding did not.

NOTHING HERE TOUCHES A LIVE STORE. Every test that needs a root gets a private
one, and no test performs a vendor call: the append is injected.
"""
from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from paper_trader.alpha_agent import alpha_recovery as AR
from paper_trader.alpha_agent.alpha_recovery import next_open_challenger as NOC
from paper_trader.alpha_agent.alpha_recovery import next_open_runtime as NOR
from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
from paper_trader.alpha_agent.r52 import runtime as RT
from paper_trader.api import forward_producer_health as FPH

NOR_SRC = Path(NOR.__file__).read_text(encoding="utf-8")

INFO = "2026-09-14"
ENTRY = "2026-09-15"

BEFORE_WINDOW = "2026-09-14T22:00:00+00:00"     # 18:00 ET on the information session
AFTER_OPEN = "2026-09-15T14:00:00+00:00"        # 10:00 ET - the open has passed


@pytest.fixture()
def root(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    return tmp_path


@pytest.fixture()
def appends(monkeypatch):
    """Record every append the advance attempts; never call the vendor."""
    seen = []

    def _append(information_session, **kw):
        seen.append({"information_session": information_session, **kw})
        return {"state": NOR.APPEND_ALREADY_OWNED,
                "information_session": information_session,
                "detail": "injected by the regression test"}

    monkeypatch.setattr(NOR, "append_information_session", _append)
    return seen


# --------------------------------------------------------------------------- #
# 1-3. THE DEFECT ITSELF
# --------------------------------------------------------------------------- #
def test_01_a_forfeited_boundary_still_collects_its_session(root, appends):
    """THE regression. MISSED is a statement about a DECISION, not about data."""
    res = NOR.advance_daily(now=AFTER_OPEN, information_session=INFO,
                            probe=False, append=True)
    assert res["state"] == NOR.ADV_MISSED
    assert [a["information_session"] for a in appends] == [INFO], (
        "the append was skipped on a forfeited cycle - this is the defect that "
        "cost nine consecutive SPY boundaries")
    assert res["append"]["state"] == NOR.APPEND_ALREADY_OWNED
    assert res["acquisition_precedes_decision_state"] is True


def test_02_an_already_frozen_boundary_still_collects_its_session(root, appends,
                                                                  monkeypatch):
    """The second short-circuit. A frozen decision does not freeze the panel."""
    monkeypatch.setattr(
        NOC, "entry_state",
        lambda entry, **kw: {"entry_state": NOC.AWAITING_MATURITY,
                             "source": {"owned": True}})
    res = NOR.advance_daily(now=AFTER_OPEN, information_session=INFO,
                            probe=False, append=True)
    assert res["state"] == NOR.ADV_ALREADY_FROZEN
    assert [a["information_session"] for a in appends] == [INFO]
    assert res["acquisition_precedes_decision_state"] is True


def test_03_the_source_owned_precheck_is_gone(root, appends, monkeypatch):
    """``source.owned`` required the entry state the append now runs ahead of.

    It was never the authority either: ``append_information_session`` answers
    ALREADY_OWNED itself, before any network call and before any dollar.
    """
    monkeypatch.setattr(
        NOC, "entry_state",
        lambda entry, **kw: {"entry_state": NOC.AWAITING_SOURCE_PUBLICATION,
                             "source": {"owned": True,
                                        "usable_for_a_freeze": False}})
    NOR.advance_daily(now=BEFORE_WINDOW, information_session=INFO,
                      probe=False, append=True)
    assert len(appends) == 1
    assert 'append and not (st.get("source") or {}).get("owned")' not in NOR_SRC


# --------------------------------------------------------------------------- #
# 4-6. WHAT MAY NOT HAVE MOVED
# --------------------------------------------------------------------------- #
def test_04_the_entry_contract_is_unchanged(root, appends):
    """Collecting is now unconditional. Deciding is not."""
    res = NOR.advance_daily(now=AFTER_OPEN, information_session=INFO,
                            probe=False, append=True)
    assert res["state"] == NOR.ADV_MISSED
    assert res["backfill_refused"] is True
    assert res["backfill_allowed"] is False
    assert PD.load_decision(NOC.CHALLENGER_ID, ENTRY) in (None, {})
    assert NOC.EXECUTION_BOUNDARY == "NEXT_ELIGIBLE_SESSION_OPEN"
    assert NOC.ENTRY_MARK_ET == (9, 30)


def test_05_the_safety_boundary_is_unchanged(root, appends):
    res = NOR.advance_daily(now=AFTER_OPEN, information_session=INFO,
                            probe=False, append=True)
    for k in ("creates_orders", "creates_fills", "allocates_capital",
              "promotes_model", "backfill_allowed"):
        assert res[k] is False, k
    assert res["paid_dollars"] == 0.0


def test_06_append_false_still_collects_nothing(root, appends):
    """The switch the tests and a dry run rely on keeps working."""
    NOR.advance_daily(now=AFTER_OPEN, information_session=INFO,
                      probe=False, append=False)
    assert appends == []


# --------------------------------------------------------------------------- #
# 7-9. THE FACT HAS TO SURVIVE EVERY ALLOW-LIST BETWEEN HERE AND THE OPERATOR
# --------------------------------------------------------------------------- #
def test_07_the_ordering_is_attested_in_the_advance_payload():
    """Silence is not proof. Every other field reads the same either way."""
    src = inspect.getsource(NOR.advance_daily)
    append_at = src.index('out["append"] = append_information_session(')
    missed_at = src.index('"state": ADV_MISSED')
    frozen_at = src.index('"state": ADV_ALREADY_FROZEN')
    assert append_at < frozen_at < missed_at, (
        "the append must be reached before any decision-state return")


def test_08_the_runtime_journal_keeps_the_field():
    """R66/R68/R72/R74.2 each lost a fact to this allow-list. Not a sixth time."""
    assert "acquisition_precedes_decision_state" in RT._JOURNAL_STAGE_FIELDS
    rt_src = Path(inspect.getfile(RT)).read_text(encoding="utf-8")
    assert "acquisition_precedes_decision_state=adv.get(" in rt_src


def test_09_producer_health_keeps_the_field():
    fph_src = Path(inspect.getfile(FPH)).read_text(encoding="utf-8")
    assert fph_src.count("acquisition_precedes_decision_state") >= 2, (
        "the heartbeat extraction AND the producer_heartbeat projection must "
        "both carry it, or the operator-facing answer is None while the fact "
        "sits on disk")
