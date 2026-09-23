r"""Release 66 - the SPY skew evidence defect: the vendor published, and the
LOCAL COLLECTION step silently bought nothing.

WHAT WAS ACTUALLY WRONG
    ``options_acquisition.plan()`` centres each expiry's strike band on a median
    of ``underlying_levels()``. That anchor read a FROZEN one-off ES futures
    research panel which ends 2026-09-10 and which nothing refreshes - correct
    for a frozen panel, fatal as a band anchor. From 2026-09-12 the near leg the
    frozen rule needed (2026-10-16) found no owned level in either of ``plan``'s
    two windows, so it was dropped by a bare ``continue`` that recorded nothing.
    Every planned request was then discarded by the near-leg filter, leaving
    ``n_requests = 0``. The append priced $0.00, passed its own budget gate
    trivially, downloaded nothing, built no rows, and the challenger reported
    ``AWAITING_SOURCE_PUBLICATION`` - a VENDOR-shaped word for a LOCAL bug. The
    publication probe had recorded PUBLISHED for every one of those sessions.

    It stayed invisible for eight days because the runtime journal kept exactly
    three fields per stage - ``stage``, ``state``, ``duration_ms`` - so six-plus
    cycles a day recorded the word DATA_BLOCKED and never once the reason.

WHAT THESE TESTS GUARD
    1-3   the anchor is EXTENDED, never replaced: every date the frozen panel
          covers keeps exactly the level it had, so every band centre ever used
          - and therefore every frozen surface row and frozen decision - is
          reproduced byte-identically. Only dates beyond the frozen panel, which
          had no anchor at all and could only be dropped, come from the owned
          SPY close series.
    4-5   an expiry that cannot be priced is RECORDED, not silently dropped.
    6-8   an append whose NEEDED near leg was not priced REFUSES by name
          instead of buying nothing and blaming the vendor.
    9-11  ``blocked_on`` resolves its disjunction: the probe already knows
          whether the vendor published, so the message names VENDOR or
          LOCAL_COLLECTION rather than offering both.
    12-14 the runtime journal carries the reason and the owner.
    15-16 FORFEITURE SEMANTICS ARE UNCHANGED. A missing frozen decision is not
          a forfeiture and this release does not make it one.

WHAT THESE TESTS MAY NOT DO
    Nothing here reaches a network, spends a cent, writes to the campaign's
    research root, or touches a frozen artifact. Every fixture is private.
"""
from __future__ import annotations

import inspect
from pathlib import Path

import numpy as np
import pytest

from paper_trader.alpha_agent.alpha_recovery import next_open_challenger as NOC
from paper_trader.alpha_agent.alpha_recovery import next_open_runtime as NOR
from paper_trader.alpha_agent.alpha_recovery import options_acquisition as OA
from paper_trader.alpha_agent.r52 import runtime as R52
from paper_trader.api import canonical_forward_accrual as CFA

OA_SRC = Path(OA.__file__).read_text(encoding="utf-8")
NOR_SRC = Path(NOR.__file__).read_text(encoding="utf-8")
NOC_SRC = Path(NOC.__file__).read_text(encoding="utf-8")
R52_SRC = Path(R52.__file__).read_text(encoding="utf-8")
CFA_SRC = Path(CFA.__file__).read_text(encoding="utf-8")

#: A frozen panel that stops, exactly like the real one.
FROZEN_DATES = ["2026-09-08", "2026-09-09", "2026-09-10"]
FROZEN_ES = np.array([7700.0, 7710.0, 7718.0])          # /10 -> ~770 SPY
OWNED_CLOSES = {
    "2026-09-09": 111.0,        # overlaps the frozen panel - must be IGNORED
    "2026-09-10": 222.0,        # overlaps the frozen panel - must be IGNORED
    "2026-09-11": 764.17,
    "2026-09-14": 760.82,
    "2026-09-21": 773.52,
}


def _fake_panel():
    close = np.zeros((len(FROZEN_DATES), 1, 1))
    close[:, 0, 0] = FROZEN_ES
    return {"dates": list(FROZEN_DATES), "instruments": ["ES"], "close": close}


@pytest.fixture()
def anchored(monkeypatch):
    """``underlying_levels`` over a frozen panel plus an owned close series."""
    from paper_trader.alpha_agent.alpha_recovery import futures_intraday as FI

    monkeypatch.setattr(FI, "panel", _fake_panel)
    monkeypatch.setattr(FI, "minute_index", lambda h, m: 0)
    monkeypatch.setattr(OA, "session_closes", lambda **kw: dict(OWNED_CLOSES))
    return None


# --------------------------------------------------------------------------- #
# 1-3  the anchor is EXTENDED, never replaced
# --------------------------------------------------------------------------- #
def test_01_frozen_panel_dates_keep_their_exact_level(anchored):
    """Every date the frozen panel covers keeps EXACTLY the level it had.

    This is the whole reason the fix extends rather than replaces. A band centre
    that moved would move the strikes bought for that date, which would change
    the surface, which would change a frozen decision. The owned close series
    deliberately disagrees violently (111.0, 222.0) on the overlapping dates so
    that any precedence slip is impossible to miss.
    """
    dates, levels = OA.underlying_levels()
    got = dict(zip([str(d) for d in dates], levels))
    for d, es in zip(FROZEN_DATES, FROZEN_ES):
        assert got[d] == pytest.approx(es / 10.0), (
            "frozen-panel date %s must keep its ES/10 level" % d)
    assert got["2026-09-09"] != pytest.approx(111.0)
    assert got["2026-09-10"] != pytest.approx(222.0)


def test_02_dates_beyond_the_frozen_panel_are_supplied(anchored):
    """The dates that previously had NO anchor now have one."""
    dates, levels = OA.underlying_levels()
    got = dict(zip([str(d) for d in dates], levels))
    for d in ("2026-09-11", "2026-09-14", "2026-09-21"):
        assert d in got, "%s had no anchor at all before this release" % d
        assert got[d] == pytest.approx(OWNED_CLOSES[d])
    assert list(dates) == sorted(dates), "the anchor must stay date-ordered"
    assert len(dates) == len(levels)


def test_03_a_missing_spot_series_is_not_an_error(monkeypatch):
    """An unreadable close series leaves the anchor exactly as it was.

    The caller's own refusal names the consequence; this function does not get
    to decide that a frozen panel alone is fatal.
    """
    from paper_trader.alpha_agent.alpha_recovery import futures_intraday as FI

    monkeypatch.setattr(FI, "panel", _fake_panel)
    monkeypatch.setattr(FI, "minute_index", lambda h, m: 0)

    def _boom(**kw):
        raise OSError("no spot file")

    monkeypatch.setattr(OA, "session_closes", _boom)
    dates, levels = OA.underlying_levels()
    assert [str(d) for d in dates] == FROZEN_DATES
    assert np.allclose(levels, FROZEN_ES / 10.0)


# --------------------------------------------------------------------------- #
# 4-5  an unpriceable expiry is RECORDED, never silently dropped
# --------------------------------------------------------------------------- #
def test_04_plan_records_the_expiry_it_cannot_centre(monkeypatch):
    """``plan`` must leave a trace for an expiry it drops.

    The sibling failure path (a vendor cost error) already recorded into
    ``errors``; this path did not, which is exactly how a collection step that
    bought nothing came to look like a successful plan that cost $0.00.
    """
    monkeypatch.setattr(OA, "underlying_levels",
                        lambda: (np.array(["2020-01-02"]), np.array([300.0])))

    class _C:
        def cost(self, *a, **k):
            return 0.01

        def dataset_range(self, *a, **k):
            return {"end": "2026-09-21"}

    out = OA.plan(_C(), budget_usd=1.0, window=("2026-09-14", "2026-10-16"),
                  available_end="2026-09-21")
    assert out["errors"], "an expiry dropped for want of an anchor must be recorded"
    assert any("no owned underlying level" in str(v)
               for v in out["errors"].values())


def test_05_the_silent_continue_is_gone_from_the_source():
    """Guard the asymmetry that caused this, not merely today's behaviour."""
    i = OA_SRC.find("def plan(")
    j = OA_SRC.find("\ndef ", i + 1)
    body = OA_SRC[i:j if j > 0 else len(OA_SRC)]
    k = body.find("if not m.any():", body.find("if not m.any():") + 1)
    assert k > 0, "the second anchor guard must still exist"
    assert "errors[exp.isoformat()]" in body[k:k + 900], (
        "the second `if not m.any()` must RECORD before it continues")


# --------------------------------------------------------------------------- #
# 6-8  the append refuses by name instead of buying nothing
# --------------------------------------------------------------------------- #
class _Client:
    def dataset_range(self, *a, **k):
        return {"schema": {OA.SCHEMA: {"end": "2026-09-22"}}, "end": "2026-09-22"}

    def cost(self, *a, **k):
        return 0.01


@pytest.fixture()
def appendable(monkeypatch):
    """An append that reaches the planner with the vendor covering the session."""
    from paper_trader.alpha_agent.alpha_recovery import databento_acquisition as DA

    monkeypatch.setattr(DA, "credential_state", lambda: {"usable": True})
    monkeypatch.setattr(DA, "_available_end", lambda *a, **k: "2026-09-21")
    monkeypatch.setattr(NOR, "_surface_last_session", lambda surface=None: "2026-09-11")
    return None


def test_06_append_refuses_when_the_needed_near_leg_was_not_priced(
        appendable, monkeypatch):
    """``n_requests == 0`` must be a NAMED refusal, not a $0.00 success.

    This is the safeguard. Before it, the append proceeded through its own
    budget gate ($0.00 <= $0.225), downloaded nothing, and reported the
    downstream symptom BUILT_NO_ROWS - which reads as "the vendor gave us
    nothing" when the truth is "we asked for nothing".
    """
    monkeypatch.setattr(OA, "plan", lambda *a, **k: {
        "requests": [], "errors": {"2026-10-16": "no owned underlying level"},
        "selection": {}})
    out = NOR.append_information_session("2026-09-21", execute=False,
                                         client=_Client())
    assert out["state"] == NOR.APPEND_NEAR_LEG_NOT_PRICEABLE
    assert "2026-10-16" in out["near_leg_unpriceable"]
    assert "2026-10-16" in str(out["detail"])
    assert "no owned underlying level" in str(out["detail"]), (
        "the refusal must carry the planner's own reason")


def test_07_the_refusal_spends_nothing(appendable, monkeypatch):
    monkeypatch.setattr(OA, "plan", lambda *a, **k: {
        "requests": [], "errors": {}, "selection": {}})

    def _never(*a, **k):
        raise AssertionError("a refused append must not download")

    from paper_trader.alpha_agent.alpha_recovery import databento_acquisition as DA
    monkeypatch.setattr(DA, "download", _never)
    out = NOR.append_information_session("2026-09-21", execute=True,
                                         client=_Client())
    assert out["state"] == NOR.APPEND_NEAR_LEG_NOT_PRICEABLE
    assert float(out.get("paid_dollars") or 0.0) == 0.0


def test_08_a_priced_near_leg_does_not_trip_the_refusal(appendable, monkeypatch):
    """The refusal must fire on the NEEDED set, not on any dropped expiry.

    ``expiries_not_bought`` is a normal, healthy outcome - a plan legitimately
    prices expiries the near-leg filter then discards. Only a needed expiry
    that never reached the plan at all is a collection failure.
    """
    monkeypatch.setattr(OA, "plan", lambda *a, **k: {
        "requests": [{"expiry": "2026-10-16", "cost_usd": 0.02},
                     {"expiry": "2026-09-18", "cost_usd": 0.01}],
        "errors": {}, "selection": {}})
    out = NOR.append_information_session("2026-09-21", execute=False,
                                         client=_Client())
    assert out["state"] != NOR.APPEND_NEAR_LEG_NOT_PRICEABLE
    assert out["near_leg_unpriceable"] == []
    assert out["expiries_not_bought"] == ["2026-09-18"]


# --------------------------------------------------------------------------- #
# 9-11  blocked_on resolves its disjunction
# --------------------------------------------------------------------------- #
def _entry_state_with(monkeypatch, *, published, owned_last):
    monkeypatch.setattr(NOC, "source_available", lambda info, surface=None: {
        "owned": False, "published": published,
        "publication_state": "PUBLISHED" if published else "NOT_PUBLISHED",
        "owned_last_session": owned_last, "usable_for_a_freeze": False})
    return NOC.entry_state("2026-09-23", now="2026-09-23T05:00:00+00:00")


def test_09_a_published_session_names_local_collection(monkeypatch):
    """The eight-day failure. The vendor HAD published; we had not collected."""
    out = _entry_state_with(monkeypatch, published=True, owned_last="2026-09-11")
    assert out["blocked_owner"] == "LOCAL_COLLECTION"
    assert "IS published" in out["blocked_on"]
    assert "LOCAL COLLECTION" in out["blocked_on"]
    assert " or it has not been appended" not in out["blocked_on"], (
        "the unresolved disjunction is the defect; it must not come back")


def test_10_an_unpublished_session_names_the_vendor(monkeypatch):
    out = _entry_state_with(monkeypatch, published=False, owned_last="2026-09-21")
    assert out["blocked_owner"] == "VENDOR"
    assert "has not published" in out["blocked_on"]
    assert "no local action" in out["blocked_on"]


def test_11_an_unknown_probe_verdict_is_not_blamed_on_either(monkeypatch):
    out = _entry_state_with(monkeypatch, published=None, owned_last="2026-09-21")
    assert out["blocked_owner"] == "UNDETERMINED"


# --------------------------------------------------------------------------- #
# 12-14  the journal carries the reason
# --------------------------------------------------------------------------- #
def test_12_the_journal_allow_list_carries_the_blocker():
    for field in ("blocked_on", "blocked_owner", "append_state", "advance_state"):
        assert field in R52._JOURNAL_STAGE_FIELDS, (
            "%s must survive the journal projection" % field)
    for field in ("stage", "state", "duration_ms"):
        assert field in R52._JOURNAL_STAGE_FIELDS


def test_13_the_three_field_projection_is_gone():
    """Guard the exact shape that hid this for eight days."""
    assert '"stages": [{"stage": s.get("stage"), "state": s.get("state"),' \
        not in R52_SRC, "the three-field journal projection must not return"
    assert "_JOURNAL_STAGE_FIELDS" in R52_SRC


def test_14_the_runtime_reads_blocked_on_off_the_advance():
    """``blocked_on`` was in the advance dict all along and never read."""
    i = R52_SRC.find("next_open_prospective_decision")
    seg = R52_SRC[i:i + 1400]
    assert 'blocked_on=adv.get("blocked_on")' in seg
    assert 'blocked_owner=adv.get("blocked_owner")' in seg
    assert 'blocked_owner' in NOR_SRC, "the advance must carry the owner"


# --------------------------------------------------------------------------- #
# 17-19  the SECOND silence: a present date with a dead feature
# --------------------------------------------------------------------------- #
def _surface_csv(tmp_path, rows) -> Path:
    import pandas as pd

    p = tmp_path / "surface.csv"
    pd.DataFrame(rows).to_csv(p, index=False)
    return p


def test_17_last_usable_session_ignores_a_present_but_dead_date(monkeypatch):
    """A date can sit in the surface with a NaN ``skew``.

    That is exactly what the band-anchor defect produced: the surface went on
    advancing its last DATE while the only field the challenger scores had been
    dead for nine sessions. Measured on the real pre-repair archive: last date
    2026-09-11, last usable session 2026-08-28.
    """
    import pandas as pd

    df = pd.DataFrame({
        "date": ["2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01"],
        "skew": [0.070, 0.069, float("nan"), float("nan")]})
    monkeypatch.setattr(NOR.OS, "features", lambda **kw: df)
    assert NOR._surface_last_usable_session("x") == "2026-08-28"
    assert NOR._surface_last_session("x") == "2026-09-01"


def test_18_the_two_answers_are_reported_separately(monkeypatch):
    """The append must publish BOTH, and flag the divergence."""
    import pandas as pd
    from paper_trader.alpha_agent.alpha_recovery import databento_acquisition as DA

    df = pd.DataFrame({"date": ["2026-08-28", "2026-09-11"],
                       "skew": [0.069, float("nan")]})
    monkeypatch.setattr(NOR.OS, "features", lambda **kw: df)
    monkeypatch.setattr(DA, "credential_state", lambda: {"usable": False})
    out = NOR.append_information_session("2026-09-21", execute=False)
    assert out["surface_ends"] == "2026-09-11"
    assert out["surface_last_usable"] == "2026-08-28"
    assert out["surface_usable_lag_sessions_detected"] is True
    assert "2026-08-28" in out["surface_usable_lag_detail"]


def test_19_no_divergence_is_not_flagged(monkeypatch):
    import pandas as pd
    from paper_trader.alpha_agent.alpha_recovery import databento_acquisition as DA

    df = pd.DataFrame({"date": ["2026-09-18", "2026-09-21"], "skew": [0.069, 0.069]})
    monkeypatch.setattr(NOR.OS, "features", lambda **kw: df)
    monkeypatch.setattr(DA, "credential_state", lambda: {"usable": False})
    out = NOR.append_information_session("2026-09-22", execute=False)
    assert out["surface_last_usable"] == "2026-09-21"
    assert "surface_usable_lag_sessions_detected" not in out


# --------------------------------------------------------------------------- #
# 15-16  FORFEITURE SEMANTICS ARE UNCHANGED
# --------------------------------------------------------------------------- #
def test_15_a_missing_frozen_decision_is_still_not_a_forfeiture():
    """R66 fixed a producer. It did NOT redefine what a loss is.

    The contract reserves forfeiture for a REAL missed opportunity - a frozen
    decision whose emission window shut. A cadence boundary at which the
    originating owner never froze anything is AWAITING_NEW_GOVERNED_FREEZE, a
    fact about governance. Making the producer work must not quietly convert
    eight days of producer silence into eight recorded losses.
    """
    assert "Forfeiture is reserved for a REAL missed opportunity" in CFA_SRC
    assert "AWAITING_NEW_GOVERNED_FREEZE" in CFA_SRC
    assert "is NOT a forfeiture and\nis never counted as one" in CFA_SRC


def test_16_release_66_did_not_touch_the_forfeiture_owner():
    """No R66 marker may appear in the accrual owner's forfeiture logic."""
    assert "R66" not in CFA_SRC, (
        "R66 changes no line of the canonical forward accrual owner")
