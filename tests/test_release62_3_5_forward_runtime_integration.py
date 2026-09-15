r"""Release 62.3.5 - both forward challengers run through ONE runtime, and the
next-open challenger's SECOND cadence decision is armed before its window opens.

THE DEFECT, REPRODUCED HERE BEFORE IT WAS FIXED
    ``api.canonical_forward_accrual.decision_grid`` spaces cadence boundaries on
    the REALISED price calendar. The FIRST decision session of
    ``REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1`` (2026-09-15) is armed from the
    registrar's clock plus the declared execution offset, but every LATER
    boundary entered the grid only once its own session had printed - after that
    session's close. The challenger decides between 00:00 and 09:30 ET ON its
    entry session, so when the grid finally learned of 2026-09-22 the window had
    already shut: the owner's frozen decision was recorded FORFEITED
    (WINDOW_CLOSED) and never emitted. The first decision would accrue; every
    later one would be lost while the registration looked merely young.

    The first entry's INFORMATION session has printed by the time its window
    opens; so has every later one's. A release that declares it enters N eligible
    sessions after its information session therefore has exactly N decision
    sessions the realised calendar cannot show yet, and the exchange calendar
    names them.

WHAT MAY NOT CHANGE
    A registration that declares no future-entry execution offset - the R58
    books, the same-session sibling, the FX cadence (a settlement boundary with
    offset 0 and its own owner-armed seam) - gets exactly the grid it got before.
    No cadence, sign, cost, signal or historical row is touched, and nothing is
    ever backfilled.

WHAT THESE TESTS MAY NOT DO
    Every test gets a private research root and a private accrual store.
"""
from __future__ import annotations

import inspect
from datetime import datetime
from pathlib import Path

import pytest

from paper_trader.alpha_agent import alpha_recovery as AR
from paper_trader.alpha_agent.alpha_recovery import next_open_challenger as NOC
from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
from paper_trader.api import canonical_forward_accrual as CFA

RUNTIME_SRC = Path(inspect.getfile(__import__("paper_trader.alpha_agent.r52.runtime",
                                              fromlist=["runtime"]))).read_text(encoding="utf-8")

CID = NOC.CHALLENGER_ID
IDENTITY_HASH = "spy-next-open-identity"
SESSIONS = ["2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14",
            "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21",
            "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25", "2026-09-28",
            "2026-09-29", "2026-09-30"]
FIRST_ENTRY = "2026-09-15"
NEXT_INFO = "2026-09-21"
NEXT_ENTRY = "2026-09-22"
NEXT_MATURITY = "2026-09-29"

FIRST_WINDOW_OPEN = "2026-09-15T05:00:00+00:00"   # 01:00 ET on the first entry session
FIRST_EMIT = "2026-09-15T05:30:00+00:00"
EVENING_BEFORE_NEXT = "2026-09-21T23:00:00+00:00"  # 19:00 ET on the next information session
NEXT_WINDOW_OPEN = "2026-09-22T05:00:00+00:00"    # 01:00 ET on the next entry session
NEXT_EMIT = "2026-09-22T05:30:00+00:00"
NEXT_AFTER_OPEN = "2026-09-22T14:00:00+00:00"     # 10:00 ET - the next open has passed
NEXT_EVENING = "2026-09-22T23:00:00+00:00"


@pytest.fixture()
def root(tmp_path, monkeypatch):
    """A private research root holding the challenger's REAL declared policy."""
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    PD.declare_policy(**NOC.policy_declaration({"identity_hash": IDENTITY_HASH}),
                      now="2026-09-12T21:20:51+00:00")
    return tmp_path


def _registration(cid: str = CID, *, identity_hash: str = IDENTITY_HASH,
                  release: str = "ALPHA_RECOVERY_OFFENSIVE") -> dict:
    return {"challenger_id": cid, "registration_session": "2026-09-12",
            "asset_class": "US_ETF", "horizon_sessions": 5, "instrument_scope": ["SPY"],
            "identity": {"challenger_id": cid, "release": release,
                         "identity_hash": identity_hash},
            "observation_clock": {"first_eligible_observation_session": "2026-09-14"}}


def _panel(through: str) -> dict:
    days = [d for d in SESSIONS if d <= through]
    return {"series": {"SPY": {"dates": days,
                               "adj": [500.0 + 0.5 * i for i in range(len(days))]}}}


def _freeze(entry: str, now: str) -> dict:
    info = NOC.information_session_for(entry)
    res = PD.freeze_decision(challenger_id=CID, eligible_session=entry,
                             weights={"SPY": -1.0}, source_data_hash="src",
                             feature_state_hash="feat",
                             feature_observed_at="%sT19:45:00+00:00" % info,
                             now=now, context=NOC.decision_context(entry))
    return res


def _advance(now: str, through: str, acc: Path) -> dict:
    return CFA.advance_canonical_forward_accrual(
        now=datetime.fromisoformat(now), registrations=[_registration()],
        price_panel=_panel(through), store_dir_override=acc)


def _assess(now: str, through: str, acc: Path, reg: dict = None) -> dict:
    return CFA.assess_registration(registration=reg or _registration(),
                                   series=_panel(through)["series"],
                                   now=now, today=now[:10], store_dir_override=acc)


def _emit_first(acc: Path) -> dict:
    assert _freeze(FIRST_ENTRY, FIRST_WINDOW_OPEN)["frozen"]
    return _advance(FIRST_EMIT, "2026-09-14", acc)


def _cell(row: dict, session: str) -> dict:
    return next((c for c in row["cells"] if c["decision_session"] == session), {})


# --------------------------------------------------------------------------- #
# 1. THE FIRST ENTRY (already correct before this release)
# --------------------------------------------------------------------------- #
def test_01_the_first_entry_emits_inside_its_window(root, tmp_path):
    adv = _emit_first(tmp_path / "acc")
    row = adv["challengers"][0]
    assert adv["n_emitted_this_run"] == 1, row
    assert row["decision_grid"][0] == FIRST_ENTRY
    assert adv["n_forfeitures_recorded_this_run"] == 0


# --------------------------------------------------------------------------- #
# 2-5. THE SECOND CADENCE ENTRY - the defect
# --------------------------------------------------------------------------- #
def test_02_the_next_entry_is_armed_before_its_window_opens(root, tmp_path):
    acc = tmp_path / "acc"
    _emit_first(acc)
    row = _assess(EVENING_BEFORE_NEXT, NEXT_INFO, acc)
    assert row["latest_realised_session"] == NEXT_INFO
    assert row["decision_grid"][:2] == [FIRST_ENTRY, NEXT_ENTRY], row["decision_grid"]
    cell = _cell(row, NEXT_ENTRY)
    assert cell.get("state") == CFA.ACC_NOT_DUE, cell
    assert cell.get("reason") == CFA.NOT_DUE_AWAITING_DECISION_BOUNDARY, cell
    assert (cell.get("emission_window") or {}).get("state") == CFA.window.WINDOW_NOT_OPEN


def test_03_the_owner_decided_next_entry_is_emitted_inside_its_window(root, tmp_path):
    acc = tmp_path / "acc"
    _emit_first(acc)
    assert _freeze(NEXT_ENTRY, NEXT_WINDOW_OPEN)["frozen"]
    adv = _advance(NEXT_EMIT, NEXT_INFO, acc)
    row = adv["challengers"][0]
    assert adv["n_emitted_this_run"] == 1, row["cells"]
    assert adv["n_forfeitures_recorded_this_run"] == 0
    assert [e["decision_session"] for e in row["emissions"]] == [FIRST_ENTRY, NEXT_ENTRY]
    assert row["latest_realised_session"] == NEXT_INFO


def test_04_the_next_entry_is_never_forfeited_once_it_prints(root, tmp_path):
    acc = tmp_path / "acc"
    _emit_first(acc)
    assert _freeze(NEXT_ENTRY, NEXT_WINDOW_OPEN)["frozen"]
    _advance(NEXT_EMIT, NEXT_INFO, acc)
    adv = _advance(NEXT_EVENING, NEXT_ENTRY, acc)
    row = adv["challengers"][0]
    assert row["forfeitures_recorded"] == 0, row["forfeitures"]
    assert row["predictions_emitted"] == 2
    assert not [c for c in row["cells"] if c["state"] == CFA.ACC_FORFEITED]


def test_05_the_next_entry_matures_five_sessions_after_it(root, tmp_path):
    ctx = NOC.decision_context(NEXT_ENTRY)
    assert ctx["information_session"] == NEXT_INFO
    assert ctx["maturity_session"] == NEXT_MATURITY
    acc = tmp_path / "acc"
    _emit_first(acc)
    assert _freeze(NEXT_ENTRY, NEXT_WINDOW_OPEN)["frozen"]
    _advance(NEXT_EMIT, NEXT_INFO, acc)
    row = _assess("2026-09-30T23:00:00+00:00", "2026-09-30", acc)
    mat = {m["decision_session"]: m for m in row["maturation"]}
    assert mat[NEXT_ENTRY]["matured"] is True
    assert mat[NEXT_ENTRY]["maturity_session"] == NEXT_MATURITY
    assert row["backfilled"] is False


# --------------------------------------------------------------------------- #
# 6-7. NOTHING IS WIDENED, NOTHING IS BACKFILLED
# --------------------------------------------------------------------------- #
def test_06_a_missed_next_entry_is_never_backfilled(root, tmp_path):
    acc = tmp_path / "acc"
    _emit_first(acc)
    late = _freeze(NEXT_ENTRY, NEXT_AFTER_OPEN)
    assert late["frozen"] is False
    assert late["outcome"] == PD.REFUSED_TOO_LATE
    adv = _advance(NEXT_AFTER_OPEN, NEXT_INFO, acc)
    row = adv["challengers"][0]
    assert adv["n_emitted_this_run"] == 0
    assert [e["decision_session"] for e in row["emissions"]] == [FIRST_ENTRY]
    assert row["backfilled"] is False


def test_07_a_session_between_boundaries_never_becomes_one(root, tmp_path):
    acc = tmp_path / "acc"
    _emit_first(acc)
    for through in ("2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"):
        row = _assess("%sT23:00:00+00:00" % through, through, acc)
        assert row["decision_grid"] == [FIRST_ENTRY], (through, row["decision_grid"])


# --------------------------------------------------------------------------- #
# 8. A REGISTRATION THAT DECLARES NO FUTURE ENTRY KEEPS ITS GRID
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("contract", [
    None,                                                   # the same-session shape
    {"decision_session_is": "the ENTRY session",            # the FX settlement shape
     "execution_boundary": "SETTLEMENT_OF_THE_ELIGIBLE_SESSION_AFTER_THE_NEWEST_PUBLISHED_SESSION"},
])
def test_08_no_declared_offset_means_the_realised_grid_exactly(root, tmp_path, contract):
    cid = "SAME_GRID_LIKE"
    PD.declare_policy(challenger_id=cid, information_cutoff_et=(15, 45),
                      entry_mark_et=(16, 0), rebalance_cadence_sessions=5,
                      evaluation_horizon_sessions=5, cost_policy={"bps_per_side": 1.0},
                      instrument_scope=["SPY"], execution_contract=contract,
                      now="2026-09-12T21:00:00+00:00")
    reg = _registration(cid, identity_hash="same-grid-identity")
    assert CFA.execution_offset_sessions(reg)["offset_sessions"] == 0
    realised = [d for d in SESSIONS if d <= NEXT_INFO]
    row = _assess(EVENING_BEFORE_NEXT, NEXT_INFO, tmp_path / "acc", reg)
    assert row["decision_grid"] == CFA.decision_grid(
        reg, sessions=realised, cadence_sessions=5, offset_sessions=0)
    assert row["decision_grid"] == ["2026-09-14", "2026-09-21"]
    assert NEXT_ENTRY not in row["decision_grid"]


# --------------------------------------------------------------------------- #
# 9. ONE RUNTIME, ONE ORDER
# --------------------------------------------------------------------------- #
def test_09_one_runtime_runs_spy_then_fx_then_the_accrual():
    lock = RUNTIME_SRC.index("RL.acquire_path(_lock_file()")
    spy = RUNTIME_SRC.index('_stage(\n                "next_open_prospective_decision"')
    fx = RUNTIME_SRC.index('_stage(\n                "fx_carry_cadence_prospective_decision"')
    accrual = RUNTIME_SRC.index("CFA.advance_canonical_forward_accrual(")
    assert lock < spy < fx < accrual
    assert RUNTIME_SRC.count("NOR.advance_daily(") == 1
    assert RUNTIME_SRC.count("FXR.advance(") == 1
    assert RUNTIME_SRC.count("advance_canonical_forward_accrual(") == 1


# --------------------------------------------------------------------------- #
# 10-13. THE ARMING SEAM ITSELF
# --------------------------------------------------------------------------- #
def test_10_the_row_names_the_armed_entry_session(root, tmp_path):
    row = _assess(EVENING_BEFORE_NEXT, NEXT_INFO, tmp_path / "acc")
    assert row["armed_entry_sessions"] == [NEXT_ENTRY]
    assert row["latest_realised_session"] == NEXT_INFO      # never the armed one
    assert CFA.armed_entry_sessions(_registration(), [NEXT_INFO]) == [NEXT_ENTRY]
    assert CFA.armed_entry_sessions(_registration(), []) == []


def test_11_the_armed_entry_steps_the_exchange_calendar(root):
    assert CFA.armed_entry_sessions(_registration(), ["2026-09-18"]) == ["2026-09-21"]
    assert CFA.armed_entry_sessions(_registration(), ["2026-11-25"]) == ["2026-11-27"]


def test_12_an_unanswerable_calendar_arms_nothing(root, monkeypatch):
    monkeypatch.setattr(CFA, "_shift_eligible", lambda session, n: None)
    assert CFA.armed_entry_sessions(_registration(), [NEXT_INFO]) == []


def test_13_nothing_without_a_declared_future_entry_is_calendar_armed(root):
    for reg in (_registration("R58_FCF_PURE_V1", release="R58"),
                _registration("REVERSED_SPY_PUT_CALL_SKEW_H5"),        # the sibling
                _registration("ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7")):
        assert CFA.execution_offset_sessions(reg)["offset_sessions"] == 0
        assert CFA.armed_entry_sessions(reg, [NEXT_INFO]) == []
    # and the FX owner-armed seam still refuses this release, as it was built to
    assert CFA.armed_owner_sessions(_registration(), [NEXT_INFO]) == []
