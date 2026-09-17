r"""S25 PROSPECTIVE EPOCH BOUNDARY - the hermetic proof of the correction.

Run: S25_EPOCH_BOUNDARY_AND_SINGLE_AGENT_FIX_SEP17_V1

WHAT IS UNDER TEST
    Not the strategy. ``s25_operating_profitability`` is not rescored, refit or
    re-specified anywhere in this file, its inception is not moved and its
    membership is not touched. What is under test is the BOUNDARY:

    * that "had this session already completed?" is answered by the CANONICAL
      exchange-calendar and market-session owners against the EXCHANGE CLOSE,
      and never by the arrival of owned data;
    * that a panel which lags the exchange at activation can no longer leave an
      already-completed session collectable;
    * that the ORIGINAL activation artifact is never rewritten, and the
      correction that supersedes its floor is append-only;
    * that a mark for such a session - if one ever appears - is PRESERVED and
      excluded from every governed evidence count;
    * that nothing else moved: no backfill path, no second calendar, no second
      scheduler, no second accrual owner, and the SPY / FX / R58 lanes untouched.

Every test builds its own Stage-8 store in ``tmp_path`` and passes its own
instant, so no test here reads a vendor, a network, the live estate or the wall
clock. A boundary derived from ``datetime.now()`` is a time bomb, and this file
exists because a boundary was got wrong once.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

from alpha_agent import stage26_forward_runtime as S26F
from alpha_agent import tournament as T
from alpha_agent.r59 import stage25_owner as S25O
from engine import exchange_calendar as XC
from engine import market_session as MS

CAND = "c9_qualityprofi_e490533606"
BOOK_ID = "sb_c9_qualityprofi_e490533606"
SPEC_HASH = "67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e"
INCEPTION = "2026-08-16"
MEMBERSHIP = 100
HORIZON = 63

#: The REAL activation instant. 20:15:01 UTC is **16:15:01 ET**: fifteen minutes
#: AFTER the 16:00 exchange close and seventy-five minutes BEFORE the 17:30
#: owned-data cutoff. The entire defect lives in that window.
ACTIVATION_AT = "2026-09-16T20:15:01.093648+00:00"
#: What the panel could see at that instant, and what the exchange had done.
PANEL_LATEST_AT_ACTIVATION = "2026-09-15"
COMPLETED_AT_ACTIVATION = "2026-09-16"
#: The floor the defective rule stamped, and the floor that governs now.
DEFECTIVE_FLOOR = "2026-09-15"
EFFECTIVE_FLOOR = "2026-09-16"

FIRST_VALID = "2026-09-17"
SECOND_VALID = "2026-09-18"


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
def _membership() -> list:
    out = []
    for i in range(50):
        out.append({"symbol": "LONG%02d" % i, "leg": "LONG",
                    "weight": 1.0 / 50, "entry_price": 100.0})
    for i in range(50):
        out.append({"symbol": "SHRT%02d" % i, "leg": "SHORT",
                    "weight": -1.0 / 50, "entry_price": 100.0})
    return out


@pytest.fixture
def store(tmp_path: Path) -> Path:
    """A Stage-8 store holding the frozen S25 book with ZERO marks."""
    root = tmp_path / "stage8"
    root.mkdir(parents=True)

    registry = T.CandidateRegistry(root / S25O.REGISTRY_DB)
    try:
        registry.create_shadow_book(CAND, BOOK_ID, inception_date=INCEPTION,
                                    meta={"frozen": True})
    finally:
        registry.close()

    con = sqlite3.connect(str(root / S25O.REGISTRY_DB))
    try:
        wanted = {
            "candidate_id": CAND,
            "name": "s25_operating_profitability",
            "family": "fundamental_quality",
            "spec_json": json.dumps({"spec_hash": SPEC_HASH,
                                     "horizon_days": HORIZON}),
            "spec_hash": SPEC_HASH,
            "lifecycle_state": "SHADOW_BOOK_ACTIVE",
            "evidence_status": "FORWARD_PENDING",
            "universe": "US_EQUITY",
            "active_shadow_book_id": BOOK_ID,
        }
        cols, vals = [], []
        for row in con.execute("PRAGMA table_info(candidates)"):
            name, ctype, notnull = row[1], str(row[2]).upper(), row[3]
            if name in wanted:
                cols.append(name)
                vals.append(wanted[name])
            elif notnull:
                cols.append(name)
                vals.append(0 if ctype in ("INTEGER", "REAL")
                            else INCEPTION + "T02:58:21")
        con.execute(
            "INSERT OR REPLACE INTO candidates (%s) VALUES (%s)"
            % (", ".join(cols), ", ".join("?" * len(cols))), vals)
        con.commit()
    finally:
        con.close()

    book = T.ShadowBook(root / S25O.SHADOW_SUBDIR, BOOK_ID)
    book.inception(candidate_id=CAND, inception_date=INCEPTION,
                   membership=_membership(), benchmark="SPY", cost_bps=50.0,
                   spec={"spec_hash": SPEC_HASH, "horizon_days": HORIZON},
                   notional=100000.0)
    return root


def _defective_governance(store: Path) -> dict:
    """The activation record EXACTLY as the defective rule wrote it.

    Written through the module's own ``activate`` and then reduced to the
    defective floor, so the document under test is the real schema rather than
    a hand-built stand-in. This reproduces the live artifact, which is the thing
    the correction has to be able to supersede without touching.
    """
    S26F.activate(latest_completed_session=PANEL_LATEST_AT_ACTIVATION,
                  confirm=S26F.ACTIVATE_CONFIRM_TOKEN, now=ACTIVATION_AT,
                  store_root=store)
    p = S26F.governance_path(store)
    gov = json.loads(p.read_text(encoding="utf-8"))
    gov["prospective_epoch_floor_session"] = DEFECTIVE_FLOOR
    gov["latest_completed_session_at_activation"] = PANEL_LATEST_AT_ACTIVATION
    gov.pop("latest_completed_eligible_session_at_activation", None)
    gov["first_collectable_session"] = (
        "the first eligible completed session STRICTLY AFTER %s"
        % DEFECTIVE_FLOOR)
    p.write_text(json.dumps(gov, indent=1, sort_keys=True), encoding="utf-8")
    return gov


def _panel(sessions: list, *, flat: bool = False, members=None) -> dict:
    members = members if members is not None else _membership()
    series = {}
    for pos in members:
        series[pos["symbol"]] = {
            "dates": list(sessions),
            "adj": [100.0 if flat else 101.0] * len(sessions)}
    series["SPY"] = {"dates": list(sessions), "adj": [400.0] * len(sessions)}
    return {"series": series, "manifest": {"path": "<fixture>"}}


def _advance(store: Path, sessions: list, **kw) -> dict:
    return S26F.advance(store_root=store,
                        panel_loader=lambda: _panel(sessions, **kw))


def _marks(store: Path) -> list:
    return (S25O.read_book(store, BOOK_ID).get("marks") or [])


def _correct(store: Path, **kw) -> dict:
    return S26F.record_epoch_correction(
        confirm=S26F.CORRECTION_CONFIRM_TOKEN, store_root=store, **kw)


def _legacy_mark(store: Path, date: str) -> None:
    """A mark as the DEFECTIVE code would have left it, for the CASE B setup.

    It is written through the FROZEN producer's own append-only writer, because
    that is the only way such a row could have appeared: the corrected
    ``advance`` refuses this date outright, so the precondition cannot be built
    through the stage any more. Nothing in production calls this - the whole
    point of the fix is that the path no longer exists - and it is confined to
    a ``tmp_path`` store.
    """
    T.ShadowBook(store / S25O.SHADOW_SUBDIR, BOOK_ID).record_mark(
        date=date, nav=100_500.0)


# =========================================================================== #
# 1. A STALE PANEL AFTER THE CLOSE CANNOT MAKE A COMPLETED SESSION PROSPECTIVE
# =========================================================================== #
def test_01_after_close_activation_with_a_stale_panel_floors_on_the_session(store):
    """THE DEFECT, stated as the rule that now holds.

    Activation wall clock: after the close on session S. Panel latest: S-1.
    Required: the floor is S, not S-1.
    """
    res = S26F.activate(latest_completed_session=PANEL_LATEST_AT_ACTIVATION,
                        confirm=S26F.ACTIVATE_CONFIRM_TOKEN,
                        now=ACTIVATION_AT, store_root=store)
    gov = res["governance"]
    assert gov["prospective_epoch_floor_session"] == COMPLETED_AT_ACTIVATION
    assert gov["latest_completed_eligible_session_at_activation"] \
        == COMPLETED_AT_ACTIVATION
    # The panel's lag is recorded rather than obeyed.
    assert gov["latest_completed_session_at_activation"] \
        == PANEL_LATEST_AT_ACTIVATION
    assert gov["owned_panel_lagged_the_exchange_at_activation"] is True


def test_02_the_completed_session_is_not_collectable_when_the_panel_catches_up(store):
    """S must stay uncollectable once the data arrives. That is the whole point."""
    S26F.activate(latest_completed_session=PANEL_LATEST_AT_ACTIVATION,
                  confirm=S26F.ACTIVATE_CONFIRM_TOKEN, now=ACTIVATION_AT,
                  store_root=store)
    # The panel has now caught up and can price 2026-09-16 perfectly well.
    res = _advance(store, ["2026-09-14", "2026-09-15", COMPLETED_AT_ACTIVATION])
    assert res["state"] == S26F.STATE_NOT_DUE
    assert res["reason"] == S26F.R_NO_NEW_SESSION
    assert _marks(store) == []


def test_03_before_close_activation_does_not_over_block_the_live_session(store):
    """The control. The rule must not block a session that has NOT closed yet.

    Activation before the close on S; the latest completed session is S-1, so a
    later legitimate mark for S stays possible.
    """
    res = S26F.activate(latest_completed_session="2026-09-14",
                        confirm=S26F.ACTIVATE_CONFIRM_TOKEN,
                        now="2026-09-16T15:00:00+00:00",   # 11:00 ET, open
                        store_root=store)
    gov = res["governance"]
    assert gov["latest_completed_eligible_session_at_activation"] == "2026-09-15"
    # Floor is max(last forfeited 2026-09-15, 2026-09-15) = 2026-09-15.
    assert gov["prospective_epoch_floor_session"] == DEFECTIVE_FLOOR
    # ... and S is therefore still collectable when it completes and prices.
    res2 = _advance(store, [COMPLETED_AT_ACTIVATION])
    assert res2["state"] == S26F.STATE_ADVANCED
    assert res2["evidence_session"] == COMPLETED_AT_ACTIVATION


def test_04_a_panel_that_runs_ahead_still_cannot_lower_the_floor(store):
    """Neither term of the floor may be moved DOWN by the data, either way."""
    res = S26F.activate(latest_completed_session="2026-09-30",
                        confirm=S26F.ACTIVATE_CONFIRM_TOKEN,
                        now=ACTIVATION_AT, store_root=store)
    assert (res["governance"]["prospective_epoch_floor_session"]
            == COMPLETED_AT_ACTIVATION)


# =========================================================================== #
# 2. THE CANONICAL OWNERS DETERMINE THE BOUNDARY - THERE IS NO SECOND CALENDAR
# =========================================================================== #
def test_05_the_boundary_is_derived_from_the_canonical_session_owner():
    """The declared constant is RECOMPUTED here, not trusted.

    "Do not hard-code around the bug merely because we know the expected
    answer": the corrected floor is what the canonical owners derive from the
    activation instant, and this test is what binds the literal to them.
    """
    non_sessions = XC.non_sessions_between("2026-08-17", "2026-09-16")
    expected = MS.resolve_expected_session(
        dt.datetime.fromisoformat(ACTIVATION_AT),
        close_cutoff_et=MS.REGULAR_CLOSE_ET,
        non_sessions=non_sessions)
    assert expected.market_date_iso == COMPLETED_AT_ACTIVATION
    assert S26F.latest_completed_eligible_session(ACTIVATION_AT) \
        == expected.market_date_iso
    assert S26F.CORRECTED_ACTIVATION_FLOOR == expected.market_date_iso
    assert S26F.epoch_floor_for(expected.market_date_iso) \
        == S26F.CORRECTED_ACTIVATION_FLOOR


def test_06_the_owned_data_arrival_cutoff_is_not_the_session_boundary():
    """The defect, one level up: 17:30 ET is a DATA cutoff, not a close.

    Resolved with the arrival cutoff the same instant yields the WRONG session -
    which is exactly the value the defective floor carried. The module must use
    the exchange close.
    """
    non_sessions = XC.non_sessions_between("2026-08-17", "2026-09-16")
    now = dt.datetime.fromisoformat(ACTIVATION_AT)
    by_arrival = MS.resolve_expected_session(
        now, close_cutoff_et=MS.DEFAULT_CLOSE_CUTOFF_ET,
        non_sessions=non_sessions)
    by_close = MS.resolve_expected_session(
        now, close_cutoff_et=MS.REGULAR_CLOSE_ET, non_sessions=non_sessions)
    assert by_arrival.market_date_iso == DEFECTIVE_FLOOR
    assert by_close.market_date_iso == COMPLETED_AT_ACTIVATION
    assert S26F.latest_completed_eligible_session(ACTIVATION_AT) \
        == by_close.market_date_iso
    assert S26F.SESSION_BOUNDARY_RULE \
        == "EXCHANGE_REGULAR_CLOSE_ET_NOT_OWNED_DATA_ARRIVAL_CUTOFF"


def test_07_an_exchange_holiday_is_never_a_completed_session():
    """The authoritative calendar is honoured, not re-derived.

    2026-09-07 is Labor Day. After its "close" the latest completed session must
    be the previous trading day, and this module declares no holiday itself.
    """
    assert XC.is_non_session("2026-09-07") is True
    assert S26F.latest_completed_eligible_session(
        "2026-09-07T21:00:00+00:00") == "2026-09-04"
    # A weekend resolves back to the Friday.
    assert S26F.latest_completed_eligible_session(
        "2026-09-19T21:00:00+00:00") == "2026-09-18"


def test_08_the_module_implements_no_calendar_of_its_own():
    """Structural: the epoch owner borrows the calendar and owns none."""
    src = Path(S26F.__file__).read_text(encoding="utf-8")
    body = src.split('"""', 2)[-1]
    for banned in ("def next_trading_day(", "def previous_trading_day(",
                   "def walk_back_to_trading_day(",
                   "def completed_sessions_after(", "def holidays_for_year(",
                   "def is_non_session(", "weekday() < 5"):
        assert banned not in body, banned
    assert "resolve_expected_session" in body
    assert "REGULAR_CLOSE_ET" in body
    assert S26F.SESSION_BOUNDARY_OWNER == \
        "paper_trader.engine.market_session.resolve_expected_session"
    assert S26F.CALENDAR_OWNER == "paper_trader.engine.exchange_calendar"


# =========================================================================== #
# 3. THE ORIGINAL ACTIVATION ARTIFACT IS NEVER REWRITTEN
# =========================================================================== #
def test_09_the_correction_does_not_mutate_the_activation_artifact(store):
    """The record states truthfully what was done, wrong floor included."""
    _defective_governance(store)
    p = S26F.governance_path(store)
    before_bytes = p.read_bytes()
    before_mtime = p.stat().st_mtime_ns

    res = _correct(store, now="2026-09-17T09:00:00+00:00")
    assert res["state"] == "CORRECTED"
    assert res["wrote"] is True

    assert p.read_bytes() == before_bytes
    assert p.stat().st_mtime_ns == before_mtime
    gov = json.loads(p.read_text(encoding="utf-8"))
    assert gov["prospective_epoch_floor_session"] == DEFECTIVE_FLOOR
    assert gov["activated_at"] == ACTIVATION_AT
    assert gov["original_inception"] == INCEPTION


def test_10_the_correction_is_a_separate_document_beside_it(store):
    _defective_governance(store)
    _correct(store)
    assert S26F.correction_path(store).exists()
    assert S26F.correction_path(store) != S26F.governance_path(store)
    assert (S26F.correction_path(store).parent
            == S26F.governance_path(store).parent)
    doc = json.loads(S26F.correction_path(store).read_text(encoding="utf-8"))
    assert doc["append_only"] is True
    assert doc["corrects_artifact"] == S26F.GOVERNANCE_ARTIFACT
    assert doc["corrects_artifact_mutated"] is False


def test_11_the_correction_states_every_required_fact(store):
    _defective_governance(store)
    entry = _correct(store)["correction"]
    assert entry["original_activation_record"] == "PRESERVED"
    assert entry["original_activation_mutated"] is False
    assert entry["original_prospective_epoch_floor_session"] == DEFECTIVE_FLOOR
    assert entry["defect"] == \
        "STALE_PANEL_COULD_LEAVE_AN_ALREADY_COMPLETED_SESSION_COLLECTABLE"
    assert entry["activation_timestamp"] == ACTIVATION_AT
    assert entry["canonical_completed_market_session_at_activation"] \
        == COMPLETED_AT_ACTIVATION
    assert entry["effective_prospective_epoch_floor_session"] == EFFECTIVE_FLOOR
    assert "STRICTLY AFTER %s" % EFFECTIVE_FLOOR \
        in entry["first_valid_evidence_session"]
    assert entry["backfill"] == "FORBIDDEN"
    assert entry["candidate_identity"] == "UNCHANGED"
    assert entry["strategy_identity"] == "UNCHANGED"
    assert entry["membership_state"] == "UNCHANGED"
    assert entry["original_inception"] == INCEPTION
    assert entry["evidence_row_deleted"] is False
    assert entry["evidence_row_restated"] is False
    assert entry["session_boundary_owner"] == S26F.SESSION_BOUNDARY_OWNER
    assert entry["calendar_owner"] == S26F.CALENDAR_OWNER


def test_12_the_identity_is_untouched_by_the_correction(store):
    _defective_governance(store)
    book_before = S25O.book_path(store, BOOK_ID).read_bytes()
    _correct(store)
    assert S25O.book_path(store, BOOK_ID).read_bytes() == book_before
    inc = S25O.read_book(store, BOOK_ID)["inception"]
    assert inc["date"] == INCEPTION
    assert inc["spec"]["spec_hash"] == SPEC_HASH
    assert len(inc["membership"]) == MEMBERSHIP


# =========================================================================== #
# 4. THE CORRECTION ARTIFACT IS APPEND-ONLY / FIRST-WRITE-WINS
# =========================================================================== #
def test_13_a_second_correction_run_appends_nothing(store):
    _defective_governance(store)
    first = _correct(store, now="2026-09-17T09:00:00+00:00")
    assert first["state"] == "CORRECTED"
    p = S26F.correction_path(store)
    before = p.read_bytes()

    second = _correct(store, now="2026-09-17T23:00:00+00:00")
    assert second["state"] == "ALREADY_RECORDED"
    assert second["wrote"] is False
    assert p.read_bytes() == before
    doc = json.loads(p.read_text(encoding="utf-8"))
    assert len(doc["corrections"]) == 1
    assert doc["corrections"][0]["recorded_at"] == "2026-09-17T09:00:00+00:00"


def test_14_a_later_correction_appends_and_preserves_the_earlier_one(store):
    """Append-only means the log GROWS; an existing entry is never edited."""
    _defective_governance(store)
    _correct(store, now="2026-09-17T09:00:00+00:00")
    first_entry = json.loads(
        S26F.correction_path(store).read_text(encoding="utf-8")
    )["corrections"][0]

    # A different correction id, resolving to a LATER session boundary.
    _correct(store, correction_id="A_LATER_CORRECTION",
             now="2026-09-30T21:00:00+00:00",
             resolver=lambda _ts: "2026-09-29")
    doc = json.loads(S26F.correction_path(store).read_text(encoding="utf-8"))
    assert len(doc["corrections"]) == 2
    assert doc["corrections"][0] == first_entry          # byte-identical entry
    assert doc["corrections"][1]["correction_id"] == "A_LATER_CORRECTION"


def test_15_the_effective_floor_is_monotonic_and_can_only_rise(store):
    """No artifact, and no replay of one, may reopen a governed-out session."""
    _defective_governance(store)
    _correct(store)
    assert S26F.governed_epoch(store)[
        "effective_prospective_epoch_floor_session"] == EFFECTIVE_FLOOR

    # A correction that would LOWER the floor is refused, not applied.
    lower = _correct(store, correction_id="WOULD_LOWER",
                     resolver=lambda _ts: "2026-08-20")
    assert lower["state"] == "NOT_REQUIRED"
    assert lower["wrote"] is False
    assert S26F.governed_epoch(store)[
        "effective_prospective_epoch_floor_session"] == EFFECTIVE_FLOOR


def test_16_a_correction_requires_the_explicit_confirmation_token(store):
    _defective_governance(store)
    res = S26F.record_epoch_correction(store_root=store)
    assert res["state"] == "REFUSED"
    assert res["wrote"] is False
    assert not S26F.correction_path(store).exists()


def test_17_a_correction_cannot_create_an_activation(store):
    """Fail-closed: a correction corrects; it never authorises collection."""
    res = _correct(store)
    assert res["state"] == "REFUSED"
    assert res["reason"] == S26F.R_NO_ACTIVATION
    assert not S26F.correction_path(store).exists()
    assert not S26F.governance_path(store).exists()


def test_18_deleting_the_correction_artifact_does_not_reopen_the_session(store):
    """The rule is DECLARED in code, so it survives the loss of a JSON file.

    A boundary that depends on an artifact being present is the same class of
    silence this book already lost 21 sessions to.
    """
    _defective_governance(store)
    _correct(store)
    S26F.correction_path(store).unlink()
    assert S26F.governed_epoch(store)[
        "effective_prospective_epoch_floor_session"] == EFFECTIVE_FLOOR
    res = _advance(store, [COMPLETED_AT_ACTIVATION])
    assert res["state"] == S26F.STATE_NOT_DUE
    assert _marks(store) == []


def test_19_the_declared_correction_is_bound_to_that_one_record(store):
    """It may never be applied to a different activation."""
    assert S26F.declared_correction_floor(None) is None
    assert S26F.declared_correction_floor(
        {"activated_at": ACTIVATION_AT,
         "prospective_epoch_floor_session": DEFECTIVE_FLOOR}) == EFFECTIVE_FLOOR
    # A different activation timestamp: untouched.
    assert S26F.declared_correction_floor(
        {"activated_at": "2026-10-01T20:15:01.093648+00:00",
         "prospective_epoch_floor_session": DEFECTIVE_FLOOR}) is None
    # The same timestamp but a floor that is already correct: untouched.
    assert S26F.declared_correction_floor(
        {"activated_at": ACTIVATION_AT,
         "prospective_epoch_floor_session": EFFECTIVE_FLOOR}) is None


# =========================================================================== #
# 5-6. A 2026-09-16 MARK CAN NEVER BE VALID - AND IF ONE EXISTS IT IS PRESERVED
# =========================================================================== #
def test_20_no_completed_at_activation_mark_can_be_written(store):
    _defective_governance(store)
    _correct(store)
    res = _advance(store, ["2026-09-15", COMPLETED_AT_ACTIVATION])
    assert res["state"] == S26F.STATE_NOT_DUE
    assert res["effective_prospective_epoch_floor_session"] == EFFECTIVE_FLOOR
    assert res["prospective_epoch_floor_session"] == DEFECTIVE_FLOOR
    assert res["epoch_floor_was_corrected"] is True
    assert _marks(store) == []


def test_21_the_refusal_stands_next_to_the_write_as_well(store):
    """Defence in depth: the guard is not only in the resolver's threshold."""
    _defective_governance(store)
    _correct(store)
    res = S26F.resolve_evidence_session(
        panel_sessions=[COMPLETED_AT_ACTIVATION], last_recorded=INCEPTION,
        floor=DEFECTIVE_FLOOR)            # a stale floor handed in directly
    assert res["session"] == COMPLETED_AT_ACTIVATION
    # ... and advance still refuses it, naming the effective epoch.
    out = _advance(store, [COMPLETED_AT_ACTIVATION])
    assert out["state"] in (S26F.STATE_NOT_DUE, S26F.STATE_REFUSED_RETROACTIVE)
    assert _marks(store) == []
    src = Path(S26F.__file__).read_text(encoding="utf-8")
    assert "R_PRE_EFFECTIVE_EPOCH" in src


def test_22_the_declared_rule_blocks_the_session_before_any_artifact_exists(store):
    """The strongest form: no correction has been RECORDED yet, and it holds.

    The defective activation record is in place and the correction artifact does
    not exist. 2026-09-16 must already be uncollectable, because the rule is
    declared in code and bound to that record - not conjured by running a script.
    """
    _defective_governance(store)
    assert S26F.load_corrections(store) is None
    assert S26F.governed_epoch(store)[
        "effective_prospective_epoch_floor_session"] == EFFECTIVE_FLOOR
    res = _advance(store, [COMPLETED_AT_ACTIVATION])
    assert res["state"] == S26F.STATE_NOT_DUE
    assert _marks(store) == []


def test_22b_a_preexisting_quarantined_mark_is_preserved_and_excluded(store):
    """CASE B. The raw row is immutable; it is simply not evidence."""
    _defective_governance(store)
    _legacy_mark(store, COMPLETED_AT_ACTIVATION)
    raw = _marks(store)
    assert [m["date"] for m in raw] == [COMPLETED_AT_ACTIVATION]
    book_bytes = S25O.book_path(store, BOOK_ID).read_bytes()

    out = _correct(store)
    assert out["state"] == "CORRECTED"
    # PRESERVED: not deleted, not edited, not restated, not reordered.
    assert S25O.book_path(store, BOOK_ID).read_bytes() == book_bytes
    assert _marks(store) == raw

    entry = out["correction"]
    assert entry["raw_marks_at_correction"] == 1
    assert entry["quarantined_mark_dates_at_correction"] \
        == [COMPLETED_AT_ACTIVATION]
    assert entry["valid_marks_at_correction"] == 0
    assert entry["quarantine_class"] == \
        "NONCOUNTING_PRE_EFFECTIVE_EPOCH_OBSERVATION"
    assert entry["quarantine_reason"] == \
        "SESSION_COMPLETED_BEFORE_GOVERNED_PROSPECTIVE_ACTIVATION_BOUNDARY"
    assert entry["quarantined_counts_toward"] == ["RAW_MARK_COUNT"]
    for excluded in ("TRUE_FORWARD_OBSERVATIONS", "EFFECTIVE_OBSERVATIONS",
                     "H63_LEGITIMATE_MARKS", "PROMOTION_EVIDENCE",
                     "CAPITAL_ELIGIBILITY_EVIDENCE"):
        assert excluded in entry["quarantined_counts_toward_none_of"]


def test_23_the_quarantined_mark_never_becomes_valid_later(store):
    """Not by a rerun, not by a restart, not by a later legitimate mark."""
    _defective_governance(store)
    _legacy_mark(store, COMPLETED_AT_ACTIVATION)
    _correct(store)

    for _ in range(3):
        _advance(store, [COMPLETED_AT_ACTIVATION])
    st = S26F.status(store_root=store, today="2026-09-17")
    assert st["raw_marks"] == 1
    assert st["valid_marks"] == 0
    assert st["quarantined_marks"] == 1

    # A genuine post-epoch session now marks, and the quarantine is unaffected.
    assert _advance(store, [FIRST_VALID])["state"] == S26F.STATE_ADVANCED
    st = S26F.status(store_root=store, today="2026-09-18")
    assert st["raw_marks"] == 2
    assert st["valid_marks"] == 1
    assert st["quarantined_marks"] == 1
    assert st["valid_mark_dates"] == [FIRST_VALID]
    assert st["quarantined_mark_dates"] == [COMPLETED_AT_ACTIVATION]


def test_24_classification_is_pure_and_deletes_nothing():
    marks = [{"date": "2026-09-16", "nav": 1.0}, {"date": "2026-09-17"},
             {"date": "2026-09-18"}]
    out = S26F.classify_marks(marks, EFFECTIVE_FLOOR)
    assert out["raw_marks"] == 3
    assert out["valid_marks"] == 2
    assert out["quarantined_marks"] == 1
    assert out["valid_mark_dates"] == ["2026-09-17", "2026-09-18"]
    assert out["quarantined_mark_dates"] == ["2026-09-16"]
    assert marks == [{"date": "2026-09-16", "nav": 1.0}, {"date": "2026-09-17"},
                     {"date": "2026-09-18"}]        # the input is untouched
    # Fail-closed with no governed epoch: nothing is ASSERTED to be valid.
    ungoverned = S26F.classify_marks(marks, None)
    assert ungoverned["raw_marks"] == 3
    assert ungoverned["valid_marks"] is None
    assert ungoverned["governed"] is False


# =========================================================================== #
# 7-8. H63 AND THE EFFECTIVE OBSERVATION COUNT USE VALID MARKS ONLY
# =========================================================================== #
def test_25_h63_counts_valid_marks_only(store):
    _defective_governance(store)
    _legacy_mark(store, COMPLETED_AT_ACTIVATION)     # the quarantined one
    _correct(store)
    _advance(store, [FIRST_VALID])                   # one legitimate mark

    st = S26F.status(store_root=store, today="2026-11-30")
    assert st["h63_valid_marks"] == 1
    assert st["forward_observations"] == 1
    assert st["effective_observations"] == 1
    assert st["marks"] == 1                          # the GOVERNED count
    assert st["raw_marks"] == 2                      # what is on disk
    assert st["horizon_pending"] is True
    assert st["h63_maturity_rule"] == "COUNT_OF_LEGITIMATELY_COLLECTED_MARKS"


def test_26_a_book_whose_only_mark_is_quarantined_has_zero_evidence(store):
    _defective_governance(store)
    _legacy_mark(store, COMPLETED_AT_ACTIVATION)
    _correct(store)
    st = S26F.status(store_root=store, today="2026-09-17")
    assert st["raw_marks"] == 1
    assert st["forward_observations"] == 0
    assert st["h63_valid_marks"] == 0
    assert st["effective_observations"] == 0
    assert st["last_mark"] is None            # a quarantined row is not a mark
    # ... and the stream does not read as ACCRUING on the strength of it.
    assert st["stream_state"] == S25O.ST_RETIRED


def test_27_the_raw_replay_count_is_never_silently_used_as_evidence(store):
    """``ShadowBook.replay`` still reports the RAW count; it is not the answer."""
    _defective_governance(store)
    _legacy_mark(store, COMPLETED_AT_ACTIVATION)
    _correct(store)
    replay = T.ShadowBook(store / S25O.SHADOW_SUBDIR, BOOK_ID).replay()
    assert replay["forward_observations"] == 1              # raw, unchanged
    st = S26F.status(store_root=store, today="2026-09-17")
    assert st["forward_observations"] == 0                  # governed
    assert st["raw_marks"] == replay["forward_observations"]


def test_28_the_owner_seam_read_model_distinguishes_raw_from_valid(store):
    _defective_governance(store)
    _legacy_mark(store, COMPLETED_AT_ACTIVATION)
    _correct(store)
    rec = S25O.records(store, today="2026-09-17")[0]
    assert rec["raw_marks"] == 1
    assert rec["valid_marks"] == 0
    assert rec["quarantined_marks"] == 1
    assert rec["quarantined_mark_dates"] == [COMPLETED_AT_ACTIVATION]
    assert rec["marks"] == 0                     # the governed count keeps the name
    assert rec["last_mark"] is None
    assert rec["governed_epoch_applied"] is True
    assert rec["effective_prospective_epoch_floor_session"] == EFFECTIVE_FLOOR
    assert rec["stream_state"] == S25O.ST_RETIRED


def test_29_the_seam_reports_an_ungoverned_book_exactly_as_stored(store):
    """No activation, so this seam asserts NO classification of its own."""
    rec = S25O.records(store, today="2026-09-17")[0]
    assert rec["raw_marks"] == 0
    assert rec["valid_marks"] is None
    assert rec["governed_epoch_applied"] is False
    assert rec["marks"] == 0


def test_30_the_runtime_digest_publishes_both_counts(store):
    from alpha_agent.r52 import runtime as R52

    _defective_governance(store)
    _legacy_mark(store, COMPLETED_AT_ACTIVATION)
    _correct(store)
    s25 = _advance(store, [FIRST_VALID])
    digest = R52._stage26_digest(s25)
    assert digest["raw_marks"] == 1              # before this run's write
    assert digest["valid_marks_before"] == 0
    assert digest["valid_marks_after"] == 1
    assert digest["marks_after"] == 2
    assert digest["quarantined_marks"] == 1
    assert digest["effective_prospective_epoch_floor_session"] == EFFECTIVE_FLOOR
    assert digest["prospective_epoch_floor_session"] == DEFECTIVE_FLOOR
    assert digest["epoch_floor_was_corrected"] is True
    assert digest["backfill"] == "FORBIDDEN"


# =========================================================================== #
# 9. SAME-SESSION RERUN REMAINS IDEMPOTENT
# =========================================================================== #
def test_31_repeated_advances_in_one_session_write_one_mark(store):
    _defective_governance(store)
    _correct(store)
    first = _advance(store, [FIRST_VALID])
    assert first["state"] == S26F.STATE_ADVANCED
    for _ in range(5):
        again = _advance(store, [FIRST_VALID])
        assert again["state"] in (S26F.STATE_NOT_DUE, S26F.STATE_ALREADY_MARKED)
    assert [m["date"] for m in _marks(store)] == [FIRST_VALID]


def test_32_a_restart_writes_no_duplicate(store):
    import importlib

    import alpha_agent.stage26_forward_runtime as reloaded

    _defective_governance(store)
    _correct(store)
    _advance(store, [FIRST_VALID])
    reloaded = importlib.reload(reloaded)
    res = reloaded.advance(store_root=store,
                           panel_loader=lambda: _panel([FIRST_VALID]))
    assert res["state"] == reloaded.STATE_NOT_DUE
    assert len(_marks(store)) == 1


def test_33_the_correction_is_idempotent_under_a_rerun_of_the_whole_fix(store):
    """Deployment may be re-run. Nothing may move."""
    _defective_governance(store)
    _correct(store)
    _advance(store, [FIRST_VALID])
    gov_bytes = S26F.governance_path(store).read_bytes()
    corr_bytes = S26F.correction_path(store).read_bytes()
    book_bytes = S25O.book_path(store, BOOK_ID).read_bytes()

    _correct(store)
    _advance(store, [FIRST_VALID])
    assert S26F.governance_path(store).read_bytes() == gov_bytes
    assert S26F.correction_path(store).read_bytes() == corr_bytes
    assert S25O.book_path(store, BOOK_ID).read_bytes() == book_bytes


# =========================================================================== #
# 10. NO BACKFILL PATH EXISTS
# =========================================================================== #
def test_34_the_forfeited_window_is_still_unreachable(store):
    _defective_governance(store)
    _correct(store)
    forfeited = ["2026-08-17", "2026-08-31", "2026-09-04", "2026-09-15"]
    res = _advance(store, forfeited)
    assert res["state"] == S26F.STATE_NOT_DUE
    assert _marks(store) == []
    assert all(S26F.is_forfeited_session(d) for d in forfeited)


def test_35_no_catch_up_over_the_sessions_between_the_epoch_and_now(store):
    """One invocation marks the NEWEST session and never walks back."""
    _defective_governance(store)
    _correct(store)
    res = _advance(store, [FIRST_VALID, SECOND_VALID, "2026-09-21"])
    assert res["state"] == S26F.STATE_ADVANCED
    assert res["evidence_session"] == "2026-09-21"
    assert res["n_sessions_skipped"] == 2
    # The skipped ones are never marked by a later call.
    assert _advance(store, [FIRST_VALID, SECOND_VALID,
                            "2026-09-21"])["state"] == S26F.STATE_NOT_DUE
    assert [m["date"] for m in _marks(store)] == ["2026-09-21"]


def test_36_no_module_in_the_repair_offers_a_backfill_or_override(store):
    for mod in (S26F, S25O):
        body = Path(mod.__file__).read_text(encoding="utf-8").split('"""', 2)[-1]
        for banned in ("def backfill", "def replay_marks", "def catch_up",
                       "def force_mark", "def set_epoch_floor",
                       "def override_floor", "def delete_mark",
                       "def restate_mark"):
            assert banned not in body, "%s: %s" % (mod.__name__, banned)
    for script in ("correct_s25_prospective_epoch.py",
                   "rearm_s25_prospective_collection.py"):
        src = (Path(S26F.__file__).resolve().parents[1] / "scripts"
               / script).read_text(encoding="utf-8")
        body = src.split('"""', 2)[-1]
        for banned in ("--session", "--date", "--floor", "--backfill",
                       "--force", "record_mark"):
            assert banned not in body, "%s: %s" % (script, banned)
    assert S26F.status(store_root=store)["backfill"] == "FORBIDDEN"


def test_37_the_correction_takes_no_date_from_a_caller(store):
    """The boundary is derived. There is no operator-supplied session anywhere.

    ``resolver`` exists so a hermetic test can supply a CLOCK, and the
    production scripts never pass one - proved by reading them.
    """
    _defective_governance(store)
    for script in ("correct_s25_prospective_epoch.py",
                   "rearm_s25_prospective_collection.py"):
        src = (Path(S26F.__file__).resolve().parents[1] / "scripts"
               / script).read_text(encoding="utf-8")
        assert "resolver" not in src.split('"""', 2)[-1]
    entry = _correct(store)["correction"]
    # The derived value, from the record's OWN activation timestamp.
    assert entry["canonical_completed_market_session_at_activation"] == \
        S26F.latest_completed_eligible_session(ACTIVATION_AT)


# =========================================================================== #
# 11. THE SPY / FX / R58 LANES ARE UNAFFECTED
# =========================================================================== #
def test_38_the_existing_forward_lanes_are_untouched_by_this_change():
    """Structural: this hotfix names no other lane and reaches none of them."""
    body = Path(S26F.__file__).read_text(encoding="utf-8").split('"""', 2)[-1]
    for foreign in ("next_open", "fx_carry", "r58", "canonical_forward_accrual",
                    "forward_challenger_registry", "shadow_portfolio_evidence"):
        assert foreign not in body.lower(), foreign


def test_39_the_runtime_still_wires_every_governed_stage_in_order():
    src = Path(__file__).resolve().parents[1] / "alpha_agent" / "r52" \
        / "runtime.py"
    body = src.read_text(encoding="utf-8")
    for stage in ("next_open_prospective_decision",
                  "fx_carry_cadence_prospective_decision",
                  "stage26_prospective_mark",
                  "canonical_forward_accrual"):
        assert body.count('"%s"' % stage) >= 1, stage
    # Order preserved: the S25 stage sits between the FX adapter and accrual.
    assert (body.index("fx_carry_cadence_prospective_decision")
            < body.index("stage26_prospective_mark")
            < body.index("advance_canonical_forward_accrual"))


def test_40_the_canonical_accrual_and_registrar_owners_are_not_reimplemented():
    from paper_trader.api import canonical_forward_accrual as CFA
    from paper_trader.api import forward_challenger_registry as FCR

    assert hasattr(CFA, "advance_canonical_forward_accrual")
    assert hasattr(FCR, "load_registrations")
    body = Path(S26F.__file__).read_text(encoding="utf-8").split('"""', 2)[-1]
    assert "advance_canonical_forward_accrual" not in body
    assert "load_registrations" not in body


# =========================================================================== #
# 12. NO SECOND SCHEDULER, CALENDAR OR ACCRUAL OWNER APPEARS
# =========================================================================== #
def test_41_the_repair_creates_no_scheduler_task_or_daemon():
    for mod in (S26F, S25O):
        body = Path(mod.__file__).read_text(encoding="utf-8").split('"""', 2)[-1]
        # Naming the retired host is REQUIRED (the record says it stays
        # disabled); reviving it, or growing a clock of any kind, is banned.
        for banned in ("schtasks", "Register-ScheduledTask",
                       "Enable-ScheduledTask", "Start-ScheduledTask",
                       "threading", "Thread(", "subprocess", "asyncio",
                       "time.sleep", "while True", "Timer("):
            assert banned not in body, "%s: %s" % (mod.__name__, banned)
    assert S26F.RUNTIME_OWNER == "alpha_agent.r52.runtime.research_runtime_cycle"
    assert S26F.RETIRED_HOST == "AlphaAgent-Collect"


def test_42_the_correction_script_starts_and_restarts_nothing():
    src = (Path(S26F.__file__).resolve().parents[1] / "scripts"
           / "correct_s25_prospective_epoch.py").read_text(encoding="utf-8")
    body = src.split('"""', 2)[-1]
    for banned in ("subprocess", "schtasks", "Start-ScheduledTask",
                   "manage_research_runtime", "restart_paper_trader_backend",
                   "create_order", "daily_close", "portfolio_cycle",
                   "record_mark", "advance_shadow_books"):
        assert banned not in body, banned


def test_43_the_mark_owner_is_unchanged_and_singular():
    assert S26F.MARK_OWNER == "alpha_agent.tournament.advance_shadow_books"
    body = Path(S26F.__file__).read_text(encoding="utf-8").split('"""', 2)[-1]
    assert "record_mark" not in body
    assert body.count("advance_shadow_books(") == 1


def test_44_the_correction_declares_and_holds_no_operational_reach(store):
    _defective_governance(store)
    safety = _correct(store)["correction"]["safety"]
    for flag in ("creates_order", "creates_fill", "mutates_holdings",
                 "mutates_cash", "mutates_nav", "promotes_model",
                 "activates_sleeve", "allocates_capital", "runs_daily_close",
                 "calls_portfolio_cycle", "may_spend_money",
                 "backfills_forward_rows", "revives_retired_scheduled_task"):
        assert safety[flag] is False, flag
    assert safety["research_only"] is True
    assert safety["paper_only"] is True


def test_45_the_governed_epoch_is_reported_from_one_place_only(store):
    """Two surfaces may not disagree about the boundary."""
    _defective_governance(store)
    _correct(store)
    epoch = S26F.governed_epoch(store)
    st = S26F.status(store_root=store, today="2026-09-17")
    adv = _advance(store, ["2026-09-15"])
    rec = S25O.records(store, today="2026-09-17")[0]
    floor = epoch["effective_prospective_epoch_floor_session"]
    assert floor == EFFECTIVE_FLOOR
    assert st["effective_prospective_epoch_floor_session"] == floor
    assert adv["effective_prospective_epoch_floor_session"] == floor
    assert rec["effective_prospective_epoch_floor_session"] == floor
    assert epoch["epoch_floor_rule"] == S26F.EPOCH_FLOOR_RULE
    assert epoch["recorded_correction_ids"] == [S26F.CORRECTION_ID]
    assert epoch["backfill"] == "FORBIDDEN"
