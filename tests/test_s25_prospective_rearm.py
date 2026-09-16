"""S25 PROSPECTIVE RE-ARM - the hermetic proof.

Run: S25_CANONICAL_FORWARD_REARM_SEP16_V1

What is under test is NOT the strategy. ``s25_operating_profitability`` is not
retested, rescored, refit or re-specified anywhere in this file. What is under
test is the REPAIR: that the existing frozen identity can accrue GENUINE FUTURE
evidence through the ONE canonical research runtime, that its 21 forfeited
sessions remain permanently unreachable, and that the forward challengers
already running are untouched.

Every test builds its own Stage-8 store in ``tmp_path`` and injects its own
price panel, so no test reads a vendor, a network or the live estate.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from alpha_agent import stage26_forward_runtime as S26F
from alpha_agent import tournament as T
from alpha_agent.r59 import stage25_owner as S25O

# The authoritative frozen facts, restated as literals. If the module's own
# constants ever drift from these, the identity tests below fail - which is the
# point: the repair may not redefine the thing it repairs.
CAND = "c9_qualityprofi_e490533606"
BOOK_ID = "sb_c9_qualityprofi_e490533606"
SPEC_HASH = "67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e"
INCEPTION = "2026-08-16"
MEMBERSHIP = 100
HORIZON = 63

FORFEITED_FIRST = "2026-08-17"
FORFEITED_LAST = "2026-09-15"

#: The 21 permanently forfeited NYSE sessions, written out in full. A range
#: expression would be a second calendar implementation; this is the list the
#: recovery recorded.
FORFEITED_21 = [
    "2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21",
    "2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28",
    "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04",
    "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14",
    "2026-09-15",
]

#: A FUTURE session, strictly after everything the repair forfeits.
FUTURE_1 = "2026-09-17"
FUTURE_2 = "2026-09-18"
FUTURE_3 = "2026-09-21"


# --------------------------------------------------------------------------- #
# Fixtures - the frozen book, rebuilt exactly, in a temp store
# --------------------------------------------------------------------------- #
def _membership() -> list:
    """The frozen shape: 100 names, 50 LONG / 50 SHORT, dollar-neutral."""
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

    # The candidate row is inserted directly because ``seed_candidate`` DERIVES
    # the candidate id from (family, spec) and this suite must reproduce the
    # EXISTING frozen id exactly. The shadow book itself is created through the
    # registry's own writer, so the ACTIVE row the mark producer reads is built
    # by the owner of that table rather than by this fixture.
    registry = T.CandidateRegistry(root / S25O.REGISTRY_DB)
    try:
        registry.create_shadow_book(CAND, BOOK_ID, inception_date=INCEPTION,
                                    meta={"frozen": True})
    finally:
        registry.close()

    con = sqlite3.connect(str(root / S25O.REGISTRY_DB))
    try:
        # Built from the table's OWN column list rather than from a literal
        # column tuple, so this fixture does not pin a schema it does not own.
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


def _panel(sessions: list, *, flat: bool = False, members=None) -> dict:
    """An owned-panel document in ``api.price_panel``'s own series shape."""
    members = members if members is not None else _membership()
    series = {}
    for pos in members:
        series[pos["symbol"]] = {
            "dates": list(sessions),
            "adj": [100.0 if flat else 101.0] * len(sessions)}
    series["SPY"] = {"dates": list(sessions), "adj": [400.0] * len(sessions)}
    return {"series": series, "manifest": {"path": "<fixture>"}}


def _activate(store: Path, *, latest="2026-09-16") -> dict:
    return S26F.activate(latest_completed_session=latest,
                         confirm=S26F.ACTIVATE_CONFIRM_TOKEN,
                         store_root=store)


def _advance(store: Path, sessions: list, **kw) -> dict:
    return S26F.advance(store_root=store,
                        panel_loader=lambda: _panel(sessions, **kw))


def _marks(store: Path) -> list:
    return (S25O.read_book(store, BOOK_ID).get("marks") or [])


# =========================================================================== #
# 1-4. THE FROZEN IDENTITY IS UNCHANGED
# =========================================================================== #
def test_the_module_carries_the_original_identity_unchanged():
    assert S26F.CANDIDATE_ID == CAND
    assert S26F.SHADOW_BOOK_ID == BOOK_ID
    assert S26F.STRATEGY_SPEC_HASH == SPEC_HASH
    assert S26F.ORIGINAL_INCEPTION == INCEPTION
    assert S26F.MEMBERSHIP_SIZE == MEMBERSHIP
    assert S26F.HORIZON_DAYS == HORIZON
    assert S26F.STRATEGY_NAME == "s25_operating_profitability"


def test_a_completed_advance_leaves_identity_spec_hash_and_inception_intact(store):
    before = S25O.read_book(store, BOOK_ID)
    _activate(store)
    res = _advance(store, [FUTURE_1])
    assert res["state"] == S26F.STATE_ADVANCED, res
    after = S25O.read_book(store, BOOK_ID)

    # Identity (1), spec hash (2), inception (3) and membership (4) unchanged.
    assert after["candidate_id"] == before["candidate_id"] == CAND
    assert after["inception"]["spec"]["spec_hash"] == SPEC_HASH
    assert after["inception"]["date"] == INCEPTION
    assert after["inception"]["membership"] == before["inception"]["membership"]
    assert len(after["inception"]["membership"]) == MEMBERSHIP
    # The ENTIRE inception snapshot is byte-identical; only marks grew.
    assert after["inception"] == before["inception"]
    assert set(after) == set(before)


def test_membership_keeps_the_frozen_dollar_neutral_shape(store):
    members = S25O.read_book(store, BOOK_ID)["inception"]["membership"]
    assert len(members) == MEMBERSHIP
    assert sum(1 for m in members if m["leg"] == "LONG") == 50
    assert sum(1 for m in members if m["leg"] == "SHORT") == 50
    assert abs(sum(m["weight"] for m in members)) < 1e-12


def test_a_drifted_identity_fails_closed_rather_than_being_repaired(store):
    _activate(store)
    p = S25O.book_path(store, BOOK_ID)
    doc = json.loads(p.read_text(encoding="utf-8"))
    doc["inception"]["spec"]["spec_hash"] = "0" * 64
    p.write_text(json.dumps(doc), encoding="utf-8")

    res = _advance(store, [FUTURE_1])
    assert res["state"] == S26F.STATE_IDENTITY_MISMATCH
    assert any("spec_hash" in d for d in res["drift"])
    assert _marks(store) == []


# =========================================================================== #
# 5-8. ZERO MARKS STAY ZERO; ONE FUTURE TICK WRITES EXACTLY ONE
# =========================================================================== #
def test_the_book_starts_with_zero_marks_and_nothing_creates_one_implicitly(store):
    assert _marks(store) == []
    # Reading the stream, the owner seam and the status report never write.
    assert S26F.status(store_root=store, today="2026-09-16")["marks"] == 0
    assert S25O.records(store, today="2026-09-16")[0]["marks"] == 0
    assert _marks(store) == []


def test_without_a_governed_activation_no_session_is_collectable(store):
    res = _advance(store, [FUTURE_1, FUTURE_2])
    assert res["state"] == S26F.STATE_AWAITING_ACTIVATION
    assert res["reason"] == S26F.R_NO_ACTIVATION
    assert res["marks_written_this_run"] == 0
    assert _marks(store) == []
    assert not S26F.governance_path(store).exists()


def test_one_synthetic_future_tick_writes_exactly_one_mark(store):
    _activate(store)
    res = _advance(store, [FUTURE_1])
    assert res["state"] == S26F.STATE_ADVANCED
    assert res["evidence_session"] == FUTURE_1
    assert res["marks_written_this_run"] == 1
    marks = _marks(store)
    assert len(marks) == 1
    assert marks[0]["date"] == FUTURE_1
    assert marks[0]["nav"] is not None


def test_a_same_session_rerun_writes_no_duplicate(store):
    _activate(store)
    assert _advance(store, [FUTURE_1])["state"] == S26F.STATE_ADVANCED
    again = _advance(store, [FUTURE_1])
    assert again["state"] == S26F.STATE_NOT_DUE
    assert again["marks_written_this_run"] == 0
    assert len(_marks(store)) == 1


def test_many_reruns_in_the_same_session_are_idempotent(store):
    _activate(store)
    for _ in range(5):
        _advance(store, [FUTURE_1])
    assert [m["date"] for m in _marks(store)] == [FUTURE_1]


def test_a_restart_writes_no_duplicate(store):
    """A fresh process is exactly a fresh module call: state lives on disk."""
    _activate(store)
    _advance(store, [FUTURE_1])
    import importlib

    import alpha_agent.stage26_forward_runtime as reloaded
    reloaded = importlib.reload(reloaded)
    res = reloaded.advance(store_root=store,
                           panel_loader=lambda: _panel([FUTURE_1]))
    assert res["state"] == reloaded.STATE_NOT_DUE
    assert len(_marks(store)) == 1


def test_activation_is_first_write_wins_and_never_moves_the_floor(store):
    first = _activate(store, latest="2026-09-16")
    assert first["state"] == "ACTIVATED"
    floor = first["governance"]["prospective_epoch_floor_session"]
    second = _activate(store, latest="2026-12-31")
    assert second["state"] == "ALREADY_ACTIVE"
    assert second["wrote"] is False
    assert (second["governance"]["prospective_epoch_floor_session"] == floor)


def test_activation_requires_the_explicit_confirmation_token(store):
    res = S26F.activate(latest_completed_session="2026-09-16", store_root=store)
    assert res["state"] == "REFUSED"
    assert res["wrote"] is False
    assert not S26F.governance_path(store).exists()


# =========================================================================== #
# 9-10. THE 21 FORFEITED SESSIONS ARE UNREACHABLE, AND NOTHING ITERATES THEM
# =========================================================================== #
@pytest.mark.parametrize("session", FORFEITED_21)
def test_every_forfeited_session_is_refused_by_the_declared_window(session):
    assert S26F.is_forfeited_session(session) is True


@pytest.mark.parametrize("session", [FUTURE_1, FUTURE_2, FUTURE_3,
                                     "2026-09-16", INCEPTION, "2026-08-14"])
def test_sessions_outside_the_gap_are_not_marked_forfeited(session):
    assert S26F.is_forfeited_session(session) is False


def test_a_panel_offering_only_forfeited_sessions_yields_no_mark(store):
    """The exact live condition when this repair was written.

    The owned panel's newest session WAS 2026-09-15, so every post-inception
    session it could price was one of the 21. ``record_mark`` alone would have
    accepted 2026-09-15 - it is strictly after inception. The epoch floor is
    what refuses it.
    """
    _activate(store)
    res = _advance(store, FORFEITED_21)
    assert res["state"] == S26F.STATE_NOT_DUE
    assert res["reason"] == S26F.R_NO_NEW_SESSION
    assert _marks(store) == []


def test_the_floor_cannot_be_lowered_into_the_gap_by_a_stale_panel(store):
    """A panel two weeks behind at activation must not reopen the gap."""
    res = _activate(store, latest="2026-09-01")
    floor = res["governance"]["prospective_epoch_floor_session"]
    assert floor == FORFEITED_LAST
    assert _advance(store, FORFEITED_21)["state"] == S26F.STATE_NOT_DUE
    assert _marks(store) == []


def test_the_forfeited_window_is_refused_even_if_the_floor_is_wrong(store):
    """Guard 2 alone must hold. The floor is deliberately corrupted here.

    The floor is rewritten to inception, which reopens the whole 21-session gap
    as far as guard 1 is concerned. No mark may still be written, and the
    reported reason must name the FORFEITED WINDOW rather than the floor - a
    tampered floor that reported "nothing new since the floor" would hide
    exactly the tampering.
    """
    _activate(store)
    p = S26F.governance_path(store)
    gov = json.loads(p.read_text(encoding="utf-8"))
    gov["prospective_epoch_floor_session"] = "2026-08-16"   # the gap reopened
    p.write_text(json.dumps(gov), encoding="utf-8")

    res = _advance(store, FORFEITED_21)
    assert res["state"] == S26F.STATE_NOT_DUE
    assert res["reason"] == S26F.R_FORFEITED_WINDOW
    assert res["n_forfeited_sessions_excluded"] == 21
    assert res.get("evidence_session") is None
    assert _marks(store) == []


def test_a_corrupted_floor_still_lets_a_genuine_future_session_through(store):
    """The guards refuse the gap, not the future."""
    _activate(store)
    p = S26F.governance_path(store)
    gov = json.loads(p.read_text(encoding="utf-8"))
    gov["prospective_epoch_floor_session"] = "2026-08-16"
    p.write_text(json.dumps(gov), encoding="utf-8")

    res = _advance(store, FORFEITED_21 + [FUTURE_1])
    assert res["state"] == S26F.STATE_ADVANCED
    assert res["evidence_session"] == FUTURE_1
    assert res["n_forfeited_sessions_excluded"] == 21
    assert [m["date"] for m in _marks(store)] == [FUTURE_1]


def test_a_mixed_panel_marks_only_the_future_session(store):
    _activate(store)
    res = _advance(store, FORFEITED_21 + [FUTURE_1])
    assert res["state"] == S26F.STATE_ADVANCED
    assert res["evidence_session"] == FUTURE_1
    assert [m["date"] for m in _marks(store)] == [FUTURE_1]
    # Not one forfeited date was marked.
    assert not any(S26F.is_forfeited_session(m["date"]) for m in _marks(store))


def test_no_code_path_iterates_the_missed_dates(store):
    """Structural, not behavioural: the resolver returns ONE session.

    ``advance_shadow_books`` takes a single ``evidence_date`` and holds no date
    loop, and the resolver above it returns a single session. There is no
    composition in this repair through which a range of dates could be walked.
    """
    res = S26F.resolve_evidence_session(
        panel_sessions=FORFEITED_21 + [FUTURE_1, FUTURE_2, FUTURE_3],
        last_recorded=INCEPTION, floor=FORFEITED_LAST)
    assert isinstance(res["session"], str)
    # The NEWEST session, never a walk forward from the oldest.
    assert res["session"] == FUTURE_3
    assert res["skipped_sessions"] == [FUTURE_1, FUTURE_2]
    # And nothing inside the gap survived the filter at all.
    assert not any(S26F.is_forfeited_session(s)
                   for s in res["skipped_sessions"])

    # Scan the CODE, not the prose: the module docstring explains at length
    # what it refuses to do, so a bare-word ban would match its own warnings.
    src = Path(S26F.__file__).read_text(encoding="utf-8")
    code = src.split('"""', 2)[-1]
    # The ban is on date GENERATION and date ARITHMETIC - the machinery a
    # catch-up would need. Filtering the panel's own session list is not that,
    # and is how the one session gets chosen.
    for banned in ("date_range", "while ", "def backfill", "catch_up",
                   "timedelta(", "rrule", "fromordinal", "toordinal"):
        assert banned not in code, banned
    # And the only date the producer is ever handed is the ONE resolved session.
    assert code.count("evidence_date=") == 1
    assert "evidence_date=session" in code


def test_a_skipped_session_is_never_marked_by_a_later_call(store):
    """No catch-up: an outage forfeits, it does not queue."""
    _activate(store)
    res = _advance(store, [FUTURE_1, FUTURE_2, FUTURE_3])
    assert res["state"] == S26F.STATE_ADVANCED
    assert res["evidence_session"] == FUTURE_3
    assert res["n_sessions_skipped"] == 2
    # A later invocation does not go back for FUTURE_1 / FUTURE_2.
    again = _advance(store, [FUTURE_1, FUTURE_2, FUTURE_3])
    assert again["state"] == S26F.STATE_NOT_DUE
    assert [m["date"] for m in _marks(store)] == [FUTURE_3]


def test_the_frozen_producer_still_refuses_a_retroactive_date(store):
    """The pre-existing third guard is intact and was not defeated."""
    book = T.ShadowBook(store / S25O.SHADOW_SUBDIR, BOOK_ID)
    book.record_mark(date=FUTURE_2, nav=100000.0)
    with pytest.raises(T.RetroactiveError):
        book.record_mark(date=FUTURE_1, nav=100000.0)
    with pytest.raises(T.RetroactiveError):
        book.record_mark(date=INCEPTION, nav=100000.0)
    with pytest.raises(T.RetroactiveError):
        book.record_mark(date=FUTURE_2, nav=100000.0)


# =========================================================================== #
# THE FROZEN HORIZON CONTRACT IS PRESERVED, NOT REWRITTEN
# =========================================================================== #
def test_h63_maturity_counts_marks_and_not_elapsed_sessions(store):
    """The frozen rule is (B). This repair reads it; it does not redefine it."""
    assert S26F.H63_MATURITY_RULE == "COUNT_OF_LEGITIMATELY_COLLECTED_MARKS"
    _activate(store)
    _advance(store, [FUTURE_1])
    st = S26F.status(store_root=store, today="2026-11-30")
    # Many sessions have elapsed since inception; ONE mark exists.
    assert st["marks"] == 1
    assert st["forward_observations"] == 1
    assert st["horizon_pending"] is True
    assert st["days_since_inception"] > HORIZON


def test_the_frozen_contract_still_states_the_mark_counting_rule():
    """Read the contract itself, so a silent edit to it fails here."""
    from alpha_agent import stage26_challenger_expansion as S26

    contract = S26.forward_evidence_contract(
        readiness={}, frozen={"spec_hash": SPEC_HASH})
    g = contract["guarantees"]
    assert "counts recorded marks" in g["pending_vs_matured_distinguished"]
    assert "once that many marks exist" in g["matures_only_when_the_horizon_arrives"]
    assert contract["fake_maturation_forbidden"] is True
    assert contract["backdating_forbidden"] is True


def test_replay_reports_forward_observations_as_the_mark_count(store):
    _activate(store)
    _advance(store, [FUTURE_1])
    replay = T.ShadowBook(store / S25O.SHADOW_SUBDIR, BOOK_ID).replay()
    assert replay["forward_observations"] == 1
    assert replay["inception_date"] == INCEPTION


# =========================================================================== #
# NO HISTORICAL P&L IS RECONSTRUCTED
# =========================================================================== #
def test_nothing_reports_a_forward_statistic_over_the_missed_sessions(store):
    st = S26F.status(store_root=store, today="2026-09-16")
    assert st["marks"] == 0
    assert st["last_mark"] is None
    assert st["forward_observations"] == 0
    assert st["backfill"] == "FORBIDDEN"
    # A book with zero marks has no return path, and none is manufactured.
    replay = T.ShadowBook(store / S25O.SHADOW_SUBDIR, BOOK_ID).replay()
    assert replay["forward_observations"] == 0
    assert replay["cumulative_pnl"] == 0


def test_a_stalled_zero_mark_stream_reads_as_blocked_not_as_young(store):
    """Reporting a dead clock as merely pending is what let 31 days pass."""
    st = S26F.status(store_root=store, today="2026-09-16")
    assert st["stream_state"] == S25O.ST_RETIRED
    rec = S25O.records(store, today="2026-09-16")[0]
    assert rec["stream_state"] == S25O.ST_RETIRED
    assert rec["marks"] == 0


def test_a_freshly_marked_stream_reads_as_accruing(store):
    _activate(store)
    _advance(store, [FUTURE_1])
    assert (S26F.status(store_root=store, today="2026-09-18")["stream_state"]
            == S25O.ST_ACCRUING)


# =========================================================================== #
# COVERAGE AND CURRENCY - the kernel refuses rather than assuming flat
# =========================================================================== #
def test_insufficient_priced_coverage_blocks_the_mark(store):
    _activate(store)
    thin = [m for m in _membership()[:5]]
    res = S26F.advance(
        store_root=store,
        panel_loader=lambda: _panel([FUTURE_1], members=thin))
    assert res["state"] == S26F.STATE_DATA_BLOCKED
    assert res["reason"] == S26F.R_COVERAGE
    assert _marks(store) == []


def test_a_missing_benchmark_axis_blocks_the_mark(store):
    _activate(store)
    panel = _panel([FUTURE_1])
    panel["series"].pop("SPY")
    res = S26F.advance(store_root=store, panel_loader=lambda: panel)
    assert res["state"] == S26F.STATE_DATA_BLOCKED
    assert res["reason"] == S26F.R_NO_BENCHMARK
    assert _marks(store) == []


def test_the_close_provider_never_forward_fills_a_missing_session():
    """Coverage is not currency: an absent bar is absent, not the last one."""
    provider = S26F.panel_close_provider(
        {"series": {"AAA": {"dates": ["2026-09-14", "2026-09-15"],
                            "adj": [10.0, 11.0]}}})
    assert provider(["AAA"], "2026-09-15") == {"AAA": 11.0}
    # A session the series does not hold yields NOTHING for that symbol.
    assert provider(["AAA"], FUTURE_1) == {}
    assert provider(["AAA"], "2026-09-16") == {}


def test_a_missing_panel_blocks_rather_than_marking(store):
    _activate(store)
    res = S26F.advance(store_root=store, panel_loader=lambda: None)
    assert res["state"] == S26F.STATE_DATA_BLOCKED
    assert res["reason"] == S26F.R_NO_PANEL
    assert _marks(store) == []


# =========================================================================== #
# 11-12. ONE RUNTIME; THE EXISTING FORWARD CHALLENGERS ARE UNAFFECTED
# =========================================================================== #
def test_the_canonical_runtime_hosts_the_stage_and_no_second_scheduler_exists():
    rt = Path(
        __file__).resolve().parents[1] / "alpha_agent" / "r52" / "runtime.py"
    src = rt.read_text(encoding="utf-8")
    assert "stage26_forward_runtime" in src
    assert '_stage("stage26_prospective_mark"' in src
    # The ONE orchestrator is still the only one.
    assert src.count("def research_runtime_cycle") == 1
    # The retired host is NOT revived and the tournament tick is NOT called.
    # The names may appear in prose - the stage documents what it replaces -
    # so the assertion is on the CALL and IMPORT forms, not on the words.
    assert "run_tournament_tick(" not in src
    assert "run_tournament_cycle(" not in src
    assert "import run_tournament_tick" not in src
    assert "Register-ScheduledTask" not in src
    # No second orchestrator and no timer of its own.
    assert "while True" not in src


def test_the_adapter_never_calls_the_retired_tournament_tick():
    src = Path(S26F.__file__).read_text(encoding="utf-8")
    # Named in prose (the docstring explains what it does NOT do), never called.
    assert "run_tournament_tick(" not in src
    assert "run_tournament_cycle(" not in src
    assert "maybe_activate_shadow_books" not in src
    assert "Register-ScheduledTask" not in src
    assert S26F.RETIRED_HOST == "AlphaAgent-Collect"
    assert S26F.RUNTIME_OWNER == (
        "alpha_agent.r52.runtime.research_runtime_cycle")
    assert S26F.MARK_OWNER == "alpha_agent.tournament.advance_shadow_books"


def test_no_second_registry_ledger_or_accrual_owner_is_created(store):
    """The only thing this repair writes, besides a mark, is ONE record."""
    _activate(store)
    before = {p.name for p in store.iterdir()}
    _advance(store, [FUTURE_1])
    after = {p.name for p in store.iterdir()}
    # No new top-level store appeared: the mark went into the EXISTING book.
    assert after == before
    assert (store / S26F.GOVERNANCE_ARTIFACT).exists()
    assert (store / S25O.REGISTRY_DB).exists()
    assert (store / S25O.SHADOW_SUBDIR / BOOK_ID / "shadow_book.json").exists()


def test_the_stage_does_not_mark_a_book_it_did_not_rearm(store):
    """Blast radius: exactly the identity this repair re-armed."""
    other_cand = "other_candidate_v1"
    other_id = "sb_%s" % other_cand
    con = sqlite3.connect(str(store / S25O.REGISTRY_DB))
    con.execute(
        "INSERT OR REPLACE INTO shadow_books (shadow_book_id, candidate_id, "
        "inception_date, created_at, status, meta_json) VALUES (?,?,?,?,?,?)",
        (other_id, other_cand, INCEPTION, INCEPTION, "ACTIVE", "{}"))
    con.commit()
    con.close()
    other = T.ShadowBook(store / S25O.SHADOW_SUBDIR, other_id)
    other.inception(candidate_id=other_cand, inception_date=INCEPTION,
                    membership=_membership(), benchmark="SPY", cost_bps=50.0,
                    spec={"spec_hash": "beef" * 16})

    _activate(store)
    res = _advance(store, [FUTURE_1])
    assert res["state"] == S26F.STATE_ADVANCED
    assert len(_marks(store)) == 1
    # The other book was iterated by the tournament owner and NOT marked.
    assert (other.replay()["forward_observations"]) == 0
    assert res["n_out_of_scope_books"] == 1


def test_the_forward_challenger_registry_and_accrual_owners_are_untouched():
    """SPY next-open, FX carry and the R58 challengers keep their own owners.

    This repair adds no registration, no resolver and no originating-release
    entry, so every existing canonical forward path is byte-identical.
    """
    from paper_trader.api import canonical_forward_accrual as CFA
    from paper_trader.api import forward_challenger_registry as FCR

    assert set(CFA.FROZEN_DECISION_OWNERS) == {"R58",
                                               "ALPHA_RECOVERY_OFFENSIVE"}
    assert CFA.REGISTRAR_OWNER == "api.forward_challenger_registry"
    assert CFA.MATURATION_OWNER == "alpha_agent.r52.runtime"
    assert CFA.SCHEMA_VERSION == "canonical_forward_accrual.v1"
    assert FCR.SCHEMA_VERSION == "forward_challenger_registry.v1"
    # S25 is deliberately ABSENT from the canonical registry: its evidence
    # accrues in its own frozen shadow book under the frozen h63 clock, and a
    # second ledger for one identity is exactly what this repair refuses.
    for mod in (CFA, FCR):
        assert S26F.CANDIDATE_ID not in Path(
            mod.__file__).read_text(encoding="utf-8")


def test_the_existing_runtime_adapter_stages_are_still_wired():
    """The two precedent stages this one sits beside must remain intact."""
    src = (Path(__file__).resolve().parents[1] / "alpha_agent" / "r52"
           / "runtime.py").read_text(encoding="utf-8")
    for stage in ('_stage("next_open_prospective_decision"',
                  '_stage("fx_carry_cadence_prospective_decision"',
                  '_stage("canonical_forward_accrual"',
                  '_stage("tournament_advance"',
                  '_stage("forfeiture_sweep"',
                  '_stage("promotion_frontier"'):
        assert stage in src, stage
    assert "next_open_runtime" in src
    assert "fx_carry_cadence_runtime" in src
    assert "canonical_forward_accrual" in src


# =========================================================================== #
# 13-14. NO SIDE EFFECTS OF ANY KIND
# =========================================================================== #
def test_the_adapter_declares_and_holds_no_operational_reach():
    safety = S26F._safety() if hasattr(S26F, "_safety") else {}
    for flag in ("backfills_forward_rows", "creates_order", "creates_fill",
                 "mutates_holdings", "mutates_cash", "mutates_nav",
                 "promotes_model", "activates_sleeve", "allocates_capital",
                 "runs_daily_close", "calls_portfolio_cycle",
                 "may_spend_money", "revives_retired_scheduled_task"):
        assert safety[flag] is False, flag

    # The operational owners are not merely unused - there is no import in this
    # module through which any of them is REACHABLE. The safety flags above are
    # declarations; these are the closed doors behind them. The flag names
    # themselves contain some of these words, so the ban is on the import and
    # call forms rather than on the bare token.
    src = Path(S26F.__file__).read_text(encoding="utf-8")
    for banned in ("import daily_close", "import rebalance_execution",
                   "import portfolio_decision", "import normal_cycle",
                   "import requests", "requests.post", "requests.get",
                   "urllib.request", "127.0.0.1:8001", "create_order(",
                   "record_fill(", "approve(", "promote_model(",
                   "record_decision(", "APPROVED_FOR_OPERATION"):
        assert banned not in src, banned


def test_an_advance_reports_zero_paid_dollars(store):
    _activate(store)
    res = _advance(store, [FUTURE_1])
    assert res["paid_dollars"] == 0.0
    assert res["backfill"] == "FORBIDDEN"


def test_the_governance_record_states_what_it_does_not_authorise(store):
    gov = _activate(store)["governance"]
    assert gov["decision"] == "REARM_PROSPECTIVE_COLLECTION_ONLY"
    assert gov["decision_date"] == "2026-09-16"
    assert gov["backfill"] == "FORBIDDEN"
    assert gov["candidate_identity"] == "UNCHANGED"
    assert gov["strategy_identity"] == "UNCHANGED"
    assert gov["membership_state"] == "UNCHANGED"
    assert gov["forfeited_sessions"] == {"first": FORFEITED_FIRST,
                                        "last": FORFEITED_LAST,
                                        "count": 21,
                                        "state": "PERMANENTLY_FORFEITED"}
    assert gov["marks_at_activation"] == 0
    assert gov["observation_epoch_is_not_inception"] is True
    assert gov["original_inception"] == INCEPTION
    for excluded in ("does NOT backfill a mark",
                     "does NOT promote the strategy",
                     "does NOT make it capital eligible",
                     "does NOT erase the missing 21 sessions"):
        assert excluded in gov["authorisation_excludes"]


def test_the_prospective_epoch_is_recorded_separately_from_inception(store):
    gov = _activate(store, latest="2026-09-16")["governance"]
    assert gov["original_inception"] == INCEPTION
    assert gov["prospective_epoch_floor_session"] == "2026-09-16"
    assert gov["latest_completed_session_at_activation"] == "2026-09-16"
    assert gov["prospective_epoch_floor_session"] != gov["original_inception"]


def test_the_epoch_floor_is_the_later_of_the_gap_and_the_panel():
    assert S26F.epoch_floor_for("2026-09-01") == FORFEITED_LAST
    assert S26F.epoch_floor_for(FORFEITED_LAST) == FORFEITED_LAST
    assert S26F.epoch_floor_for("2026-09-16") == "2026-09-16"
    assert S26F.epoch_floor_for("2026-10-30") == "2026-10-30"
    assert S26F.epoch_floor_for(None) == FORFEITED_LAST
    assert S26F.epoch_floor_for("") == FORFEITED_LAST


def test_the_git_resident_governance_record_matches_the_module(store):
    p = (Path(__file__).resolve().parents[1] / "research" / "alpha_agent"
         / "S25_PROSPECTIVE_REARM_GOVERNANCE.json")
    doc = json.loads(p.read_text(encoding="utf-8"))
    assert doc["decision"] == S26F.DECISION
    assert doc["decision_date"] == S26F.DECISION_DATE
    assert doc["identity"]["candidate_id"] == S26F.CANDIDATE_ID
    assert doc["identity"]["strategy_spec_hash"] == S26F.STRATEGY_SPEC_HASH
    assert doc["identity"]["original_inception"] == S26F.ORIGINAL_INCEPTION
    assert doc["identity"]["membership_count"] == S26F.MEMBERSHIP_SIZE
    assert doc["forfeited_sessions"]["count"] == S26F.FORFEITED_SESSION_COUNT
    assert doc["forfeited_sessions"]["first"] == S26F.FORFEITED_FIRST
    assert doc["forfeited_sessions"]["last"] == S26F.FORFEITED_LAST
    assert doc["frozen_horizon_contract"]["rule"] == S26F.H63_MATURITY_RULE
    assert doc["frozen_horizon_contract"]["blocked_by_frozen_horizon_contract"] \
        is False
    assert doc["runtime"]["new_scheduler_created"] is False
    assert doc["runtime"]["new_accrual_owner_created"] is False
    assert doc["runtime"]["new_runtime_created"] is False
    assert doc["root_cause"]["retired_host_revived"] is False
    assert doc["safety"]["paid_data_cost_usd"] == 0


def test_the_owner_seam_is_read_only_and_creates_nothing(tmp_path):
    absent = tmp_path / "nothing_here"
    assert S25O.records(absent) is None
    assert not absent.exists()
    src = Path(S25O.__file__).read_text(encoding="utf-8")
    assert "mode=ro" in src
    assert "record_mark" not in src.split('"""', 2)[-1]
    for banned in ("mkdir", "write_text", "INSERT", "UPDATE", "DELETE"):
        assert banned not in src, banned
