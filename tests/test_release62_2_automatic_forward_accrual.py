r"""Release 62.2 - THE AUTOMATIC FORWARD ACCRUAL LOOP.

R62.1 built the canonical registrar and R62.1.1 built the governed operator
entrypoint that fills it. On 2026-09-09 four ACTIVE R58 freezes were adopted and
the live estate reported ``canonical_forward_registration_count = 4``.

And then nothing would ever have happened: a registration NAMED its accrual
owner and its maturation owner, and no code path connected the two. This suite
proves the connection, and proves it cannot be abused:

    discovery      the registrar is the ONE source of what exists
    emission       exactly once, and STRICTLY BEFORE the session it stamps
    forfeiture     a real missed opportunity, recorded, never repaired
    maturation     on the instrument's OWN realised bar calendar
    independence   overlapping horizons are one observation, not several
    refusal        withdrawn, invalidated, unhashed or unpriced never advance

Every test is hermetic: the canonical registry, the accrual store and the
originating release's research root are redirected into pytest temp roots, and
no test reads a live production store, promotes a model, allocates capital,
changes a holding or creates an order. Every test also pins ``today``
explicitly - a suite whose verdicts moved with the wall clock would pass today
and fail tomorrow for no reason anybody could find.
"""
from __future__ import annotations

import ast
import json
from datetime import date, timedelta
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

from paper_trader.alpha_agent import r58 as R58                          # noqa: E402
from paper_trader.alpha_agent.r52 import runtime as R52RT                # noqa: E402
from paper_trader.api import canonical_forward_accrual as CFA            # noqa: E402
from paper_trader.api import forward_challenger_registry as FCR          # noqa: E402
from paper_trader.api import prospective_adoption as PA                  # noqa: E402
from paper_trader.engine import shadow_portfolio_evidence as kernel      # noqa: E402


CHALLENGER = "R58_DISCLOSURE_INTENSITY_V1"
FREEZE_ID = "H_bf7c02b3_37d591c4b3c4"
FREEZE_SESSION = "2026-09-03"
REGISTERED_ON = "2026-09-09"
FIRST_OBSERVATION = "2026-09-10"
RECORD_HASH = "8699962b5d3974d2b921522ee78a4c561060e2e22db7c6997372b2ceaa9163bf"

#: A short, explicit NYSE-shaped run of sessions. 2026-09-05 and 2026-09-06 are
#: a weekend and 2026-09-07 is Labor Day, so none of them appears.
EQUITY_SESSIONS = ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04",
                   "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11",
                   "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
                   "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23",
                   "2026-09-24", "2026-09-25", "2026-09-28", "2026-09-29",
                   "2026-09-30", "2026-10-01", "2026-10-02", "2026-10-05",
                   "2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09",
                   "2026-10-12", "2026-10-13"]

BOOK = {"AAA": 0.25, "BBB": 0.25, "CCC": 0.25, "DDD": 0.25}


# =========================================================================== #
# Fixtures - every store redirected, nothing live touched
# =========================================================================== #
@pytest.fixture()
def stores(tmp_path, monkeypatch):
    registry = tmp_path / "registry"
    accrual = tmp_path / "accrual"
    r58root = tmp_path / "r58"
    monkeypatch.setenv(FCR.REGISTRY_DIR_ENV, str(registry))
    monkeypatch.setenv(CFA.STORE_DIR_ENV, str(accrual))
    monkeypatch.setenv(R58.RESEARCH_ROOT_ENV, str(r58root))
    return {"registry": registry, "accrual": accrual, "r58": r58root}


def write_frozen_decision(root, challenger_id=CHALLENGER, *,
                          session=FREEZE_SESSION, weights=None,
                          record_hash=RECORD_HASH, cadence=21, horizon=21,
                          bps=12.5):
    """One immutable frozen decision, shaped exactly as R58 writes it."""
    d = Path(root) / "challengers"
    d.mkdir(parents=True, exist_ok=True)
    rec = {
        "challenger_id": challenger_id,
        "eligible_session": session,
        "weights": dict(weights if weights is not None else BOOK),
        "record_hash": record_hash,
        "spec_hash": "spec_%s" % challenger_id,
        "weights_hash": "weights_%s" % challenger_id,
        "n_held": len(weights if weights is not None else BOOK),
        "construction": {"type": "LONG_ONLY_EQUAL_WEIGHT_TOP_N",
                         "top_n": 50, "cash_weight": 0.0,
                         "rebalance_cadence_sessions": cadence,
                         "evaluation_horizon_sessions": horizon},
        "cost_policy": {"bps_per_side": bps,
                        "charged_to": "strategy AND benchmark symmetrically"},
        "inception_rule": ("signal uses information available through the close "
                           "of %s; forward evidence begins strictly after "
                           "inception and is NEVER back-filled" % session),
    }
    (d / ("%s.json" % challenger_id)).write_text(
        json.dumps(rec, indent=1, sort_keys=True), encoding="utf-8")
    return rec


def identity(challenger_id=CHALLENGER, *, asset_class="US_EQUITY",
             release="R58", horizon=21, record_hash=RECORD_HASH,
             identity_hash=None):
    return {
        "challenger_id": challenger_id,
        "freeze_id": FREEZE_ID,
        "freeze_record_hash": record_hash,
        "model_spec_hash": "model_%s" % challenger_id,
        "feature_snapshot_hash": "r58_challenger_freeze",
        "asset_class": asset_class,
        "release": release,
        "model_family": "RANK_TOPN",
        "horizon_sessions": horizon,
        "instrument_scope": [],
        "inception": FREEZE_SESSION,
        "identity_hash": identity_hash or ("ih_%s_%s" % (challenger_id,
                                                         asset_class)),
    }


ACTIVE = {"lifecycle_state": "ACTIVE", "adoptable": True,
          "never_resurrectable": False, "evidence": "NO_PERSISTED_FACT"}


def register(ident=None, *, lifecycle=None, starts=REGISTERED_ON,
             challenger_class="FORWARD_SIGNAL_CANONICAL"):
    """Register through the CANONICAL registrar - never a hand-built record."""
    out = FCR.register_forward_challenger(
        identity=ident or identity(), observation_clock_starts=starts,
        challenger_class=challenger_class, lifecycle=lifecycle or dict(ACTIVE))
    assert out["outcome"] in (FCR.REGISTERED, FCR.ALREADY_REGISTERED), out
    return out["registration"]


def series(tickers=None, sessions=None, *, start=100.0, step=0.5,
           missing=()):  # noqa: B006
    """A panel in the estate's own ``{ticker: {dates, adj}}`` shape."""
    tickers = list(tickers or BOOK.keys())
    sessions = list(sessions if sessions is not None else EQUITY_SESSIONS)
    out = {}
    for k, tk in enumerate(tickers):
        if tk in missing:
            continue
        out[tk] = {"dates": list(sessions),
                   "adj": [start + step * (i + k) for i in range(len(sessions))]}
    return out


def upto(session):
    return [s for s in EQUITY_SESSIONS if s <= session]


def advance(panel_upto, *, today, execute=True, panel=None, **kw):
    """One accrual run with the panel and the date BOTH pinned."""
    if panel is None:
        panel = {"series": series(sessions=upto(panel_upto))}
    return CFA.advance_canonical_forward_accrual(
        price_panel=panel, today=today, execute=execute, **kw)


def one(out, identity_hash=None):
    rows = out["challengers"]
    if identity_hash is None:
        assert len(rows) == 1, [r["challenger_id"] for r in rows]
        return rows[0]
    return [r for r in rows if r["identity_hash"] == identity_hash][0]


def cell(row, session):
    return [c for c in row["cells"] if c["decision_session"] == session][0]


def files_in(store):
    return sorted(str(p) for p in Path(store).rglob("*.json"))


def evidence_files_in(store):
    """Every file EXCEPT the read model the run publishes.

    A run always republishes its projection - that is the point of it - so a
    "wrote nothing" assertion has to be about EVIDENCE (emissions and
    forfeitures), not about the store being untouched.
    """
    return sorted(p for p in files_in(store)
                  if not p.endswith(CFA.PROJECTION_ARTIFACT))


# --------------------------------------------------------------------------- #
# Source probes. A module's PROSE is not its behaviour: an architecture test
# that greps the docstring proves the author avoided a word, not that the code
# avoided a dependency. These read the parse tree instead.
# --------------------------------------------------------------------------- #
def _tree(rel):
    return ast.parse((REPO / rel).read_text(encoding="utf-8"))


def imported_modules(rel):
    """Every module name this file imports, at any depth."""
    out = set()
    for n in ast.walk(_tree(rel)):
        if isinstance(n, ast.Import):
            out.update(a.name for a in n.names)
        elif isinstance(n, ast.ImportFrom):
            base = n.module or ""
            out.add(base)
            out.update("%s.%s" % (base, a.name) for a in n.names)
    return out


def called_names(rel):
    """Every dotted callee this file invokes."""
    out = set()
    for n in ast.walk(_tree(rel)):
        if not isinstance(n, ast.Call):
            continue
        f, parts = n.func, []
        while isinstance(f, ast.Attribute):
            parts.append(f.attr)
            f = f.value
        if isinstance(f, ast.Name):
            parts.append(f.id)
        if parts:
            out.add(".".join(reversed(parts)))
    return out


def code_without_docstring(rel):
    src = (REPO / rel).read_text(encoding="utf-8")
    doc = ast.get_docstring(_tree(rel), clean=False)
    return src.replace(doc, "", 1) if doc else src


ACCRUAL_SRC = "api/canonical_forward_accrual.py"
RUNTIME_SRC = "alpha_agent/r52/runtime.py"


# =========================================================================== #
# 1. DISCOVERY
# =========================================================================== #
def test_01_adopted_registration_is_discovered_automatically(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    out = advance("2026-09-09", today="2026-09-09", execute=False)
    assert out["n_registered"] == 1
    row = one(out)
    assert row["challenger_id"] == CHALLENGER
    assert row["identity_hash"] == reg["identity"]["identity_hash"]
    assert out["registrar_owner"] == "api.forward_challenger_registry"


def test_01b_discovery_needs_no_second_registry(stores):
    write_frozen_decision(stores["r58"])
    register()
    code = code_without_docstring(ACCRUAL_SRC)
    assert "register_forward_challenger(" not in code
    assert "load_registrations(" in code


# =========================================================================== #
# 2. THE CORRECT SILENCE - only the NEXT boundary takes its turn
# =========================================================================== #
def test_02_only_the_next_boundary_is_ever_due(stores):
    write_frozen_decision(stores["r58"], cadence=1)
    register()
    out = advance("2026-09-14", today="2026-09-09", execute=True)
    row = one(out)
    assert out["n_emitted_this_run"] == 1
    assert cell(row, FIRST_OBSERVATION)["state"] == CFA.ACC_EMITTED
    later = [c for c in row["cells"] if c["decision_session"] > FIRST_OBSERVATION]
    assert len(later) >= 2, "the cadence grid must reach past the first session"
    assert {c["state"] for c in later} == {CFA.ACC_NOT_DUE}
    assert {c["reason"] for c in later} <= set(CFA.NOT_DUE_REASONS)
    # The boundary immediately after the emitted one takes the next turn and is
    # answered on its merits: no governed decision was frozen for it.
    assert later[0]["reason"] == CFA.NOT_DUE_AWAITING_NEW_FREEZE
    # Everything beyond that is simply not its turn yet.
    assert {c["reason"] for c in later[1:]} == {CFA.NOT_DUE_SESSION_NOT_REACHED}


def test_02b_nothing_due_writes_nothing_at_all(stores):
    write_frozen_decision(stores["r58"])
    register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    before = files_in(stores["accrual"])
    out = advance("2026-09-09", today="2026-09-09", execute=True)
    assert out["n_emitted_this_run"] == 0
    assert out["n_forfeitures_recorded_this_run"] == 0
    assert files_in(stores["accrual"]) == before


def test_02c_a_registration_with_no_panel_emits_nothing(stores):
    write_frozen_decision(stores["r58"])
    register()
    out = advance(None, today="2026-09-09", panel={"series": {}}, execute=True)
    assert out["n_emitted_this_run"] == 0
    assert evidence_files_in(stores["accrual"]) == []
    # The run still publishes its read model, and it says zero.
    proj = CFA.load_accrual_projection()
    assert [r["predictions_emitted"] for r in proj.values()] == [0]


# =========================================================================== #
# 3-4. EMISSION - exactly once, strictly before its session, never twice
# =========================================================================== #
def test_03_due_challenger_emits_exactly_once(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    out = advance("2026-09-09", today="2026-09-09", execute=True)
    assert out["n_emitted_this_run"] == 1
    row = one(out)
    assert row["state"] == CFA.ACC_EMITTED
    assert row["predictions_emitted"] == 1
    assert row["last_emission_session"] == FIRST_OBSERVATION
    rows = CFA.load_emissions(reg["identity"]["identity_hash"])
    assert len(rows) == 1
    assert rows[0]["decision_session"] == FIRST_OBSERVATION
    assert rows[0]["backfilled"] is False


def test_03c_one_instant_decides_the_stamp_and_the_arrival(stores):
    """The run reads its clock ONCE.

    A run that started at 23:59:59 UTC and asked a second time whether the
    session had arrived would forfeit an observation it was entitled to emit a
    second earlier. ``now`` supplies both answers.
    """
    from datetime import datetime as _dt, timezone as _tz
    write_frozen_decision(stores["r58"])
    reg = register()
    out = CFA.advance_canonical_forward_accrual(
        now=_dt(2026, 9, 9, 23, 59, 59, tzinfo=_tz.utc),
        price_panel={"series": series(sessions=upto("2026-09-09"))},
        execute=True)
    assert out["n_emitted_this_run"] == 1
    row = CFA.load_emissions(reg["identity"]["identity_hash"])[0]
    assert row["emitted_at"].startswith("2026-09-09T23:59:59")
    assert row["decision_session"] == FIRST_OBSERVATION
    assert one(out)["today"] == "2026-09-09"


def test_03d_a_run_after_midnight_forfeits_rather_than_stamping_late(stores):
    from datetime import datetime as _dt, timezone as _tz
    write_frozen_decision(stores["r58"])
    reg = register()
    out = CFA.advance_canonical_forward_accrual(
        now=_dt(2026, 9, 10, 0, 0, 1, tzinfo=_tz.utc),
        price_panel={"series": series(sessions=upto("2026-09-09"))},
        execute=True)
    assert out["n_emitted_this_run"] == 0
    assert out["n_forfeitures_recorded_this_run"] == 1
    assert CFA.load_emissions(reg["identity"]["identity_hash"]) == []


def test_03b_the_emission_is_strictly_earlier_than_its_session(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    row = CFA.load_emissions(reg["identity"]["identity_hash"])[0]
    # Emitted on the 9th, stamped for the 10th: the emitter has seen neither
    # the entry mark nor any outcome.
    assert row["emitted_at"][:10] <= "2026-09-09"
    assert row["decision_session"] == FIRST_OBSERVATION
    assert row["emitted_at"][:10] < row["decision_session"]
    assert row["forward_evidence_starts_after"] == FIRST_OBSERVATION


def test_04_retry_does_not_duplicate(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    first = advance("2026-09-09", today="2026-09-09", execute=True)
    second = advance("2026-09-09", today="2026-09-09", execute=True)
    assert first["n_emitted_this_run"] == 1
    assert second["n_emitted_this_run"] == 0
    assert len(CFA.load_emissions(reg["identity"]["identity_hash"])) == 1


def test_04b_a_second_emission_is_refused_by_the_store_itself(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    book = CFA.resolve_frozen_decision(reg, None)
    a = CFA.emit_prospective_prediction(registration=reg,
                                        decision_session=FIRST_OBSERVATION,
                                        book=book)
    b = CFA.emit_prospective_prediction(registration=reg,
                                        decision_session=FIRST_OBSERVATION,
                                        book=book)
    assert a["outcome"] == CFA.EMIT_WROTE
    assert b["outcome"] == CFA.EMIT_DUPLICATE
    assert b["emission"]["emitted_at"] == a["emission"]["emitted_at"]


# =========================================================================== #
# 5-6. FORFEITURE - a real loss, recorded, never manufactured
# =========================================================================== #
def test_05_an_arrived_session_becomes_forfeiture_not_backfill(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    # The runtime does not run until the 10th has begun. The opportunity to
    # decide BEFORE that session is gone and may never be written.
    out = advance("2026-09-10", today="2026-09-10", execute=True)
    row = one(out)
    assert out["n_forfeitures_recorded_this_run"] == 1
    assert row["predictions_emitted"] == 0
    assert row["forfeitures_recorded"] == 1
    forf = CFA.load_forfeitures(reg["identity"]["identity_hash"])
    assert forf[0]["decision_session"] == FIRST_OBSERVATION
    assert forf[0]["reason"] == CFA.FORFEIT_WINDOW_CLOSED
    assert forf[0]["backfill_refused"] is True
    assert forf[0]["may_never_be_reconstructed"] is True


def test_05b_forfeiture_is_idempotent(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    advance("2026-09-11", today="2026-09-11", execute=True)
    again = advance("2026-09-11", today="2026-09-11", execute=True)
    assert again["n_forfeitures_recorded_this_run"] == 0
    assert len(CFA.load_forfeitures(reg["identity"]["identity_hash"])) == 1


def test_06_no_past_observation_is_ever_synthesised(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    ih = reg["identity"]["identity_hash"]
    # Run a month late: every grid session has arrived unemitted.
    advance("2026-10-13", today="2026-10-13", execute=True)
    assert [e["decision_session"] for e in CFA.load_emissions(ih)] == []
    assert [f["decision_session"]
            for f in CFA.load_forfeitures(ih)] == [FIRST_OBSERVATION]


def test_06b_a_forfeited_session_can_never_be_emitted_afterwards(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    advance("2026-09-11", today="2026-09-11", execute=True)
    # Even handed an earlier date afterwards, the window stays shut.
    out = advance("2026-09-09", today="2026-09-09", execute=True)
    assert cell(one(out), FIRST_OBSERVATION)["state"] == CFA.ACC_FORFEITED
    assert CFA.load_emissions(reg["identity"]["identity_hash"]) == []


def test_06c_a_cadence_boundary_without_a_new_freeze_is_not_a_forfeiture(stores):
    """The distinction that keeps the forfeiture count honest.

    No decision was frozen for a later cadence boundary, so there was nothing to
    emit. That is a fact about governance, never a missed opportunity.
    """
    write_frozen_decision(stores["r58"], cadence=1)
    reg = register()
    out = advance("2026-09-14", today="2026-09-14", execute=True)
    row = one(out)
    later = [c for c in row["cells"] if c["decision_session"] > FIRST_OBSERVATION]
    assert later
    assert {c["state"] for c in later} == {CFA.ACC_NOT_DUE}
    assert {c["reason"] for c in later} == {CFA.NOT_DUE_AWAITING_NEW_FREEZE}
    assert row["forfeitures_recorded"] == 1     # only the first, real, loss
    assert reg["challenger_id"] == CHALLENGER


def test_06d_forfeiture_and_emission_are_mutually_exclusive(stores):
    write_frozen_decision(stores["r58"], cadence=1)
    register()
    out = advance("2026-09-14", today="2026-09-09", execute=True)
    row = one(out)
    sessions_emitted = {e["decision_session"] for e in row["emissions"]}
    sessions_forfeited = {f["decision_session"] for f in row["forfeitures"]}
    assert sessions_emitted & sessions_forfeited == set()


# =========================================================================== #
# 7-8. THE FROZEN IDENTITY, and the information it may use
# =========================================================================== #
def test_07_frozen_spec_identity_is_preserved(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    row = CFA.load_emissions(reg["identity"]["identity_hash"])[0]
    sid = row["prediction"]["strategy_identity"]
    assert sid["freeze_record_hash"] == RECORD_HASH
    assert sid["weights_hash"] == "weights_%s" % CHALLENGER
    assert sid["spec_hash"] == "spec_%s" % CHALLENGER
    assert sid["identity_hash"] == reg["identity"]["identity_hash"]
    assert row["prediction"]["weights"] == {k: round(v, 8)
                                            for k, v in BOOK.items()}


def test_07b_a_hash_mismatch_is_refused_not_repaired(stores):
    write_frozen_decision(stores["r58"], record_hash="a" * 64)
    reg = register(identity(record_hash="b" * 64))
    out = advance("2026-09-09", today="2026-09-09", execute=True)
    row = one(out)
    assert row["state"] == CFA.ACC_INTEGRITY_BLOCKED
    assert row["latest_blocker"] == CFA.INTEGRITY_HASH_MISMATCH
    assert CFA.load_emissions(reg["identity"]["identity_hash"]) == []


def test_08_only_information_available_at_the_freeze_is_used(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    row = CFA.load_emissions(reg["identity"]["identity_hash"])[0]
    pit = row["prediction"]["pit_input_identity"]
    assert pit["information_cutoff"] == FREEZE_SESSION
    assert pit["no_information_after_the_freeze_was_used"] is True
    assert row["prediction"]["rebalancing"] == "NONE_BUY_AND_HOLD"


def test_08b_the_kernel_refuses_to_score_a_bar_on_or_before_inception(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    row = CFA.load_emissions(reg["identity"]["identity_hash"])[0]
    fwd = kernel.forward_sessions(row["prediction"],
                                  series(sessions=upto("2026-09-11")))
    assert all(d > FIRST_OBSERVATION for d in fwd)
    assert FIRST_OBSERVATION not in fwd


def test_08c_no_second_signal_implementation_exists(stores):
    code = code_without_docstring(ACCRUAL_SRC)
    for banned in ("xs_rank01", "xs_z", "numpy", "argsort",
                   "def _book_from_scores"):
        assert banned not in code, banned
    assert "record_hash" in code and "resolve_frozen_decision" in code


# =========================================================================== #
# 9-10. CALENDARS - each asset class on its own
# =========================================================================== #
def test_09_equity_holiday_calendar_is_respected(stores):
    """Registered on Friday 2026-09-04, the first observation is NOT Labor Day."""
    write_frozen_decision(stores["r58"])
    reg = register(starts="2026-09-04")
    clock = reg["observation_clock"]
    assert clock["state"] == FCR.CLOCK_RESOLVED
    assert clock["first_eligible_observation_session"] == "2026-09-08"
    assert clock["first_eligible_observation_session"] != "2026-09-07"
    assert clock["uses_weekday_arithmetic"] is False


def test_09b_a_holiday_never_enters_an_equity_decision_grid(stores):
    write_frozen_decision(stores["r58"])
    reg = register(starts="2026-09-04")
    out = advance("2026-09-11", today="2026-09-04", execute=False)
    grid = one(out)["decision_grid"]
    assert "2026-09-07" not in grid
    assert "2026-09-05" not in grid and "2026-09-06" not in grid
    assert reg["asset_class"] == "US_EQUITY"


def test_10_a_futures_class_is_not_forced_onto_nyse_sessions(stores):
    """A market that trades on Labor Day observes on Labor Day.

    The registrar publishes NO session clock for an instrument-calendar class -
    deliberately - and the accrual owner therefore reads the instrument's own
    realised bars. Imposing equity sessions here would be a fabrication.
    """
    fx_book = {"6E": 0.5, "6J": 0.5}
    write_frozen_decision(stores["r58"], challenger_id="R58_FX_CARRY_V1",
                          weights=fx_book, record_hash="c" * 64, cadence=1)
    reg = register(identity("R58_FX_CARRY_V1", asset_class="FX_FUTURES",
                            record_hash="c" * 64, identity_hash="ih_fx"),
                   starts="2026-09-04")
    assert reg["observation_clock"]["state"] == FCR.CLOCK_INSTRUMENT_OWNED
    assert reg["observation_clock"]["first_eligible_observation_session"] is None

    # The FX panel prints on 2026-09-07, which the NYSE does not keep.
    fx_sessions = ["2026-09-03", "2026-09-04", "2026-09-07", "2026-09-08"]
    out = advance(None, today="2026-09-04", execute=False,
                  panel={"series": series(tickers=fx_book.keys(),
                                          sessions=fx_sessions)})
    row = one(out, "ih_fx")
    # Labor Day IS this market's next session, and IS its decision session.
    assert row["decision_grid"][0] == "2026-09-07"
    assert row["observation_calendar_owner"] == FCR.CAL_INSTRUMENT_REALISED
    assert row["observation_calendar_owner"] != FCR.CAL_EXCHANGE_NYSE
    assert cell(row, "2026-09-07")["state"] == CFA.ACC_DUE


def test_10b_the_instrument_calendar_decides_a_futures_maturity(stores):
    fx_book = {"6E": 1.0}
    write_frozen_decision(stores["r58"], challenger_id="R58_FX_CARRY_V1",
                          weights=fx_book, record_hash="c" * 64, horizon=2)
    register(identity("R58_FX_CARRY_V1", asset_class="FX_FUTURES",
                      record_hash="c" * 64, identity_hash="ih_fx"))
    fx = ["2026-09-09", "2026-09-10", "2026-09-11", "2026-09-12", "2026-09-13"]
    panel = {"series": series(tickers=fx_book.keys(), sessions=fx)}
    advance(None, today="2026-09-09", panel=panel, execute=True)
    out = advance(None, today="2026-09-14", panel=panel, execute=True)
    row = one(out, "ih_fx")
    assert row["predictions_emitted"] == 1
    m = row["maturation"][0]
    assert m["decision_session"] == FIRST_OBSERVATION
    # 2026-09-12 and 2026-09-13 are a weekend the NYSE does not keep; this
    # market prints on both, and they ARE its sessions.
    assert m["maturity_session"] == "2026-09-12"
    assert m["matured"] is True
    assert date.fromisoformat("2026-09-12").weekday() == 5


# =========================================================================== #
# 11-12. MATURATION and INDEPENDENCE
# =========================================================================== #
def test_11_outcome_matures_only_after_the_defined_horizon(stores):
    write_frozen_decision(stores["r58"], horizon=3)
    reg = register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    emission = CFA.load_emissions(reg["identity"]["identity_hash"])[0]

    early = CFA.mature_emission(emission, series(sessions=upto("2026-09-14")))
    assert early["forward_sessions_observed"] == 2       # 09-11, 09-14
    assert early["matured"] is False
    assert early["maturity_session"] is None

    done = CFA.mature_emission(emission, series(sessions=upto("2026-09-15")))
    assert done["forward_sessions_observed"] == 3        # + 09-15
    assert done["matured"] is True
    assert done["maturity_session"] == "2026-09-15"


def test_11b_maturity_counts_realised_bars_not_calendar_days(stores):
    write_frozen_decision(stores["r58"], horizon=2)
    reg = register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    emission = CFA.load_emissions(reg["identity"]["identity_hash"])[0]
    m = CFA.mature_emission(emission, series(sessions=upto("2026-09-14")))
    # Two realised sessions after 09-10 are 09-11 and 09-14: the weekend is
    # skipped by OBSERVATION, not by a holiday table.
    assert m["maturity_session"] == "2026-09-14"
    d0 = date.fromisoformat(FIRST_OBSERVATION)
    assert date.fromisoformat(m["maturity_session"]) - d0 == timedelta(days=4)


def test_12_effective_independent_observations_advance_correctly():
    """Overlapping horizons are ONE observation observed repeatedly."""
    overlapping = [
        {"matured": True, "decision_session": "2026-09-10",
         "maturity_session": "2026-10-09"},
        {"matured": True, "decision_session": "2026-09-11",
         "maturity_session": "2026-10-12"},
        {"matured": True, "decision_session": "2026-10-12",
         "maturity_session": "2026-11-10"},
    ]
    out = CFA.effective_independent_observations(overlapping)
    assert out["matured_observations"] == 3
    assert out["effective_independent_observations"] == 2
    assert [w["decision_session"] for w in out["independent_windows"]] == [
        "2026-09-10", "2026-10-12"]


def test_12b_pending_observations_are_never_counted_as_evidence():
    rows = [{"matured": False, "decision_session": "2026-09-10",
             "maturity_session": None}]
    out = CFA.effective_independent_observations(rows)
    assert out["matured_observations"] == 0
    assert out["effective_independent_observations"] == 0


def test_12c_one_emission_gives_one_independent_observation(stores):
    write_frozen_decision(stores["r58"], horizon=2)
    reg = register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    out = advance("2026-09-14", today="2026-09-14", execute=True)
    row = one(out)
    assert row["matured_observations"] == 1
    assert row["effective_independent_observations"] == 1
    assert row["pending_observations"] == 0
    assert reg["horizon_sessions"] == 21     # the registration is untouched


# =========================================================================== #
# 13-16. EXPLICIT REFUSALS
# =========================================================================== #
def test_13_missing_required_data_blocks_explicitly(stores):
    write_frozen_decision(stores["r58"])
    register()
    out = advance(None, today="2026-09-09", execute=True,
                  panel={"series": series(sessions=upto("2026-09-09"),
                                          missing=("CCC", "DDD"))})
    row = one(out)
    assert row["state"] == CFA.ACC_DATA_BLOCKED
    assert row["latest_blocker"] == CFA.BLOCK_COVERAGE
    assert row["predictions_emitted"] == 0


def test_13b_an_unreadable_frozen_artifact_blocks_explicitly(stores):
    register()                             # no frozen decision was written
    out = advance("2026-09-09", today="2026-09-09", execute=True)
    row = one(out)
    assert row["state"] == CFA.ACC_DATA_BLOCKED
    assert row["latest_blocker"] == CFA.BLOCK_SPEC_MISSING


def test_13c_an_undeclared_release_is_never_reconstructed(stores):
    write_frozen_decision(stores["r58"])
    register(identity(release="R99", identity_hash="ih_r99"))
    out = advance("2026-09-09", today="2026-09-09", execute=True)
    row = one(out, "ih_r99")
    assert row["state"] == CFA.ACC_DATA_BLOCKED
    assert row["latest_blocker"] == CFA.BLOCK_SPEC_UNRESOLVABLE
    assert "R99" in row["frozen_decision"]["detail"]
    assert "R58" in CFA.FROZEN_DECISION_OWNERS


def test_13d_an_empty_panel_blocks_rather_than_forfeits(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    out = advance(None, today="2026-09-11", panel={"series": {}}, execute=True)
    row = one(out)
    assert row["state"] == CFA.ACC_DATA_BLOCKED
    assert row["latest_blocker"] == CFA.BLOCK_NO_PANEL
    assert CFA.load_forfeitures(reg["identity"]["identity_hash"]) == []


def test_14_bad_identity_blocks_explicitly(stores):
    write_frozen_decision(stores["r58"])
    reg = dict(register())
    reg["identity"] = {}
    out = CFA.assess_registration(registration=reg,
                                  series=series(sessions=upto("2026-09-09")),
                                  today="2026-09-09")
    assert out["state"] == CFA.ACC_INTEGRITY_BLOCKED
    assert out["latest_blocker"] == CFA.INTEGRITY_NO_IDENTITY
    res = CFA.emit_prospective_prediction(registration=reg,
                                          decision_session=FIRST_OBSERVATION,
                                          book={"weights": BOOK})
    assert res["outcome"] == CFA.EMIT_REFUSED


@pytest.mark.parametrize("state", ["WITHDRAWN", "INVALIDATED", "SUPERSEDED"])
def test_15_16_a_closed_lifecycle_never_advances(stores, state):
    write_frozen_decision(stores["r58"])
    reg = register()
    closed = {CHALLENGER: {"lifecycle_state": state, "adoptable": False,
                           "never_resurrectable": state == "WITHDRAWN"}}
    out = advance("2026-09-09", today="2026-09-09", execute=True,
                  lifecycle_by_challenger=closed)
    row = one(out)
    assert row["state"] == CFA.ACC_INTEGRITY_BLOCKED
    assert row["latest_blocker"] == CFA.INTEGRITY_LIFECYCLE_CLOSED
    assert row["lifecycle_state"] == state
    assert row["lifecycle_rechecked"] is True
    assert CFA.load_emissions(reg["identity"]["identity_hash"]) == []


def test_15b_the_registrar_refuses_a_withdrawn_freeze_before_any_of_this(stores):
    out = FCR.register_forward_challenger(
        identity=identity(), observation_clock_starts=REGISTERED_ON,
        challenger_class="FORWARD_SIGNAL_CANONICAL",
        lifecycle={"lifecycle_state": "WITHDRAWN", "adoptable": False,
                   "never_resurrectable": True})
    assert out["outcome"] == FCR.REFUSED_LIFECYCLE
    assert out["registered"] is False
    assert FCR.load_registrations() == []


def test_16b_a_lifecycle_that_cannot_be_rechecked_is_reported_not_assumed(stores):
    write_frozen_decision(stores["r58"])
    register()
    out = advance("2026-09-09", today="2026-09-09", execute=False,
                  lifecycle_by_challenger=None)
    row = one(out)
    assert row["lifecycle_rechecked"] is False
    assert row["lifecycle_state"] == "ACTIVE"


def test_16c_a_withdrawal_after_registration_stops_the_accrual(stores):
    """The case the registration's own immutable verdict cannot catch."""
    write_frozen_decision(stores["r58"], cadence=1)
    reg = register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    assert len(CFA.load_emissions(reg["identity"]["identity_hash"])) == 1
    closed = {CHALLENGER: {"lifecycle_state": "WITHDRAWN", "adoptable": False,
                           "never_resurrectable": True}}
    out = advance("2026-09-11", today="2026-09-10", execute=True,
                  lifecycle_by_challenger=closed)
    assert out["n_emitted_this_run"] == 0
    assert one(out)["state"] == CFA.ACC_INTEGRITY_BLOCKED
    assert len(CFA.load_emissions(reg["identity"]["identity_hash"])) == 1


# =========================================================================== #
# 17-18. THE EXISTING FORWARD ESTATE IS UNTOUCHED
# =========================================================================== #
def test_17_r46_forward_estate_remains_compatible(stores):
    imports = imported_modules(ACCRUAL_SRC)
    assert not [m for m in imports if "r46" in m], sorted(imports)
    assert not [m for m in imports if "r52" in m], sorted(imports)
    code = code_without_docstring(ACCRUAL_SRC)
    for banned in ("CAMPAIGN_ID", "contract_hash", "ALL_SPECS"):
        assert banned not in code, banned


def test_17b_the_runtime_stage_is_additive_only(stores):
    src = (REPO / RUNTIME_SRC).read_text(encoding="utf-8")
    assert "AD.advance(campaign_id" in src
    assert "FF.sweep(started" in src
    assert src.index("AD.advance(campaign_id") < src.index("FF.sweep(started")
    assert (src.index("FF.sweep(started")
            < src.index("advance_canonical_forward_accrual("))


def test_18_r56_portfolio_challengers_are_unaffected(stores):
    code = code_without_docstring(ACCRUAL_SRC)
    assert "from paper_trader.engine import shadow_portfolio_evidence" in code
    assert "api.shadow_portfolio_evidence" not in code
    assert "PAPER_TRADER_R56_SHADOW_DIR" not in code
    assert "freeze_challengers(" not in code


def test_18b_the_accrual_store_is_its_own(stores):
    assert CFA.STORE_DIR_ENV == "PAPER_TRADER_CANONICAL_FORWARD_ACCRUAL_DIR"
    assert str(CFA.store_dir()) == str(stores["accrual"])
    assert "r56" not in str(CFA.store_dir()).lower()


# =========================================================================== #
# 19. SINGLETON / IDEMPOTENCE / NO SECOND SCHEDULER
# =========================================================================== #
def test_19_runtime_remains_singleton_and_idempotent(stores):
    src = (REPO / RUNTIME_SRC).read_text(encoding="utf-8")
    assert "RL.acquire_path(_lock_file()" in src
    assert src.count("RUNTIME_LOCK_NAME = ") == 1
    assert src.index("RL.acquire_path(_lock_file()") < src.index(
        "advance_canonical_forward_accrual(")
    imports = imported_modules(ACCRUAL_SRC)
    for banned in ("threading", "sched", "asyncio", "multiprocessing",
                   "subprocess", "time"):
        assert banned not in imports, banned
    calls = called_names(ACCRUAL_SRC)
    assert not [c for c in calls
                if any(t in c for t in ("acquire", "Timer", "Thread"))], calls


def test_19b_no_second_scheduler_was_created(stores):
    code = code_without_docstring(ACCRUAL_SRC)
    assert "def research_runtime_cycle" not in code
    installers = sorted(p.name for p in (REPO / "scripts").glob("install_*task*.ps1"))
    assert installers == ["install_alpha_agent_tasks.ps1",
                          "install_information_collection_task.ps1",
                          "install_intraday_emission_task.ps1",
                          "install_research_runtime_task.ps1"], installers


# =========================================================================== #
# 20-24. SAFETY - what this slice may never do
# =========================================================================== #
def test_20_21_22_23_24_no_promotion_no_portfolio_no_order_no_close(stores):
    # CALLS, not prose: the safety block legitimately contains the WORD
    # "ran_daily_close", and a grep that failed on it would only teach the
    # author to stop declaring what the module does not do.
    calls = called_names(ACCRUAL_SRC)
    banned = ("promote", "sleeve", "order", "fill", "daily_close",
              "portfolio_cycle", "approve", "governed_decision", "book_nav",
              "paper_trading_desk", "holdings", "rebalance")
    for c in calls:
        low = c.lower()
        for b in banned:
            assert b not in low, "%s -> %s" % (c, b)
    imports = imported_modules(ACCRUAL_SRC)
    for b in ("paper_trader.api.daily_close", "paper_trader.api.orders",
              "paper_trader.api.paper_trading_desk",
              "paper_trader.api.portfolio_cycle"):
        assert b not in imports, b


def test_20b_the_safety_block_is_declared_and_negative(stores):
    write_frozen_decision(stores["r58"])
    register()
    out = advance("2026-09-09", today="2026-09-09", execute=True)
    safety = out["safety"]
    for key in ("promoted_model", "automatic_model_promotion_allowed",
                "activated_sleeve", "allocated_capital", "changed_holdings",
                "changed_cash", "changed_nav", "created_orders",
                "created_fills", "created_proposal", "approved_anything",
                "ran_daily_close", "called_portfolio_cycle",
                "backfilled_forward_evidence", "rewrote_history",
                "amends_registrations", "automation_enabled"):
        assert safety[key] is False, key
    assert safety["manual_review_remains_mandatory"] is True
    assert out["automatic_promotion_allowed"] is False


def test_21b_registrations_are_never_amended(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    path = (Path(stores["registry"]) / "registrations"
            / ("%s.json" % reg["identity"]["identity_hash"]))
    before = path.read_bytes()
    advance("2026-09-09", today="2026-09-09", execute=True)
    assert path.read_bytes() == before


def test_23b_the_read_model_writes_nothing_at_all(stores):
    write_frozen_decision(stores["r58"])
    register()
    CFA.load_canonical_forward_accrual(
        price_panel={"series": series(sessions=upto("2026-09-11"))},
        today="2026-09-11")
    assert files_in(stores["accrual"]) == []


def test_23c_the_read_model_and_the_writer_agree(stores):
    write_frozen_decision(stores["r58"])
    register()
    panel = {"series": series(sessions=upto("2026-09-09"))}
    ro = CFA.load_canonical_forward_accrual(price_panel=panel,
                                            today="2026-09-09")
    assert ro["executed"] is False
    assert one(ro)["state"] == CFA.ACC_DUE
    rw = advance(None, today="2026-09-09", panel=panel, execute=True)
    assert rw["n_emitted_this_run"] == 1


# =========================================================================== #
# 25. HERMETICITY
# =========================================================================== #
def test_25_no_live_production_store_is_used_by_this_suite(stores):
    for owner in (CFA.store_dir(), Path(str(FCR.registry_dir())),
                  R58.research_root()):
        p = str(owner)
        assert "Stock_Prediction_app_data" not in p, p
        assert not p.startswith("C:\\Users\\binis\\paper_trader"), p


def test_25b_the_default_store_root_is_declared_and_separate():
    code = code_without_docstring(ACCRUAL_SRC)
    assert "_DEFAULT_STORE_DIR" in code
    assert "canonical_forward_accrual" in str(CFA._DEFAULT_STORE_DIR)


# =========================================================================== #
# The read-model seam: the registrar overlays, and computes nothing
# =========================================================================== #
def test_26_the_registrar_overlays_the_accrual_owners_counters(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    panel = {"series": series(sessions=upto("2026-09-09"))}
    advance(None, today="2026-09-09", panel=panel, execute=True)
    proj = CFA.accrual_projection(CFA.load_canonical_forward_accrual(
        price_panel=panel, today="2026-09-09"))
    ih = reg["identity"]["identity_hash"]
    assert proj[ih]["predictions_emitted"] == 1
    row = FCR.registration_row(reg, accrual=proj[ih])
    assert row["predictions_emitted"] == 1
    assert row["current_accrual_state"] == CFA.ACC_EMITTED
    assert row["accrual_owner"] == "api.canonical_forward_accrual"
    assert row["accrual_projection_supplied"] is True


def test_26d_the_run_persists_the_read_model(stores):
    """A GET must not recompute this: the outcomes route already loads the
    operational price panel once, and a second load would put a heavy
    composition back on a read path."""
    write_frozen_decision(stores["r58"])
    reg = register()
    assert CFA.load_accrual_projection() == {}
    out = advance("2026-09-09", today="2026-09-09", execute=True)
    assert out["projection_persisted"] is True
    proj = CFA.load_accrual_projection()
    ih = reg["identity"]["identity_hash"]
    assert proj[ih]["predictions_emitted"] == 1
    assert proj[ih]["current_accrual_state"] == CFA.ACC_EMITTED
    artifact = CFA.load_accrual_projection_artifact()
    assert artifact["generated_at"] == out["generated_at"]
    assert artifact["owner"] == "api.canonical_forward_accrual"
    assert artifact["read_only"] is True


def test_26e_the_read_only_view_persists_nothing(stores):
    write_frozen_decision(stores["r58"])
    register()
    CFA.load_canonical_forward_accrual(
        price_panel={"series": series(sessions=upto("2026-09-09"))},
        today="2026-09-09")
    assert CFA.load_accrual_projection() == {}
    assert files_in(stores["accrual"]) == []


def test_26f_the_outcomes_route_reads_the_artifact_not_the_panel():
    src = (REPO / "api" / "alphaagent_outcomes.py").read_text(encoding="utf-8")
    assert "CFA.load_accrual_projection()" in src
    assert "load_canonical_forward_accrual(" not in src
    assert "advance_canonical_forward_accrual(" not in src
    assert "canonical_forward_accrual_generated_at" in src


def test_26b_without_a_projection_the_registrar_still_reports_its_own_zeros(stores):
    write_frozen_decision(stores["r58"])
    reg = register()
    row = FCR.registration_row(reg)
    assert row["predictions_emitted"] == 0
    assert row["accrual_projection_supplied"] is False
    assert row["evidence_status"] == FCR.EV_AWAITING_FIRST_SESSION


def test_26c_the_registrar_computes_no_accrual_of_its_own(stores):
    src = (REPO / "api" / "forward_challenger_registry.py").read_text(
        encoding="utf-8")
    assert "canonical_forward_accrual" not in src
    assert "accrue_forward" not in src
    assert "ACCRUAL_OVERLAY_FIELDS" in src


def test_27_a_matured_registration_reads_as_accruing(stores):
    write_frozen_decision(stores["r58"], horizon=2)
    reg = register()
    advance("2026-09-09", today="2026-09-09", execute=True)
    proj = CFA.accrual_projection(CFA.load_canonical_forward_accrual(
        price_panel={"series": series(sessions=upto("2026-09-14"))},
        today="2026-09-14"))
    ih = reg["identity"]["identity_hash"]
    assert proj[ih]["matured_observations"] == 1
    row = FCR.registration_row(reg, accrual=proj[ih])
    assert row["evidence_status"] == FCR.EV_ACCRUING


# =========================================================================== #
# The runtime stage itself
# =========================================================================== #
def test_28_the_runtime_declares_the_stage_and_its_states(stores):
    src = (REPO / RUNTIME_SRC).read_text(encoding="utf-8")
    assert '"canonical_forward_accrual"' in src
    assert "_lifecycle_by_challenger()" in src
    assert "_canonical_digest(" in src
    for state in ("SUCCESS", "NOT_DUE", "DATA_BLOCKED", "FORFEITED",
                  "FAILED_RETRYABLE"):
        assert state in R52RT.STAGE_STATES


def test_28b_the_accrual_states_are_the_declared_vocabulary():
    assert CFA.ACCRUAL_STATES == ("NOT_DUE", "DUE", "EMITTED", "FORFEITED",
                                  "DATA_BLOCKED", "INTEGRITY_BLOCKED")
    out = CFA.advance_canonical_forward_accrual(registrations=[],
                                                price_panel={"series": {}},
                                                today="2026-09-09",
                                                execute=False)
    assert out["state_vocabulary"] == list(CFA.ACCRUAL_STATES)
    assert out["n_registered"] == 0


def test_29_the_adoption_owner_still_names_this_registrar(stores):
    assert PA.REGISTRARS["FORWARD_SIGNAL_CANONICAL"] == (
        "api.forward_challenger_registry")
    assert FCR.EVIDENCE_ACCRUAL_OWNERS["FORWARD_SIGNAL_CANONICAL"]


# =========================================================================== #
# 30. THE TEST SUITE MAY NEVER REACH THE PRODUCTION FORWARD-EVIDENCE STORES
#
# These take no ``stores`` fixture ON PURPOSE. They describe what an ORDINARY
# test - one that never heard of this release - resolves to. The runtime stage
# added here is reachable from every existing research-cycle test, so if the
# default resolved to production those tests would discover the operator's real
# challengers and write real emissions under their own frozen clocks. Emission
# is first-write-wins and a record is immutable, so that would not merely add a
# bad row: it would permanently consume the session and suppress the governed
# runtime's legitimate emission. The evidence would be destroyed by the act of
# testing it.
# =========================================================================== #
def test_30_the_accrual_store_default_is_hermetic_under_pytest():
    resolved = CFA.store_dir()
    assert resolved != CFA._DEFAULT_STORE_DIR, (
        "a test resolved the PRODUCTION accrual store: %s" % resolved)
    assert not str(resolved).startswith(str(CFA._DEFAULT_STORE_DIR))


def test_30b_the_registry_default_is_hermetic_under_pytest():
    resolved = FCR.registry_dir()
    assert resolved != FCR._DEFAULT_REGISTRY_DIR, (
        "a test resolved the PRODUCTION registry: %s" % resolved)
    assert not str(resolved).startswith(str(FCR._DEFAULT_REGISTRY_DIR))


def test_30c_an_ordinary_run_discovers_nothing_and_writes_nothing():
    """No ``stores`` fixture, no override - exactly a research-cycle test."""
    before = files_in(CFA.store_dir()) if CFA.store_dir().exists() else []
    out = CFA.advance_canonical_forward_accrual(
        price_panel={"series": {}}, today="2026-09-09", execute=True)
    assert out["n_registered"] == 0
    assert out["n_emitted_this_run"] == 0
    assert evidence_files_in(CFA.store_dir()) == [
        p for p in before if not p.endswith(CFA.PROJECTION_ARTIFACT)]


def test_30d_the_conftest_guard_mirrors_the_owning_constants():
    """The guard repeats two paths; drift would silently re-open the hole."""
    declared = None
    for node in _tree("tests/conftest.py").body:
        if not isinstance(node, ast.Assign):
            continue
        if any(getattr(t, "id", None) == "_FORWARD_EVIDENCE_PRODUCTION_ROOTS"
               for t in node.targets):
            declared = ast.literal_eval(node.value)
    assert declared, "tests/conftest.py no longer declares the hermetic guard"
    guard = dict((env_var, default) for env_var, default, _leaf in declared)
    assert Path(guard[FCR.REGISTRY_DIR_ENV]) == FCR._DEFAULT_REGISTRY_DIR
    assert Path(guard[CFA.STORE_DIR_ENV]) == CFA._DEFAULT_STORE_DIR
