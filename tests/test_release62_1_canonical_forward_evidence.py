"""R62.1 — CANONICAL MULTI-ASSET TRUE_FORWARD REGISTRATION.

What these tests prove:

ONE CANONICAL SIGNAL-CHALLENGER FORWARD OWNER (Workstream A)
  * every qualified freeze routes through ONE registrar table, and the two frozen
    cohorts (R46, R56) are reached by reference and are never edited;
  * the registrar writes no prediction, no outcome and no score, and names the
    canonical owner that will accrue the evidence instead of computing it.

PROSPECTIVE ADOPTION OF THE FOUR R58 FREEZES (Workstream B)
  * each of the four ACTIVE R58 freezes registers prospectively, with its EXACT
    immutable freeze identity preserved;
  * registration is idempotent, retry-safe after a partial run, and cannot
    duplicate a challenger;
  * the WITHDRAWN R59 Calendar Term Structure challenger is refused — twice, by
    two independent owners — and can never be revived.

MULTI-ASSET BY CONSTRUCTION (Workstream C)
  * equity, equity-index / rates / commodity / FX futures, volatility and
    cross-asset challengers all register on the same contract, and nothing is
    squeezed into an equity ticker semantic.

THE OBSERVATION CLOCK (Workstream D)
  * the first legitimate observation is the first eligible session STRICTLY AFTER
    registration — no session between an old inception and today is synthesised;
  * an equity challenger can never observe or mature on 2026-09-07 (Labor Day);
  * forward maturity scheduling consumes the authoritative exchange calendar, not
    weekday arithmetic, and a non-equity market keeps its OWN realised calendar.

CURRENT VS HISTORICAL RUNTIME IDENTITY (Workstreams E, F, G)
  * a completed cycle's release is PROVENANCE and can never make the current
    collection service stale;
  * every surface reads ONE authoritative current collection state;
  * the historical Sep-8 withheld event stays immutable and is labelled historical.

SAFETY (Workstream I)
  * no path leads from TRUE_FORWARD evidence to model promotion, from registration
    to a portfolio allocation, or from registration to an order or fill.

Every write path is hermetic (``tmp_path``); no production store, no live
research state, no provider, no backend and no scheduler is touched.
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from paper_trader.alpha_agent.r46 import clock as CK
from paper_trader.api import active_manager_state as ams
from paper_trader.api import forward_challenger_registry as FCR
from paper_trader.api import prospective_adoption as PA
from paper_trader.api import runtime_identity as rid
from paper_trader.engine import exchange_calendar as EC

REPO = Path(__file__).resolve().parents[1]

#: The four R58 freezes this release exists to register, with the EXACT record
#: hashes and the inception session the immutable R58 artifact recorded.
R58_FREEZES = {
    "R58_SHORT_VOLUME_PRESSURE_V1":
        "332fe398cf312388f06adb4e105dde6f784ed046332616aa898eb737a2942d3e",
    "R58_DISCLOSURE_INTENSITY_V1":
        "8699962b5d3974d2b921522ee78a4c561060e2e22db7c6997372b2ceaa9163bf",
    "R58_FUND_MOMENTUM_VETO_V1":
        "4196a017b57fa674d21e2059379647371e5821d2e81aaad38f761df9da21c083",
    "R58_FCF_PURE_V1":
        "e9ac82897fa0547d49c056599c78330bd80777aff362e92b54fe15031d5ea31d",
}
R58_INCEPTION = "2026-09-03"
R58_HORIZON = 21

#: The withdrawn R59 challenger. Its own owner's sentence is what classifies it.
R59_WITHDRAWN_ID = "R59_CALENDAR_TERM_STRUCTURE_F9BE2426"
WITHDRAWN_REASON = ("withdrawn at inception: the effective independent sample "
                    "and the validation materiality both failed once two gate "
                    "defects were fixed")

#: A registration session AFTER every R58 inception. 2026-09-08 is a Tuesday and
#: a real NYSE session; 2026-09-07 before it is Labor Day.
REGISTERED_ON = "2026-09-08"


def _r58_freeze(challenger_id: str, *, release: str = "R58",
                asset_class: str = "US_EQUITY",
                instrument_scope=None,
                inception: str = R58_INCEPTION,
                invalidated_reason=None) -> dict:
    """A research-memory freeze row shaped exactly as ``alpha_agent.r59.memory``
    returns one (``spec_json``/``forward_challenger`` already de-serialised)."""
    row = {
        "hypothesis_id": "H_%s" % challenger_id,
        "release": release,
        "origin": "HUMAN_TEMPLATE",
        "generation_method": "PROSPECTIVE_FREEZE",
        "information_family": "SHORT_VOLUME",
        "economic_family": challenger_id,
        "asset_class": asset_class,
        "model_family": "RANK_TOPN",
        "horizon_sessions": R58_HORIZON,
        "input_data_identity": "r58_challenger_freeze",
        "outcome": "FORWARD_FROZEN",
        "evidence_maturity": "PROSPECTIVE_INCEPTION",
        "spec": {"release": release, "challenger_id": challenger_id,
                 "instrument_scope": list(instrument_scope
                                          or ["AAPL", "MSFT", "NVDA"]),
                 "cost_model": {"bps_per_side": 12.5},
                 "feature_snapshot_hash": "universe_84771159b57b7885"},
        "forward_challenger": {
            "challenger_id": challenger_id,
            "inception": inception,
            "record_hash": R58_FREEZES.get(challenger_id, "h_%s" % challenger_id),
            "forward_observations_at_freeze": 0},
    }
    if invalidated_reason is not None:
        row["invalidated_reason"] = invalidated_reason
    return row


def _adopt(freeze_row, tmp_path, *, starts=REGISTERED_ON, **kw):
    return PA.adopt_prospective_freeze(
        freeze_row=freeze_row, observation_clock_starts=starts,
        confirm=PA.ADOPT_CONFIRM_TOKEN,
        adoption_dir_override=str(tmp_path / "adoption"),
        registry_dir_override=str(tmp_path / "registry"), **kw)


# =========================================================================== #
# A. ONE CANONICAL SIGNAL-CHALLENGER FORWARD OWNER
# =========================================================================== #
def test_01_there_is_exactly_one_registrar_table_and_it_names_three_owners():
    """The consolidation, stated structurally: ONE routing table, and the two
    frozen cohorts keep their own owners rather than being replaced."""
    assert PA.REGISTRARS[PA.CLASS_SIGNAL_CANONICAL] == \
        "api.forward_challenger_registry"
    assert PA.REGISTRARS[PA.CLASS_SIGNAL_R46] == "alpha_agent.r46.registry"
    assert PA.REGISTRARS[PA.CLASS_PAPER_PORTFOLIO] == \
        "api.shadow_portfolio_evidence"
    # No fourth business owner appeared beside them.
    assert len(PA.REGISTRARS) == 3


def test_02_every_non_cohort_release_routes_to_the_canonical_registrar():
    for release in ("R58", "R59", "R63", "", None):
        assert PA.classify_challenger_class({"release": release}) == \
            PA.CLASS_SIGNAL_CANONICAL
    assert PA.classify_challenger_class({"release": "R46"}) == PA.CLASS_SIGNAL_R46
    assert PA.classify_challenger_class({"release": "R56"}) == \
        PA.CLASS_PAPER_PORTFOLIO


def test_03_the_registrar_is_not_a_second_true_forward_evidence_store():
    """It registers and clocks. It never mints, scores or matures evidence."""
    src = (REPO / "api" / "forward_challenger_registry.py").read_text(
        encoding="utf-8", errors="replace").lower()
    for banned in ("def append_prediction", "def append_outcome", "def score(",
                   "def mature(", "def accrue_forward(", "def leaderboard("):
        assert banned not in src, banned
    # It NAMES the accrual owner instead of being one.
    assert "engine.shadow_portfolio_evidence" in \
        FCR.EVIDENCE_ACCRUAL_OWNERS[PA.CLASS_SIGNAL_CANONICAL]
    assert FCR.EVIDENCE_ACCRUAL_OWNERS[PA.CLASS_SIGNAL_R46].startswith(
        "alpha_agent.r46")


def test_04_the_registrar_keeps_no_calendar_and_no_holiday_table_of_its_own():
    src = (REPO / "api" / "forward_challenger_registry.py").read_text(
        encoding="utf-8", errors="replace")
    for banned in ("Labor Day\":", "HOLIDAYS = ", "easter_sunday(",
                   "_nth_weekday(", "weekday() >= 5"):
        assert banned not in src, banned
    assert "exchange_calendar" in src and "market_session" in src


# =========================================================================== #
# B. THE FOUR R58 FREEZES REGISTER PROSPECTIVELY
# =========================================================================== #
@pytest.mark.parametrize("challenger_id", sorted(R58_FREEZES))
def test_05_each_active_r58_freeze_registers_prospectively(challenger_id,
                                                           tmp_path):
    row = _r58_freeze(challenger_id)
    lc = PA.classify_lifecycle(row)
    assert lc["lifecycle_state"] == PA.LC_ACTIVE
    assert lc["adoptable"] is True

    out = _adopt(row, tmp_path)
    assert out["outcome"] == PA.ADOPTED, out.get("reason")
    assert out["adopted"] is True
    assert out["challenger_class"] == PA.CLASS_SIGNAL_CANONICAL
    assert out["canonical_registrar"] == "api.forward_challenger_registry"
    reg = out["registration"]["registration"]
    assert reg["challenger_id"] == challenger_id
    assert reg["owner"] == "api.forward_challenger_registry"


@pytest.mark.parametrize("challenger_id", sorted(R58_FREEZES))
def test_06_the_exact_immutable_freeze_identity_is_preserved(challenger_id,
                                                             tmp_path):
    """Everything that identifies WHAT was frozen survives registration byte
    for byte: the freeze row hash, the model specification and the feature
    snapshot the cross-section was computed from."""
    row = _r58_freeze(challenger_id)
    out = _adopt(row, tmp_path)
    reg = out["registration"]["registration"]
    assert reg["freeze_record_hash"] == R58_FREEZES[challenger_id]
    assert reg["freeze_id"] == "H_%s" % challenger_id
    assert reg["model_spec_hash"] == out["identity"]["model_spec_hash"]
    assert reg["feature_snapshot_hash"] == "universe_84771159b57b7885"
    assert reg["horizon_sessions"] == R58_HORIZON
    assert reg["asset_class"] == "US_EQUITY"
    assert reg["cost_model"] == {"bps_per_side": 12.5}
    assert reg["price_mark_owner"] == PA.DEFAULT_MARK_OWNERS["US_EQUITY"]
    # The identity hash is the key, and it covers the whole identity.
    assert reg["identity"]["identity_hash"]
    stored = FCR.load_registration(reg["identity"]["identity_hash"],
                                   str(tmp_path / "registry"))
    assert stored == reg


def test_07_registration_is_idempotent_and_creates_no_duplicate(tmp_path):
    cid = "R58_FCF_PURE_V1"
    first = _adopt(_r58_freeze(cid), tmp_path)
    again = _adopt(_r58_freeze(cid), tmp_path)
    third = _adopt(_r58_freeze(cid), tmp_path, starts="2026-09-09")
    assert first["outcome"] == PA.ADOPTED
    assert again["outcome"] == PA.ALREADY_ADOPTED
    assert third["outcome"] == PA.ALREADY_ADOPTED
    files = sorted((tmp_path / "registry" / "registrations").glob("*.json"))
    assert len(files) == 1, [p.name for p in files]
    rows = FCR.load_registrations(str(tmp_path / "registry"))
    assert len(rows) == 1
    assert rows[0]["registered_at"] == first[
        "registration"]["registration"]["registered_at"]


def test_08_a_retry_after_a_partial_persistence_is_recoverable(tmp_path):
    """The registrar committed and the ADOPTION intent did not. Re-running must
    resolve to the SAME registration rather than creating a second one."""
    cid = "R58_DISCLOSURE_INTENSITY_V1"
    row = _r58_freeze(cid)
    identity = PA.build_adoption_identity(row)
    direct = FCR.register_forward_challenger(
        identity=identity, observation_clock_starts=REGISTERED_ON,
        challenger_class=PA.CLASS_SIGNAL_CANONICAL,
        lifecycle=PA.classify_lifecycle(row),
        registry_dir_override=str(tmp_path / "registry"))
    assert direct["outcome"] == FCR.REGISTERED
    # The adoption intent was never written (the crash window). Re-running the
    # whole governed operation finds the existing registration.
    assert PA.open_intents(str(tmp_path / "adoption")) == []
    resumed = _adopt(row, tmp_path)
    assert resumed["outcome"] == PA.ADOPTED
    assert resumed["registration"]["already_present"] is True
    assert len(list((tmp_path / "registry" / "registrations").glob("*.json"))) == 1
    assert PA.open_intents(str(tmp_path / "adoption")) == []


def test_09_two_different_freezes_never_collapse_into_one_registration(tmp_path):
    for cid in sorted(R58_FREEZES):
        assert _adopt(_r58_freeze(cid), tmp_path)["outcome"] == PA.ADOPTED
    rows = FCR.load_registrations(str(tmp_path / "registry"))
    assert len(rows) == 4
    assert sorted(r["challenger_id"] for r in rows) == sorted(R58_FREEZES)
    assert len({r["identity"]["identity_hash"] for r in rows}) == 4


# =========================================================================== #
# C. THE WITHDRAWN R59 CHALLENGER IS REFUSED, BY EVERY OWNER, FOREVER
# =========================================================================== #
def test_10_the_withdrawn_r59_challenger_is_refused_by_the_adoption_owner(
        tmp_path):
    row = _r58_freeze(R59_WITHDRAWN_ID, release="R59",
                      invalidated_reason=WITHDRAWN_REASON)
    lc = PA.classify_lifecycle(row)
    assert lc["lifecycle_state"] == PA.LC_WITHDRAWN
    assert lc["adoptable"] is False
    assert lc["never_resurrectable"] is True

    out = _adopt(row, tmp_path)
    assert out["outcome"] == PA.REFUSED_LIFECYCLE
    assert out["adopted"] is False
    assert out["never_resurrectable"] is True
    # Nothing was written anywhere - not an intent, not a registration.
    assert not (tmp_path / "registry").exists()
    assert FCR.load_registrations(str(tmp_path / "registry")) == []


def test_11_the_registrar_refuses_it_independently_of_its_caller(tmp_path):
    """Defence in depth: a caller that lost the lifecycle verdict, or lied about
    it, still cannot resurrect a withdrawn freeze."""
    row = _r58_freeze(R59_WITHDRAWN_ID, release="R59",
                      invalidated_reason=WITHDRAWN_REASON)
    identity = PA.build_adoption_identity(row)
    out = FCR.register_forward_challenger(
        identity=identity, observation_clock_starts=REGISTERED_ON,
        challenger_class=PA.CLASS_SIGNAL_CANONICAL,
        freeze_row=row, registry_dir_override=str(tmp_path / "registry"))
    assert out["outcome"] == FCR.REFUSED_LIFECYCLE
    assert out["registered"] is False
    assert out["never_resurrectable"] is True
    assert FCR.load_registrations(str(tmp_path / "registry")) == []


@pytest.mark.parametrize("reason,expected", [
    (WITHDRAWN_REASON, PA.LC_WITHDRAWN),
    ("the input feature was not what its name claims", PA.LC_INVALIDATED),
])
def test_12_no_terminal_lifecycle_state_can_ever_register(reason, expected,
                                                          tmp_path):
    row = _r58_freeze("R58_FCF_PURE_V1", invalidated_reason=reason)
    assert PA.classify_lifecycle(row)["lifecycle_state"] == expected
    out = _adopt(row, tmp_path)
    assert out["outcome"] == PA.REFUSED_LIFECYCLE
    direct = FCR.register_forward_challenger(
        identity=PA.build_adoption_identity(row),
        observation_clock_starts=REGISTERED_ON,
        challenger_class=PA.CLASS_SIGNAL_CANONICAL, freeze_row=row,
        registry_dir_override=str(tmp_path / "registry"))
    assert direct["outcome"] == FCR.REFUSED_LIFECYCLE


def test_13_a_superseded_freeze_cannot_register(tmp_path):
    row = _r58_freeze("R58_FCF_PURE_V1")
    lc = PA.classify_lifecycle(row, later_freezes_with_same_id=1)
    assert lc["lifecycle_state"] == PA.LC_SUPERSEDED
    out = FCR.register_forward_challenger(
        identity=PA.build_adoption_identity(row),
        observation_clock_starts=REGISTERED_ON,
        challenger_class=PA.CLASS_SIGNAL_CANONICAL, lifecycle=lc,
        registry_dir_override=str(tmp_path / "registry"))
    assert out["outcome"] == FCR.REFUSED_LIFECYCLE


def test_14_a_registration_with_no_established_lifecycle_fails_closed(tmp_path):
    out = FCR.register_forward_challenger(
        identity=PA.build_adoption_identity(_r58_freeze("R58_FCF_PURE_V1")),
        observation_clock_starts=REGISTERED_ON,
        challenger_class=PA.CLASS_SIGNAL_CANONICAL,
        registry_dir_override=str(tmp_path / "registry"))
    assert out["outcome"] == FCR.REFUSED_LIFECYCLE_UNKNOWN
    assert out["registered"] is False
    assert FCR.load_registrations(str(tmp_path / "registry")) == []


# =========================================================================== #
# D. EFFECTIVE-FROM / NO BACKFILL
# =========================================================================== #
def test_15_no_historical_prediction_or_observation_is_ever_created(tmp_path):
    """The five sessions between the 2026-09-03 inception and the 2026-09-08
    registration are NOT manufactured into evidence."""
    out = _adopt(_r58_freeze("R58_SHORT_VOLUME_PRESSURE_V1"), tmp_path)
    reg = out["registration"]["registration"]
    assert reg["predictions_emitted"] == 0
    assert reg["matured_observations"] == 0
    assert reg["pending_observations"] == 0
    assert reg["effective_independent_observations"] == 0
    assert reg["forward_observations_at_registration"] == 0
    assert reg["backfilled"] is False
    assert reg["observation_clock"]["backfilled"] is False
    assert reg["observation_clock"][
        "sessions_between_inception_and_registration_are_never_synthesised"] \
        is True
    # And the store holds exactly one file: the registration. No prediction
    # ledger, no outcome ledger and no observation file appeared beside it.
    files = sorted(p.name for p in (tmp_path / "registry").rglob("*")
                   if p.is_file())
    assert files == ["%s.json" % reg["identity"]["identity_hash"]]
    dirs = sorted(p.name for p in (tmp_path / "registry").rglob("*")
                  if p.is_dir())
    assert dirs == ["registrations"]


def test_16_the_first_observation_respects_the_effective_from_boundary(tmp_path):
    out = _adopt(_r58_freeze("R58_FCF_PURE_V1"), tmp_path)
    clock = out["registration"]["registration"]["observation_clock"]
    assert clock["effective_from_session"] == REGISTERED_ON
    # STRICTLY after: 2026-09-08 is a session, and it is not eligible because it
    # had already begun when registration happened.
    assert clock["first_eligible_observation_session"] > REGISTERED_ON
    assert clock["first_eligible_observation_session"] == "2026-09-09"


def test_17_an_observation_clock_before_the_inception_is_refused(tmp_path):
    out = _adopt(_r58_freeze("R58_FCF_PURE_V1"), tmp_path, starts="2026-09-01")
    assert out["outcome"] == PA.REFUSED_BACKDATED
    direct = FCR.register_forward_challenger(
        identity=PA.build_adoption_identity(_r58_freeze("R58_FCF_PURE_V1")),
        observation_clock_starts="2026-09-01",
        challenger_class=PA.CLASS_SIGNAL_CANONICAL,
        lifecycle={"lifecycle_state": PA.LC_ACTIVE},
        registry_dir_override=str(tmp_path / "registry"))
    assert direct["outcome"] == FCR.REFUSED_BACKDATED
    assert FCR.load_registrations(str(tmp_path / "registry")) == []


def test_18_a_registration_with_no_declared_session_is_refused(tmp_path):
    out = _adopt(_r58_freeze("R58_FCF_PURE_V1"), tmp_path, starts="")
    assert out["outcome"] == PA.REFUSED_BACKDATED


# =========================================================================== #
# E. THE MARKET CALENDAR — Sep-7-2026 IS NEVER A SESSION
# =========================================================================== #
def test_19_labor_day_2026_is_a_full_day_closure_on_the_canonical_calendar():
    assert EC.holiday_name("2026-09-07") == "Labor Day"
    assert EC.is_non_session("2026-09-07") is True
    assert EC.is_weekend("2026-09-07") is False


def test_20_an_equity_challenger_can_never_observe_on_labor_day():
    verdict = FCR.is_eligible_observation_session("US_EQUITY", "2026-09-07")
    assert verdict["eligible"] is False
    assert verdict["holiday_name"] == "Labor Day"
    assert verdict["state"] == FCR.CLOCK_RESOLVED
    assert FCR.next_exchange_session_after("2026-09-04") == "2026-09-08"
    assert FCR.next_exchange_session_after("2026-09-06") == "2026-09-08"


def test_21_an_equity_maturity_schedule_never_lands_on_labor_day(tmp_path):
    """The live symptom: next_material_maturity = 2026-09-07, a session that
    does not exist. A clock resolved on the authoritative calendar cannot
    produce it, at any horizon."""
    for horizon in range(1, 25):
        clock = FCR.resolve_observation_clock(
            asset_class="US_EQUITY", effective_from="2026-09-04",
            horizon_sessions=horizon)
        assert clock["state"] == FCR.CLOCK_RESOLVED
        assert clock["first_eligible_observation_session"] != "2026-09-07"
        assert clock["next_expected_maturity_session"] != "2026-09-07"
        assert not EC.is_non_session(clock["next_expected_maturity_session"])


def test_22_forward_maturity_scheduling_does_not_use_naive_weekday_arithmetic():
    """The R46 scheduling estimate. Weekday arithmetic lands on Labor Day; the
    calendar-aware estimate does not — and the two genuinely differ, which is
    what proves the fix is doing something."""
    entry = _dt.date(2026, 9, 4)
    naive = CK.expected_maturity_date(entry, 1)
    assert naive.isoformat() == "2026-09-07", "the frozen weekday rule is intact"

    non = CK.exchange_non_sessions("US_EQUITY", entry, _dt.date(2026, 12, 31))
    assert non is not None and "2026-09-07" in non
    resolved = CK.expected_maturity_date(entry, 1, non_sessions=non)
    assert resolved.isoformat() == "2026-09-08"
    assert resolved != naive


def test_23_a_non_equity_market_is_never_forced_onto_the_nyse_calendar():
    """A rates future really does print on days the NYSE is shut. Imposing
    equity sessions on it would be a fabrication, not a fix."""
    entry = _dt.date(2026, 9, 4)
    for asset_class in ("RATES_FUTURES", "FX_FUTURES", "COMMODITY_FUTURES",
                        "VOLATILITY", "EQUITY_INDEX_FUTURES", "CROSS_ASSET"):
        cal = FCR.observation_calendar_for(asset_class)
        assert cal["declared"] is True
        assert cal["is_exchange_session_calendar"] is False
        assert cal["calendar_owner"] == FCR.CAL_INSTRUMENT_REALISED
        assert CK.exchange_non_sessions(asset_class, entry,
                                        _dt.date(2026, 12, 31)) is None
        clock = FCR.resolve_observation_clock(asset_class=asset_class,
                                              effective_from="2026-09-04",
                                              horizon_sessions=21)
        assert clock["state"] == FCR.CLOCK_INSTRUMENT_OWNED
        assert clock["first_eligible_observation_session"] is None
        assert clock["uses_weekday_arithmetic"] is False


def test_24_an_undeclared_asset_class_publishes_no_clock_rather_than_a_guess():
    clock = FCR.resolve_observation_clock(asset_class="TROPICAL_WEATHER",
                                          effective_from="2026-09-08",
                                          horizon_sessions=21)
    assert clock["state"] == FCR.CLOCK_ASSET_CLASS_UNDECLARED
    assert clock["first_eligible_observation_session"] is None
    assert clock["next_expected_maturity_session"] is None


def test_25_there_is_exactly_one_exchange_calendar_owner():
    """No forward-evidence component may keep a holiday table of its own."""
    offenders = []
    for path in sorted((REPO / "api").glob("*.py")) + \
            sorted((REPO / "alpha_agent" / "r46").glob("*.py")) + \
            sorted((REPO / "alpha_agent" / "r52").glob("*.py")):
        src = path.read_text(encoding="utf-8", errors="replace")
        if "Labor Day" in src and path.name != "exchange_calendar.py":
            # A comment naming the incident is fine; a TABLE is not.
            if "def holidays_for_year(" in src or "AD_HOC_CLOSURES" in src:
                offenders.append(str(path.relative_to(REPO)))
    assert offenders == [], offenders
    assert EC.CALENDAR_ID == "NYSE_RULE_BASED_R60_1"


# =========================================================================== #
# F. MULTI-ASSET BY CONSTRUCTION
# =========================================================================== #
MULTI_ASSET_CASES = [
    ("US_EQUITY", ["AAPL", "MSFT"]),
    ("EQUITY_INDEX_FUTURES", ["ES"]),
    ("RATES_FUTURES", ["ZN", "ZB"]),
    ("COMMODITY_FUTURES", ["CL", "NG"]),
    ("FX_FUTURES", ["6E", "6J"]),
    ("VOLATILITY", ["VX"]),
    ("CROSS_ASSET", ["ES", "ZN", "CL", "6E"]),
]


@pytest.mark.parametrize("asset_class,scope", MULTI_ASSET_CASES)
def test_26_a_representative_contract_registers_for_every_asset_class(
        asset_class, scope, tmp_path):
    row = _r58_freeze("CH_%s" % asset_class, release="R59",
                      asset_class=asset_class, instrument_scope=scope)
    out = _adopt(row, tmp_path)
    assert out["outcome"] == PA.ADOPTED, (asset_class, out.get("reason"))
    reg = out["registration"]["registration"]
    assert reg["asset_class"] == asset_class
    assert reg["instrument_scope"] == [str(x) for x in scope]
    assert reg["observation_clock"]["calendar_owner"] == \
        FCR.OBSERVATION_CALENDARS[asset_class]


def test_27_the_registration_contract_carries_no_equity_only_field():
    src = (REPO / "api" / "forward_challenger_registry.py").read_text(
        encoding="utf-8", errors="replace")
    assert '"ticker"' not in src
    assert "instrument_scope" in src
    body = FCR.load_forward_challenger_registry(registry_dir_override="/nonexistent")
    assert body["n_registered"] == 0
    assert set(FCR.EXCHANGE_SESSION_ASSET_CLASSES) <= set(
        FCR.OBSERVATION_CALENDARS)


def test_28_two_asset_classes_never_collide_on_one_registration(tmp_path):
    a = _r58_freeze("SAME_ID", release="R59", asset_class="RATES_FUTURES",
                    instrument_scope=["ZN"])
    b = _r58_freeze("SAME_ID", release="R59", asset_class="FX_FUTURES",
                    instrument_scope=["ZN"])
    assert _adopt(a, tmp_path)["outcome"] == PA.ADOPTED
    assert _adopt(b, tmp_path)["outcome"] == PA.ADOPTED
    assert len(FCR.load_registrations(str(tmp_path / "registry"))) == 2


# =========================================================================== #
# G. COMPATIBILITY — R46, R56 AND R52 ARE UNTOUCHED
# =========================================================================== #
def test_29_r46_challengers_still_route_to_their_own_frozen_cohort(tmp_path):
    """The R46 contract is hashed into every row it emitted. It is reached by
    reference and is never edited from the adoption path."""
    row = _r58_freeze("r46_eq_xs_reversal_5d", release="R46")
    assert PA.classify_challenger_class(row) == PA.CLASS_SIGNAL_R46
    calls = []

    def _cohort(*, identity, observation_clock_starts, challenger_class):
        calls.append(identity["challenger_id"])
        return {"registered_with": "alpha_agent.r46.registry",
                "already_present": True}

    out = _adopt(row, tmp_path, registrar=_cohort)
    assert out["outcome"] == PA.ADOPTED
    assert calls == ["r46_eq_xs_reversal_5d"]
    # The canonical registry was NOT written for an R46 challenger.
    assert FCR.load_registrations(str(tmp_path / "registry")) == []


def test_30_the_r46_registry_module_is_unchanged_by_this_release():
    """R62.1 adds no challenger to the frozen R46 cohort and edits no contract."""
    src = (REPO / "alpha_agent" / "r46" / "registry.py").read_text(
        encoding="utf-8", errors="replace")
    assert "forward_challenger_registry" not in src
    assert "R58_" not in src
    from paper_trader.alpha_agent.r46 import contract as C46
    assert callable(C46.contract_hash)


def test_31_r56_portfolio_challengers_remain_the_shadow_owners(tmp_path):
    row = _r58_freeze("r56_zero_base_research_v1", release="R56")
    assert PA.classify_challenger_class(row) == PA.CLASS_PAPER_PORTFOLIO
    assert PA.REGISTRARS[PA.CLASS_PAPER_PORTFOLIO] == \
        "api.shadow_portfolio_evidence"
    from paper_trader.api import shadow_portfolio_evidence as r56
    assert r56.COMPOSITION_OWNER == "api.shadow_portfolio_evidence"
    # The R56 owner still refuses to invent a record for a single challenger.
    src = (REPO / "api" / "shadow_portfolio_evidence.py").read_text(
        encoding="utf-8", errors="replace")
    assert "forward_challenger_registry" not in src


def test_32_r52_maturation_still_consumes_the_canonical_evidence_chain():
    """R62.1 introduces no second maturation owner and changes none of the
    chains the R52 runtime verifies before it writes anything."""
    from paper_trader.alpha_agent.r52 import runtime as R52
    src = (REPO / "alpha_agent" / "r52" / "runtime.py").read_text(
        encoding="utf-8", errors="replace")
    assert "forward_challenger_registry" not in src
    assert R52.CALCULATION_OWNER == "alpha_agent.r52.runtime"
    assert "def _chains_ok(" in src
    for chain in ("r46_forward", "adopted_continuation", "r52_forfeiture"):
        assert chain in src
    # Every registration names R52 as the owner that will mature it.
    assert FCR.EVIDENCE_ACCRUAL_OWNERS[PA.CLASS_SIGNAL_CANONICAL].endswith(
        "alpha_agent.r52.runtime")


def test_33_the_r46_row_records_which_calendar_produced_its_estimate():
    """A reader never has to guess whether a holiday was considered."""
    from paper_trader.alpha_agent.r46 import emit as EM
    assert EM._maturity_calendar_owner("US_EQUITY") == FCR.CAL_EXCHANGE_NYSE
    assert EM._maturity_calendar_owner("RATES_FUTURES") == \
        FCR.CAL_INSTRUMENT_REALISED


# =========================================================================== #
# H. CURRENT VS HISTORICAL RUNTIME IDENTITY
# =========================================================================== #
def test_34_the_two_identity_kinds_are_named_and_distinct():
    assert rid.IDENTITY_CURRENT_RUNTIME == "CURRENT_RUNTIME_IDENTITY"
    assert rid.IDENTITY_EVENT_CYCLE_RUNTIME == "EVENT_CYCLE_RUNTIME_IDENTITY"
    assert rid.IDENTITY_CURRENT_RUNTIME != rid.IDENTITY_EVENT_CYCLE_RUNTIME
    assert set(rid.IDENTITY_KINDS) == {rid.IDENTITY_CURRENT_RUNTIME,
                                       rid.IDENTITY_EVENT_CYCLE_RUNTIME}


def test_35_a_cycle_that_predates_the_current_process_is_historical():
    """The live case: the 2026-09-08 18:16Z cycle ran under the pre-R61 worker;
    the worker running now started afterwards, on the deployed release."""
    out = rid.classify_event_cycle_provenance(
        cycle_generated_at="2026-09-08T18:16:00Z",
        current_runtime_started_at="2026-09-08T19:40:00Z")
    assert out["verdict"] == rid.EVC_EARLIER_RUNTIME
    assert out["is_historical_event_cycle"] is True
    assert out["decides_current_service_health"] is False
    assert "HISTORICAL" in out["statement"]


def test_36_a_recorded_commit_outranks_the_timestamps():
    older = rid.classify_event_cycle_provenance(
        cycle_generated_at="2026-09-08T21:00:00Z",
        current_runtime_started_at="2026-09-08T19:40:00Z",
        cycle_loaded_commit="dc0ca39" + "0" * 33,
        current_loaded_commit="ba0d9a3" + "0" * 33)
    assert older["verdict"] == rid.EVC_EARLIER_RUNTIME
    same = rid.classify_event_cycle_provenance(
        cycle_generated_at="2026-09-08T21:00:00Z",
        current_runtime_started_at="2026-09-08T19:40:00Z",
        cycle_loaded_commit="ba0d9a3" + "0" * 33,
        current_loaded_commit="ba0d9a3" + "0" * 33)
    assert same["verdict"] == rid.EVC_CURRENT_RUNTIME


def test_37_provenance_fails_closed_and_never_claims_the_current_runtime():
    out = rid.classify_event_cycle_provenance(cycle_generated_at=None)
    assert out["verdict"] == rid.EVC_NOT_ESTABLISHED
    assert out["is_historical_event_cycle"] is False
    assert out["decides_current_service_health"] is False
    for value in (rid.EVC_CURRENT_RUNTIME, rid.EVC_EARLIER_RUNTIME,
                  rid.EVC_NOT_ESTABLISHED):
        assert value in rid.EVENT_CYCLE_PROVENANCE_VERDICTS


def _ams(**kw):
    return ams.build_active_manager_state(**kw)


def _healthy_worker(commit="ba0d9a3" + "0" * 33):
    return {"service": {
        "service_state": "RUNNING", "worker_activity": "IDLE",
        "reason": "The worker is idle between iterations.",
        "worker_pid": 4242, "instance_id": "worker-1",
        "started_at": "2026-09-08T19:40:00+00:00",
        "loaded_release": {"commit": commit, "commit_short": commit[:12],
                           "captured_at": "2026-09-08T19:40:00+00:00"}}}


def test_38_a_historical_stale_cycle_cannot_make_current_collection_stale():
    """The R62.1 defect, end to end. The worker running NOW loaded the deployed
    release; the newest completed cycle was produced by its predecessor. Current
    health must be RUNNING and the cycle must be labelled historical."""
    deployed = "ba0d9a3" + "0" * 33
    payload = _ams(
        # The DECISION-snapshot section still carries the older worker's facts.
        information_collection={"service": {
            "service_state": "DEGRADED", "worker_activity": "DEAD",
            "started_at": "2026-09-01T14:12:09+00:00",
            "loaded_release": {"commit": "dc0ca39" + "0" * 33}}},
        current_collection=_healthy_worker(deployed),
        event_refresh={"last_run": {
            "run_id": "r1", "state": "PROPOSAL_WITHHELD",
            "generated_at": "2026-09-08T18:16:00+00:00"}},
        runtime_alignment=None)
    li = payload["live_information"]
    assert li["collection_service_state"] == "RUNNING"
    assert li["collection_running"] is True
    assert li["current_collection"]["source"] == ams.CC_SOURCE_CURRENT_OWNER
    cycle = li["last_event_cycle"]
    assert cycle["is_historical_event_cycle"] is True
    assert cycle["decides_current_collection_health"] is False


def test_39_the_alignment_row_reads_the_current_runtime_not_the_snapshot():
    deployed = "ba0d9a3" + "0" * 33
    payload = _ams(
        information_collection={"service": {
            "service_state": "DEGRADED",
            "loaded_release": {"commit": "dc0ca39" + "0" * 33}}},
        current_collection=_healthy_worker(deployed))
    rows = [r for r in payload["runtime_alignment"].get("runtimes") or []
            if r.get("runtime") == "information_collection_worker"]
    assert rows, "the collection runtime row must exist"
    assert rows[0]["loaded_commit"] == deployed
    assert rows[0]["loaded_commit"] != "dc0ca39" + "0" * 33


def test_40_every_surface_reads_one_current_collection_state():
    """Today / Portfolio / Alpha & Capital / System-Audit all render the Active
    Manager payload; there is ONE field, and it names the read that produced it."""
    payload = _ams(current_collection=_healthy_worker())
    cc = payload["live_information"]["current_collection"]
    assert cc["service_state"] == payload["live_information"][
        "collection_service_state"]
    assert cc["available"] is True
    assert cc["identity_kind"] == "CURRENT_RUNTIME_IDENTITY"
    assert cc["source"] in ams.CURRENT_COLLECTION_SOURCES
    ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                        errors="replace")
    assert "li.current_collection" in ui
    # The browser reconciles nothing: it must not decide a service state itself.
    for banned in ("=== 'STALE_RUNTIME'", "collection_service_state ==="):
        assert banned not in ui, banned


def test_40b_the_only_other_collection_token_declares_itself_non_authoritative():
    """The operator presentation echoes a snapshot-bound collection token in its
    Audit-only raw-states block. It is not rendered on any normal surface, and it
    now says in the payload that it is not the current authority."""
    from paper_trader.api import operator_presentation as op
    src = (REPO / "api" / "operator_presentation.py").read_text(
        encoding="utf-8", errors="replace")
    assert '"collection_service_state_is_authoritative": False' in src
    assert "api.information_collection.resolve_service_lifecycle" in src
    assert op.SOURCE_OWNERS["information_collection"] == \
        "api.information_collection"
    ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                        errors="replace")
    # The Audit echo reaches no rendered surface; the ONE rendered collection
    # state is the Active Manager row, which reads live_information.
    assert ui.count("collection_service_state") == 1
    assert "li.collection_service_state" in ui


def test_41_an_unavailable_current_read_is_reported_not_hidden():
    payload = _ams()
    cc = payload["live_information"]["current_collection"]
    assert cc["available"] is False
    assert cc["source"] == ams.CC_SOURCE_UNAVAILABLE
    assert cc["service_state"] is None


def test_41b_the_current_collection_read_is_an_injectable_owner():
    """It must be injectable, or a hermetic caller would silently grade whichever
    collection worker happens to be running on the machine."""
    calls = []

    def _boom():
        raise RuntimeError("owner down")

    def _current():
        calls.append("current_collection")
        return _healthy_worker()

    payload = ams.load_active_manager_state(loaders={
        "workflow": _boom, "portfolio_state": _boom, "constrained": _boom,
        "information_collection": _boom, "current_collection": _current,
        "rebalance": _boom, "event_refresh": _boom, "reassessment": _boom,
        "scoring": _boom, "runtime_health": _boom, "intraday_emission": _boom,
        "governed_decision": _boom})
    assert calls == ["current_collection"]
    li = payload["live_information"]
    assert li["current_collection"]["source"] == ams.CC_SOURCE_CURRENT_OWNER
    assert li["collection_service_state"] == "RUNNING"


def test_42_the_production_loader_reads_the_canonical_current_owner():
    src = (REPO / "api" / "active_manager_state.py").read_text(
        encoding="utf-8", errors="replace")
    assert "def _current_collection(" in src
    assert "_ic.resolve_service_lifecycle(" in src
    assert 'current_collection=current_collection' in src
    # And it still delegates the alignment verdict rather than deriving one.
    assert "rid.build_runtime_alignment(" in src
    for banned in ("rev-parse", "read_source_identity(",
                   "capture_loaded_identity("):
        assert banned not in src, banned


# =========================================================================== #
# I. THE HISTORICAL SEP-8 WITHHELD EVENT STAYS IMMUTABLE
# =========================================================================== #
def test_43_the_historical_withheld_event_is_labelled_not_rewritten():
    payload = _ams(
        current_collection=_healthy_worker(),
        event_refresh={"last_run": {
            "run_id": "sep8", "state": "PROPOSAL_WITHHELD",
            "generated_at": "2026-09-08T18:16:00+00:00",
            "governed_decision": {
                "withheld_reason": "HOC_ARTIFACT_IDENTITY_MISMATCH",
                "second_reason": "DUPLICATE_CANDIDATE"}}})
    cycle = payload["live_information"]["last_event_cycle"]
    # The facts the cycle recorded are carried through VERBATIM.
    assert cycle["run_id"] == "sep8"
    assert cycle["state"] == "PROPOSAL_WITHHELD"
    assert cycle["governed_decision"]["withheld_reason"] == \
        "HOC_ARTIFACT_IDENTITY_MISMATCH"
    # And the release that produced them is stated, so the current code is not
    # accused of having just reproduced the defect.
    assert cycle["is_historical_event_cycle"] is True
    assert "earlier runtime" in cycle["historical_note"]


def test_44_no_owner_replays_rebinds_or_erases_a_recorded_cycle():
    """R62.1 adds no replay, no rebind and no history rewrite anywhere."""
    for rel in ("api/forward_challenger_registry.py", "api/runtime_identity.py",
                "api/active_manager_state.py"):
        src = (REPO / rel).read_text(encoding="utf-8", errors="replace")
        for banned in ("def replay", "def rebind", "def rewrite_history",
                       "_persist_run(", "save_service_state("):
            assert banned not in src, (rel, banned)


def test_45_a_future_cycle_records_its_own_producing_runtime():
    """From R62.1 the cycle carries its release, so provenance survives the
    process. Cycles written earlier simply do not carry it — and absent is the
    truth for those, never a backfilled value."""
    from paper_trader.api import event_signal_refresh as esr
    release = esr._producing_runtime_release()
    assert release is None or release["identity_kind"] == \
        "EVENT_CYCLE_RUNTIME_IDENTITY"
    summary = esr.build_last_run_summary({"run_id": "x", "state": "COMPLETED"})
    assert summary["runtime_release"] is None, "absent stays absent"


# =========================================================================== #
# J. THE FORWARD-EVIDENCE READ MODEL
# =========================================================================== #
def test_46_the_read_model_answers_every_operator_question(tmp_path):
    _adopt(_r58_freeze("R58_SHORT_VOLUME_PRESSURE_V1"), tmp_path)
    body = FCR.load_forward_challenger_registry(
        registry_dir_override=str(tmp_path / "registry"), today=REGISTERED_ON)
    assert body["n_registered"] == 1
    row = body["registrations"][0]
    for field in ("challenger_id", "freeze_id", "lifecycle_state", "asset_class",
                  "horizon_sessions", "canonical_registrar",
                  "registration_timestamp", "prospective_effective_from",
                  "predictions_emitted", "matured_observations",
                  "pending_observations",
                  "effective_independent_observations",
                  "next_legitimate_evidence_gate",
                  "next_legitimate_maturity_session", "evidence_status"):
        assert field in row, field
    assert row["lifecycle_state"] == PA.LC_ACTIVE
    assert row["canonical_registrar"] == "api.forward_challenger_registry"
    assert row["evidence_status"] == FCR.EV_AWAITING_FIRST_SESSION
    assert row["promotion_ready"] is False


def test_47_the_browser_computes_no_maturity_or_lifecycle():
    ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                        errors="replace")
    assert "aao-registrations" in ui
    assert "canonical_forward_registrations" in ui
    # Scan the R62.1 renderer BODY only: the rest of the page legitimately
    # colours other owners' vocabularies, and a whole-file grep would forbid
    # that too.
    start = ui.find("// ---- R62.1: canonical prospective registrations")
    end = ui.find("// ---- best current evidence", start + 1)
    body = ui[start:end]
    assert start > 0 and end > start, "the R62.1 renderer must be locatable"
    for banned in ("r.lifecycle_state ===", "r.evidence_status ===",
                   "next_legitimate_maturity_session >",
                   "matured_observations >", "new Date(",
                   "r.promotion_ready ?"):
        assert banned not in body, banned


def test_48_the_research_read_model_links_a_freeze_to_its_registration(tmp_path):
    """A registered freeze is no longer an orphan, and the row it publishes is
    the registrar's own."""
    from paper_trader.api import alphaagent_outcomes as AAO
    _adopt(_r58_freeze("R58_FCF_PURE_V1"), tmp_path)
    rows = [FCR.registration_row(r) for r in
            FCR.load_registrations(str(tmp_path / "registry"))]
    by_id = {r["challenger_id"]: r for r in rows}
    freeze = AAO._freeze_row(_r58_freeze("R58_FCF_PURE_V1"),
                             {"R58_FCF_PURE_V1": "api.forward_challenger_registry"},
                             by_id)
    assert freeze["forward_evidence_link"] == AAO.FWD_LINK_ADOPTED
    assert freeze["forward_registration"]["challenger_id"] == "R58_FCF_PURE_V1"
    assert freeze["challenger_class"] == PA.CLASS_SIGNAL_CANONICAL
    assert freeze["canonical_registrar"] == "api.forward_challenger_registry"


# =========================================================================== #
# K. SAFETY
# =========================================================================== #
def test_49_registration_promotes_no_model_and_allocates_no_capital(tmp_path):
    out = _adopt(_r58_freeze("R58_FCF_PURE_V1"), tmp_path)
    for block in (out["safety"], out["registration"]["registration"]["safety"]):
        for flag in ("promoted_model", "automatic_model_promotion_allowed",
                     "activated_sleeve", "changed_holdings", "changed_cash",
                     "changed_nav", "created_orders", "created_order_plan",
                     "created_fills", "approved_anything", "automation_enabled",
                     "broker_enabled", "backfilled_forward_evidence",
                     "rewrote_history"):
            assert block.get(flag) is False, flag
    reg = out["registration"]["registration"]
    assert reg["promotion_allowed"] is False
    assert reg["automatic_promotion_allowed"] is False
    assert reg["manual_review_required"] is True


def test_50_there_is_no_path_from_forward_evidence_to_promotion():
    src = (REPO / "api" / "forward_challenger_registry.py").read_text(
        encoding="utf-8", errors="replace")
    for banned in ("promote(", "def promote", "activate_sleeve(",
                   "create_order", "build_order_plan", "submit(", "approve(",
                   "rebalance_execution", "paper_trading_desk"):
        assert banned not in src, banned
    body = FCR.load_forward_challenger_registry(registry_dir_override="/nonexistent")
    assert body["automatic_promotion_allowed"] is False
    assert body["backfill_allowed"] is False
    assert body["historical_result_is_never_forward_evidence"] is True
    assert "PROMOTION_READY" not in FCR.EVIDENCE_STATES


def test_51_no_live_store_is_written_by_any_test_in_this_module(tmp_path):
    """The registry root is overridable and the default is never touched here."""
    assert FCR.REGISTRY_DIR_ENV == \
        "PAPER_TRADER_FORWARD_CHALLENGER_REGISTRY_DIR"
    assert FCR.registry_dir(str(tmp_path)) == Path(str(tmp_path))
    default = FCR.registry_dir()
    assert Path(tmp_path) != default
    _adopt(_r58_freeze("R58_FCF_PURE_V1"), tmp_path)
    written = sorted(str(p) for p in tmp_path.rglob("*.json"))
    assert written, "the hermetic root received the writes"
    assert all(str(tmp_path) in p for p in written)


def test_52_the_registry_record_is_json_and_carries_its_own_schema(tmp_path):
    out = _adopt(_r58_freeze("R58_FCF_PURE_V1"), tmp_path)
    path = (tmp_path / "registry" / "registrations" /
            ("%s.json" % out["identity"]["identity_hash"]))
    body = json.loads(path.read_text(encoding="utf-8"))
    assert body["schema_version"] == FCR.SCHEMA_VERSION
    assert body["owner"] == FCR.COMPOSITION_OWNER
    assert body["phase"] == "R62.1"
    assert body["records_are_immutable"] is True
    assert body["first_write_wins"] is True
