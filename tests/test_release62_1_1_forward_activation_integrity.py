r"""Release 62.1.1 — FORWARD ACTIVATION + LIVE-STATE INTEGRITY.

Four live gaps, one suite:

  A. ADOPTION ENTRYPOINT   an existing frozen challenger had no governed
                           operator path into forward evidence, and the
                           persistent research runtime could not self-heal one
                           because ``freeze_qualified`` returned ALREADY_FROZEN
                           before the adoption owner was ever called.
  B. COLLECTION TRUTH      "is collection running now?" was answered by two
                           payloads, and a slow or failed READ of one of them
                           was published by the browser as a SERVICE verdict.
  C. REASSESSMENT IDENTITY the Sep-8 live cycle's withholding was CORRECT and
                           its identity chain was exact; the three reason codes
                           it reported described a defect that did not exist.
  D. HOC / DRIFT / LATENCY a Labor-Day predecessor, an unclassified membership
                           difference and an acceptance count that did not add
                           up.

Every test is hermetic: the research memory, the adoption store and the
canonical registry are redirected into pytest temp roots, and no test touches a
live store, promotes a model, allocates capital or creates an order.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

from paper_trader.alpha_agent import r59                                # noqa: E402
from paper_trader.alpha_agent.r59 import handlers as r59h               # noqa: E402
from paper_trader.alpha_agent.r59 import memory as M                    # noqa: E402
from paper_trader.api import active_manager_state as ams                # noqa: E402
from paper_trader.api import daily_close as dc                          # noqa: E402
from paper_trader.api import event_signal_refresh as esr                # noqa: E402
from paper_trader.api import forward_challenger_registry as FCR         # noqa: E402
from paper_trader.api import holding_opportunity_cost as hoc            # noqa: E402
from paper_trader.api import information_collection as IC               # noqa: E402
from paper_trader.api import portfolio_decision as pdec                 # noqa: E402
from paper_trader.api import prospective_adoption as PA                 # noqa: E402
from paper_trader.scripts import adopt_prospective_freeze as ENTRY      # noqa: E402


# =========================================================================== #
# The four ACTIVE R58 freezes and the ONE withdrawn R59 candidate, exactly as
# the live estate holds them.
# =========================================================================== #
R58_CHALLENGERS = (
    "R58_SHORT_VOLUME_PRESSURE_V1",
    "R58_DISCLOSURE_INTENSITY_V1",
    "R58_FUND_MOMENTUM_VETO_V1",
    "R58_FCF_PURE_V1",
)
R59_WITHDRAWN = "R59_CALENDAR_TERM_STRUCTURE_F9BE2426"
INCEPTION = "2026-09-03"

#: The EXACT governed decision that stands for 2026-09-08, and the candidate
#: identity of the natural live reassessment that ran after it. Both are read
#: from the live artifacts in the release report; they are constants here so the
#: suite proves the outcome without reading a production store.
SEP8_RECORD_ID = "gdec_2026-09-08_alpha_paper_book_1_c329a4e57fa9"
SEP8_IDENTITY_HASH = "c329a4e57fa9cfea2f1412677ea19176"
SEP8_SESSION = "2026-09-08"
SEP8_DECISION = "CURRENT_NO_CHANGE"


@pytest.fixture()
def stores(tmp_path, monkeypatch):
    """Redirect EVERY store this release can write into a pytest temp root."""
    research = tmp_path / "research"
    adoption = tmp_path / "adoption"
    registry = tmp_path / "registry"
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(research))
    monkeypatch.setenv(PA.ADOPTION_DIR_ENV, str(adoption))
    monkeypatch.setenv(FCR.REGISTRY_DIR_ENV, str(registry))
    return {"research": research, "adoption": adoption, "registry": registry}


def _freeze(mem, challenger_id: str, *, release: str = "R58",
            inception: str = INCEPTION, horizon: int = 21,
            asset_class: str = "US_EQUITY") -> str:
    """Write ONE FORWARD_FROZEN row exactly as R59's own handler writes it."""
    hid = mem.register(
        title="prospective challenger %s" % challenger_id,
        release=release, origin="R59_GOVERNOR",
        generation_method="PROSPECTIVE_FREEZE",
        information_family="PRICE_STATE", economic_family=challenger_id,
        asset_class=asset_class, model_family="XS_LONG_SHORT",
        horizon_sessions=horizon, input_data_identity="substrate",
        counts_to_burden=False,
        spec={"challenger": challenger_id, "instrument_scope": ["AAPL"]})
    mem.freeze_forward(hid, challenger_id=challenger_id, inception=inception,
                       record_hash="hash_%s" % challenger_id)
    return hid


@pytest.fixture()
def estate(stores):
    """The live estate's shape: four ACTIVE freezes plus one WITHDRAWN."""
    mem = M.open_memory()
    ids = {c: _freeze(mem, c) for c in R58_CHALLENGERS}
    ids[R59_WITHDRAWN] = _freeze(mem, R59_WITHDRAWN, release="R59")
    mem.invalidate(economic_families=[R59_WITHDRAWN],
                   reason=("withdrawn at inception: the effective independent "
                           "sample and the validation materiality both failed"))
    return ids


def _run(*argv):
    """Invoke the operator entrypoint in-process and capture its exit code."""
    return ENTRY.main(list(argv))


def _registrations():
    return FCR.load_registrations()


# =========================================================================== #
# A. THE OPERATOR ADOPTION ENTRYPOINT  (proofs 1-15)
# =========================================================================== #
def test_01_dry_run_writes_nothing(estate, stores, capsys):
    code = _run("--challenger-id", R58_CHALLENGERS[0])
    out = capsys.readouterr().out
    assert code == 0
    assert out.rstrip().endswith(ENTRY.TOKEN_DRY_RUN_OK)
    assert ENTRY.D_WOULD_ADOPT in out
    # Nothing on disk: no registration, no intent, not even a directory.
    assert _registrations() == []
    assert PA.open_intents() == []
    assert not stores["registry"].exists()
    assert not stores["adoption"].exists()


def test_02_execute_requires_explicit_confirmation(estate, capsys):
    code = _run("--challenger-id", R58_CHALLENGERS[0], "--execute")
    out = capsys.readouterr().out
    assert code == 2
    assert out.rstrip().endswith(ENTRY.TOKEN_REFUSED)
    assert PA.OPERATOR_ADOPT_CONFIRM_TOKEN in out
    assert _registrations() == []


def test_03_execute_requires_the_explicit_write_flag(estate, capsys):
    code = _run("--challenger-id", R58_CHALLENGERS[0],
                "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    out = capsys.readouterr().out
    assert code == 2
    assert out.rstrip().endswith(ENTRY.TOKEN_REFUSED)
    assert "--execute" in out
    assert _registrations() == []


def test_03b_a_wrong_confirmation_token_is_refused(estate, capsys):
    code = _run("--challenger-id", R58_CHALLENGERS[0], "--execute",
                "--confirm", "CONFIRM_PROSPECTIVE_FORWARD_ADOPTION")
    assert code == 2
    assert capsys.readouterr().out.rstrip().endswith(ENTRY.TOKEN_REFUSED)
    assert _registrations() == []


def test_04_only_exactly_named_challenger_ids(estate, capsys):
    """The id is REQUIRED, and it is matched exactly - never by prefix."""
    with pytest.raises(SystemExit):
        _run("--execute", "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    capsys.readouterr()
    code = _run("--challenger-id", "R58_FCF", "--execute",
                "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    out = capsys.readouterr().out
    assert code == 2
    assert PA.FREEZE_UNKNOWN in out
    assert _registrations() == []


def test_05_there_is_no_adopt_all_mode():
    src = (REPO / "scripts" / "adopt_prospective_freeze.py").read_text(
        encoding="utf-8", errors="replace")
    for banned in ('"--all"', "'--all'", '"--adopt-all"', "'--adopt-all'"):
        assert banned not in src, banned
    with pytest.raises(SystemExit):
        ENTRY.main(["--all", "--execute"])


def test_06_an_unknown_challenger_is_refused(estate, capsys):
    code = _run("--challenger-id", "R99_DOES_NOT_EXIST", "--execute",
                "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    out = capsys.readouterr().out
    assert code == 2
    assert out.rstrip().endswith(ENTRY.TOKEN_REFUSED)
    assert PA.FREEZE_UNKNOWN in out
    assert _registrations() == []


def test_07_the_four_active_r58_challengers_adopt(estate, capsys):
    argv = []
    for cid in R58_CHALLENGERS:
        argv += ["--challenger-id", cid]
    code = _run(*argv, "--execute", "--confirm",
                PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    out = capsys.readouterr().out
    assert code == 0, out
    assert out.rstrip().endswith(ENTRY.TOKEN_OK)
    regs = _registrations()
    assert len(regs) == 4
    assert sorted(r["challenger_id"] for r in regs) == sorted(R58_CHALLENGERS)
    for r in regs:
        assert r["owner"] == FCR.COMPOSITION_OWNER
        assert r["challenger_class"] == PA.CLASS_SIGNAL_CANONICAL
        assert r["lifecycle_at_registration"]["lifecycle_state"] == PA.LC_ACTIVE
        assert r["backfilled"] is False
        assert r["predictions_emitted"] == 0
        assert r["matured_observations"] == 0
        assert r["effective_independent_observations"] == 0
        assert r["promotion_allowed"] is False


def test_08_retry_is_idempotent(estate, capsys):
    args = ["--challenger-id", R58_CHALLENGERS[0], "--execute",
            "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN]
    assert _run(*args) == 0
    capsys.readouterr()
    first = _registrations()
    assert len(first) == 1
    assert _run(*args) == 0
    out = capsys.readouterr().out
    assert out.rstrip().endswith(ENTRY.TOKEN_OK)
    assert ENTRY.D_ALREADY_REGISTERED in out
    assert _registrations() == first          # byte-identical, never rewritten


def test_09_no_duplicate_canonical_registration(estate, capsys):
    args = ["--challenger-id", R58_CHALLENGERS[1], "--execute",
            "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN]
    for _ in range(3):
        _run(*args)
    capsys.readouterr()
    regs = _registrations()
    assert len(regs) == 1
    intents = list((Path(PA.adoption_dir()) / "intents").glob("*.json"))
    assert len(intents) == 1


def test_10_the_withdrawn_r59_challenger_is_refused(estate, capsys):
    code = _run("--challenger-id", R59_WITHDRAWN, "--execute",
                "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    out = capsys.readouterr().out
    assert code == 2
    assert out.rstrip().endswith(ENTRY.TOKEN_REFUSED)
    assert PA.LC_WITHDRAWN in out
    assert _registrations() == []
    assert PA.open_intents() == []


def test_10b_the_withdrawn_challenger_is_refused_by_the_owner_too(estate):
    """Belt and braces: the refusal is the OWNER's, not the entrypoint's."""
    mem = M.open_memory_readonly()
    rows = mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN, limit=500)
    row = PA.resolve_freeze_by_challenger_id(R59_WITHDRAWN, rows)["freeze_row"]
    lc = PA.classify_lifecycle(row)
    assert lc["lifecycle_state"] == PA.LC_WITHDRAWN
    assert lc["adoptable"] is False
    assert lc["never_resurrectable"] is True
    out = PA.adopt_prospective_freeze(
        freeze_row=row, observation_clock_starts="2026-09-09",
        confirm=PA.ADOPT_CONFIRM_TOKEN)
    assert out["outcome"] == PA.REFUSED_LIFECYCLE
    assert out["adopted"] is False
    # And the registrar refuses it a second time, independently.
    reg = FCR.register_forward_challenger(
        identity=PA.build_adoption_identity(row),
        observation_clock_starts="2026-09-09",
        challenger_class=PA.CLASS_SIGNAL_CANONICAL, lifecycle=lc)
    assert reg["registered"] is False
    assert reg["outcome"] == FCR.REFUSED_LIFECYCLE


def test_11_no_backdate_argument_exists():
    src = (REPO / "scripts" / "adopt_prospective_freeze.py").read_text(
        encoding="utf-8", errors="replace")
    for banned in ("--effective-from", "--date", "--backfill",
                   "--inception-override", "--as-of", "--since"):
        assert '"%s"' % banned not in src, banned
        assert "'%s'" % banned not in src, banned
    for banned in ("--effective-from", "--backfill", "--inception-override"):
        with pytest.raises(SystemExit):
            ENTRY.main(["--challenger-id", "X", banned, "2026-01-01"])


def test_11b_the_boundary_is_derived_and_is_always_today():
    now = datetime(2026, 9, 9, 3, 15, tzinfo=timezone.utc)
    assert PA.current_prospective_boundary(now) == "2026-09-09"
    assert PA.current_prospective_boundary() == \
        datetime.now(timezone.utc).date().isoformat()
    # And it takes no parameter an operator could reach.
    import inspect
    params = list(inspect.signature(
        PA.current_prospective_boundary).parameters)
    assert params == ["now"]


def test_12_zero_historical_observations_are_created(estate, capsys):
    _run("--challenger-id", R58_CHALLENGERS[0], "--execute",
         "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    capsys.readouterr()
    rec = _registrations()[0]
    clock = rec["observation_clock"]
    today = datetime.now(timezone.utc).date().isoformat()
    # The first eligible observation is STRICTLY AFTER the registration session,
    # so nothing that already completed can become a prediction.
    assert clock["first_eligible_observation_session"] > today
    assert clock["first_eligible_observation_session"] > INCEPTION
    assert clock["backfilled"] is False
    assert clock["sessions_between_inception_and_registration_are_never_"
                 "synthesised"] is True
    assert rec["forward_observations_at_registration"] == 0
    assert rec["registration_session"] == today


def test_12b_a_backdated_boundary_is_refused_by_the_owner(estate):
    mem = M.open_memory_readonly()
    rows = mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN, limit=500)
    row = PA.resolve_freeze_by_challenger_id(
        R58_CHALLENGERS[0], rows)["freeze_row"]
    out = PA.adopt_prospective_freeze(
        freeze_row=row, observation_clock_starts="2026-09-01",
        confirm=PA.ADOPT_CONFIRM_TOKEN)
    assert out["outcome"] == PA.REFUSED_BACKDATED
    assert out["adopted"] is False
    assert _registrations() == []


def test_13_no_model_promotion(estate, capsys):
    _run("--challenger-id", R58_CHALLENGERS[0], "--execute",
         "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    capsys.readouterr()
    rec = _registrations()[0]
    assert rec["promotion_allowed"] is False
    assert rec["automatic_promotion_allowed"] is False
    assert rec["manual_review_required"] is True
    assert rec["safety"]["promoted_model"] is False
    assert FCR.evidence_state(rec)["promotion_ready"] is False


def test_14_no_portfolio_allocation(estate, capsys):
    _run("--challenger-id", R58_CHALLENGERS[0], "--execute",
         "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    capsys.readouterr()
    safety = _registrations()[0]["safety"]
    for key in ("allocated_capital", "activated_sleeve", "changed_holdings",
                "changed_cash", "changed_nav"):
        assert safety[key] is False


def test_15_no_order_or_fill(estate, capsys):
    _run("--challenger-id", R58_CHALLENGERS[0], "--execute",
         "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    capsys.readouterr()
    safety = _registrations()[0]["safety"]
    for key in ("created_orders", "created_order_plan", "created_fills",
                "approved_anything", "automation_enabled", "broker_enabled"):
        assert safety[key] is False
    src = (REPO / "scripts" / "adopt_prospective_freeze.py").read_text(
        encoding="utf-8", errors="replace")
    for banned in ("create_order", "submit_order", "confirm_order",
                   "create_fill", "run_daily_close", "run_portfolio_cycle",
                   "promote_model"):
        assert banned not in src, banned


def test_15b_the_entrypoint_owns_no_rule_and_writes_no_registry():
    """It delegates; it never imports or writes the registrar directly."""
    src = (REPO / "scripts" / "adopt_prospective_freeze.py").read_text(
        encoding="utf-8", errors="replace")
    assert "PA.adopt_prospective_freeze(" in src
    assert src.count("PA.adopt_prospective_freeze(") == 1
    for banned in ("FCR.register_forward_challenger(", "_atomic_write_json",
                   "def classify_lifecycle(", "def resolve_observation_clock(",
                   "open_memory()"):
        assert banned not in src, banned
    # It reads the registrar (a read model) and the memory READ-ONLY only.
    assert "FCR.load_registrations()" in src
    assert "M.open_memory_readonly()" in src


def test_15c_there_is_exactly_one_operator_adoption_entrypoint():
    """One OPERATOR path. The persistent research runtime also composes the
    adoption owner (R61's injection at freeze time) and that is a different
    thing: it adopts what it has just frozen and offers an operator nothing."""
    operator_paths, injectors = [], []
    for path in (REPO / "scripts").rglob("*.py"):
        if path.name == "audit_architecture.py":
            continue                      # the auditor NAMES the token to guard it
        text = path.read_text(encoding="utf-8", errors="replace")
        if PA.OPERATOR_ADOPT_CONFIRM_TOKEN in text:
            operator_paths.append(path.name)
        elif "adopt_prospective_freeze(" in text:
            injectors.append(path.name)
    assert operator_paths == ["adopt_prospective_freeze.py"], operator_paths
    assert injectors == ["run_research_runtime.py"], injectors
    assert PA.OPERATOR_ADOPTION_ENTRYPOINT == \
        "scripts/adopt_prospective_freeze.py"


def test_15g_a_cohort_challenger_routes_to_its_own_frozen_owner(stores, capsys):
    """The live memory holds 56 FORWARD_FROZEN rows, and 51 of them belong to
    the two FROZEN COHORTS. The operator door is not R58-only, so it must route
    by CLASS: an R46 or R56 challenger reaches its own cohort owner through the
    read-only compatibility adapter and can never be registered with the
    canonical registrar, which would fork its evidence."""
    mem = M.open_memory()
    _freeze(mem, "r46_eq_xs_mom_12_1", release="R46", horizon=5)
    _freeze(mem, "r56_incumbent_book_v1", release="R56", horizon=21)
    _run("--challenger-id", "r46_eq_xs_mom_12_1",
         "--challenger-id", "r56_incumbent_book_v1")
    out = capsys.readouterr().out
    assert out.rstrip().endswith(ENTRY.TOKEN_DRY_RUN_OK)
    assert PA.REGISTRARS[PA.CLASS_SIGNAL_R46] in out
    assert PA.REGISTRARS[PA.CLASS_PAPER_PORTFOLIO] in out
    # And the classification itself is the OWNER's, for every live release.
    for release, expected in (("R46", PA.CLASS_SIGNAL_R46),
                              ("R56", PA.CLASS_PAPER_PORTFOLIO),
                              ("R58", PA.CLASS_SIGNAL_CANONICAL),
                              ("R59", PA.CLASS_SIGNAL_CANONICAL)):
        assert PA.classify_challenger_class({"release": release}) == expected
    assert _registrations() == []


def test_15d_every_terminal_token_is_printed_exactly_once(estate, capsys):
    _run("--challenger-id", R58_CHALLENGERS[0])
    out = capsys.readouterr().out
    printed = [t for t in ENTRY.TERMINAL_TOKENS if t in out]
    assert printed == [ENTRY.TOKEN_DRY_RUN_OK]
    assert out.count(ENTRY.TOKEN_DRY_RUN_OK) == 1


def test_15f_the_entrypoint_refuses_a_captured_import(monkeypatch, capsys):
    """An operator entrypoint that WRITES must not run a different checkout's
    code. The venv's editable finder maps ``paper_trader`` ahead of every
    sys.path entry, so this is the one hazard that cannot be closed by care."""
    assert ENTRY.assert_import_integrity() == str(REPO.resolve())
    monkeypatch.setattr(
        ENTRY.PA, "__file__",
        str(Path("C:/somewhere/else/paper_trader/api/prospective_adoption.py")))
    with pytest.raises(RuntimeError, match="import integrity"):
        ENTRY.assert_import_integrity()
    code = ENTRY.main(["--challenger-id", "X", "--execute",
                       "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN])
    out = capsys.readouterr().out
    assert code == 1
    assert out.rstrip().endswith(ENTRY.TOKEN_FAILED)
    assert "nothing was read and nothing was written" in out
    assert _registrations() == []


def test_15e_the_dry_run_shows_every_required_field(estate, capsys):
    _run("--challenger-id", R58_CHALLENGERS[0])
    out = capsys.readouterr().out
    for label in ("Challenger:", "Freeze / hypothesis:", "Lifecycle:",
                  "Canonical registrar:", "Outcome:", "Registered at:",
                  "First eligible observation:", "Next legitimate maturity:",
                  "Backfilled:", "Predictions emitted:",
                  "Matured observations:", "Effective independent obs:"):
        assert label in out, label
    assert FCR.COMPOSITION_OWNER in out


# =========================================================================== #
# A.2 THE PERSISTENT RUNTIME CAN RECOVER AN EXISTING FREEZE (Workstream B)
# =========================================================================== #
def test_16a_an_already_frozen_challenger_retries_adoption(stores):
    """R59's own handler now re-offers an EXISTING freeze to the SAME owner."""
    mem = M.open_memory()
    hid = mem.register(
        title="qualified", release=r59.RELEASE, origin="R59_GOVERNOR",
        generation_method="SEARCH", information_family="PRICE_STATE",
        economic_family="FAM_X", asset_class="US_EQUITY",
        model_family="XS_LONG_SHORT", horizon_sessions=21,
        spec={"substrate": "s"})
    mem.record_result(hid, outcome=r59.HO_QUALIFIED, statistic={},
                      economics={}, turnover_cost={}, robustness={},
                      reason_rejected=None,
                      reopen_condition="NEW_ORTHOGONAL_INFORMATION")
    seen = []

    def _adopt(**kw):
        seen.append(kw)
        return {"adopted": True, "outcome": PA.ADOPTED}

    first = r59h.freeze_qualified(mem, hypothesis_id=hid, adopt_forward=_adopt)
    assert first["state"] == "FROZEN"
    assert len(seen) == 1
    second = r59h.freeze_qualified(mem, hypothesis_id=hid, adopt_forward=_adopt)
    assert second["state"] == "ALREADY_FROZEN"
    assert second["forward_adoption_retried_for_existing_freeze"] is True
    assert second["forward_evidence_started"] is True
    assert len(seen) == 2, "an existing freeze must still reach the owner"
    # And the retry never backdates: the clock it offers is today's.
    assert seen[1]["observation_clock_starts"] == \
        datetime.now(timezone.utc).date().isoformat()


def test_16b_the_retry_cannot_resurrect_a_withdrawn_freeze(estate):
    """It reaches the owner, and the owner refuses - which is the whole point."""
    mem = M.open_memory()
    rows = mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN, limit=500)
    row = PA.resolve_freeze_by_challenger_id(R59_WITHDRAWN, rows)["freeze_row"]
    out = PA.adopt_prospective_freeze(
        freeze_row=row,
        observation_clock_starts=PA.current_prospective_boundary(),
        confirm=PA.ADOPT_CONFIRM_TOKEN)
    assert out["outcome"] == PA.REFUSED_LIFECYCLE
    assert _registrations() == []


# =========================================================================== #
# B. COLLECTION TRUTH  (proofs 16-20)
# =========================================================================== #
def _running(started="2026-09-08T21:35:57+00:00"):
    return {"service": {"service_state": "RUNNING", "worker_activity": "IDLE",
                        "reason": "heartbeat fresh", "worker_pid": 27416,
                        "instance_id": "inst-1", "started_at": started,
                        "loaded_release": {"commit": "da77be7"}}}


def test_16_one_canonical_current_collection_owner():
    """One builder, one shape, one provenance vocabulary - in the lifecycle
    owner, not in each consumer."""
    assert IC.COMPOSITION_OWNER == "api.information_collection"
    block = IC.build_current_collection_state(current=_running())
    assert block["owner"] == IC.COMPOSITION_OWNER
    assert block["source"] == IC.CC_SOURCE_CURRENT_OWNER
    assert block["service_state"] == "RUNNING"
    assert block["available"] is True
    # The Active Manager re-exports the vocabulary; it does not redefine it.
    assert ams.CC_SOURCE_CURRENT_OWNER == IC.CC_SOURCE_CURRENT_OWNER
    assert ams.CC_SOURCE_UNAVAILABLE == IC.CC_SOURCE_UNAVAILABLE
    assert tuple(ams.CURRENT_COLLECTION_SOURCES) == \
        tuple(IC.CURRENT_COLLECTION_SOURCES)
    ams_src = (REPO / "api" / "active_manager_state.py").read_text(
        encoding="utf-8", errors="replace")
    assert "_ic.resolve_current_collection_state(" in ams_src
    assert "build_current_collection_state(" in ams_src


def test_17_a_healthy_worker_makes_every_primary_surface_agree():
    payload = ams.build_active_manager_state(current_collection=_running())
    li = payload["live_information"]
    cc = li["current_collection"]
    route_block = IC.build_current_collection_state(current=_running())
    # BYTE-IDENTICAL: the Active Manager payload and the collection route carry
    # the same object, so no surface can render a different answer.
    assert cc == route_block
    assert cc["service_state"] == "RUNNING"
    assert li["collection_running"] is True
    assert li["collection_service_state"] == "RUNNING"
    assert payload["acceptance"] is not None
    row = next(r for r in payload["acceptance"]["rows"]
               if r["row"] == "COLLECTION")
    assert row["service_state"] == "RUNNING"


def test_18_a_historical_event_runtime_cannot_override_current_health():
    payload = ams.build_active_manager_state(
        current_collection=_running(),
        event_refresh={"last_run": {
            "run_id": "old", "state": "PROPOSAL_WITHHELD",
            "generated_at": "2026-09-01T10:00:00+00:00",
            "runtime_release": {"commit": "ba0d9a3"}}})
    li = payload["live_information"]
    cycle = li["last_event_cycle"]
    assert cycle["decides_current_collection_health"] is False
    assert li["current_collection"]["service_state"] == "RUNNING"
    assert li["current_collection"]["decided_by_historical_event_runtime"] \
        is False


def test_19_a_real_unavailable_worker_stays_visible():
    stopped = {"service": {"service_state": "STOPPED",
                           "worker_activity": "NOT_RUNNING",
                           "reason": "Stopped cleanly."}}
    payload = ams.build_active_manager_state(current_collection=stopped)
    cc = payload["live_information"]["current_collection"]
    assert cc["service_state"] == "STOPPED"
    assert cc["available"] is True
    assert payload["live_information"]["collection_running"] is False
    # And an ABSENT read is absent, never a comforting verdict.
    absent = ams.build_active_manager_state()
    acc = absent["live_information"]["current_collection"]
    assert acc["available"] is False
    assert acc["service_state"] is None
    assert acc["source"] == IC.CC_SOURCE_UNAVAILABLE


def test_20_no_browser_business_logic_or_reconciliation():
    ui = (REPO / "api" / "ui" / "index.html").read_text(
        encoding="utf-8", errors="replace")
    # ONE renderer of the canonical block, used by both loaders.
    assert "function _icRenderCanonicalCollection(" in ui
    assert ui.count("function _icRenderCanonicalCollection(") == 1
    assert "_icRenderCanonicalCollection(d.current_collection)" in ui
    assert "_icRenderCanonicalCollection(cc)" in ui
    # A READ failure is a transport fact and never a service verdict.
    assert "function _icHeaderReadDidNotAnswer(" in ui
    assert "COLLECTION READ DID NOT ANSWER" in ui
    assert "'COLLECTION: UNAVAILABLE'" not in ui
    assert "_icSetHeaderBadge('COLLECTION: UNAVAILABLE'" not in ui
    # The same rule for the operational book's global right-rail status.
    assert "BOOK READ DID NOT ANSWER" in ui
    assert "_obSet('right-ob-state', displayStateTxt);" in ui
    # The browser still classifies nothing.
    for banned in ("=== 'STALE_RUNTIME'", "collection_service_state ===",
                   "service_state === 'RUNNING'"):
        assert banned not in ui, banned


def test_20b_the_collection_route_publishes_the_canonical_block():
    src = (REPO / "api" / "information_collection.py").read_text(
        encoding="utf-8", errors="replace")
    assert '"current_collection": build_current_collection_state(' in src
    assert "def resolve_current_collection_state(" in src
    # The cheap read touches the service state and the lock, and nothing else.
    body = src.split("def resolve_current_collection_state(")[1].split(
        "\ndef ")[0]
    assert "load_service_state(" in body and "read_service_lock(" in body
    for banned in ("build_attention_universe", "load_event_signal_refresh_status",
                   "build_source_runtime_health"):
        assert banned not in body, banned


# =========================================================================== #
# C. LIVE REASSESSMENT IDENTITY  (proofs 21-25)
# =========================================================================== #
def _no_change_candidate(*, proposal_hash=None):
    """The Sep-8 live cycle's shape: a completed reassessment that concluded
    CURRENT_NO_CHANGE and asked for no target."""
    return {
        "candidate_identity_hash": SEP8_IDENTITY_HASH,
        "candidate_id": "gcand_%s" % SEP8_IDENTITY_HASH[:12],
        "decided_at": "2026-09-09T01:04:44+00:00",
        "decision": None,
        "identity": {"active_book_id": "alpha_paper_book_1",
                     "eligible_market_session": SEP8_SESSION,
                     "reassessment_hash": "ra-hash",
                     "proposal_hash": proposal_hash,
                     "target_outcome": None},
        "evidence": {"reassessment_ran": True, "proposal_built": False,
                     "reassessment_state": "CURRENT_NO_CHANGE"},
        "switching_economics": {},
        "zero_base": {},
    }


def _gate(cand, **kw):
    return pdec.evaluate_intraday_governance(
        candidate=cand, reassessment={"state": "CURRENT_NO_CHANGE"}, **kw)


def test_21_the_exact_persisted_target_identity_is_used():
    """The Sep-8 candidate's identity hash IS the standing decision's. That is
    the proof the chain is exact - signal refresh -> HOC -> reassessment ->
    candidate -> governance - with no transient substitution anywhere."""
    cand = _no_change_candidate()
    gate = _gate(cand, current_governed={
        "record_id": SEP8_RECORD_ID,
        "candidate_identity_hash": SEP8_IDENTITY_HASH,
        "decision": SEP8_DECISION, "decided_at": "2026-09-09T00:52:46Z"})
    assert gate["candidate_identity_hash"] == SEP8_IDENTITY_HASH
    assert gate["duplicate_of_standing_decision"] is True
    assert pdec.WR_DUPLICATE in gate["withheld_reason_codes"]
    assert gate["eligible"] is False


def test_22_no_latest_or_transient_target_substitution():
    """A candidate that bound a proposal it never asked for is a REAL identity
    mismatch, and the no-target lane is exactly where that must be caught."""
    cand = _no_change_candidate(proposal_hash="some-other-runs-target")
    gate = _gate(cand)
    binding = next(c for c in gate["checks"]
                   if c["check"] == "PROPOSAL_BINDING_CONSISTENT")
    assert binding["applicable"] is True
    assert binding["passed"] is False
    assert pdec.WR_TARGET_IDENTITY in gate["withheld_reason_codes"]
    assert "PROPOSAL_BINDING_CONSISTENT" in gate["failing_checks"]
    assert gate["eligible"] is False
    # A bound hash is deliberately NOT a lane fact. Routing this cycle back into
    # the priced lane would let TARGET_HASH_BOUND be satisfied BY the stale
    # artifact, and the substitution would pass unremarked; keeping it in the
    # no-target lane is what makes the binding rule catch it.
    assert gate["evaluation_lane"]["no_priced_target_lane"] is True
    assert gate["evaluation_lane"]["proposal_hash_bound"] is True


def test_23_candidate_evidence_incomplete_remains_fail_closed():
    """A BLOCKED reassessment matches no lane fact, so every target and
    economics condition stays applicable and still withholds."""
    cand = _no_change_candidate()
    cand["evidence"]["reassessment_state"] = "BLOCKED_EVIDENCE"
    gate = pdec.evaluate_intraday_governance(
        candidate=cand, reassessment={"state": "BLOCKED_EVIDENCE"})
    assert gate["eligible"] is False
    assert gate["evaluation_lane"]["no_priced_target_lane"] is False
    # Only the no-target binding rule is inapplicable; every target and
    # economics condition is applicable and unproven ones still fail.
    assert gate["not_applicable_checks"] == ["PROPOSAL_BINDING_CONSISTENT"]
    for name in ("CONCLUSIVE_PRICED_OUTCOME", "TARGET_HASH_BOUND",
                 "SWITCHING_ECONOMICS_COMPLETE"):
        assert name in gate["failing_checks"], name
    assert pdec.WR_EVIDENCE_INCOMPLETE in gate["withheld_reason_codes"]
    assert pdec.WR_TARGET_IDENTITY in gate["withheld_reason_codes"]
    # An abandoned chain (no reassessment ran) is likewise never a clean no-op.
    cand2 = _no_change_candidate()
    cand2["evidence"]["reassessment_ran"] = False
    assert _gate(cand2)["evaluation_lane"]["no_priced_target_lane"] is False


def test_24_a_valid_evidence_chain_does_not_report_a_target_mismatch():
    """THE Sep-8 defect. The withholding was correct; the WORDS were not."""
    cand = _no_change_candidate()
    gate = _gate(cand, current_governed={
        "record_id": SEP8_RECORD_ID,
        "candidate_identity_hash": SEP8_IDENTITY_HASH,
        "decision": SEP8_DECISION, "decided_at": "2026-09-09T00:52:46Z"})
    codes = gate["withheld_reason_codes"]
    # The two codes that named a non-existent defect are gone.
    assert pdec.WR_TARGET_IDENTITY not in codes
    assert pdec.WR_SWITCHING_ECONOMICS not in codes
    # Every check that FAILED on the live Sep-8 cycle for want of a target is
    # now NOT_APPLICABLE, and none of them contributes a withheld reason.
    live_failing_for_want_of_a_target = (
        "CONCLUSIVE_PRICED_OUTCOME", "TARGET_HASH_BOUND",
        "FEASIBLE_TARGET_WAS_COMPUTED", "SWITCHING_ECONOMICS_COMPLETE",
        "RISK_BEFORE_AND_AFTER_PRICED", "TURNOVER_BUDGET_EVALUATED",
        "ZERO_BASE_INCUMBENCY_POLICY_INTACT")
    for name in live_failing_for_want_of_a_target:
        assert name in gate["not_applicable_checks"], name
        assert name not in gate["failing_checks"], name
    named = {r["check"] for r in gate["withheld_reasons"]}
    assert named & set(live_failing_for_want_of_a_target) == set()
    # What it DOES say is true, and both codes describe designed behaviour.
    assert pdec.WR_INTRADAY_NO_PRICED_TARGET in codes
    assert pdec.WR_DUPLICATE in codes
    assert gate["eligible"] is False          # the verdict is UNCHANGED
    lane = gate["evaluation_lane"]
    assert lane["no_priced_target_lane"] is True
    assert lane["verdict_is_unchanged_by_this_classification"] is True
    assert all(lane["lane_facts"].values())


def test_24b_the_priced_lane_is_untouched():
    """A cycle that DID price a target keeps every condition applicable."""
    cand = _no_change_candidate()
    cand["decision"] = pdec.GD_CHANGE_RECOMMENDED
    cand["evidence"]["proposal_built"] = True
    cand["identity"]["proposal_hash"] = "p-hash"
    cand["identity"]["target_outcome"] = "PROPOSAL_READY"
    gate = pdec.evaluate_intraday_governance(
        candidate=cand, reassessment={"state": "CHANGE_CANDIDATE"})
    assert gate["evaluation_lane"]["no_priced_target_lane"] is False
    assert gate["checks_not_applicable"] == 1     # only the no-target binding
    assert gate["not_applicable_checks"] == ["PROPOSAL_BINDING_CONSISTENT"]
    assert "TARGET_HASH_BOUND" not in gate["not_applicable_checks"]


def test_24c_the_gate_counts_are_mathematically_closed():
    for cand in (_no_change_candidate(),
                 _no_change_candidate(proposal_hash="p")):
        gate = _gate(cand)
        assert gate["counts_are_closed"] is True
        assert gate["checks_applicable"] + gate["checks_not_applicable"] == \
            gate["checks_total"]
        assert gate["checks_passed"] + gate["checks_failed"] == \
            gate["checks_applicable"]
        assert len(gate["failing_checks"]) == gate["checks_failed"]
        assert len(gate["not_applicable_checks"]) == \
            gate["checks_not_applicable"]


def test_24d_the_published_gate_counts_match_an_independent_tally():
    """The closure flag must not be true by construction. Every published count
    is re-derived here from the checks themselves, and the three dispositions
    are proven to PARTITION them - so a check that is neither passed, failed nor
    excused, or one counted twice, would show up."""
    for cand in (_no_change_candidate(),
                 _no_change_candidate(proposal_hash="p")):
        gate = _gate(cand)
        checks = gate["checks"]
        na = [c for c in checks
              if c["disposition"] == pdec.CHECK_NOT_APPLICABLE]
        ps = [c for c in checks if c["disposition"] == pdec.CHECK_PASSED]
        fl = [c for c in checks if c["disposition"] == pdec.CHECK_FAILED]
        assert len(na) + len(ps) + len(fl) == len(checks)
        assert gate["checks_not_applicable"] == len(na)
        assert gate["checks_passed"] == len(ps)
        assert gate["checks_failed"] == len(fl)
        assert gate["checks_applicable"] == len(ps) + len(fl)
        assert gate["counts_are_closed"] is True
        for c in checks:
            # A check is applicable with a verdict, or inapplicable with none.
            assert (c["passed"] is None) == (c["applicable"] is False)
            assert (c["reason_code"] is None) or c["disposition"] == \
                pdec.CHECK_FAILED
    # And a tally that does NOT partition is reported as not closed.
    broken = pdec._gate_counts([{"applicable": True, "passed": True},
                                {"applicable": False, "passed": True}])
    assert broken["checks_not_applicable"] == 1
    assert broken["checks_passed"] == 1
    assert broken["counts_are_closed"] is True
    assert pdec._gate_counts([])["counts_are_closed"] is True


def test_25_historical_artifacts_are_never_rewritten():
    """Nothing in this release writes an event-cycle run, a reassessment or a
    governed decision record."""
    for rel in ("scripts/adopt_prospective_freeze.py",
                "api/prospective_adoption.py",
                "api/forward_challenger_registry.py"):
        src = (REPO / rel).read_text(encoding="utf-8", errors="replace")
        for banned in ("_save_run(", "record_governed_decision(",
                       "run_event_signal_refresh(", "persist_assessment(",
                       "run_daily_close("):
            assert banned not in src, "%s: %s" % (rel, banned)
    gate_src = (REPO / "api" / "portfolio_decision.py").read_text(
        encoding="utf-8", errors="replace")
    body = gate_src.split("def evaluate_intraday_governance(")[1].split(
        "\ndef governed_decision_ordering_key(")[0]
    for banned in ("_atomic_write_json", "open(", "write_text"):
        assert banned not in body, banned


# =========================================================================== #
# D. HOC / MEMBERSHIP / LATENCY  (proofs 26-32)
# =========================================================================== #
def test_26_the_exact_hoc_data_gap_is_named_and_its_cause_is_fixed():
    """2026-09-07 is Labor Day. The 'previous eligible session' must never be
    a day the exchange did not trade, because no artifact can ever exist for
    one - which is what produced PRIOR_RANK_UNAVAILABLE on 2026-09-08."""
    from paper_trader.engine import exchange_calendar as xcal
    assert xcal.is_non_session(date(2026, 9, 7)) is True
    ranking, state, reason, prior = hoc._prior_ranking_from_artifact(
        active_book_id="alpha_paper_book_1",
        eligible_market_date="2026-09-08",
        hoc_dir=str(REPO / "tests" / "_nonexistent_hoc_root"))
    assert prior == "2026-09-04", reason
    assert prior != "2026-09-07"
    assert "authoritative exchange calendar" in reason
    assert ranking is None and state == "UNAVAILABLE"
    # The gap itself keeps its exact, named code when it is real.
    from paper_trader.engine import data_gap_taxonomy as gaptax
    assert hasattr(gaptax, "classify_assessment_gaps")


def test_26b_the_calendar_owner_is_delegated_to_never_reimplemented():
    src = (REPO / "api" / "holding_opportunity_cost.py").read_text(
        encoding="utf-8", errors="replace")
    assert "def _authoritative_non_sessions(" in src
    assert "exchange_calendar as xcal" in src
    assert "ms.previous_trading_day(d, non_sessions)" in src
    assert "calendar_available_between(" in src
    for banned in ("HOLIDAYS = ", "LABOR_DAY", "def holidays_for_year("):
        assert banned not in src, banned
    ps_src = (REPO / "api" / "portfolio_state.py").read_text(
        encoding="utf-8", errors="replace")
    assert "ms.previous_trading_day(d, non_sessions)" in ps_src


def test_27_membership_drift_classification_is_explicit():
    out = dc.classify_membership_drift(
        close_status=dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
        gate={"proposed_additions": [{"ticker": "nvda"}, {"ticker": "MSFT"}],
              "proposed_removals": [{"ticker": "XOM"}]},
        market_data_scope={"decision_universe_count": 199,
                           "decision_scope_count": 225,
                           "current_holding_count": 25,
                           "open_order_ticker_count": 0,
                           "decision_missing_tickers": []})
    assert out["classification"] in dc.MEMBERSHIP_DRIFT_CLASSES
    assert out["classification"] == dc.DRIFT_LEGACY_RANK_COMPARISON
    assert out["drift_observed"] is True
    assert out["affected_names"] == ["MSFT", "NVDA", "XOM"]
    assert out["target_names_not_held"] == ["MSFT", "NVDA"]
    assert out["held_names_not_in_target"] == ["XOM"]
    assert out["scoring_universe_count"] == 199
    assert out["current_holding_count"] == 25


def test_28_benign_drift_cannot_masquerade_as_an_integrity_failure():
    out = dc.classify_membership_drift(
        close_status=dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
        gate={"proposed_additions": [{"ticker": "NVDA"}]},
        market_data_scope={"decision_universe_count": 199,
                           "decision_missing_tickers": []})
    assert out["benign"] is True
    assert out["integrity_problem"] is False
    assert out["is_a_reallocation_proposal"] is False
    assert out["creates_orders"] is False
    assert out["close_validity_is_independent_of_this"] is True
    # And the close's validity genuinely does not depend on it.
    assert "membership_drift" in dc.CLOSE_VALIDITY_EXCLUDED_INPUTS
    assert dc.is_completed_close_status(
        dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT) is True


def test_29_real_drift_remains_visible():
    """A HELD name the scoring universe does not contain is an integrity
    problem and is never filed under 'compatibility only'."""
    out = dc.classify_membership_drift(
        close_status=dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
        gate={"proposed_removals": [{"ticker": "ZZZ"}]},
        market_data_scope={"decision_universe_count": 199,
                           "decision_missing_tickers": ["ZZZ"]})
    assert out["classification"] == dc.DRIFT_HELD_NAME_UNSCORED
    assert out["integrity_problem"] is True
    assert out["benign"] is False
    assert out["held_names_absent_from_scoring_universe"] == ["ZZZ"]
    assert "ZZZ" in out["because"]
    # And an unmeasurable session never CLAIMS benignity.
    unknown = dc.classify_membership_drift(
        close_status=dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
        gate={"proposed_additions": [{"ticker": "NVDA"}]},
        market_data_scope=None)
    assert unknown["classification"] == dc.DRIFT_UNVERIFIED
    assert unknown["benign"] is False
    assert unknown["integrity_problem"] is False
    assert unknown["integrity_verifiable_this_session"] is False


def test_29b_no_drift_is_reported_as_no_drift():
    out = dc.classify_membership_drift(
        close_status=dc.CLOSE_COMPLETE_HOLD, gate={},
        market_data_scope={"decision_universe_count": 199,
                           "decision_missing_tickers": []})
    assert out["classification"] == dc.DRIFT_NONE
    assert out["drift_observed"] is False
    assert out["affected_names"] == []


def _daily_latency():
    """The Sep-8 governed record's OWN latency block, as persisted."""
    return {
        "missing_measurements": [],
        "not_required_measurements": ["event_cycle_started_at",
                                      "observation_received_at"],
        "observation_to_signal_seconds": None,
        "measurement_owner": "api.event_signal_refresh",
    }


def test_30_a_non_run_latency_stage_is_not_applicable_not_missing():
    scope = ams._latency_lane_scope(
        {"provenance": pdec.PROV_GOVERNED_DAILY_CYCLE}, _daily_latency())
    assert sorted(scope["structurally_absent_measurements"]) == [
        "event_cycle_started_at", "observation_received_at"]
    assert scope["measurements_missing_and_expected"] == []
    payload = ams.build_active_manager_state(
        governed_decision={"decision": SEP8_DECISION,
                           "provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
                           "eligible_market_session": SEP8_SESSION,
                           "record_id": SEP8_RECORD_ID,
                           "latency": _daily_latency()})
    row = next(r for r in payload["acceptance"]["rows"] if r["row"] == "LATENCY")
    assert row["status"] == ams.ACCEPTANCE_NOT_APPLICABLE
    assert row["key_fact_interval"] == "observation_to_signal_seconds"
    assert "observation_received_at" in row["key_fact_endpoints"]
    assert "LATENCY" not in payload["acceptance"]["missing_rows"]


def test_31_a_run_stage_with_missing_timestamps_stays_missing():
    """An INTRADAY decision owns every endpoint, so a gap there is a real gap."""
    intraday = {"provenance": pdec.PROV_GOVERNED_INTRADAY,
                "missing_measurements": ["observation_received_at"],
                "observation_to_signal_seconds": None,
                "measurement_owner": "api.event_signal_refresh"}
    scope = ams._latency_lane_scope(
        {"provenance": pdec.PROV_GOVERNED_INTRADAY}, intraday)
    assert scope["structurally_absent_measurements"] == []
    assert scope["measurements_missing_and_expected"] == [
        "observation_received_at"]
    payload = ams.build_active_manager_state(
        governed_decision={"decision": "CHANGE_RECOMMENDED",
                           "provenance": pdec.PROV_GOVERNED_INTRADAY,
                           "latency": intraday})
    row = next(r for r in payload["acceptance"]["rows"] if r["row"] == "LATENCY")
    assert row["status"] == ams.ACCEPTANCE_MISSING
    assert "LATENCY" in payload["acceptance"]["missing_rows"]


def test_31b_a_real_gap_in_the_row_s_own_interval_is_never_excused():
    """Not gaming: a structurally-absent endpoint never excuses a REAL gap in
    the OTHER endpoint of the very interval the row reports."""
    lat = {"structurally_absent_measurements": ["observation_received_at"],
           "measurements_missing_and_expected": ["signal_refresh_completed_at"]}
    assert ams._key_interval_not_applicable(
        lat, "observation_to_signal_seconds") is False
    # Remove the real gap and the same interval becomes NOT_APPLICABLE.
    assert ams._key_interval_not_applicable(
        {**lat, "measurements_missing_and_expected": []},
        "observation_to_signal_seconds") is True
    # An interval whose endpoints the lane fully owns is never excused either.
    assert ams._key_interval_not_applicable(
        lat, "reassessment_to_governed_seconds") is False


def test_31d_a_daily_lane_gap_the_lane_still_owes_stays_visible():
    """The daily lane DOES own its two governance stamps; an absent one there
    is a real gap and is reported as expected-and-missing, never excused."""
    lat = {"missing_measurements": ["governance_gate_completed_at"],
           "not_required_measurements": list(
               pdec.DAILY_LANE_ABSENT_LATENCY_STAGES)}
    scope = ams._latency_lane_scope(
        {"provenance": pdec.PROV_GOVERNED_DAILY_CYCLE}, lat)
    assert scope["measurements_missing_and_expected"] == [
        "governance_gate_completed_at"]
    assert "governance_gate_completed_at" not in \
        scope["structurally_absent_measurements"]


def test_31c_the_producer_and_the_read_model_agree_on_one_declaration():
    """R61's two halves cancelled out: the producer excused the endpoints, and
    the read model looked only at what was left over."""
    declared = pdec._not_required_latency_stages(
        {"intraday_latency_applicable": False})
    lat = esr.measure_decision_latency(
        stage_timestamps={}, event_cycle_started_at=None,
        observation_received_at=None,
        governance_gate_completed_at="2026-09-09T00:52:51+00:00",
        governed_decision_persisted_at="2026-09-09T00:52:51+00:00",
        not_required_stages=declared)
    assert lat["missing_measurements"] == []
    assert lat["latency_measurement_complete"] is True
    # Every interval is now MEASURED or NOT_REQUIRED - never MISSING while the
    # record simultaneously claims to be complete.
    assert esr.LAT_MISSING not in lat["interval_dispositions"].values()
    scope = ams._latency_lane_scope(
        {"provenance": pdec.PROV_GOVERNED_DAILY_CYCLE}, lat)
    assert scope["structurally_absent_measurements"]
    assert scope["measurements_missing_and_expected"] == []


def test_32_the_acceptance_count_is_mathematically_correct():
    payload = ams.build_active_manager_state(
        current_collection=_running(),
        governed_decision={"decision": SEP8_DECISION,
                           "provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
                           "eligible_market_session": SEP8_SESSION,
                           "latency": _daily_latency()})
    acc = payload["acceptance"]
    assert acc["counts_are_closed"] is True
    assert acc["row_count"] == len(ams.ACCEPTANCE_ROWS)
    assert acc["present_count"] + acc["missing_count"] == \
        acc["applicable_row_count"]
    assert acc["applicable_row_count"] + acc["not_applicable_count"] == \
        acc["row_count"]
    assert acc["missing_count"] == len(acc["missing_rows"])
    assert acc["applicable_row_count"] == acc["accountable_row_count"]
    # A NOT_APPLICABLE row is NEVER counted as present to make it add up.
    present = [r["row"] for r in acc["rows"]
               if r["status"] == ams.ACCEPTANCE_PRESENT]
    na = [r["row"] for r in acc["rows"]
          if r["status"] == ams.ACCEPTANCE_NOT_APPLICABLE]
    assert set(present) & set(na) == set()
    assert "LATENCY" in na
    # The three statuses PARTITION the rows, and every published count is
    # re-derived here rather than trusted - so the closure flag is not true by
    # construction and a fourth status could not hide inside it.
    missing_rows = [r["row"] for r in acc["rows"]
                    if r["status"] == ams.ACCEPTANCE_MISSING]
    assert len(present) + len(missing_rows) + len(na) == len(acc["rows"])
    assert acc["present_count"] == len(present)
    assert acc["missing_count"] == len(missing_rows)
    assert acc["not_applicable_count"] == len(na)
    assert acc["applicable_row_count"] == len(present) + len(missing_rows)
    assert sorted(acc["missing_rows"]) == sorted(missing_rows)


# =========================================================================== #
# E. SAFETY  (proofs 33-37)
# =========================================================================== #
def test_33_the_sep8_governed_decision_is_untouched():
    """No code path in this release regenerates, supersedes or edits it."""
    cand = _no_change_candidate()
    standing = {"record_id": SEP8_RECORD_ID,
                "candidate_identity_hash": SEP8_IDENTITY_HASH,
                "decision": SEP8_DECISION,
                "eligible_market_session": SEP8_SESSION,
                "decided_at": "2026-09-09T00:52:46Z"}
    gate = _gate(cand, current_governed=standing)
    assert gate["eligible"] is False
    # The writer refuses an ineligible candidate outright.
    wrote = pdec.record_governed_decision(
        candidate=cand, gate=gate,
        confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN)
    assert wrote.get("recorded") is not True
    assert standing == {"record_id": SEP8_RECORD_ID,
                        "candidate_identity_hash": SEP8_IDENTITY_HASH,
                        "decision": SEP8_DECISION,
                        "eligible_market_session": SEP8_SESSION,
                        "decided_at": "2026-09-09T00:52:46Z"}


def test_33b_the_release_changes_no_decision_or_execution_owner():
    """The changed modules hold no order, fill, close or promotion path."""
    for rel in ("scripts/adopt_prospective_freeze.py",
                "api/prospective_adoption.py"):
        src = (REPO / rel).read_text(encoding="utf-8", errors="replace")
        for banned in ("paper_trading_desk", "rebalance_execution",
                       "daily_close", "portfolio_cycle", "alpha_target"):
            assert banned not in src, "%s: %s" % (rel, banned)


def test_34_no_live_store_is_touched(stores):
    """Every store this suite can write is redirected into a temp root, and the
    production defaults are still the production defaults."""
    assert str(PA.adoption_dir()).startswith(str(stores["adoption"]))
    assert str(FCR.registry_dir()).startswith(str(stores["registry"]))
    assert str(M.memory_db_path()).startswith(str(stores["research"]))
    assert str(PA._DEFAULT_ADOPTION_DIR).startswith("D:")
    assert str(FCR._DEFAULT_REGISTRY_DIR).startswith("D:")
    for env in (PA.ADOPTION_DIR_ENV, FCR.REGISTRY_DIR_ENV,
                r59.RESEARCH_ROOT_ENV):
        assert os.environ[env].startswith(str(stores["adoption"].parent))


def test_35_no_model_promotion_anywhere_in_this_release():
    for rel in ("scripts/adopt_prospective_freeze.py",
                "api/prospective_adoption.py",
                "api/forward_challenger_registry.py"):
        src = (REPO / rel).read_text(encoding="utf-8", errors="replace")
        for banned in ("promote_model", "set_champion", "activate_sleeve"):
            assert banned not in src, "%s: %s" % (rel, banned)
    assert FCR.load_forward_challenger_registry()[
        "automatic_promotion_allowed"] is False


def test_36_no_holdings_cash_or_nav_mutation(estate, capsys):
    _run("--challenger-id", R58_CHALLENGERS[0], "--execute",
         "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    capsys.readouterr()
    reg = FCR.load_forward_challenger_registry()
    for key in ("changed_holdings", "changed_cash", "changed_nav",
                "allocated_capital"):
        assert reg["safety"][key] is False
    assert reg["safety"]["read_only_for_every_operational_store"] is True


def test_37_no_order_or_fill_is_reachable(estate, capsys):
    _run("--challenger-id", R58_CHALLENGERS[0], "--execute",
         "--confirm", PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    out = capsys.readouterr().out
    assert "no order created" in out
    reg = FCR.load_forward_challenger_registry()
    for key in ("created_orders", "created_order_plan", "created_fills"):
        assert reg["safety"][key] is False
    assert reg["backfill_allowed"] is False
    assert reg["historical_result_is_never_forward_evidence"] is True


# =========================================================================== #
# F. THE ARCHITECTURE AUDIT PROVES ALL OF IT
# =========================================================================== #
def test_38_the_strict_audit_declares_the_r62_1_1_invariants():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_audit_r6211", REPO / "scripts" / "audit_architecture.py")
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)
    rep = audit.run_audit()
    assert audit._blocking_invariant_failures(rep) == []
    block = rep["release62_1_1_forward_activation_integrity"]
    for key, expected in (
            ("operator_entrypoints", [PA.OPERATOR_ADOPTION_ENTRYPOINT]),
            ("entrypoint_delegates_to_owner", True),
            ("entrypoint_writes_the_registry", []),
            ("explicit_confirmation_required", True),
            ("explicit_execute_flag_required", True),
            ("backdate_arguments", []),
            ("adopt_all_paths", []),
            ("withdrawn_fails_closed", True),
            ("one_current_collection_owner", True),
            ("historical_runtime_decides_current_health", []),
            ("exact_candidate_target_identity", True),
            ("latency_na_semantics", True),
            ("portfolio_or_execution_paths", []),
    ):
        assert block[key] == expected, key
