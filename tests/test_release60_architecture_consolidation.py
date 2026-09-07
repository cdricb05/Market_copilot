"""Release 60 - architecture consolidation + AlphaAgent outcomes visibility.

R59 made the researcher persistent. It did not make the research legible:
there was no API surface of any kind over the R59 persistent memory, and the
only thing the estate could say about a running AlphaAgent was a process line.
R60 adds ONE read-only projection over that record. These tests protect the
properties that make it safe to read - and that make reading it change
nothing:

* READING IS NOT WRITING. Before R60 the only way to open either research
  store was to construct a writer that created the directory and ran the
  schema script; and the right owner of "what should we research next" (the
  governor) writes a capacity fingerprint and an event every time it is
  asked. The projection therefore uses READ-ONLY handles that refuse every
  mutation and create nothing, and calls none of the writing owners.
* ONE BRAIN. One research memory, one work queue, one persistent runtime.
* PROCESS HEALTH IS NOT RESEARCH SUCCESS. Two disjoint vocabularies, and the
  operator-facing state is the evidence one.
* THE BROWSER RENDERS AND DOES NOT DECIDE.
* NOTHING HERE REACHES CAPITAL. No promotion, no proposal, no allocation, no
  order - and no import that could reach one.
* ABSENCE IS REPORTED AS ABSENCE. A missing store is NOT_AVAILABLE with a
  reason, never an empty research record that reads like a measured zero.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import autonomous_research as AR          # noqa: E402
from alpha_agent import r59                                # noqa: E402
from alpha_agent.r46 import runlock as RL                  # noqa: E402
from alpha_agent.r59 import loop as LP                     # noqa: E402
from alpha_agent.r59 import memory as M                    # noqa: E402
from alpha_agent.r59 import runtime as RT                  # noqa: E402

from paper_trader.api import alphaagent_outcomes as AO     # noqa: E402

UI = (_ROOT / "api" / "ui" / "index.html").read_text(encoding="utf-8")
OWNER_SRC = (_ROOT / "api" / "alphaagent_outcomes.py").read_text(encoding="utf-8")
APP_SRC = (_ROOT / "api" / "app.py").read_text(encoding="utf-8")
INVENTORY = json.loads(
    (_ROOT / "docs" / "architecture" / "system_inventory.json").read_text(
        encoding="utf-8"))


# =========================================================================== #
# Fixtures - a hermetic research estate. No test touches the live store.
# =========================================================================== #
@pytest.fixture()
def root(tmp_path, monkeypatch):
    """A private R59 research root for this test only."""
    d = tmp_path / "r59"
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(d))
    return d


@pytest.fixture()
def no_forward_owners(monkeypatch):
    """Isolate the projection from the live forward-evidence artifacts.

    The forward owners are real read models over the estate's own roots. A
    unit test asserting the projection's CONTRACT must not depend on what
    those roots happen to contain today.
    """
    from paper_trader.api import prospective_tournament as PT
    from paper_trader.api import research_runtime as RR
    from paper_trader.api import shadow_portfolio_evidence as SP
    monkeypatch.setattr(RR, "load_runtime_health",
                        lambda: {"state": "RUNTIME_NEVER_RAN",
                                 "runtime_health": None})
    monkeypatch.setattr(PT, "load_prospective_tournament",
                        lambda *a, **k: {"how_many_models_are_competing": 0,
                                         "which_are_winning": [],
                                         "which_are_losing": [],
                                         "which_are_too_early_to_judge": [],
                                         "which_were_killed": [],
                                         "which_are_data_blocked": []})
    monkeypatch.setattr(SP, "load_records", lambda *a, **k: [])


def _seed(mem, *, n: int = 3):
    """A small, realistic research record: settled hypotheses with recorded
    statistics, a rejection reason and a named reopen condition."""
    ids = []
    for i in range(n):
        hid = mem.register(
            title="hypothesis %d" % i, release=r59.RELEASE, origin="TEST",
            generation_method="PRE_REGISTERED_FAMILY",
            information_family="PRICE_STATE",
            economic_family="TREND_%d" % (i % 2),
            asset_class=r59.AC_COMMODITY, model_family="LINEAR",
            horizon_sessions=21, spec={"i": i})
        mem.record_result(
            hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
            statistic={"lockbox_t": 2.5 + i, "lockbox_p_one_sided": 0.01,
                       "burden_corrected_p": 0.9, "burden_denominator": 40,
                       "lockbox_observations": 60},
            economics={"lockbox_materiality": 0.02,
                       "validation_materiality": 0.001},
            robustness={"checks": {"has_lockbox_observations": True,
                                   "lockbox_material": True,
                                   "validation_material": False,
                                   "validation_same_sign": True,
                                   "lockbox_t_positive": True,
                                   "burden_corrected_significant": False}},
            reason_rejected="validation_material, burden_corrected_significant",
            reopen_condition="NEW_ORTHOGONAL_INFORMATION")
        ids.append(hid)
    return ids


@pytest.fixture()
def estate(root):
    """A seeded memory + queue, closed, ready to be READ."""
    mem = M.open_memory()
    _seed(mem)
    mem.set_frontier(r59.AC_COMMODITY, state=r59.FS_RESEARCH_READY,
                     reason="2 of 9 executable families remain open",
                     detail={"instruments": 103, "search_burden": 3})
    mem.set_opportunity(
        "OPTIONS_IV_SURFACE", title="implied volatility surface history",
        state=r59.DO_PURCHASE_CANDIDATE, asset_class=r59.AC_US_EQUITY,
        gate_verdict="INSUFFICIENT_EVIDENCE_TO_RECOMMEND_PURCHASE",
        cost_usd_year=None)
    mem.close()
    q = LP.open_queue()
    q.enqueue(AR.CAT_EXPERIMENT, lane="r59.economic.commodity_futures",
              payload={"mandate_id": "M59_test", "kind": "ECONOMIC_FAMILY",
                       "asset_class": r59.AC_COMMODITY, "family": "TREND_0",
                       "expected_information_value": 0.61,
                       "reason": "an unprosecuted enumerable family",
                       "issued_by": "alpha_agent.r59.governor",
                       "batch_rank": 0})
    return root


def _snapshot(d: Path) -> dict:
    """Content and identity of every research file, ignoring SQLite's own
    shared-memory companions (which a reader may legitimately recreate and
    which carry no research state)."""
    out = {}
    if not d.exists():
        return out
    for p in sorted(d.rglob("*")):
        if p.is_file() and not p.name.endswith(("-wal", "-shm")):
            out[str(p.relative_to(d))] = hashlib.sha256(
                p.read_bytes()).hexdigest()
    return out


# =========================================================================== #
# 1. The outcome projection is READ ONLY - in store access, not merely intent
# =========================================================================== #
def test_projection_writes_nothing(estate, no_forward_owners):
    before = _snapshot(estate)
    assert before, "the fixture must have created a research estate to read"
    body = AO.load_alphaagent_outcomes()
    after = _snapshot(estate)
    assert after == before, (
        "reading the AlphaAgent outcomes changed the research store: %s"
        % sorted(set(before) ^ set(after)))
    assert body["safety"]["read_only"] is True
    assert body["safety"]["owns_research_state"] is False
    assert body["safety"]["writes_research_store"] is False


def test_read_only_memory_refuses_every_mutation(estate):
    mem = M.open_memory_readonly()
    assert mem.read_only is True
    with pytest.raises(M.ReadOnlyMemory):
        mem.event("PROBE")
    with pytest.raises(M.ReadOnlyMemory):
        mem.set_meta("probe", 1)
    with pytest.raises(M.ReadOnlyMemory):
        mem.register(title="x", release="T", origin="T",
                     generation_method="T", information_family="PRICE_STATE",
                     economic_family="F", asset_class=r59.AC_COMMODITY)
    with pytest.raises(M.ReadOnlyMemory):
        mem.record_result("nope", outcome=r59.HO_REJECTED)
    with pytest.raises(M.ReadOnlyMemory):
        mem.freeze_forward("nope", challenger_id="c", inception="i")
    with pytest.raises(M.ReadOnlyMemory):
        mem.invalidate(economic_families=["F"], reason="r")
    with pytest.raises(M.ReadOnlyMemory):
        mem.set_frontier(r59.AC_US_EQUITY, state=r59.FS_BLOCKED)
    with pytest.raises(M.ReadOnlyMemory):
        mem.set_opportunity("x", title="t", state=r59.DO_BLOCKED)
    with pytest.raises(M.ReadOnlyMemory):
        mem.set_provider_usage("P", "C")
    with pytest.raises(M.ReadOnlyMemory):
        mem.record_generator_yield(asset_class=r59.AC_US_EQUITY, kind="AUTO",
                                   generated=1, novel=1, duplicates=0)
    # Reads still work.
    assert mem.summary()["hypotheses_settled"] == 3


def test_read_only_queue_refuses_every_transition(estate):
    q = LP.open_queue(read_only=True)
    assert q.read_only is True
    with pytest.raises(AR.ReadOnlyQueue):
        q.enqueue(AR.CAT_EXPERIMENT, lane="r59.economic", payload={})
    with pytest.raises(AR.ReadOnlyQueue):
        q.claim_next()
    with pytest.raises(AR.ReadOnlyQueue):
        q.complete("job")
    with pytest.raises(AR.ReadOnlyQueue):
        q.block_specific("job", "reason")
    with pytest.raises(AR.ReadOnlyQueue):
        q.mark_retryable("job", "reason")
    with pytest.raises(AR.ReadOnlyQueue):
        q.requeue_stale()
    assert q.counts_by_state()["QUEUED"] == 1


def test_read_only_handles_create_nothing(root):
    """An absent store stays absent. Opening a reader is not a migration."""
    assert not root.exists()
    with pytest.raises(FileNotFoundError):
        M.open_memory_readonly()
    with pytest.raises(FileNotFoundError):
        LP.open_queue(read_only=True)
    assert not (root / M.DB_NAME).exists()
    assert not (root / LP.QUEUE_NAME).exists()
    # The runtime's READ paths must not create the artifact directory either.
    assert RT.read_status() == {}
    assert not RT.runtime_dir(create=False).exists()


def test_projection_never_calls_a_writing_research_owner():
    """The governor is the right owner of what to research next - and asking
    it WRITES a capacity fingerprint and an event. A GET that generated
    mandates would make opening a dashboard change the research plan."""
    for call in ("generate_mandates(", "stop_reason(", "capacity_allocation(",
                 "measure(", "report.build(", "run_session(", "run_forever(",
                 "open_memory(", "record_result(", "freeze_forward(",
                 "set_frontier(", "set_opportunity(", "set_meta(",
                 "seed_mandates("):
        assert call not in OWNER_SRC, (
            "the read model calls a WRITING research owner: %s" % call)
    assert "import sqlite3" not in OWNER_SRC
    assert "sqlite3.connect" not in OWNER_SRC


# =========================================================================== #
# 2. The UI renders; it does not calculate research verdicts
# =========================================================================== #
def _ui_region() -> str:
    """The R60 renderer. Bounded by markers so a guard can never drift onto a
    neighbouring release's code."""
    a = UI.find("RELEASE 60 ALPHAAGENT OUTCOMES START")
    b = UI.find("RELEASE 60 ALPHAAGENT OUTCOMES END")
    assert a != -1 and b > a, "the R60 UI script region markers are missing"
    return UI[a:b]


def _ui_panel() -> str:
    a = UI.find("RELEASE 60 ALPHAAGENT OUTCOMES PANEL START")
    b = UI.find("RELEASE 60 ALPHAAGENT OUTCOMES PANEL END")
    assert a != -1 and b > a, "the R60 UI panel region markers are missing"
    return UI[a:b]


def test_ui_has_exactly_one_loader_against_the_owner_route():
    assert UI.count("function loadAlphaAgentOutcomes") == 1
    assert UI.count("_mhzGet('/v1/research/alphaagent-outcomes')") == 1


def test_ui_does_not_derive_the_governance_state():
    region = _ui_region()
    for state in AO.GOVERNANCE_STATES:
        assert ("=== '%s'" % state) not in region
        assert ('=== "%s"' % state) not in region
        assert ("state = '%s'" % state) not in region
    # The badge is whatever the backend said it was.
    assert "var state = gov.state" in region
    assert "stateEl.textContent = state" in region


def test_ui_computes_no_research_mathematics():
    region = _ui_region()
    for tok in ("Math.log", "Math.sqrt", "Math.exp", "Math.pow",
                "lockbox_t >", "t_stat >", "p_value", "* 10000"):
        assert tok not in region, "research arithmetic in the browser: %s" % tok


def test_ui_offers_no_action_and_no_browser_dialog():
    region = _ui_region()
    for tok in ("alert(", "confirm(", "method: 'POST'", 'method: "POST"',
                "runUiAction("):
        assert tok not in region
    assert "alert(" not in UI and "confirm(" not in UI


def test_ui_extends_the_existing_research_surface():
    """No second dashboard, and no lost deep link."""
    assert 'id="ra-nav-alphaagent"' in UI
    assert 'data-rasub="alphaagent"' in UI
    assert 'data-route="alphaagent"' not in UI
    tab = UI.find('id="tab-audit-advanced"')
    panel = UI.find('id="aaout-panel"')
    assert tab != -1 and panel > tab
    for sub in ("research-agent", "research-bridge", "performance",
                "daily-operations", "paper-books", "revalidation",
                "tournament", "alpha-factory", "price-alpha", "alpha-agent",
                "stage11", "stage12", "stage13a", "data-expansion",
                "diagnostics"):
        assert 'data-rasub="%s"' % sub in UI, "deep link lost: " + sub


def test_ui_keeps_the_safety_badges_visible():
    panel = _ui_panel()
    for badge in ("READ ONLY", "RESEARCH ONLY", "MANUAL REVIEW",
                  "NO MODEL PROMOTION", "NO PORTFOLIO MUTATION", "NO ORDERS",
                  "AUTOMATION OFF", "NO PURCHASE"):
        assert badge in panel, "safety badge missing: " + badge
    # The evidence badge and the worker badge are two different nodes, so a
    # green process can never stand in for a research result.
    assert 'id="aao-state"' in panel
    assert 'id="aao-worker-badge"' in panel
    assert "PROCESS HEALTH &ne; RESEARCH SUCCESS" in panel


def test_ui_panel_carries_no_action_control():
    panel = _ui_panel()
    for tok in ("Create Order", "Approve", "Promote", "Allocate", "Purchase ",
                "Execute", "onsubmit", "<form"):
        assert tok not in panel, "an action control on a read surface: " + tok
    # The one control is a read-only reload.
    assert panel.count("<button") == 1
    assert 'onclick="loadAlphaAgentOutcomes()"' in panel


# =========================================================================== #
# 3-4. No second research queue / store / runtime
# =========================================================================== #
def test_one_memory_one_queue_one_persistent_runtime():
    agent = _ROOT / "alpha_agent"
    src = "\n".join(p.read_text(encoding="utf-8", errors="replace")
                    for p in sorted(agent.rglob("*.py")))
    assert src.count("class ResearchMemory") == 1
    assert src.count("class ResearchQueue") == 1
    assert src.count("def run_forever(") == 1


def test_r60_added_no_research_store_module():
    for path in ("api/research_memory.py", "api/alphaagent_memory.py",
                 "api/alphaagent_queue.py", "api/research_queue.py",
                 "engine/research_memory.py", "alpha_agent/r60"):
        assert not (_ROOT / path).exists(), "a second research store: " + path


def test_projection_reuses_the_r59_owners_rather_than_querying_sqlite():
    body_reads = AO.load_alphaagent_outcomes.__doc__ or ""
    assert "canonical owner" in body_reads.lower()
    assert "alpha_agent.r59.memory" in OWNER_SRC
    assert "alpha_agent.autonomous_research" in OWNER_SRC


# =========================================================================== #
# 5-8. Nothing here reaches a model, a portfolio, an order or capital
# =========================================================================== #
def test_owner_contains_no_promotion_execution_or_allocation_call():
    for call in ("place_order(", "submit_order(", "create_order(",
                 "run_fill_cycle(", "confirm_rebalance_order_plan(",
                 "run_daily_close(", "run_portfolio_cycle(", "promote_model(",
                 "promote_challenger(", "approve_proposal(",
                 "activate_sleeve(", "recalibrate("):
        assert call not in OWNER_SRC, "forbidden call in the read model: " + call


def test_owner_imports_no_operational_write_path():
    for mod in ("rebalance_execution", "paper_trading_desk", "daily_close",
                "portfolio_cycle", "alpha_book", "portfolio_decision",
                "reallocation_proposal", "db.session", "sqlalchemy"):
        assert "import %s" % mod not in OWNER_SRC
        assert "from paper_trader.api import %s" % mod not in OWNER_SRC


def test_no_challenger_to_capital_path(estate, no_forward_owners):
    body = AO.load_alphaagent_outcomes()
    gov = body["governance"]
    assert gov["automatic_promotion_allowed"] is False
    assert gov["portfolio_mutation_allowed"] is False
    assert gov["capital_allocation_allowed"] is False
    assert gov["manual_review_required"] is True
    assert body["safety"]["promotes_model"] is False
    assert body["safety"]["allocates_capital"] is False
    assert body["safety"]["creates_orders"] is False
    assert body["safety"]["approves_proposal"] is False
    # The strongest thing a challenger can reach is a sentence for a person.
    assert AO.G_CHALLENGER_REVIEW == "CHALLENGER_WARRANTS_GOVERNED_REVIEW"


def test_route_is_get_only_and_has_no_action_sibling():
    assert '"/v1/research/alphaagent-outcomes"' in APP_SRC
    assert "@app.get(\n    \"/v1/research/alphaagent-outcomes\"" in APP_SRC
    for verb in ("@app.post(\n    \"/v1/research/alphaagent",
                 "@app.put(\n    \"/v1/research/alphaagent",
                 "@app.delete(\n    \"/v1/research/alphaagent"):
        assert verb not in APP_SRC
    for tail in ("alphaagent-outcomes/run", "alphaagent-outcomes/promote",
                 "alphaagent-outcomes/approve", "alphaagent-outcomes/purchase",
                 "alphaagent-outcomes/mandate"):
        assert tail not in APP_SRC


# =========================================================================== #
# 9. Forward evidence comes from the canonical owners, never from here
# =========================================================================== #
def test_forward_evidence_is_read_from_the_canonical_owners():
    assert "api import research_runtime" in OWNER_SRC
    assert "api import prospective_tournament" in OWNER_SRC
    assert "api import shadow_portfolio_evidence" in OWNER_SRC
    assert "historical_result_is_never_forward_evidence" in OWNER_SRC


def test_an_unadopted_freeze_is_reported_as_orphaned_not_as_a_zero(
        estate, no_forward_owners, monkeypatch):
    """A freeze no forward owner knows about accrues nothing, and says so."""
    mem = M.open_memory()
    hid = mem.register(
        title="frozen", release=r59.RELEASE, origin="TEST",
        generation_method="PROSPECTIVE_FREEZE",
        information_family="PRICE_STATE", economic_family="ORPHAN_V1",
        asset_class=r59.AC_US_EQUITY, counts_to_burden=False,
        spec={"frozen": True})
    mem.freeze_forward(hid, challenger_id="ORPHAN_V1",
                       inception="2026-09-01T00:00:00+00:00",
                       record_hash="abc")
    mem.close()

    body = AO.load_alphaagent_outcomes()
    pr = body["prospective"]
    assert pr["freezes_recorded_in_research_memory"] == 1
    assert pr["freezes_not_registered_with_a_forward_owner"] == 1
    row = pr["orphan_freezes"][0]
    assert row["forward_evidence_link"] == AO.FWD_LINK_ORPHAN
    assert row["challenger_id"] == "ORPHAN_V1"
    assert row["accruing_through"] is None
    # And it becomes a named piece of human work, not a silent zero.
    actions = [a["action"] for a in body["human_action"]["actions"]]
    assert "REGISTER_ORPHAN_PROSPECTIVE_FREEZE" in actions


def test_an_adopted_freeze_names_the_owner_that_accrues_it(
        estate, monkeypatch):
    from paper_trader.api import prospective_tournament as PT
    from paper_trader.api import research_runtime as RR
    from paper_trader.api import shadow_portfolio_evidence as SP
    monkeypatch.setattr(RR, "load_runtime_health",
                        lambda: {"runtime_health": {"promotion_ready_count": 0}})
    monkeypatch.setattr(PT, "load_prospective_tournament",
                        lambda *a, **k: {
                            "which_are_winning": [],
                            "which_are_losing": [],
                            "which_are_too_early_to_judge": [
                                {"challenger_id": "ADOPTED_V1"}],
                            "which_were_killed": [],
                            "which_are_data_blocked": []})
    monkeypatch.setattr(SP, "load_records", lambda *a, **k: [])

    mem = M.open_memory()
    hid = mem.register(
        title="frozen", release=r59.RELEASE, origin="TEST",
        generation_method="PROSPECTIVE_FREEZE",
        information_family="PRICE_STATE", economic_family="ADOPTED_V1",
        asset_class=r59.AC_US_EQUITY, counts_to_burden=False,
        spec={"frozen": True})
    mem.freeze_forward(hid, challenger_id="ADOPTED_V1",
                       inception="2026-09-01T00:00:00+00:00")
    mem.close()

    body = AO.load_alphaagent_outcomes()
    pr = body["prospective"]
    assert pr["freezes_not_registered_with_a_forward_owner"] == 0
    assert pr["freezes"][0]["forward_evidence_link"] == AO.FWD_LINK_ADOPTED
    assert "prospective_tournament" in pr["freezes"][0]["accruing_through"]
    assert body["governance"]["state"] == AO.G_FORWARD_MATURING


# =========================================================================== #
# 10. Missing evidence renders NOT_AVAILABLE, never a fabricated zero
# =========================================================================== #
def test_absent_research_memory_is_an_absence_not_a_negative_result(
        root, no_forward_owners):
    assert not root.exists()
    body = AO.load_alphaagent_outcomes()
    assert body["availability"]["research_memory"] == AO.NOT_AVAILABLE
    assert body["governance"]["state"] == AO.G_MEMORY_ABSENT
    assert "absence of evidence" in body["governance"]["because"]
    assert body["governance"]["manual_review_required"] is True
    # Asking the question did not create the store.
    assert not root.exists()


def test_undecidable_window_is_reported_not_guessed(estate, no_forward_owners):
    """No worker has ever started here, and no eligible session is available
    from the forward owner - so both windows say so rather than showing a
    zero that reads like a measured fact."""
    body = AO.load_alphaagent_outcomes()
    wins = body["recent_activity"]["windows"]
    assert wins["since_worker_start"]["state"] == AO.NOT_AVAILABLE
    assert wins["since_worker_start"]["reason"] == \
        "R59_WORKER_NEVER_STARTED_ON_THIS_MACHINE"
    assert wins["since_latest_eligible_session"]["state"] == AO.NOT_AVAILABLE
    assert wins["lifetime"]["hypotheses_settled"] == 3


def test_duplicate_rejections_are_declared_unavailable_with_a_reason(
        estate, no_forward_owners):
    body = AO.load_alphaagent_outcomes()
    dup = body["recent_outcomes"]["duplicates_rejected_before_evaluation"]
    assert dup["state"] == AO.NOT_AVAILABLE
    assert dup["reason"] == "NOT_PERSISTED"
    assert "generator_yield" in dup["detail"]


def test_windows_separate_measured_work_from_imported_work(
        estate, no_forward_owners):
    """settled_at is the instant THIS memory recorded a verdict, which for an
    imported prior-release result is the import instant. A backfill is not a
    day of research."""
    mem = M.open_memory()
    hid = mem.register(
        title="imported", release="R46", origin="IMPORT",
        generation_method="PRE_REGISTERED_FAMILY",
        information_family="PRICE_STATE", economic_family="IMPORTED",
        asset_class=r59.AC_US_EQUITY, spec={"imported": True})
    mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE)
    mem.close()

    body = AO.load_alphaagent_outcomes()
    life = body["recent_activity"]["windows"]["lifetime"]
    assert life["hypotheses_settled"] == 4
    assert life["measured_here"] == 3
    assert life["imported_from_prior_release"] == 1
    assert "backfill is not a day" in body["recent_activity"]["why_split"]


# =========================================================================== #
# 11. Process health is distinct from alpha-evidence state
# =========================================================================== #
def test_process_health_and_alpha_evidence_are_different_questions(
        estate, no_forward_owners):
    body = AO.load_alphaagent_outcomes()
    assert body["runtime"]["process_health_is_not_research_success"] is True
    assert body["governance"]["process_health_is_a_separate_question"] is True
    # The two vocabularies do not overlap.
    assert not (set(RT.WORKER_STATES) & set(AO.GOVERNANCE_STATES))
    # And the headline leads with the evidence state.
    assert body["headline"].startswith(body["governance"]["state"])


def test_no_qualified_alpha_is_the_state_while_research_proceeds(
        estate, no_forward_owners):
    body = AO.load_alphaagent_outcomes()
    assert body["governance"]["state"] == AO.G_NO_QUALIFIED
    assert body["governance"]["qualified_historically"] == 0
    assert body["governance"]["research_is_blocked"] is False


def test_research_waiting_is_distinguished_from_research_blocked(root,
                                                                monkeypatch,
                                                                no_forward_owners):
    """No open scope and no runnable work is WAITING FOR NEW INFORMATION, and
    a scope with no substrate is BLOCKED. They are different sentences."""
    mem = M.open_memory()
    _seed(mem, n=1)
    mem.set_frontier(r59.AC_COMMODITY, state=r59.FS_EXHAUSTED,
                     reason="every executable family has been prosecuted")
    mem.set_frontier(r59.AC_CREDIT, state=r59.FS_BLOCKED,
                     reason="no owned substrate")
    mem.close()
    LP.open_queue()  # exists, and is empty

    body = AO.load_alphaagent_outcomes()
    gov = body["governance"]
    assert gov["state"] == AO.G_WAITING
    assert gov["blocked_scopes"] == [r59.AC_CREDIT]
    assert gov["exhausted_scopes"] == [r59.AC_COMMODITY]
    assert gov["research_is_blocked"] is True


# =========================================================================== #
# 12. Graveyard / rejection evidence remains persisted and readable
# =========================================================================== #
def test_graveyard_reports_what_failed_and_why_it_may_reopen(
        estate, no_forward_owners):
    body = AO.load_alphaagent_outcomes()
    gy = body["graveyard"]
    assert gy["rejected_total"] == 3
    assert gy["nothing_is_deleted"] is True
    assert gy["rejections_stay_in_the_denominator"] is True
    fam = gy["major_families_rejected"][0]
    assert fam["reopen_condition"] == "NEW_ORTHOGONAL_INFORMATION"
    # The refusing gates are the RECORDED ones, translated to the operator's
    # dimensions by a dictionary lookup and nothing else.
    assert "MULTIPLICITY" in fam["reasons"]
    assert "MATERIALITY_VALIDATION" in fam["reasons"]


def test_graveyard_total_is_the_census_not_the_page_size(estate,
                                                        no_forward_owners):
    body = AO.load_alphaagent_outcomes()
    gy = body["graveyard"]
    vol = body["research_volume"]["by_outcome"]
    assert gy["rejected_total"] == vol.get(r59.HO_NO_ALPHA_EVIDENCE, 0) \
        + vol.get(r59.HO_REJECTED, 0)
    assert gy["family_breakdown_is_complete"] is True


def test_best_candidate_reports_the_recorded_gate_not_a_recomputed_one(
        estate, no_forward_owners):
    body = AO.load_alphaagent_outcomes()
    cands = body["best_current_candidates"]["candidates"]
    assert cands, "a settled hypothesis with a lockbox statistic must appear"
    top = cands[0]
    # Ranked by the SIGNED statistic: the seeded t values are 2.5, 3.5, 4.5.
    assert top["historical_statistics"]["lockbox_t"] == 4.5
    assert top["evidence_class"] == "HISTORICAL"
    assert top["confers_no_capital"] is True
    why = top["why_not_yet_qualified"]
    assert set(why["failed_dimensions"]) == {"MATERIALITY_VALIDATION",
                                             "MULTIPLICITY"}
    assert why["gate_owner"] == "alpha_agent.r59.engines.gate"
    assert why["checks_as_recorded"]["validation_material"] is False


# =========================================================================== #
# 13. Current research intent comes from AlphaAgent's own state
# =========================================================================== #
def test_current_intent_and_next_research_come_from_the_queue(
        estate, no_forward_owners):
    body = AO.load_alphaagent_outcomes()
    nxt = body["next_research"]
    assert nxt["queued"], "the seeded mandate must be visible as next research"
    job = nxt["queued"][0]
    assert job["mandate_id"] == "M59_test"
    assert job["mandate_kind"] == "ECONOMIC_FAMILY"
    assert job["expected_information_value"] == 0.61
    assert job["why_selected"] == "an unprosecuted enumerable family"
    assert job["issued_by"] == "alpha_agent.r59.governor"
    assert "never asks the governor" in nxt["generated_by"]
    intent = body["current_research_intent"]
    assert intent["expected_information_value_is_the_governors"] is True
    assert intent["current_mandate"] is None  # nothing has been CLAIMED


# =========================================================================== #
# 14. Data-opportunity states come from the canonical opportunity frontier
# =========================================================================== #
def test_data_opportunities_come_from_the_frontier_and_recommend_nothing(
        estate, no_forward_owners):
    body = AO.load_alphaagent_outcomes()
    op = body["data_opportunities"]
    assert op["state_counts"] == {"PURCHASE_CANDIDATE": 1}
    row = op["purchase_candidates"][0]
    assert row["opportunity_id"] == "OPTIONS_IV_SURFACE"
    assert row["gate_verdict"] == "INSUFFICIENT_EVIDENCE_TO_RECOMMEND_PURCHASE"
    # PURCHASE_CANDIDATE is a state, not a recommendation.
    assert op["purchase_actually_recommended"] is False
    assert op["nothing_is_purchased_here"] is True
    assert op["manual_purchase_approval_required"] is True
    assert "api.data_expansion" in op["purchase_verdict_owner"]
    assert not any(a["action"] == "REVIEW_DATA_PURCHASE_RECOMMENDATION"
                   for a in body["human_action"]["actions"])


def test_a_gate_recommendation_becomes_a_named_human_decision(
        root, no_forward_owners):
    mem = M.open_memory()
    _seed(mem, n=1)
    mem.set_opportunity("REC", title="a recommended dataset",
                        state=r59.DO_PURCHASE_CANDIDATE,
                        gate_verdict="PURCHASE_RECOMMENDED")
    mem.close()
    LP.open_queue()
    body = AO.load_alphaagent_outcomes()
    assert body["data_opportunities"]["purchase_actually_recommended"] is True
    actions = [a["action"] for a in body["human_action"]["actions"]]
    assert "REVIEW_DATA_PURCHASE_RECOMMENDATION" in actions


# =========================================================================== #
# 15. The architecture inventory has ONE owner per canonical business concept
# =========================================================================== #
def test_every_canonical_concept_has_exactly_one_authoritative_owner():
    for concept in INVENTORY["canonical_concepts"]:
        owner = concept.get("authoritative_owner")
        assert isinstance(owner, str) and owner.strip(), (
            "concept without a single authoritative owner: %s"
            % concept.get("concept"))


def test_inventory_records_the_research_concepts_r60_mapped():
    names = {c["concept"] for c in INVENTORY["canonical_concepts"]}
    for concept in ("persistent_research_runtime_lifecycle",
                    "research_persistent_memory", "research_work_queue",
                    "research_hypothesis_generation", "search_burden",
                    "research_graveyard", "prospective_challenger_freeze",
                    "true_forward_maturation", "data_opportunity_frontier",
                    "alphaagent_research_outcomes"):
        assert concept in names, "inventory concept missing: " + concept


def test_inventory_records_the_new_owner_and_its_route():
    paths = {m["path"] for m in INVENTORY["modules"]}
    assert "api/alphaagent_outcomes.py" in paths
    prefixes = {r["prefix"] for r in INVENTORY["route_ownership"]}
    assert "/v1/research/alphaagent-outcomes" in prefixes
    row = [m for m in INVENTORY["modules"]
           if m["path"] == "api/alphaagent_outcomes.py"][0]
    assert row["classification"] == "KEEP"
    assert row["endpoints"] == ["/v1/research/alphaagent-outcomes"]
    assert row["direct_writes"] and "none" in row["direct_writes"][0]


# =========================================================================== #
# Bounded consolidations landed in the canonical owners
# =========================================================================== #
def test_lease_does_not_call_a_healthy_worker_dead():
    """A worker started by a scheduled task cannot be queried by an
    unprivileged reader. Reading that refusal as death licensed
    _reclaim_if_stale to delete a HEALTHY AlphaAgent's lease and let a second
    one start - which is exactly what the single-worker lease exists to
    prevent. Access-denied is now undecidable, and undecidable fails closed."""
    import os
    assert RL.pid_alive(os.getpid()) is True
    # A pid the kernel says does not exist is decidably dead.
    assert RL.pid_alive(999999) is False
    # Undecidable inputs are never asserted dead.
    assert RL.pid_alive(None) is None
    assert RL.pid_alive(0) is None
    if os.name == "nt":
        src = (_ROOT / "alpha_agent" / "r46" / "runlock.py").read_text(
            encoding="utf-8")
        assert "_ERROR_ACCESS_DENIED" in src
        assert "kernel32.GetLastError()" in src


def test_reclaim_refuses_an_undecidable_but_fresh_lease(tmp_path,
                                                        monkeypatch):
    lease = tmp_path / "worker.lease"
    RL.acquire_path(lease, "holder-a", wait_s=0, stale_after_s=3600)
    monkeypatch.setattr(RL, "pid_alive", lambda pid: None)
    assert RL._reclaim_if_stale(lease, 3600) == {}
    assert lease.exists(), (
        "an undecidable pid must not license reclaiming a fresh lease")


def test_runtime_status_reports_the_total_and_the_settled_count(estate):
    mem = M.open_memory_readonly()
    q = LP.open_queue(read_only=True)
    st = RT.status(mem=mem, queue=q, read_only=True)
    summary = mem.summary()
    assert st["cumulative_hypotheses"] == summary["hypotheses_total"] == 3
    assert st["cumulative_hypotheses_settled"] == summary["hypotheses_settled"]
    assert st["read_only"] is True
    assert st["queue_ready"] == 1


def test_runtime_status_read_only_creates_no_runtime_directory(estate):
    """A status read is a read. It must not bring the artifact directory into
    existence as a side effect."""
    runtime_dir = estate / RT.RUNTIME_SUBDIR
    assert not runtime_dir.exists()
    mem = M.open_memory_readonly()
    q = LP.open_queue(read_only=True)
    RT.status(mem=mem, queue=q, read_only=True)
    assert not runtime_dir.exists()
