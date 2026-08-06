"""Phase 29A architecture-contract tests.

These assert the STRUCTURE of the canonical architecture documents, the
machine-readable inventory, the route-ownership mapping, and the read-only
static audit tool. They validate structure (headings, tables, JSON schema,
route parsing) rather than counting arbitrary text.

All tests are read-only: they import the audit tool (which never imports or
starts the application) and read repository files. Nothing here mutates the
repository, the database, or any ledger.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs"
CHARTER = DOCS / "PROJECT_CHARTER.md"
CURRENT = DOCS / "CURRENT_ARCHITECTURE.md"
TARGET = DOCS / "TARGET_ARCHITECTURE.md"
ROADMAP = DOCS / "CONSOLIDATION_ROADMAP.md"
DECISIONS = DOCS / "ARCHITECTURE_DECISIONS.md"
INVENTORY = DOCS / "architecture" / "system_inventory.json"
CLAUDE_MD = REPO / "CLAUDE.md"
AUDIT_PY = REPO / "scripts" / "audit_architecture.py"

CANONICAL_DOCS = [CHARTER, CURRENT, TARGET, ROADMAP, DECISIONS, INVENTORY]
CLASSIFICATIONS = {"KEEP", "CONSOLIDATE", "DEPRECATE", "REMOVE_CANDIDATE",
                   "INVESTIGATE"}
LOCAL_ONLY = (".claude/settings.json", ".playwright-mcp/",
              "paper_trader_8001.stderr.log", "paper_trader_8001.stdout.log")


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def audit():
    spec = importlib.util.spec_from_file_location("audit_architecture", AUDIT_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def inventory():
    return json.loads(_read(INVENTORY))


# --------------------------------------------------------------------------- #
# Documents exist
# --------------------------------------------------------------------------- #
def test_all_canonical_documents_exist():
    missing = [str(p.relative_to(REPO)) for p in CANONICAL_DOCS if not p.exists()]
    assert not missing, f"missing canonical documents: {missing}"


# --------------------------------------------------------------------------- #
# Charter structure
# --------------------------------------------------------------------------- #
def test_charter_has_exactly_seven_milestones():
    heads = re.findall(r"^### Milestone (\d+) —", _read(CHARTER), re.M)
    assert [int(h) for h in heads] == [1, 2, 3, 4, 5, 6, 7], heads


def test_charter_has_exactly_eight_principles():
    heads = re.findall(r"^### Principle (\d+) —", _read(CHARTER), re.M)
    assert [int(h) for h in heads] == [1, 2, 3, 4, 5, 6, 7, 8], heads


def test_charter_states_three_operating_cycles():
    text = _read(CHARTER).lower()
    assert "three operating cycles" in text
    for cycle in ("signal refresh", "portfolio reassessment",
                  "model recalibration"):
        assert cycle in text, f"missing operating cycle: {cycle}"


def test_charter_defers_intraday_and_execution():
    text = _read(CHARTER)
    assert "Explicitly Deferred Scope" in text
    assert "Milestone 6" in text and "Milestone 7" in text


# --------------------------------------------------------------------------- #
# CLAUDE.md references the canonical documents
# --------------------------------------------------------------------------- #
def test_claude_md_references_all_canonical_docs():
    text = _read(CLAUDE_MD)
    for ref in ("docs/PROJECT_CHARTER.md", "docs/CURRENT_ARCHITECTURE.md",
                "docs/TARGET_ARCHITECTURE.md", "docs/CONSOLIDATION_ROADMAP.md",
                "docs/ARCHITECTURE_DECISIONS.md"):
        assert ref in text, f"CLAUDE.md missing reference: {ref}"


def test_claude_md_preserves_existing_workflow_rules():
    # The append must not have destroyed the existing UI-workflow instructions.
    text = _read(CLAUDE_MD)
    assert "Paper Trader Mandatory UI Redesign Workflow" in text
    assert "END PAPER TRADER MANDATORY UI REDESIGN WORKFLOW" in text


# --------------------------------------------------------------------------- #
# Inventory schema
# --------------------------------------------------------------------------- #
def test_inventory_parses_and_follows_schema(inventory):
    for key in ("schema_version", "systems", "canonical_concepts",
                "route_ownership", "modules"):
        assert key in inventory, f"inventory missing top-level key: {key}"
    assert isinstance(inventory["modules"], list) and inventory["modules"]
    required = {"path", "system", "responsibility", "classification",
                "target_owner", "duplicate_with", "direct_reads",
                "direct_writes", "endpoints", "ui_consumers", "tests",
                "migration_priority", "risk", "notes"}
    for m in inventory["modules"]:
        assert required <= set(m), f"module missing keys: {m.get('path')}"
        assert m["classification"] in CLASSIFICATIONS, m["path"]
        assert isinstance(m["migration_priority"], int)
        assert isinstance(m["duplicate_with"], list)
        assert m["path"] and m["target_owner"]


def test_inventory_module_paths_exist_on_disk(inventory):
    missing = [m["path"] for m in inventory["modules"]
               if not (REPO / m["path"]).exists()]
    assert not missing, f"inventory lists non-existent modules: {missing}"


def test_every_canonical_concept_has_one_owner_in_inventory(inventory):
    seen = set()
    for c in inventory["canonical_concepts"]:
        assert c["concept"] not in seen, f"duplicate concept: {c['concept']}"
        seen.add(c["concept"])
        assert c["authoritative_owner"].strip(), c["concept"]


# --------------------------------------------------------------------------- #
# Every inventoried API route has an owner
# --------------------------------------------------------------------------- #
def _segs(path: str) -> list[str]:
    return [s for s in path.split("/") if s and not s.startswith("{")]


def _owner_for(path: str, ownership: list[dict]):
    if path == "/":
        for e in ownership:
            if e["prefix"] == "/":
                return e
        return None
    segs = _segs(path)
    best, best_len = None, -1
    for e in ownership:
        if e["prefix"] == "/":
            continue
        psegs = _segs(e["prefix"])
        if psegs and psegs == segs[:len(psegs)] and len(psegs) > best_len:
            best, best_len = e, len(psegs)
    return best


def test_every_declared_route_has_an_owner(audit, inventory):
    routes = audit.check_routes()["routes"]
    assert len(routes) >= 170, f"unexpected route count: {len(routes)}"
    ownership = inventory["route_ownership"]
    assert ownership and all(e.get("owner") for e in ownership)
    unmapped = sorted({r["path"] for r in routes
                       if _owner_for(r["path"], ownership) is None})
    assert not unmapped, f"routes with no owner: {unmapped}"


# --------------------------------------------------------------------------- #
# Audit reporting capabilities
# --------------------------------------------------------------------------- #
def test_audit_reports_duplicate_route_declarations(audit):
    dupes = audit.check_routes()["duplicate_declarations"]
    assert isinstance(dupes, list)
    # There are currently none; the reporting mechanism must still exist.
    assert dupes == [], f"unexpected duplicate route declarations: {dupes}"


def test_research_only_modules_have_no_execution_calls(audit):
    files = audit._iter_source_files()
    hits = audit.check_research_execution_terms(files)
    assert hits == [], f"execution calls found in research-only code: {hits}"


def test_audit_is_read_only_and_deterministic(audit):
    # Deterministic: two runs over a fixed tree are byte-identical.
    a = json.dumps(audit.run_audit(), sort_keys=True)
    b = json.dumps(audit.run_audit(), sort_keys=True)
    assert a == b
    # Read-only: run_audit() must not mutate any repository file.
    watched = [AUDIT_PY, CHARTER, INVENTORY, CLAUDE_MD]
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in watched}
    audit.run_audit()
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in watched}
    assert before == after


def test_local_only_files_are_not_release_artifacts(audit, inventory):
    # Local-only files must never appear as inventoried modules or route owners.
    inv_paths = {m["path"] for m in inventory["modules"]}
    for f in LOCAL_ONLY:
        assert f not in inv_paths
        assert all(f != e["owner"] for e in inventory["route_ownership"])
    reported = audit.check_local_only_not_released()["local_only_files"]
    assert set(LOCAL_ONLY) <= set(reported)


# --------------------------------------------------------------------------- #
# Target architecture: one owner per canonical concept
# --------------------------------------------------------------------------- #
def _parse_owner_table(text: str) -> list[tuple[str, str]]:
    """Parse the '| Concept | Target owner | ...' markdown table rows."""
    rows = []
    in_table = False
    for line in text.splitlines():
        if re.match(r"^\|\s*Concept\s*\|\s*Target owner\s*\|", line):
            in_table = True
            continue
        if in_table:
            if not line.strip().startswith("|"):
                break
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if set(cells[0]) <= set("-: "):  # separator row
                continue
            if len(cells) >= 2 and cells[0]:
                rows.append((cells[0], cells[1]))
    return rows


def test_target_assigns_one_owner_per_canonical_concept():
    rows = _parse_owner_table(_read(TARGET))
    assert len(rows) >= 15, f"owner table too small: {len(rows)}"
    concepts = [c for c, _ in rows]
    assert len(concepts) == len(set(concepts)), "duplicate concept in owner table"
    for concept, owner in rows:
        assert owner, f"concept without an owner: {concept}"
        # exactly one owner cell (no ' / ' listing multiple modules as co-owners
        # for the operational concepts that must be single-owner)
        assert owner.strip()


# --------------------------------------------------------------------------- #
# Roadmap: no big-bang rewrite
# --------------------------------------------------------------------------- #
def test_roadmap_forbids_big_bang_rewrite():
    text = _read(ROADMAP)
    assert "No big-bang rewrite" in text, "missing the no-big-bang guardrail"
    # Every occurrence of 'big-bang' must be a negation, never a proposed phase.
    for m in re.finditer(r"(.{0,6})big-bang", text, re.I):
        prefix = m.group(1).lower()
        assert "no " in prefix or "not" in prefix or "never" in prefix, \
            f"big-bang not negated near: {m.group(0)!r}"
    # No slice proposes a from-scratch rewrite as its migration method.
    assert "rewrite from scratch" not in text.lower()


def test_roadmap_orders_dates_and_state_first():
    text = _read(ROADMAP)
    s1 = text.index("## Slice 1")
    s2 = text.index("## Slice 2")
    assert "market session" in text[s1:s2].lower()
    assert "workflow" in text[s2:text.index("## Slice 3")].lower()


# --------------------------------------------------------------------------- #
# Stage 13A is Data Expansion, not the main objective
# --------------------------------------------------------------------------- #
def test_stage13a_classified_under_data_expansion(inventory):
    charter = _read(CHARTER)
    m5 = charter.index("### Milestone 5 — Data Expansion")
    m6 = charter.index("### Milestone 6")
    assert "analyst revisions" in charter[m5:m6].lower()
    # Not part of Milestones 1-4.
    m1_to_4 = charter[charter.index("### Milestone 1"):m5].lower()
    assert "analyst revision" not in m1_to_4

    roadmap = _read(ROADMAP)
    s9 = roadmap.index("## Slice 9")
    s10 = roadmap.index("## Slice 10")
    assert "Data Expansion" in roadmap[s9:s10]
    assert re.search(r"stage 13a|analyst.revision", roadmap[s9:s10], re.I)

    # In the inventory, the analyst-revisions route is owned by the research OS,
    # not by an operational module.
    owners = {e["prefix"]: e for e in inventory["route_ownership"]}
    ar = owners.get("/v1/research/analyst-revisions")
    assert ar is not None and ar["system"] == "research_os"


# --------------------------------------------------------------------------- #
# Slice 1 (Phase 29B): canonical market session & data-freshness ownership
# --------------------------------------------------------------------------- #
def test_slice1_market_session_ownership_guard(audit):
    rep = audit.check_market_session_ownership(audit._iter_source_files())
    assert rep["owner_present"] is True
    assert rep["freshness_owner_present"] is True
    assert all(rep["delegating_wrappers"].values()), rep["delegating_wrappers"]
    assert all(rep["migrated_wrappers_clean"].values()), rep["migrated_wrappers_clean"]
    # No NEW independent current-session arithmetic (the desk resolver is the only
    # documented remainder and is allow-listed).
    assert rep["unexpected_session_resolvers"] == [], rep["unexpected_session_resolvers"]
    # The UI performs no market-date arithmetic and has exactly one freshness loader.
    assert rep["ui_market_date_arithmetic"] == [], rep["ui_market_date_arithmetic"]
    assert rep["ui_freshness_loader_count"] == 1, rep["ui_freshness_loader_count"]


def test_slice1_inventory_drift_zero(audit):
    d = audit.run_audit()["inventory_drift"]
    assert d["status"] == "OK", d["status"]
    assert d["on_disk_not_in_inventory"] == [], d["on_disk_not_in_inventory"]
    assert d["in_inventory_not_on_disk"] == [], d["in_inventory_not_on_disk"]


def test_slice1_new_route_present_and_owned(audit, inventory):
    paths = {r["path"] for r in audit.check_routes()["routes"]}
    assert "/v1/operations/data-freshness" in paths
    owners = {e["prefix"]: e for e in inventory["route_ownership"]}
    e = owners.get("/v1/operations/data-freshness")
    assert e is not None and e["owner"] == "api/data_freshness.py"


def test_slice1_modules_inventoried(inventory):
    paths = {m["path"] for m in inventory["modules"]}
    assert "engine/market_session.py" in paths
    assert "api/data_freshness.py" in paths


def test_slice1_canonical_concepts_owned(inventory):
    concepts = {c["concept"]: c for c in inventory["canonical_concepts"]}
    assert "market_session" in concepts["eligible_market_date"]["authoritative_owner"]
    assert concepts["data_source_freshness"]["authoritative_owner"].startswith(
        "api/data_freshness.py")


# --------------------------------------------------------------------------- #
# Slice 2 (Phase 29C): canonical workflow / operator-state ownership
# --------------------------------------------------------------------------- #
def test_slice2_workflow_state_ownership_guard(audit):
    rep = audit.check_workflow_state_ownership(audit._iter_source_files())
    assert rep["owner_present"] is True, rep
    assert rep["endpoint_present"] is True, rep
    # 55 / 63 / 64 — exactly one UI loader and NO client-side workflow-priority or
    # assessment-currency derivation in the loader/render region.
    assert rep["ui_workflow_loader_count"] == 1, rep["ui_workflow_loader_count"]
    assert rep["ui_workflow_priority_derivation"] == [], rep["ui_workflow_priority_derivation"]


def test_slice2_new_route_present_and_owned(audit, inventory):
    paths = {r["path"] for r in audit.check_routes()["routes"]}
    assert "/v1/operations/workflow-state" in paths
    owners = {e["prefix"]: e for e in inventory["route_ownership"]}
    e = owners.get("/v1/operations/workflow-state")
    assert e is not None and e["owner"] == "api/workflow_state.py"


def test_slice2_module_inventoried(inventory):
    paths = {m["path"] for m in inventory["modules"]}
    assert "api/workflow_state.py" in paths


def test_slice2_inventory_drift_zero(audit):
    d = audit.run_audit()["inventory_drift"]
    assert d["status"] == "OK", d["status"]
    assert d["on_disk_not_in_inventory"] == [], d["on_disk_not_in_inventory"]
    assert d["in_inventory_not_on_disk"] == [], d["in_inventory_not_on_disk"]


def test_67_workflow_state_is_the_canonical_owner(inventory):
    concepts = {c["concept"]: c for c in inventory["canonical_concepts"]}
    wf = concepts["workflow_gate_state"]
    assert "api/workflow_state.py" in wf["authoritative_owner"]
    # The specialized modules remain listed but as DOMAIN-fact owners, not the
    # combined interpretation.
    assert "api/workflow_state.py" in wf["current_writers"]


def test_69_no_duplicate_workflow_state_owner(inventory):
    # Exactly one route owner and one inventoried module path for workflow_state.
    owners = [e for e in inventory["route_ownership"]
              if e["owner"] == "api/workflow_state.py"]
    assert len(owners) == 1
    mods = [m for m in inventory["modules"] if m["path"] == "api/workflow_state.py"]
    assert len(mods) == 1 and mods[0]["classification"] == "KEEP"


def test_70_roadmap_still_forbids_big_bang_and_orders_state_second():
    text = _read(ROADMAP)
    assert "No big-bang rewrite" in text
    s2 = text.index("## Slice 2")
    s3 = text.index("## Slice 3")
    assert "workflow" in text[s2:s3].lower()


def test_71_slice3_landed_and_bounded_from_slice4():
    # Slice 3 (Persistent Daily Research Cycle) has LANDED with api.daily_research_cycle
    # as the sole orchestration owner, while remaining strictly bounded from Slice 4
    # (canonical scoring), Slice 5 (portfolio state), and the Milestone-2 holding
    # opportunity-cost engine. Cadence stays disabled, no automatic model promotion is
    # introduced, and the roadmap still forbids a big-bang rewrite.
    roadmap = _read(ROADMAP)
    s3 = roadmap.index("## Slice 3")
    s4 = roadmap.index("## Slice 4")
    s5 = roadmap.index("## Slice 5")
    s6 = roadmap.index("## Slice 6")
    s7 = roadmap.index("## Slice 7")
    s3text = roadmap[s3:s4]
    # Collapse whitespace so phrase checks tolerate the roadmap's hard line wrapping.
    s3flat = re.sub(r"\s+", " ", s3text)

    # 1. Slice 3 is documented as LANDED.
    assert "LANDED (Phase 29D)" in s3flat

    # 2. api.daily_research_cycle is documented as the canonical orchestration owner
    #    and the module is actually present in the repository.
    assert "api/daily_research_cycle.py" in s3flat
    assert "orchestration owner" in s3flat
    assert (REPO / "api" / "daily_research_cycle.py").exists()

    # 3. Slice 3 remains bounded from Slice 4 (scoring consolidation is still Slice 4's).
    assert "Slice 4 still owns consolidation" in s3flat
    assert "Not begun:" in s3flat and "Slice 4 (canonical scoring)" in s3flat

    # 4. Slice 4 (canonical universe scoring) has LANDED, and Slice 5 (canonical
    #    portfolio state) has now LANDED too. The boundary has advanced: Slice 6
    #    (Holding Opportunity-Cost engine) is NOT landed.
    assert "LANDED (Phase 29E)" in roadmap[s4:s5]
    assert "LANDED (Phase 29F)" in roadmap[s5:s6]

    # 5. The Milestone-2 holding opportunity-cost engine is NOT claimed complete: the
    #    Slice-3 status explicitly disclaims it, lists it under "Not begun", and the
    #    engine's own slice (Slice 6) has no LANDED status line.
    assert "Milestone-2 opportunity-cost engine" in s3flat
    assert "Milestone 2 (opportunity-cost engine)" in s3flat
    assert "LANDED" not in roadmap[s6:s7]

    # 6. Cadence remains disabled.
    assert "cadence remains disabled" in s3flat

    # 7. No automatic model promotion is introduced (canonical workflow invariant).
    ws_src = _read(REPO / "api" / "workflow_state.py")
    assert '"automatic_promotion_allowed": False' in ws_src

    # 8. The roadmap still prohibits a big-bang rewrite.
    assert "No big-bang rewrite" in roadmap


def test_72_workflow_state_target_owner_single_cell():
    # The TARGET owner table assigns exactly one owner to workflow/gate state.
    rows = dict(_parse_owner_table(_read(TARGET)))
    wf_rows = {c: o for c, o in rows.items() if "workflow" in c.lower()}
    assert wf_rows, "no workflow owner row in TARGET owner table"
    for concept, owner in wf_rows.items():
        assert owner and " / " not in owner, (concept, owner)
        assert "workflow_state" in owner
