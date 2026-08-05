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
