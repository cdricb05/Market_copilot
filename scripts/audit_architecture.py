#!/usr/bin/env python
r"""Static architecture audit for Paper Trader (Phase 29A).

READ-ONLY. This tool inspects the repository *statically* — it never imports or
executes the application, never opens a database or network connection, never
runs the prediction service, and never mutates the repository by default. It
parses source text and reports architectural signals that support the canonical
objective in docs/PROJECT_CHARTER.md.

IMPORTANT: static analysis does NOT prove runtime behavior. Every finding here is
a *candidate* derived from source text (regex/AST-free scanning), to be confirmed
against docs/CURRENT_ARCHITECTURE.md and the tests. Treat REMOVE_CANDIDATE and
orphan findings as leads, never as authorization to delete.

Usage (Windows PowerShell):

    .\.venv-win\Scripts\python.exe scripts\audit_architecture.py            # console + JSON to a temp file
    .\.venv-win\Scripts\python.exe scripts\audit_architecture.py --out out.json
    .\.venv-win\Scripts\python.exe scripts\audit_architecture.py --json-only

The JSON payload is deterministic for a fixed working tree: all collections are
sorted and no timestamps or random values are emitted, so a fixed tree always
produces byte-identical output. Exit code is always 0 (a report, not a gate)
unless --strict is given, in which case a nonzero code is returned when any
"blocking" category is non-empty.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from pathlib import Path

# --------------------------------------------------------------------------- #
# Configuration (documented, static thresholds).
# --------------------------------------------------------------------------- #
REPO_ROOT = Path(__file__).resolve().parent.parent

# Directories that hold first-party source we inventory. venvs, caches, .git,
# node_modules and build artifacts are always excluded.
SOURCE_DIRS = ("api", "engine", "db", "alpha_agent", "research_agent",
               "scripts", "workflows")
EXCLUDE_PARTS = (".venv", ".venv-win", ".git", "__pycache__", "node_modules",
                 ".pytest_cache", "egg-info", ".mypy_cache")

# A module larger than this many source lines is flagged as a large / mixed-
# responsibility candidate (documented threshold, not a hard rule).
SIZE_THRESHOLD_LINES = 1500

# The single FastAPI application module (all routes are declared here today).
APP_MODULE = "api/app.py"
UI_FILE = "api/ui/index.html"

# The one module allowed to construct database sessions (the session factory).
DB_SESSION_OWNER = "db/session.py"

# Ledger root literal that, if referenced directly outside the desk/book service
# modules, indicates ledger access bypassing a service boundary.
LEDGER_ROOT_LITERAL = ".paper_trader"
# Service modules that legitimately own direct ledger filesystem access.
LEDGER_OWNER_MODULES = {
    "api/operational_book.py", "api/paper_trading_desk.py", "api/alpha_book.py",
    "api/current_alpha_book.py", "api/alpha_target.py", "api/daily_close.py",
    "api/forward_prediction_skill.py", "api/forward_evidence.py",
    "api/current_alpha_performance.py", "api/current_alpha_daily_refresh.py",
    "api/multi_horizon_ledger.py", "api/multi_horizon_registry.py",
    "api/current_alpha_tournament_sync.py", "api/daily_operating_run.py",
    "api/current_operating_state.py",
}

# Research-only source trees + individual research modules that must never call
# order-execution primitives. (The whole point of the research/ops boundary.)
RESEARCH_ONLY_DIRS = ("alpha_agent", "research_agent")
RESEARCH_ONLY_MODULES = {
    "api/alpha_factory.py", "api/price_alpha_factory.py", "api/alpha_registry.py",
    "engine/absolute_return_research.py",
}
# Order-execution terms that must not appear in research-only code as *calls*.
EXECUTION_CALL_TERMS = (
    "place_order", "submit_order", "execute_order", "send_order",
    "broker_execute", "live_order", "route_order",
)

# Canonical business concepts and the regex that identifies a *writer/producer*
# of that concept (a function definition that computes it). Multiple modules
# matching one concept is a source-of-truth candidate.
CANONICAL_CONCEPT_PATTERNS = {
    "portfolio_nav_valuation": re.compile(
        r"def\s+[a-z_]*(nav|valuation|mark_to_market|current_mark|book_nav)"
        r"[a-z_]*\s*\(", re.I),
    "eligible_market_date": re.compile(
        r"def\s+[a-z_]*(eligible_market|market_as_of|latest_completed_market|"
        r"latest_eligible_market|market_date_alignment)[a-z_]*\s*\(", re.I),
    "universe_scoring_rankings": re.compile(
        r"def\s+[a-z_]*(score_universe|rank_universe|composite_sn|"
        r"compute_scores|build_rankings)[a-z_]*\s*\(", re.I),
    "target_portfolio": re.compile(
        r"def\s+[a-z_]*(build_target|target_state|run_refresh|"
        r"apply_joint_caps|preview_or_create_current_alpha_book)"
        r"[a-z_]*\s*\(", re.I),
    "workflow_state": re.compile(
        r"def\s+[a-z_]*(workflow_state|target_state_for|derive_lifecycle|"
        r"evaluate_daily_action_gate|derive_review)[a-z_]*\s*\(", re.I),
    "forward_evidence": re.compile(
        r"def\s+[a-z_]*(forward_evidence|capture_snapshots|forward_readiness|"
        r"true_forward)[a-z_]*\s*\(", re.I),
}

# The canonical documentation set (existence checked; drift is a finding).
CANONICAL_DOCS = (
    "docs/PROJECT_CHARTER.md",
    "docs/CURRENT_ARCHITECTURE.md",
    "docs/TARGET_ARCHITECTURE.md",
    "docs/CONSOLIDATION_ROADMAP.md",
    "docs/ARCHITECTURE_DECISIONS.md",
    "docs/architecture/system_inventory.json",
)

# Known local-only files that must NEVER be treated as release artifacts.
LOCAL_ONLY_FILES = (
    ".claude/settings.json",
    ".playwright-mcp/",
    "paper_trader_8001.stderr.log",
    "paper_trader_8001.stdout.log",
)

ROUTE_DECORATOR = re.compile(r"@app\.(get|post|put|delete|patch)\(")
PATH_LITERAL = re.compile(r"""['"](/[A-Za-z0-9_\-{}/.:]*)['"]""")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _rel(p: Path) -> str:
    return str(p.relative_to(REPO_ROOT)).replace("\\", "/")


def _read(rel_path: str) -> str:
    fp = REPO_ROOT / rel_path
    try:
        return fp.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _iter_source_files() -> list[Path]:
    out: list[Path] = []
    for d in SOURCE_DIRS:
        base = REPO_ROOT / d
        if not base.exists():
            continue
        for fp in base.rglob("*.py"):
            if any(part in EXCLUDE_PARTS or part.endswith(".egg-info")
                   for part in fp.parts):
                continue
            out.append(fp)
    return sorted(out, key=_rel)


def _line_count(text: str) -> int:
    if not text:
        return 0
    return text.count("\n") + (0 if text.endswith("\n") else 1)


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #
def check_routes() -> dict:
    """Parse route declarations from the app module; detect duplicates."""
    text = _read(APP_MODULE)
    lines = text.splitlines()
    routes: list[dict] = []
    for i, line in enumerate(lines):
        m = ROUTE_DECORATOR.search(line)
        if not m:
            continue
        method = m.group(1).upper()
        # The path literal may be on this line or the next few lines.
        path = None
        for j in range(i, min(i + 6, len(lines))):
            pm = PATH_LITERAL.search(lines[j])
            if pm:
                path = pm.group(1)
                break
        routes.append({"method": method, "path": path or "<unresolved>",
                       "declared_in": APP_MODULE, "line": i + 1})
    # Duplicate (method, path) declarations.
    seen: dict[tuple, list[int]] = {}
    for r in routes:
        seen.setdefault((r["method"], r["path"]), []).append(r["line"])
    duplicates = sorted(
        [{"method": k[0], "path": k[1], "lines": sorted(v)}
         for k, v in seen.items() if len(v) > 1 and k[1] != "<unresolved>"],
        key=lambda d: (d["path"], d["method"]))
    return {
        "total": len(routes),
        "owner_files": sorted({r["declared_in"] for r in routes}),
        "routes": sorted(routes, key=lambda r: (r["path"], r["method"])),
        "duplicate_declarations": duplicates,
    }


def check_module_sizes(files: list[Path]) -> list[dict]:
    big: list[dict] = []
    for fp in files:
        n = _line_count(fp.read_text(encoding="utf-8", errors="replace"))
        if n > SIZE_THRESHOLD_LINES:
            big.append({"path": _rel(fp), "lines": n})
    return sorted(big, key=lambda d: (-d["lines"], d["path"]))


def check_direct_ledger_refs(files: list[Path]) -> list[dict]:
    hits: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        # The ledger-owner service modules and this audit tool itself (which
        # only defines the literal as a constant) are expected references.
        if rel in LEDGER_OWNER_MODULES or rel == "scripts/audit_architecture.py":
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            if LEDGER_ROOT_LITERAL in line and "operational_ledger_roots" not in line:
                hits.append({"path": rel, "line": i, "text": line.strip()[:160]})
    return sorted(hits, key=lambda d: (d["path"], d["line"]))


def check_private_attribute_access(files: list[Path]) -> list[dict]:
    """Detect `alias._private` usage where `alias` names a first-party module.

    This complements check_private_cross_imports (the `from x import _y` form):
    most private coupling in this repo is attribute-style
    (`from . import paper_trading_desk as desk; desk._read_ledger(...)`).
    """
    # api/engine/db are importable under the installed `paper_trader.*`
    # namespace; alpha_agent/research_agent are top-level packages.
    first_party = ("paper_trader", "api", "engine", "db", "alpha_agent",
                   "research_agent")
    imp_from = re.compile(
        r"^\s*from\s+(?:\.+|(?:%s)[\w.]*)\s+import\s+(.+)$"
        % "|".join(first_party))
    imp_mod = re.compile(
        r"^\s*import\s+((?:%s)[\w.]+)(?:\s+as\s+(\w+))?" % "|".join(first_party))
    results: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        text = fp.read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        aliases: set[str] = set()
        for line in lines:
            m = imp_from.match(line)
            if m:
                for nm in m.group(1).replace("(", "").replace(")", "").split(","):
                    nm = nm.strip()
                    if not nm or nm == "*":
                        continue
                    alias = nm.split(" as ")[-1].strip()
                    if alias and not alias.startswith("_"):
                        aliases.add(alias)
            m2 = imp_mod.match(line)
            if m2:
                aliases.add(m2.group(2) or m2.group(1).split(".")[-1])
        if not aliases:
            continue
        alias_re = re.compile(
            r"\b(%s)\.(_[a-zA-Z]\w*)" % "|".join(re.escape(a) for a in aliases))
        privs: dict[str, int] = {}
        for line in lines:
            for mm in alias_re.finditer(line):
                if mm.group(2).startswith("__"):
                    continue
                privs[mm.group(1)] = privs.get(mm.group(1), 0) + 1
        total = sum(privs.values())
        if total:
            results.append({"path": rel, "total": total,
                            "by_module": dict(sorted(privs.items()))})
    return sorted(results, key=lambda d: (-d["total"], d["path"]))


def check_direct_db_sessions(files: list[Path]) -> list[dict]:
    pat = re.compile(r"\b(sessionmaker\s*\(|SessionLocal\s*\(|create_engine\s*\()")
    hits: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        if rel == DB_SESSION_OWNER:
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            if pat.search(line):
                hits.append({"path": rel, "line": i, "text": line.strip()[:160]})
    return sorted(hits, key=lambda d: (d["path"], d["line"]))


def check_private_cross_imports(files: list[Path]) -> list[dict]:
    # `from <pkg.mod> import _foo` or `from <pkg.mod> import a, _b`
    pat = re.compile(r"^\s*from\s+([\w.]+)\s+import\s+(.+)$")
    hits: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        own_mod = rel[:-3].replace("/", ".")
        text = fp.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            m = pat.match(line)
            if not m:
                continue
            src, names = m.group(1), m.group(2)
            if src.startswith(".") or src == own_mod:
                continue
            imported = [n.strip().split(" as ")[0].strip()
                        for n in names.replace("(", "").replace(")", "").split(",")]
            priv = [n for n in imported if n.startswith("_") and n != "__future__"]
            if priv and any(src.startswith(p + ".") or src == p
                            for p in ("paper_trader", "api", "engine", "db",
                                      "alpha_agent", "research_agent")):
                hits.append({"path": rel, "line": i, "imports_from": src,
                             "private_names": sorted(priv)})
    return sorted(hits, key=lambda d: (d["path"], d["line"]))


def _ui_referenced_endpoints() -> set[str]:
    text = _read(UI_FILE)
    refs: set[str] = set()
    for m in re.finditer(r"""['"`](/v1/[A-Za-z0-9_\-/{}.:]*)['"`]""", text):
        refs.add(m.group(1))
    # also template-literal fetches like fetch(`/v1/...${x}`)
    for m in re.finditer(r"""fetch\(\s*[`'"]?(/v1/[A-Za-z0-9_\-/{}.:]*)""", text):
        refs.add(m.group(1))
    return refs


def _static_prefix(path: str) -> str:
    # Normalize a route/ref to its static prefix (drop path params).
    parts = []
    for seg in path.split("/"):
        if seg.startswith("{"):
            break
        parts.append(seg)
    return "/".join(parts)


def check_ui_endpoint_wiring(routes: list[dict]) -> dict:
    declared = {r["path"] for r in routes if r["path"] != "<unresolved>"}
    declared_prefixes = {_static_prefix(p) for p in declared}
    referenced = _ui_referenced_endpoints()
    ref_prefixes = {_static_prefix(p) for p in referenced}

    # UI references that match no declared route prefix (dangling UI loaders).
    dangling = sorted(p for p in referenced
                      if _static_prefix(p) not in declared_prefixes)
    # Declared routes never referenced by the UI (orphan-endpoint candidates).
    orphan = sorted(p for p in declared
                    if p.startswith("/v1/")
                    and _static_prefix(p) not in ref_prefixes)
    return {
        "ui_referenced_count": len(referenced),
        "declared_v1_count": len([p for p in declared if p.startswith("/v1/")]),
        "dangling_ui_references": dangling,
        "orphan_endpoint_candidates": orphan,
    }


def check_canonical_concept_writers(files: list[Path]) -> dict:
    result: dict[str, list[dict]] = {}
    for concept, pat in CANONICAL_CONCEPT_PATTERNS.items():
        hits: list[dict] = []
        for fp in files:
            rel = _rel(fp)
            if rel == APP_MODULE:
                # app.py wires everything; count it but mark it.
                pass
            text = fp.read_text(encoding="utf-8", errors="replace")
            for i, line in enumerate(text.splitlines(), 1):
                if pat.search(line):
                    hits.append({"path": rel, "line": i,
                                 "symbol": line.strip()[:120]})
        result[concept] = sorted(hits, key=lambda d: (d["path"], d["line"]))
    # A concept with writers in >1 distinct module is a multi-writer candidate.
    multi = {c: sorted({h["path"] for h in hits})
             for c, hits in result.items()
             if len({h["path"] for h in hits}) > 1}
    return {"writers": result, "multi_writer_concepts": multi}


def check_research_execution_terms(files: list[Path]) -> list[dict]:
    hits: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        is_research = rel.startswith(RESEARCH_ONLY_DIRS) or rel in RESEARCH_ONLY_MODULES
        if not is_research:
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            for term in EXECUTION_CALL_TERMS:
                # only flag call-shaped usage: term followed by "("
                if re.search(r"\b" + re.escape(term) + r"\s*\(", line):
                    hits.append({"path": rel, "line": i, "term": term,
                                 "text": stripped[:160]})
    return sorted(hits, key=lambda d: (d["path"], d["line"]))


def check_inventory_drift(files: list[Path]) -> dict:
    inv_path = "docs/architecture/system_inventory.json"
    raw = _read(inv_path)
    if not raw.strip():
        return {"status": "MISSING", "inventory": inv_path,
                "on_disk_not_in_inventory": [], "in_inventory_not_on_disk": []}
    try:
        inv = json.loads(raw)
    except json.JSONDecodeError as exc:
        return {"status": f"UNPARSEABLE: {exc}", "inventory": inv_path,
                "on_disk_not_in_inventory": [], "in_inventory_not_on_disk": []}
    listed = {m.get("path", "").replace("\\", "/")
              for m in inv.get("modules", [])}
    # Drift scope: top-level api/*.py, engine/*.py and db/{models,session}.py —
    # the significant service/engine surface the inventory is responsible for.
    on_disk: set[str] = set()
    for fp in files:
        rel = _rel(fp)
        parts = rel.split("/")
        if rel.endswith("/__init__.py") or len(parts) != 2:
            continue
        if parts[0] in ("api", "engine") or rel in ("db/models.py", "db/session.py"):
            on_disk.add(rel)
    return {
        "status": "OK",
        "inventory": inv_path,
        "on_disk_not_in_inventory": sorted(on_disk - listed),
        "in_inventory_not_on_disk": sorted(
            p for p in listed if p and not (REPO_ROOT / p).exists()),
    }


def check_local_only_not_released() -> dict:
    """Local-only files must not appear in the handoff release allowlist."""
    allow_ps = REPO_ROOT.parent  # not scanned; the allowlist lives in D:\Temp
    # We can only assert the files are gitignore-eligible / present locally.
    present = sorted(f for f in LOCAL_ONLY_FILES
                     if (REPO_ROOT / f.rstrip("/")).exists())
    return {"local_only_files": sorted(LOCAL_ONLY_FILES),
            "present_locally": present}


def check_docs_present() -> dict:
    return {"docs": {d: (REPO_ROOT / d).exists() for d in sorted(CANONICAL_DOCS)},
            "missing": sorted(d for d in CANONICAL_DOCS
                              if not (REPO_ROOT / d).exists())}


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #
def run_audit() -> dict:
    files = _iter_source_files()
    routes = check_routes()
    report = {
        "schema": "paper_trader.architecture_audit/1",
        "repo_root": _rel(REPO_ROOT) or ".",
        "source_file_count": len(files),
        "routes": routes,
        "large_modules": check_module_sizes(files),
        "size_threshold_lines": SIZE_THRESHOLD_LINES,
        "direct_ledger_refs": check_direct_ledger_refs(files),
        "direct_db_sessions": check_direct_db_sessions(files),
        "private_cross_module_imports": check_private_cross_imports(files),
        "private_attribute_access": check_private_attribute_access(files),
        "ui_endpoint_wiring": check_ui_endpoint_wiring(routes["routes"]),
        "canonical_concept_writers": check_canonical_concept_writers(files),
        "research_execution_terms": check_research_execution_terms(files),
        "inventory_drift": check_inventory_drift(files),
        "local_only_files": check_local_only_not_released(),
        "canonical_docs": check_docs_present(),
    }
    return report


def _print_console(rep: dict) -> None:
    def hdr(t):
        print("\n" + "=" * 72)
        print(t)
        print("=" * 72)

    print("Paper Trader — Static Architecture Audit (read-only)")
    print(f"repo_root={rep['repo_root']}  source_files={rep['source_file_count']}")
    print("NOTE: static analysis does not prove runtime behavior.")

    hdr("ROUTES")
    r = rep["routes"]
    print(f"declared routes: {r['total']}  owner files: {', '.join(r['owner_files'])}")
    print(f"duplicate (method,path) declarations: {len(r['duplicate_declarations'])}")
    for d in r["duplicate_declarations"]:
        print(f"  DUP {d['method']} {d['path']} lines={d['lines']}")

    hdr("LARGE / MIXED-RESPONSIBILITY MODULES (> %d lines)" % rep["size_threshold_lines"])
    for m in rep["large_modules"]:
        print(f"  {m['lines']:>6}  {m['path']}")

    hdr("DIRECT LEDGER REFERENCES OUTSIDE LEDGER-OWNER MODULES")
    print(f"count: {len(rep['direct_ledger_refs'])}")
    for h in rep["direct_ledger_refs"][:40]:
        print(f"  {h['path']}:{h['line']}  {h['text']}")

    hdr("DIRECT DB SESSION CONSTRUCTION OUTSIDE %s" % DB_SESSION_OWNER)
    print(f"count: {len(rep['direct_db_sessions'])}")
    for h in rep["direct_db_sessions"][:40]:
        print(f"  {h['path']}:{h['line']}  {h['text']}")

    hdr("PRIVATE CROSS-MODULE IMPORTS")
    print(f"count: {len(rep['private_cross_module_imports'])}")
    for h in rep["private_cross_module_imports"][:40]:
        print(f"  {h['path']}:{h['line']}  from {h['imports_from']} import {h['private_names']}")

    hdr("PRIVATE ATTRIBUTE ACCESS (alias._private across module boundaries)")
    pa = rep["private_attribute_access"]
    print(f"modules reaching into another module's privates: {len(pa)}")
    for h in pa[:15]:
        print(f"  {h['total']:>4}  {h['path']}  {h['by_module']}")

    hdr("UI <-> ENDPOINT WIRING")
    w = rep["ui_endpoint_wiring"]
    print(f"ui referenced: {w['ui_referenced_count']}  declared /v1: {w['declared_v1_count']}")
    print(f"dangling UI references (no matching route): {len(w['dangling_ui_references'])}")
    for p in w["dangling_ui_references"][:40]:
        print(f"  DANGLING {p}")
    print(f"orphan endpoint candidates (declared, no UI ref): {len(w['orphan_endpoint_candidates'])}")

    hdr("CANONICAL-CONCEPT MULTI-WRITER CANDIDATES")
    multi = rep["canonical_concept_writers"]["multi_writer_concepts"]
    for concept, mods in sorted(multi.items()):
        print(f"  {concept}: {len(mods)} modules")
        for m in mods:
            print(f"      {m}")

    hdr("RESEARCH-ONLY MODULES WITH ORDER-EXECUTION TERMS (must be empty)")
    print(f"count: {len(rep['research_execution_terms'])}")
    for h in rep["research_execution_terms"][:40]:
        print(f"  {h['path']}:{h['line']}  {h['term']}  {h['text']}")

    hdr("INVENTORY DRIFT")
    d = rep["inventory_drift"]
    print(f"status: {d['status']}")
    print(f"on disk but not in inventory: {len(d['on_disk_not_in_inventory'])}")
    for p in d["on_disk_not_in_inventory"][:40]:
        print(f"  +{p}")
    print(f"in inventory but not on disk: {len(d['in_inventory_not_on_disk'])}")
    for p in d["in_inventory_not_on_disk"][:40]:
        print(f"  -{p}")

    hdr("CANONICAL DOCS")
    for doc, present in sorted(rep["canonical_docs"]["docs"].items()):
        print(f"  {'OK ' if present else 'MISS'}  {doc}")


# Categories that make --strict return nonzero when non-empty.
BLOCKING = ("duplicate_declarations", "research_execution_terms")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Static architecture audit (read-only).")
    ap.add_argument("--out", default=None,
                    help="Write JSON report to this path (default: a temp file).")
    ap.add_argument("--json-only", action="store_true",
                    help="Print only the JSON report to stdout.")
    ap.add_argument("--strict", action="store_true",
                    help="Exit nonzero if a blocking category is non-empty.")
    args = ap.parse_args(argv)

    rep = run_audit()
    payload = json.dumps(rep, indent=2, sort_keys=True, ensure_ascii=False)

    if args.json_only:
        print(payload)
    else:
        _print_console(rep)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(payload + "\n", encoding="utf-8")
        if not args.json_only:
            print(f"\nJSON report written to: {out_path}")
    else:
        # Default: write to a temp file so the repository is never mutated.
        tf = tempfile.NamedTemporaryFile(
            "w", suffix="_arch_audit.json", delete=False, encoding="utf-8")
        tf.write(payload + "\n")
        tf.close()
        if not args.json_only:
            print(f"\nJSON report written to: {tf.name}")

    if args.strict:
        blocking_hits = (len(rep["routes"]["duplicate_declarations"])
                         + len(rep["research_execution_terms"]))
        return 1 if blocking_hits else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
