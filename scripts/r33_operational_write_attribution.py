"""Attribute operational-store writes to a WRITER, for the Release-33 gate.

READ ONLY. Opens files, reads the process table and reports. It starts nothing,
stops nothing, writes nothing and probes no HTTP route.

Why this module exists
----------------------
The first Release-33 validation gate asserted:

    mtime >= campaign start  =>  Release 33 wrote the file

That is not an attribution rule, it is a clock reading. Paper Trader runs a
long-lived Release-29 continuous information-collection service whose canonical
job is to advance exactly this class of file - singleton lock, service state,
heartbeat, iteration history, source health, bounded log - on a 60-second
cadence, entirely independently of whatever research campaign happens to be
running. Under the old rule the gate blocked a commit because a background
service did its job, and the only ways to "fix" it were to stop production or
to whitelist a directory. Both are worse than the defect.

The invariant this module enforces is therefore:

    NO R33-ATTRIBUTABLE OPERATIONAL STORE WRITE

not

    NO OPERATIONAL FILE MAY CHANGE WHILE R33 RUNS

The exception is owner-specific and provenance-specific, never time-specific.
A changed file is acquitted only when it is a file the declared service owns
AND its contents identify that service as the writer AND it carries no
Release-33 marker. Everything else - an unrecognised file, a missing or foreign
``service_id``, unparseable content, an R33 marker, a raised exception - is a
FAILURE. Unmeasurable is not innocent: this gate fails closed.

Three independent things have to hold before a write is acquitted, and they are
checked separately on purpose:

1. STATIC   - the Release-33 source carries no path to an operational store at
              all: no protected-root name, no owned-file name, no operational
              owner import, no mutating owner call. A campaign that cannot
              address the store cannot have written it.
2. CONTENT  - each changed file names the declared service as its writer, and
              the service's own identity records agree with one another.
3. NEGATIVE - no file under the protected root mentions the campaign, its
              research root, its modules or its runner.

Process liveness is CORROBORATION, not a requirement. The operator is entitled
to stop the collection service, and a stopped service must not turn a clean
research campaign into a blocked commit.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Optional

#: The estate's operational data root.
OPERATIONAL_DATA_ROOT = Path(r"D:\Stock_Prediction_app_data")

#: Stores with no independent writer. Nothing may change these on or after the
#: campaign day - there is no background owner to attribute a change to.
STRICT_OPERATIONAL_ROOTS = (
    "portfolio_decisions", "daily_research_cycle", "corporate_actions",
    "reallocation_proposals", "rebalance_order_plans",
    "portfolio_reassessments", "reassessment_outcomes",
)

#: Stores maintained by a declared, independent, long-lived background service.
#: A root only earns an attribution path by naming its owner, its service
#: identity and the exact files that owner may write. There is no "skip this
#: directory" option, and adding a root here without a spec is a KeyError, not
#: a silent pass.
CONTINUOUS_SERVICE_ROOTS = {
    "information_collection": {
        # Every value below is a copy of a constant declared by the owner
        # module api/information_collection.py. They are duplicated rather
        # than imported so that this gate keeps working when the repository is
        # only partially importable, and check_owner_declarations() proves the
        # copies still match the owner.
        "service_id": "PAPER_TRADER_INFORMATION_COLLECTION",
        "owner_module": "api.information_collection",
        "worker_script": "run_information_collection_service.py",
        "scheduled_task": "PaperTrader-InformationCollection",
        "release": "RELEASE29",
        "provenance_required": True,
        #: JSON files the owner writes, each carrying ``service_id``.
        "json_files": (
            "collection_service_state.json",
            "collection_service.lock",
            "collection_iteration_history.json",
            "source_runtime_health.json",
        ),
        #: The bounded NDJSON log and its single rotation.
        "log_files": (
            "logs/collection_service.log",
            "logs/collection_service.log.1",
        ),
        "state_file": "collection_service_state.json",
        "lock_file": "collection_service.lock",
        "history_file": "collection_iteration_history.json",
    },
}

#: Both halves, and only both halves. Asserted by check_owner_declarations() so
#: that a root can never be dropped from protection by editing one list.
OPERATIONAL_ROOTS = tuple(sorted(
    set(STRICT_OPERATIONAL_ROOTS) | set(CONTINUOUS_SERVICE_ROOTS)))

#: Strings that only a Release-33 writer would leave behind. Deliberately
#: specific: a bare "r33" would collide with ordinary text, and a marker that
#: fires by accident trains the operator to ignore the gate.
R33_MARKERS = (
    "r33_predictive_edge_v1", "r33_predictive_edge_v2",
    "predictive_edge_r33", "run_release33_predictive_edge",
    "alpha_agent.r33", "alpha_agent/r33", "alpha_agent\\r33",
    "release33_predictive_edge",
)

#: Release-33 research source. The static check reads exactly these files.
R33_SOURCE_GLOBS = ("alpha_agent/r33/*.py",)
R33_SOURCE_FILES = ("scripts/run_release33_predictive_edge.py",)

#: Later releases REUSE this rule rather than shipping a second mtime check.
#: The rule is identical; only the strings that identify a given release's
#: writer differ, so each release contributes a PROFILE and nothing else. The
#: R33 profile is the default everywhere, so R33's behaviour, its gate and its
#: regression suite are unchanged by the existence of the others.
R34_MARKERS = (
    "r34_prediction_to_pnl_v1", "r34_prediction_to_pnl_v2",
    "prediction_to_pnl_r34", "run_release34_prediction_to_pnl",
    "alpha_agent.r34", "alpha_agent/r34", "alpha_agent\\r34",
    "release34_prediction_to_pnl",
)
R34_SOURCE_GLOBS = ("alpha_agent/r34/*.py",)
R34_SOURCE_FILES = ("scripts/run_release34_prediction_to_pnl.py",)

#: Release 35 downloads third-party payloads, so its profile carries the
#: acquisition root and the campaign id as markers too: a stray write of an
#: acquired archive into an operational store would be attributable, and a gate
#: that could not see it would be a gate about the wrong thing.
R35_MARKERS = (
    "r35_orthogonal_information_v1", "orthogonal_information_r35",
    "run_release35_orthogonal_information",
    "alpha_agent.r35", "alpha_agent/r35", "alpha_agent\\r35",
    "release35_orthogonal_information",
)
R35_SOURCE_GLOBS = ("alpha_agent/r35/*.py",)
R35_SOURCE_FILES = ("scripts/run_release35_orthogonal_information.py",)

R36_MARKERS = (
    "r36_global_multi_asset_frontier_v1", "r36_global_multi_asset_frontier_v2",
    "r36_global_multi_asset_frontier_v3", "global_multi_asset_frontier_r36",
    "run_release36_global_multi_asset_frontier",
    "alpha_agent.r36", "alpha_agent/r36", "alpha_agent\\r36",
    "release36_global_multi_asset_frontier",
)
R36_SOURCE_GLOBS = ("alpha_agent/r36/*.py",)
R36_SOURCE_FILES = ("scripts/run_release36_global_multi_asset_frontier.py",)

#: Release 37 downloads free samples and CALLS two gates that own their own
#: stores, so its profile carries the acquisition root and the campaign ids as
#: markers: a sample written into an operational store, or a Slice-9 evaluation
#: persisted by a research release, would both be attributable here.
R37_MARKERS = (
    "r37_native_market_data_gate_v1", "r37_native_market_data_gate_v2",
    "r37_native_market_data_gate_v3", "r37_native_market_data_gate_v4",
    "r37_native_market_data_gate_v5", "native_market_data_gate_r37",
    "run_release37_native_market_data_gate",
    "alpha_agent.r37", "alpha_agent/r37", "alpha_agent\\r37",
    "release37_native_market_data_gate",
    # Release 37.1 extended the canonical Slice-9 owners with a second decision
    # context. Those owners persist evaluations, so a write landing in the
    # Slice-9 store because a research release ran the gate is attributable here
    # - which is exactly what this release must be able to prove it did NOT do.
    "dxev_acq_", "data_expansion", "RESEARCH_ACQUISITION",
)
R37_SOURCE_GLOBS = ("alpha_agent/r37/*.py",)
R37_SOURCE_FILES = ("scripts/run_release37_native_market_data_gate.py",
                    "engine/data_expansion_gate.py", "api/data_expansion.py")

#: Release 38 READS a paid entitlement someone else purchased and CALLS the
#: canonical Slice-9 gate in its POST_ACQUISITION_VALUE context, so its
#: profile carries the campaign ids, the research root and the gate markers:
#: a native-futures artifact landing in an operational store, or a Slice-9
#: evaluation persisted by this research release, would both be attributable.
R38_MARKERS = (
    "r38_native_futures_information_frontier_v1",
    "r38_native_futures_information_frontier_v2",
    "r38_native_futures_information_frontier_v3",
    "r38_native_futures_information_frontier_v4",
    "native_futures_r38",
    "run_release38_native_futures_information_frontier",
    "alpha_agent.r38", "alpha_agent/r38", "alpha_agent\\r38",
    "release38_native_futures_information_frontier",
    "dxev_", "data_expansion", "POST_ACQUISITION_VALUE",
)
R38_SOURCE_GLOBS = ("alpha_agent/r38/*.py",)
R38_SOURCE_FILES = (
    "scripts/run_release38_native_futures_information_frontier.py",
    "engine/data_expansion_gate.py", "api/data_expansion.py")

#: Release 39 fits models over the whole owned estate and writes ONLY under
#: its research root, so its profile carries the campaign id, the research
#: root and the runner: a universal-state panel, a candidate registry or a
#: lockbox log landing in an operational store would be attributable here.
R39_MARKERS = (
    "r39_universal_alpha_discovery_v1", "universal_alpha_r39",
    "run_release39_universal_alpha_discovery",
    "alpha_agent.r39", "alpha_agent/r39", "alpha_agent\\r39",
    "release39_universal_alpha_discovery",
    # continuation campaign (same release family, new immutable id)
    "r39_universal_alpha_continuation_v2",
    "run_release39_continuation", "run_r39_shadow_capture",
)
R39_SOURCE_GLOBS = ("alpha_agent/r39/*.py",)
R39_SOURCE_FILES = (
    "scripts/run_release39_universal_alpha_discovery.py",
    "scripts/run_release39_continuation.py",
    "scripts/run_r39_shadow_capture.py",)

RELEASE_PROFILES = {
    "R33": {"markers": R33_MARKERS, "source_globs": R33_SOURCE_GLOBS,
            "source_files": R33_SOURCE_FILES,
            "attributable_key": "r33_attributable"},
    "R34": {"markers": R34_MARKERS, "source_globs": R34_SOURCE_GLOBS,
            "source_files": R34_SOURCE_FILES,
            "attributable_key": "r33_attributable"},
    "R35": {"markers": R35_MARKERS, "source_globs": R35_SOURCE_GLOBS,
            "source_files": R35_SOURCE_FILES,
            "attributable_key": "r33_attributable"},
    "R36": {"markers": R36_MARKERS, "source_globs": R36_SOURCE_GLOBS,
            "source_files": R36_SOURCE_FILES,
            "attributable_key": "r33_attributable"},
    "R37": {"markers": R37_MARKERS, "source_globs": R37_SOURCE_GLOBS,
            "source_files": R37_SOURCE_FILES,
            "attributable_key": "r33_attributable"},
    "R38": {"markers": R38_MARKERS, "source_globs": R38_SOURCE_GLOBS,
            "source_files": R38_SOURCE_FILES,
            "attributable_key": "r33_attributable"},
    "R39": {"markers": R39_MARKERS, "source_globs": R39_SOURCE_GLOBS,
            "source_files": R39_SOURCE_FILES,
            "attributable_key": "r33_attributable"},
}
DEFAULT_PROFILE = "R33"


def profile_for(release: str) -> dict:
    """The marker/source profile for one release. Unknown releases FAIL CLOSED."""
    try:
        return RELEASE_PROFILES[str(release).upper()]
    except KeyError:
        raise RuntimeError(
            "UNKNOWN_RELEASE_PROFILE:%s - a release that wants this gate must "
            "declare its markers here rather than being attributed by a "
            "profile that does not describe it" % (release,)) from None

#: Operational owners the research lane may never import.
FORBIDDEN_OWNER_IMPORTS = (
    "api.information_collection", "api.collection_replay",
    "api.operational_book", "api.daily_close", "api.portfolio_decision",
    "api.rebalance_execution", "api.corporate_actions",
    "engine.normal_cycle", "engine.collection_cadence",
    "scripts.collection_service_control",
    "run_information_collection_service",
)

#: Mutating entry points of the collection owner. Their presence in research
#: source means the campaign can advance the service, whatever it claims.
FORBIDDEN_SERVICE_CALLS = (
    "save_service_state", "save_source_runtime_state",
    "acquire_service_lock", "release_service_lock",
    "register_worker_start", "record_progress",
    "run_collection_iteration", "set_collection_automation",
    "clear_iteration_in_flight",
)

#: How many trailing log records are attributed. The mtime flagged a RECENT
#: write, so the recent records are the ones that have to be accounted for; the
#: bounded log legitimately retains older iterations that the bounded history
#: has already dropped.
LOG_TAIL_RECORDS = 50

ATTRIBUTED = "ATTRIBUTED"
R33_ATTRIBUTABLE = "R33_ATTRIBUTABLE_WRITE"
UNATTRIBUTED = "ATTRIBUTION_UNRESOLVED"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _day_of(path: Path) -> str:
    return _dt.datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d")


def changed_files(root: Path, since_day: str) -> list:
    """Regular files under ``root`` last modified on or after ``since_day``."""
    if not root.exists():
        return []
    out = []
    for f in sorted(root.rglob("*")):
        try:
            if not f.is_file():
                continue
            if _day_of(f) >= since_day:
                out.append(f)
        except OSError:
            # Unreadable is unmeasurable, and unmeasurable fails closed.
            out.append(f)
    return out


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def markers_in(path: Path, markers=R33_MARKERS) -> list:
    """Release markers present in a file, lowercased."""
    try:
        text = _read_text(path).lower()
    except OSError as exc:  # pragma: no cover - surfaced as an attribution error
        raise RuntimeError(f"UNREADABLE:{exc}") from exc
    return [m for m in markers if m in text]


def r33_markers_in(path: Path) -> list:
    """Release-33 markers present in a file, lowercased."""
    return markers_in(path, R33_MARKERS)


def _rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


# --------------------------------------------------------------------------- #
# continuous-service attribution
# --------------------------------------------------------------------------- #
def _service_identity(root: Path, spec: dict) -> dict:
    """The service's own record of who it is. Read-only, never required to
    exist - a store can legitimately hold history from a service that is not
    running right now."""
    ident = {"instance_ids": set(), "pids": set(),
             "state": None, "lock": None, "history_iteration_ids": set(),
             "coherent": None, "incoherence": None}

    def _load(name):
        p = root / name
        if not p.exists():
            return None
        return json.loads(_read_text(p))

    state = _load(spec["state_file"])
    lock = _load(spec["lock_file"])
    ident["state"], ident["lock"] = state, lock

    for blob in (state, lock):
        if isinstance(blob, dict):
            if blob.get("instance_id"):
                ident["instance_ids"].add(blob["instance_id"])
            if blob.get("pid"):
                ident["pids"].add(blob["pid"])
    if isinstance(lock, dict):
        prior = (lock.get("reclaimed") or {}).get("prior_instance_id")
        if prior:
            ident["instance_ids"].add(prior)

    hist_path = root / spec["history_file"]
    if hist_path.exists():
        hist = json.loads(_read_text(hist_path))
        records = hist.get("iterations") if isinstance(hist, dict) else hist
        for rec in (records or []):
            if not isinstance(rec, dict):
                continue
            if rec.get("iteration_id"):
                ident["history_iteration_ids"].add(rec["iteration_id"])
            if rec.get("instance_id"):
                ident["instance_ids"].add(rec["instance_id"])

    # State and lock are written in the same heartbeat. If they disagree about
    # who is writing, the writer is not established and the gate must not guess.
    if isinstance(state, dict) and isinstance(lock, dict):
        if state.get("instance_id") != lock.get("instance_id"):
            ident["coherent"] = False
            ident["incoherence"] = "STATE_AND_LOCK_DISAGREE_ON_INSTANCE_ID"
        elif state.get("pid") != lock.get("pid"):
            ident["coherent"] = False
            ident["incoherence"] = "STATE_AND_LOCK_DISAGREE_ON_PID"
        elif (state.get("service_id") != spec["service_id"]
                or lock.get("service_id") != spec["service_id"]):
            ident["coherent"] = False
            ident["incoherence"] = "SERVICE_ID_IS_NOT_THE_DECLARED_SERVICE"
        else:
            ident["coherent"] = True
    return ident


def _attribute_json(path: Path, spec: dict, ident: dict) -> Optional[str]:
    """None when attributed, else the reason it is not."""
    blob = json.loads(_read_text(path))
    if not isinstance(blob, dict):
        return "NOT_A_SERVICE_STATE_DOCUMENT"
    if blob.get("service_id") != spec["service_id"]:
        return (f"WRITER_PROVENANCE_NOT_THE_DECLARED_SERVICE "
                f"(service_id={blob.get('service_id')!r})")
    if path.name == spec["history_file"]:
        records = blob.get("iterations")
        if not isinstance(records, list) or not records:
            return "ITERATION_HISTORY_CARRIES_NO_RECORDS"
        last = records[-1]
        if not isinstance(last, dict):
            return "ITERATION_HISTORY_RECORD_IS_NOT_A_DOCUMENT"
        if last.get("service_id") != spec["service_id"]:
            return (f"LATEST_ITERATION_NOT_WRITTEN_BY_THE_DECLARED_SERVICE "
                    f"(service_id={last.get('service_id')!r})")
        if not last.get("instance_id") or last.get("pid") is None:
            return "LATEST_ITERATION_CARRIES_NO_WRITER_IDENTITY"
        if last["instance_id"] not in ident["instance_ids"]:
            return (f"LATEST_ITERATION_INSTANCE_IS_UNKNOWN_TO_THE_SERVICE "
                    f"({last['instance_id']})")
    return None


def _attribute_log(path: Path, spec: dict, ident: dict) -> Optional[str]:
    """None when attributed, else the reason it is not."""
    lines = [ln for ln in _read_text(path).splitlines() if ln.strip()]
    if not lines:
        return "SERVICE_LOG_IS_EMPTY"
    tail = lines[-LOG_TAIL_RECORDS:]
    attributed = 0
    for raw in tail:
        try:
            rec = json.loads(raw)
        except ValueError:
            return "SERVICE_LOG_RECORD_IS_NOT_JSON"
        if not isinstance(rec, dict) or "at" not in rec or "event" not in rec:
            return "SERVICE_LOG_RECORD_HAS_NO_EVENT_SHAPE"
        it = rec.get("iteration_id")
        inst = rec.get("instance_id")
        if (it and it in ident["history_iteration_ids"]) or (
                inst and inst in ident["instance_ids"]):
            attributed += 1
        elif it or inst:
            return (f"SERVICE_LOG_RECORD_NAMES_AN_UNKNOWN_WRITER "
                    f"(iteration_id={it!r} instance_id={inst!r})")
    if attributed == 0:
        return "NO_RECENT_SERVICE_LOG_RECORD_COULD_BE_ATTRIBUTED"
    return None


def attribute_continuous_service(root: Path, spec: dict, since_day: str,
                                 markers=R33_MARKERS) -> dict:
    """Attribute every recent write under one continuous-service store."""
    owned = set(spec["json_files"]) | set(spec["log_files"])
    report = {"root": str(root), "service_id": spec["service_id"],
              "owner_module": spec["owner_module"], "since_day": since_day,
              "attributed": [], "r33_attributable": [], "unattributed": [],
              "identity": None, "checked": 0}
    if not root.exists():
        report["state"] = ATTRIBUTED
        report["absent"] = True
        return report

    try:
        ident = _service_identity(root, spec)
    except Exception as exc:  # noqa: BLE001 - unmeasurable fails closed
        report["unattributed"].append(
            {"file": str(root), "reason": f"SERVICE_IDENTITY_UNREADABLE:{exc}"})
        report["state"] = UNATTRIBUTED
        return report

    report["identity"] = {
        "instance_ids": sorted(ident["instance_ids"]),
        "pids": sorted(ident["pids"]),
        "coherent": ident["coherent"],
        "incoherence": ident["incoherence"],
        "known_iterations": len(ident["history_iteration_ids"]),
    }
    if ident["coherent"] is False:
        report["unattributed"].append(
            {"file": str(root / spec["state_file"]),
             "reason": f"SERVICE_IDENTITY_INCOHERENT:{ident['incoherence']}"})

    for f in changed_files(root, since_day):
        report["checked"] += 1
        rel = _rel(f, root)
        try:
            found = markers_in(f, markers)
            if found:
                report["r33_attributable"].append(
                    {"file": rel, "reason": f"RELEASE_MARKER_IN_FILE:{found}"})
                continue
            if rel not in owned:
                report["unattributed"].append(
                    {"file": rel,
                     "reason": "UNRECOGNISED_FILE_UNDER_PROTECTED_ROOT"})
                continue
            if rel in spec["log_files"]:
                reason = _attribute_log(f, spec, ident)
            else:
                reason = _attribute_json(f, spec, ident)
            if reason:
                report["unattributed"].append({"file": rel, "reason": reason})
            else:
                report["attributed"].append(
                    {"file": rel, "writer": spec["service_id"]})
        except Exception as exc:  # noqa: BLE001 - unmeasurable fails closed
            report["unattributed"].append(
                {"file": rel, "reason": f"ATTRIBUTION_ERROR:{exc}"})

    if report["r33_attributable"]:
        report["state"] = R33_ATTRIBUTABLE
    elif report["unattributed"]:
        report["state"] = UNATTRIBUTED
    else:
        report["state"] = ATTRIBUTED
    return report


# --------------------------------------------------------------------------- #
# strict roots
# --------------------------------------------------------------------------- #
def attribute_strict_root(root: Path, since_day: str) -> dict:
    """A strict store has no independent writer, so any recent change is
    attributable to whatever else ran - which here means Release 33."""
    report = {"root": str(root), "since_day": since_day,
              "r33_attributable": [], "checked": 0, "state": ATTRIBUTED}
    if not root.exists():
        report["absent"] = True
        return report
    for f in changed_files(root, since_day):
        report["checked"] += 1
        report["r33_attributable"].append(
            {"file": _rel(f, root),
             "reason": "WRITE_TO_A_STORE_WITH_NO_INDEPENDENT_WRITER"})
    if report["r33_attributable"]:
        report["state"] = R33_ATTRIBUTABLE
    return report


# --------------------------------------------------------------------------- #
# static source check
# --------------------------------------------------------------------------- #
def source_operational_write_paths(repo: Path, *,
                                   source_globs=R33_SOURCE_GLOBS,
                                   source_files=R33_SOURCE_FILES) -> dict:
    """Every way one release's source could address an operational store.

    A campaign that cannot name the store, cannot import its owner and cannot
    call its mutators has no operational write path, whatever any timestamp
    says. This runs even when every store is clean, so the invariant cannot be
    satisfied merely by the stores happening not to change.
    """
    sources = {}
    for pattern in source_globs:
        for f in sorted(repo.glob(pattern)):
            sources[f.relative_to(repo).as_posix()] = _read_text(f)
    for name in source_files:
        f = repo / name
        if f.exists():
            sources[name] = _read_text(f)

    owned_names = set()
    for spec in CONTINUOUS_SERVICE_ROOTS.values():
        owned_names |= {Path(n).name for n in spec["json_files"]}
        owned_names |= {Path(n).name for n in spec["log_files"]}
        owned_names.add(spec["worker_script"])
        owned_names.add(spec["scheduled_task"])

    findings = []
    for name, text in sorted(sources.items()):
        low = text.lower()
        for root_name in OPERATIONAL_ROOTS:
            if root_name in low:
                findings.append({"file": name, "kind": "PROTECTED_ROOT_NAME",
                                 "token": root_name})
        for owned in sorted(owned_names):
            if owned.lower() in low:
                findings.append({"file": name, "kind": "OWNED_FILE_NAME",
                                 "token": owned})
        for imp in FORBIDDEN_OWNER_IMPORTS:
            if imp.lower() in low:
                findings.append({"file": name, "kind": "OPERATIONAL_OWNER_REF",
                                 "token": imp})
        for call in FORBIDDEN_SERVICE_CALLS:
            if re.search(rf"\b{re.escape(call)}\s*\(", text):
                findings.append({"file": name, "kind": "MUTATING_OWNER_CALL",
                                 "token": call})
    return {"sources_scanned": len(sources), "findings": findings,
            "clean": not findings}


def r33_source_operational_write_paths(repo: Path) -> dict:
    """Every way the Release-33 source could address an operational store."""
    return source_operational_write_paths(
        repo, source_globs=R33_SOURCE_GLOBS, source_files=R33_SOURCE_FILES)


# --------------------------------------------------------------------------- #
# the declaration itself must stay honest
# --------------------------------------------------------------------------- #
def check_owner_declarations(repo: Optional[Path] = None) -> dict:
    """The protected set may not shrink, and an exception may not be a skip.

    Without this, the cheapest way to make the gate green is to delete a root
    from the protected list or to give a root an empty owned-file set. Both
    are caught here rather than discovered later.
    """
    strict = set(STRICT_OPERATIONAL_ROOTS)
    continuous = set(CONTINUOUS_SERVICE_ROOTS)
    result = {
        "information_collection_protected":
            "information_collection" in OPERATIONAL_ROOTS,
        "information_collection_has_named_owner":
            "information_collection" in continuous,
        "no_root_lost_protection":
            strict | continuous == set(OPERATIONAL_ROOTS),
        "strict_and_continuous_disjoint": not (strict & continuous),
        "every_exception_names_an_owner": all(
            bool(s.get("service_id")) and bool(s.get("owner_module"))
            and s.get("provenance_required") is True
            and bool(s.get("json_files"))
            for s in CONTINUOUS_SERVICE_ROOTS.values()),
        "owner_constants_match": None,
        "owner_constants_detail": "NOT_CHECKED",
    }
    if repo is not None:
        owner = repo / "api" / "information_collection.py"
        if owner.exists():
            text = _read_text(owner)
            spec = CONTINUOUS_SERVICE_ROOTS["information_collection"]
            missing = [tok for tok in (
                f'SERVICE_ID = "{spec["service_id"]}"',
                f'COMPOSITION_OWNER = "{spec["owner_module"]}"',
                f'SCHEDULED_TASK_NAME = "{spec["scheduled_task"]}"',
                f'CANONICAL_WORKER_SCRIPT = "{spec["worker_script"]}"',
            ) if tok not in text]
            missing += [f'"{n}"' for n in spec["json_files"]
                        if f'"{n}"' not in text]
            result["owner_constants_match"] = not missing
            result["owner_constants_detail"] = (
                "all owner constants reproduced" if not missing
                else f"DRIFTED={missing}")
        else:
            result["owner_constants_match"] = False
            result["owner_constants_detail"] = f"OWNER_NOT_FOUND:{owner}"
    required = [
        "information_collection_protected",
        "information_collection_has_named_owner",
        "no_root_lost_protection",
        "strict_and_continuous_disjoint",
        "every_exception_names_an_owner",
    ]
    if repo is not None:
        # Only assertable when a repository was supplied; when it was not, the
        # unchecked key stays None and is not silently counted as a pass.
        required.append("owner_constants_match")
    result["ok"] = all(result[k] is True for k in required)
    result["asserted"] = required
    return result


# --------------------------------------------------------------------------- #
# corroboration - evidence, never a requirement
# --------------------------------------------------------------------------- #
def corroborate_worker(spec: dict) -> dict:
    """Live worker evidence for the declared service.

    Deliberately advisory. The operator may stop the collection service at any
    time, and a stopped service must not block a clean research commit. This
    only ever adds confidence; it never removes it.
    """
    out = {"scheduled_task": spec["scheduled_task"], "task_state": None,
           "worker_pids": [], "lineage": [], "error": None}
    if os.name != "nt":
        out["error"] = "NOT_WINDOWS"
        return out
    try:
        ps = ("$ErrorActionPreference='SilentlyContinue';"
              f"$t=Get-ScheduledTask -TaskName '{spec['scheduled_task']}';"
              "$s=if($t){$t.State}else{'ABSENT'};"
              "$p=Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" |"
              " Where-Object { $_.CommandLine -like "
              f"'*{spec['worker_script']}*' }} |"
              " Select-Object ProcessId,ParentProcessId,CreationDate;"
              "ConvertTo-Json -Compress @{state=$s;procs=@($p)}")
        raw = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, text=True, timeout=60)
        blob = json.loads((raw.stdout or "").strip() or "{}")
        out["task_state"] = blob.get("state")
        procs = blob.get("procs") or []
        if isinstance(procs, dict):
            procs = [procs]
        for p in procs:
            out["worker_pids"].append(p.get("ProcessId"))
            out["lineage"].append(
                {"pid": p.get("ProcessId"), "ppid": p.get("ParentProcessId")})
    except Exception as exc:  # noqa: BLE001 - advisory only
        out["error"] = str(exc)
    return out


# --------------------------------------------------------------------------- #
# top level
# --------------------------------------------------------------------------- #
def attribute(*, data_root: Optional[Path] = None, since_day: str,
              repo: Optional[Path] = None,
              corroborate: bool = False,
              release: str = DEFAULT_PROFILE) -> dict:
    """Attribute every recent operational-store write to a writer.

    ``ok`` is True only when no write is attributable to the NAMED RELEASE,
    every write under a continuous-service root is positively attributed to
    that service, the release's source carries no operational write path and
    the protected declaration is intact.

    ``release`` selects which strings identify the release's writer and which
    source tree the static check reads. It defaults to R33, so every existing
    caller behaves exactly as before. An unknown release FAILS CLOSED rather
    than being attributed by a profile that does not describe it.
    """
    profile = profile_for(release)
    data_root = Path(data_root) if data_root else OPERATIONAL_DATA_ROOT
    report = {"data_root": str(data_root), "since_day": since_day,
              "release": str(release).upper(),
              "roots": {}, "r33_attributable": [], "unattributed": [],
              "service_attributed": []}

    for name in STRICT_OPERATIONAL_ROOTS:
        r = attribute_strict_root(data_root / name, since_day)
        report["roots"][name] = r
        report["r33_attributable"] += [
            f"{name}/{x['file']}" for x in r["r33_attributable"]]

    for name, spec in CONTINUOUS_SERVICE_ROOTS.items():
        r = attribute_continuous_service(data_root / name, spec, since_day,
                                         markers=profile["markers"])
        if corroborate:
            r["corroboration"] = corroborate_worker(spec)
        report["roots"][name] = r
        report["r33_attributable"] += [
            f"{name}/{x['file']}: {x['reason']}" for x in r["r33_attributable"]]
        report["unattributed"] += [
            f"{name}/{x['file']}: {x['reason']}" for x in r["unattributed"]]
        report["service_attributed"] += [
            f"{name}/{x['file']}" for x in r["attributed"]]

    decl = check_owner_declarations(repo)
    report["declaration"] = decl

    src = {"sources_scanned": 0, "findings": [], "clean": None}
    if repo is not None:
        src = source_operational_write_paths(
            Path(repo), source_globs=profile["source_globs"],
            source_files=profile["source_files"])
    report["source"] = src

    report["ok"] = bool(
        not report["r33_attributable"]
        and not report["unattributed"]
        and decl["ok"]
        and (src["clean"] is not False))
    if report["r33_attributable"]:
        report["state"] = R33_ATTRIBUTABLE
    elif report["unattributed"] or not decl["ok"] or src["clean"] is False:
        report["state"] = UNATTRIBUTED
    else:
        report["state"] = ATTRIBUTED
    return report


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--since-day", required=True)
    ap.add_argument("--data-root", default=None)
    ap.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    ap.add_argument("--corroborate", action="store_true")
    ap.add_argument("--release", default=DEFAULT_PROFILE,
                    help="which release's writer is being attributed against; "
                         "defaults to R33, and an unknown value fails closed")
    a = ap.parse_args()
    rep = attribute(data_root=a.data_root, since_day=a.since_day,
                    repo=Path(a.repo), corroborate=a.corroborate,
                    release=a.release)
    print(json.dumps(rep, indent=1, sort_keys=True, default=str))
    print(rep["state"])
    return 0 if rep["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
