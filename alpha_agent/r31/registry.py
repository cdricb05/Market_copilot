"""alpha_agent.r31.registry - the ONE Release 31 candidate registry.

Every executed candidate lands here exactly once, keyed by its specification
hash. The registry is also where the campaign's budgets STOP being prose: each
limit in :mod:`alpha_agent.r31.contract` is checked here before a candidate is
allowed to run, and a violation raises rather than warns.

Two disciplines this module exists to enforce:

**Idempotency.** The specification hash is the key. A resumed campaign re-derives
the same hash for an already-executed candidate and returns the stored result
instead of refitting. A campaign that silently re-ran candidates would also
silently corrupt its own multiple-testing denominator.

**A denominator that cannot shrink.** Every candidate that EXECUTES is recorded,
including the ones that failed, errored or were rejected. Multiple-testing
control is meaningless if disappointing candidates quietly leave the count - that
is the mechanism by which an honest-looking t-statistic is manufactured, and it
is why ``executed_count`` is derived from the append-only log rather than from a
curated list.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Optional

from .. import r31
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r31.registry"
REGISTRY_SCHEMA = "r31_candidate_registry/1"
LOG_NAME = "candidate_results.jsonl"
ARTIFACT_NAME = "candidate_registry.json"

PHASE_KNOWN = "KNOWN_METHOD"
PHASE_NOVEL = "NOVEL_DISCOVERY"
PHASES = (PHASE_KNOWN, PHASE_NOVEL)

ROLE_BENCHMARK = "BENCHMARK"
ROLE_CANDIDATE = "CANDIDATE"
ROLES = (ROLE_BENCHMARK, ROLE_CANDIDATE)

STATE_OK = "OK"
STATE_FAILED = "FAILED"
STATE_RESOURCE_INFEASIBLE = "RESOURCE_INFEASIBLE"
#: A Track-A candidate whose score could not be defensibly mapped into economic
#: return units. It is not a capital allocator, so it carries no portfolio
#: result - and it STAYS in the denominator, because it was executed.
STATE_CALIBRATION_REFUSED = "CALIBRATION_REFUSED"
STATES = (STATE_OK, STATE_FAILED, STATE_RESOURCE_INFEASIBLE,
          STATE_CALIBRATION_REFUSED)


class BudgetExceeded(RuntimeError):
    """A campaign budget declared in the contract would be exceeded."""


class DuplicateCandidate(RuntimeError):
    """A candidate specification hash has already been executed."""


class ContractDrift(RuntimeError):
    """The campaign contract changed after the first candidate result existed."""


def log_path(campaign_id: str) -> Path:
    return r31.campaign_dir(campaign_id) / LOG_NAME


def artifact_path(campaign_id: str) -> Path:
    return r31.campaign_dir(campaign_id) / ARTIFACT_NAME


class Registry:
    """Append-only candidate log with budget enforcement."""

    def __init__(self, campaign_id: str = _contract.CAMPAIGN_ID):
        self.campaign_id = str(campaign_id)
        self.path = log_path(self.campaign_id)
        self.entries: list = []
        self.by_hash: dict = {}
        self._load()

    # -- persistence ------------------------------------------------------- #
    def _load(self) -> None:
        if not self.path.exists():
            return
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                self.entries.append(row)
                self.by_hash[row["spec_hash"]] = row

    def _append(self, row: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(row, sort_keys=True, default=str) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        self.entries.append(row)
        self.by_hash[row["spec_hash"]] = row

    # -- queries ----------------------------------------------------------- #
    def has(self, spec_hash: str) -> bool:
        return str(spec_hash) in self.by_hash

    def get(self, spec_hash: str) -> Optional[dict]:
        return self.by_hash.get(str(spec_hash))

    def executed(self, *, phase: Optional[str] = None,
                 role: str = ROLE_CANDIDATE) -> list:
        return [e for e in self.entries
                if (phase is None or e.get("phase") == phase)
                and (role is None or e.get("role") == role)]

    def families(self, phase: str) -> set:
        return {e["family"] for e in self.executed(phase=phase)}

    def family_count(self, phase: str, family: str) -> int:
        return sum(1 for e in self.executed(phase=phase)
                   if e.get("family") == family)

    def novel_campaign_count(self, campaign_no: int) -> int:
        return sum(1 for e in self.executed(phase=PHASE_NOVEL)
                   if int(e.get("novel_campaign") or 0) == int(campaign_no))

    # -- budget gate ------------------------------------------------------- #
    def check_budget(self, *, phase: str, family: str, role: str,
                     novel_campaign: Optional[int] = None,
                     refinement_depth: int = 0) -> None:
        """Raise if executing one more candidate would breach a declared budget.

        Benchmarks are exempt from the CANDIDATE-family budgets: the incumbent
        and the transparent reference books are not part of the search, and
        charging the search budget for measuring them would create an incentive
        to measure fewer baselines.
        """
        if role == ROLE_BENCHMARK:
            return
        if phase == PHASE_KNOWN:
            fams = self.families(PHASE_KNOWN)
            if family not in fams and len(fams) >= _contract.MAX_KNOWN_METHOD_FAMILIES:
                raise BudgetExceeded(
                    "known-method family budget %d reached"
                    % _contract.MAX_KNOWN_METHOD_FAMILIES)
            total = len(self.executed(phase=PHASE_KNOWN))
            if total >= _contract.MAX_KNOWN_METHOD_CONFIGS:
                raise BudgetExceeded(
                    "known-method configuration budget %d reached"
                    % _contract.MAX_KNOWN_METHOD_CONFIGS)
            if self.family_count(PHASE_KNOWN, family) >= _contract.MAX_CONFIGS_PER_KNOWN_FAMILY:
                raise BudgetExceeded(
                    "family %r reached its %d-configuration budget"
                    % (family, _contract.MAX_CONFIGS_PER_KNOWN_FAMILY))
            return

        if phase == PHASE_NOVEL:
            if int(refinement_depth) > _contract.MAX_NOVEL_REFINEMENT_DEPTH:
                raise BudgetExceeded(
                    "novel refinement depth %d exceeds %d"
                    % (refinement_depth, _contract.MAX_NOVEL_REFINEMENT_DEPTH))
            cno = int(novel_campaign or 1)
            if cno > _contract.MAX_NOVEL_CAMPAIGNS:
                raise BudgetExceeded(
                    "novel campaign %d exceeds the %d-campaign budget"
                    % (cno, _contract.MAX_NOVEL_CAMPAIGNS))
            fams = self.families(PHASE_NOVEL)
            if family not in fams and len(fams) >= _contract.MAX_NOVEL_FAMILIES:
                raise BudgetExceeded(
                    "novel family budget %d reached" % _contract.MAX_NOVEL_FAMILIES)
            if self.novel_campaign_count(cno) >= _contract.MAX_NOVEL_CANDIDATES_PER_CAMPAIGN:
                raise BudgetExceeded(
                    "novel campaign %d reached its %d-candidate budget"
                    % (cno, _contract.MAX_NOVEL_CANDIDATES_PER_CAMPAIGN))
            if len(self.executed(phase=PHASE_NOVEL)) >= _contract.MAX_NOVEL_CANDIDATES_TOTAL:
                raise BudgetExceeded(
                    "novel candidate budget %d reached"
                    % _contract.MAX_NOVEL_CANDIDATES_TOTAL)
            return
        raise ValueError("unknown phase %r" % (phase,))

    # -- record ------------------------------------------------------------ #
    def record(self, row: dict) -> dict:
        """Append ONE executed candidate. Refuses a duplicate specification."""
        h = str(row["spec_hash"])
        if self.has(h):
            raise DuplicateCandidate(
                "candidate %s already executed; a specification hash is the "
                "campaign's idempotency key" % h)
        self.check_budget(phase=row["phase"], family=row["family"],
                          role=row.get("role", ROLE_CANDIDATE),
                          novel_campaign=row.get("novel_campaign"),
                          refinement_depth=int(row.get("refinement_depth") or 0))
        self._append(row)
        return row

    # -- derived artifact -------------------------------------------------- #
    def summary(self) -> dict:
        known = self.executed(phase=PHASE_KNOWN)
        novel = self.executed(phase=PHASE_NOVEL)
        bench = [e for e in self.entries if e.get("role") == ROLE_BENCHMARK]
        body = {
            "contract": REGISTRY_SCHEMA,
            "campaign_id": self.campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "executed_total": len(self.entries),
            "executed_candidates": len(known) + len(novel),
            "benchmarks": len(bench),
            "known_method": {
                "families": sorted(self.families(PHASE_KNOWN)),
                "family_count": len(self.families(PHASE_KNOWN)),
                "family_budget": _contract.MAX_KNOWN_METHOD_FAMILIES,
                "configs": len(known),
                "config_budget": _contract.MAX_KNOWN_METHOD_CONFIGS,
                "per_family": {f: self.family_count(PHASE_KNOWN, f)
                               for f in sorted(self.families(PHASE_KNOWN))},
            },
            "novel": {
                "families": sorted(self.families(PHASE_NOVEL)),
                "family_count": len(self.families(PHASE_NOVEL)),
                "family_budget": _contract.MAX_NOVEL_FAMILIES,
                "candidates": len(novel),
                "candidate_budget": _contract.MAX_NOVEL_CANDIDATES_TOTAL,
                "per_campaign": {
                    str(c): self.novel_campaign_count(c)
                    for c in range(1, _contract.MAX_NOVEL_CAMPAIGNS + 1)},
                "campaign_budget": _contract.MAX_NOVEL_CAMPAIGNS,
            },
            "states": _counts(self.entries, "state"),
            "multiple_testing_denominator": len(known) + len(novel),
            "denominator_includes_rejected_candidates": True,
            "candidates": [
                {k: e.get(k) for k in
                 ("candidate_id", "spec_hash", "phase", "family", "role",
                  "sample", "horizon_sessions", "state", "novel_campaign",
                  "refinement_depth", "runtime_seconds")}
                for e in self.entries],
        }
        body["registry_hash"] = r31.sha(body)
        body.update(r31.safety_block())
        return body

    def persist_summary(self) -> Path:
        return _write_mutable(artifact_path(self.campaign_id), self.summary())


def _counts(rows: list, key: str) -> dict:
    out: dict = {}
    for r in rows:
        v = str(r.get(key))
        out[v] = out.get(v, 0) + 1
    return dict(sorted(out.items()))


def _write_mutable(path: Path, payload) -> Path:
    """A derived artifact that legitimately grows as the campaign runs.

    The immutable artifacts are the CONTRACTS and the RESULTS; a derived summary
    over an append-only log is recomputed, never edited, so rewriting it cannot
    change any recorded evidence.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, indent=2, sort_keys=True, default=str)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(body)
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return p


def assert_contract_stable(campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    """The contract may not change once a candidate result exists.

    Checked before every campaign stage, so a mid-flight edit to a budget or a
    partition boundary is caught at the next stage rather than discovered in the
    final report.
    """
    frozen = _contract.load(campaign_id)
    reg = Registry(campaign_id)
    if frozen is None:
        return {"state": "NO_CONTRACT", "executed": len(reg.entries)}
    check = _contract.verify(frozen)
    if not check["intact"] and reg.entries:
        raise ContractDrift(
            "campaign contract hash changed after %d candidate results existed; "
            "a material change requires a NEW campaign id" % len(reg.entries))
    return {"state": "STABLE" if check["intact"] else "NO_RESULTS_YET",
            "executed": len(reg.entries), **check}
