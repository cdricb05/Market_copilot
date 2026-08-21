"""alpha_agent.r33.registry - the ONE candidate registry and denominator owner.

Every executed configuration is appended here, including the ones that failed,
were abandoned, or turned out to be resource-infeasible. The multiple-testing
denominator is read FROM THIS LOG rather than from a curated shortlist, so a
configuration cannot improve the campaign's statistics by being forgotten.

The specification hash binds the configuration to the CONTRACT hash and the
JUDGE BEHAVIOUR hash. That is what makes the hash a real idempotency key: if the
cost model or the control changed, a result measured under the old judge can
never silently appear in the same leaderboard as one measured under the new.
Release 32 learned that the hard way across three supersessions.

Budgets are ceilings and are enforced here, not described in a document. A
campaign that may widen its budget after a disappointing result is not running
an experiment.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from .. import r33
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r33.registry"
REGISTRY_SCHEMA = "r33_candidate_registry/1"
ARTIFACT_NAME = "candidate_registry.json"


class BudgetExceeded(RuntimeError):
    """A family tried to execute more configurations than it pre-registered."""


class Registry:
    """Append-only candidate log with enforced per-family ceilings."""

    def __init__(self, *, campaign_id: str = _contract.CAMPAIGN_ID,
                 contract_hash: str, judge_behaviour_hash: str):
        self.campaign_id = campaign_id
        self.contract_hash = str(contract_hash)
        self.judge_behaviour_hash = str(judge_behaviour_hash)
        self._rows: list = []
        self._by_family: dict = {f: 0 for f in _contract.FAMILIES}
        self._seen: set = set()

    # ------------------------------------------------------------------ #
    def spec_hash(self, spec: dict) -> str:
        return r33.sha({"spec": spec,
                        "contract_hash": self.contract_hash,
                        "judge_behaviour_hash": self.judge_behaviour_hash})

    def would_exceed(self, family: str) -> bool:
        cap = _contract.MAX_CONFIGS.get(family)
        if cap is None:
            return True
        return (self._by_family.get(family, 0) + 1 > cap
                or len(self._rows) + 1 > _contract.MAX_CONFIGS_TOTAL)

    def record(self, *, family: str, spec: dict, stage: str,
               result: dict) -> dict:
        """Append one EXECUTED configuration. Refuses to exceed a ceiling."""
        if family not in _contract.FAMILIES:
            raise BudgetExceeded(f"unknown family {family!r}")
        if self.would_exceed(family):
            raise BudgetExceeded(
                f"family {family!r} has spent its budget of "
                f"{_contract.MAX_CONFIGS[family]} configurations "
                f"(total {len(self._rows)}/{_contract.MAX_CONFIGS_TOTAL})")
        h = self.spec_hash(spec)
        row = {"candidate_id": f"{family}:{len(self._rows) + 1:03d}",
               "family": family, "stage": stage, "spec": spec,
               "spec_hash": h, "result": result}
        self._rows.append(row)
        self._by_family[family] = self._by_family.get(family, 0) + 1
        self._seen.add(h)
        return row

    # ------------------------------------------------------------------ #
    @property
    def denominator(self) -> int:
        """EVERY executed configuration. Never only the survivors."""
        return len(self._rows)

    @property
    def rows(self) -> list:
        return list(self._rows)

    def by_family(self) -> dict:
        return dict(self._by_family)

    def budget_report(self) -> dict:
        return {f: {"executed": self._by_family.get(f, 0),
                    "budget": _contract.MAX_CONFIGS[f],
                    "remaining": _contract.MAX_CONFIGS[f]
                                 - self._by_family.get(f, 0)}
                for f in _contract.FAMILIES}

    def leaderboard(self, *, key: str, segment: str, top: int = 20) -> list:
        scored = []
        for r in self._rows:
            v = (r["result"].get(segment) or {}).get(key)
            if v is not None:
                scored.append((float(v), r))
        scored.sort(key=lambda t: -t[0])
        return [r for _v, r in scored[:int(top)]]

    # ------------------------------------------------------------------ #
    def artifact(self, *, created_at: str) -> dict:
        payload = {
            "calculation_owner": CALCULATION_OWNER,
            "campaign_id": self.campaign_id,
            "created_at": created_at,
            "contract_hash": self.contract_hash,
            "judge_behaviour_hash": self.judge_behaviour_hash,
            "denominator_executed_configurations": self.denominator,
            "denominator_counts_all_executed":
                _contract.DENOMINATOR_COUNTS_ALL_EXECUTED,
            "failed_configurations_stay_in_denominator": True,
            "budgets": self.budget_report(),
            "max_configs_total": _contract.MAX_CONFIGS_TOTAL,
            "adaptive_search_allowed": _contract.ADAPTIVE_SEARCH_ALLOWED,
            "candidates": [{k: v for k, v in r.items() if k != "result"}
                           | {"result": _strip_private(r["result"])}
                           for r in self._rows],
        }
        body = r33.artifact_body(REGISTRY_SCHEMA, payload)
        body["registry_hash"] = r33.sha(payload)
        return body


def _strip_private(result: dict) -> dict:
    """Drop private working series from the FROZEN artifact.

    They are returned to the caller in memory - Release 32 stripped them before
    RETURNING them too, and its correlation map came out empty, which reads as
    'nothing is related' rather than 'the relationship was never measured'.
    """
    return {k: v for k, v in (result or {}).items()
            if not str(k).startswith("_")}


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r33.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r33.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = _contract.CAMPAIGN_ID) -> Optional[dict]:
    return r33.read_json(path_for(campaign_id))
