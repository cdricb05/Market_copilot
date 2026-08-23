"""alpha_agent.r39.search_budget - the SEARCH_BUDGET_LEDGER owner.

The hierarchical budget that keeps "no artificial idea limit" from becoming
brute force: stage ceilings come from the contract, every generation and
promotion is counted, and the ledger refuses to promote past a ceiling.
The counters persisted here are the campaign's public accounting -
CANDIDATES_GENERATED through LOCKED_CONFIRMATIONS_RUN - and they are also
what the search-burden correction reads, so under-counting search would
directly weaken the campaign's own statistics.
"""
from __future__ import annotations

from pathlib import Path

from .. import r39
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r39.search_budget"
SCHEMA = "r39_search_budget_ledger/1"
ARTIFACT_NAME = "search_budget_ledger.json"


class BudgetExceeded(RuntimeError):
    pass


class Ledger:
    def __init__(self, campaign_id: str = C.CAMPAIGN_ID):
        self.campaign_id = campaign_id
        body = r39.read_json(self.path())
        self.counters = (body or {}).get("counters") or {
            k: 0 for k in C.SEARCH_BUDGET_COUNTERS}
        self.by_stage_family = (body or {}).get("by_stage_family") or {}

    def path(self) -> Path:
        return r39.campaign_dir(self.campaign_id) / ARTIFACT_NAME

    def add(self, counter: str, n: int = 1, *, family: str = None,
            stage: str = None) -> None:
        if counter not in self.counters:
            raise KeyError(counter)
        limits = {
            "CANDIDATES_GENERATED": C.STAGE1_MAX_CANDIDATES,
            "CANDIDATES_PROMOTED_TO_STAGE2": C.STAGE2_MAX_CANDIDATES,
            "CANDIDATES_PROMOTED_TO_STAGE3": C.STAGE3_MAX_CANDIDATES,
            "FINALISTS_FROZEN": C.MAX_LOCKBOX_CANDIDATES,
            "LOCKED_CONFIRMATIONS_RUN": C.MAX_LOCKBOX_CANDIDATES,
        }
        new = self.counters[counter] + int(n)
        if counter in limits and new > limits[counter]:
            raise BudgetExceeded(
                "%s would reach %d, above the frozen ceiling %d"
                % (counter, new, limits[counter]))
        self.counters[counter] = new
        if family and stage:
            key = "%s::%s" % (stage, family)
            self.by_stage_family[key] = \
                self.by_stage_family.get(key, 0) + int(n)
        self.persist()

    def persist(self) -> None:
        body = {"contract": SCHEMA, "campaign_id": self.campaign_id,
                "calculation_owner": CALCULATION_OWNER,
                "counters": self.counters,
                "by_stage_family": self.by_stage_family,
                "ceilings": {
                    "STAGE1_MAX_CANDIDATES": C.STAGE1_MAX_CANDIDATES,
                    "STAGE2_MAX_CANDIDATES": C.STAGE2_MAX_CANDIDATES,
                    "STAGE3_MAX_CANDIDATES": C.STAGE3_MAX_CANDIDATES,
                    "MAX_LOCKBOX_CANDIDATES": C.MAX_LOCKBOX_CANDIDATES},
                "optimization_zones": list(C.OPTIMIZATION_ZONES),
                "never_optimize_against_zone_c":
                    C.NEVER_OPTIMIZE_AGAINST_ZONE_C}
        body["safety_block"] = r39.safety_block()
        r39.write_json(self.path(), body, immutable=False)
