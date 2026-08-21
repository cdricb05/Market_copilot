"""alpha_agent.r32.funnel - the ONE Release 32 budget and denominator owner.

Four stages, each with a ceiling that is a number in
:mod:`alpha_agent.r32.contract`: SCREENING, QUALIFICATION, NOVEL_REFINEMENT,
LOCKBOX. This module refuses work that would exceed a ceiling instead of logging
a warning, because a budget that can be exceeded is not a budget.

It also owns the multiple-testing DENOMINATOR, and owns it strictly: every
executed hypothesis counts, including the ones that failed, produced too few
scored decisions, or were abandoned. A denominator that counts only the
survivors is not a correction for multiple testing - it is a second round of
selection wearing the costume of one.
"""
from __future__ import annotations

from typing import Optional

from .. import r32
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r32.funnel"
REGISTRY_SCHEMA = "r32_sleeve_candidate_registry/1"
ARTIFACT_NAME = "sleeve_candidate_registry.json"
LOG_NAME = "sleeve_candidate_results.jsonl"

STATE_OK = "OK"
STATE_INSUFFICIENT_DECISIONS = "INSUFFICIENT_SCORED_DECISIONS"
STATE_NO_PANEL = "PANEL_UNAVAILABLE"
STATE_FAILED = "FAILED"
STATES = (STATE_OK, STATE_INSUFFICIENT_DECISIONS, STATE_NO_PANEL, STATE_FAILED)


class BudgetExceeded(RuntimeError):
    """Raised when a stage would run more hypotheses than it declared."""


class ControlSleeveResearched(RuntimeError):
    """Raised when something tries to research the inherited control sleeve."""


class LockboxViolation(RuntimeError):
    """Raised on a second lockbox access or a post-lockbox retune."""


def assert_control_not_researched(sleeve: str) -> None:
    """The control carries Release 31's verdict and is never searched again."""
    if sleeve == _contract.CONTROL_SLEEVE:
        raise ControlSleeveResearched(
            f"{sleeve} is the Release-32 CONTROL sleeve. Release 31 already "
            f"returned {_contract.R31_VERDICT} with dominant constraint "
            f"{_contract.R31_DOMINANT_CONSTRAINT}; re-running the same search "
            "over the same information adds to the multiple-testing "
            "denominator and adds no knowledge.")


class Funnel:
    """Budget enforcement, execution log and denominator for one campaign."""

    def __init__(self, *, campaign_id: str = _contract.CAMPAIGN_ID,
                 judge_behaviour_hash: str):
        self.campaign_id = campaign_id
        self.judge_behaviour_hash = judge_behaviour_hash
        self.rows: list = []
        self._seen: set = set()
        self._lockbox_access: dict = {}
        self._lockbox_frozen = False

    # ---------------------------------------------------------------- counts #
    def count(self, *, stage: Optional[str] = None,
              sleeve: Optional[str] = None,
              family: Optional[str] = None) -> int:
        return sum(1 for r in self.rows
                   if (stage is None or r["stage"] == stage)
                   and (sleeve is None or r["sleeve"] == sleeve)
                   and (family is None or r["family"] == family))

    @property
    def denominator(self) -> int:
        """EVERY executed hypothesis, whatever its outcome."""
        return len(self.rows)

    # --------------------------------------------------------------- budgets #
    def check(self, spec) -> None:
        """Refuse a specification that would breach its stage ceiling."""
        assert_control_not_researched(spec.sleeve)
        stage = spec.stage
        if self._lockbox_frozen and stage != _contract.STAGE_LOCKBOX:
            raise LockboxViolation(
                "the lockbox has been opened; retuning after lockbox access is "
                "how a held-out sample stops being held out")
        if stage == _contract.STAGE_SCREENING:
            self._limit(self.count(stage=stage, sleeve=spec.sleeve),
                        _contract.SCREENING_MAX_PER_SLEEVE,
                        f"screening/{spec.sleeve}")
        elif stage == _contract.STAGE_QUALIFICATION:
            self._limit(self.count(stage=stage, sleeve=spec.sleeve),
                        _contract.QUALIFICATION_MAX_PER_SLEEVE,
                        f"qualification/{spec.sleeve}")
            self._limit(self.count(stage=stage, sleeve=spec.sleeve,
                                   family=spec.family),
                        _contract.QUALIFICATION_MAX_CONFIGS_PER_FAMILY,
                        f"qualification/{spec.sleeve}/{spec.family}")
            self._limit(self.count(stage=stage), _contract.QUALIFICATION_MAX_TOTAL,
                        "qualification/total")
            fams = {r["family"] for r in self.rows
                    if r["stage"] == stage and r["sleeve"] == spec.sleeve}
            if (spec.family not in fams
                    and len(fams) >= _contract.QUALIFICATION_MAX_FAMILIES_PER_SLEEVE):
                raise BudgetExceeded(
                    f"qualification/{spec.sleeve} already uses "
                    f"{len(fams)} families, the declared maximum")
        elif stage == _contract.STAGE_NOVEL:
            if spec.depth > _contract.NOVEL_MAX_DEPTH:
                raise BudgetExceeded(
                    f"novel depth {spec.depth} exceeds "
                    f"{_contract.NOVEL_MAX_DEPTH}")
            self._limit(self.count(stage=stage, sleeve=spec.sleeve),
                        _contract.NOVEL_MAX_PER_SLEEVE, f"novel/{spec.sleeve}")
            self._limit(self.count(stage=stage), _contract.NOVEL_MAX_TOTAL,
                        "novel/total")
        elif stage == _contract.STAGE_LOCKBOX:
            self._limit(self.count(stage=stage, sleeve=spec.sleeve),
                        _contract.LOCKBOX_MAX_FINALISTS_PER_SLEEVE,
                        f"lockbox/{spec.sleeve}")
            self._limit(self.count(stage=stage),
                        _contract.LOCKBOX_MAX_FINALISTS_TOTAL, "lockbox/total")
        else:
            raise BudgetExceeded(f"unknown stage: {stage}")

    @staticmethod
    def _limit(current: int, ceiling: int, what: str) -> None:
        if current >= ceiling:
            raise BudgetExceeded(
                f"{what} budget exhausted at {ceiling}; widening it after "
                "seeing results would turn this campaign into a search")

    # -------------------------------------------------------------- lockbox #
    def authorise_lockbox(self, spec_hash: str) -> None:
        n = self._lockbox_access.get(spec_hash, 0)
        if n >= _contract.LOCKBOX_MAX_ACCESSES_PER_FINALIST:
            raise LockboxViolation(
                f"finalist {spec_hash[:12]} has already used its single "
                "lockbox access; a second look is a second test")
        self._lockbox_access[spec_hash] = n + 1
        self._lockbox_frozen = True

    # -------------------------------------------------------------- recording #
    def record(self, spec, result: dict, *, state: str = STATE_OK) -> dict:
        """Log one executed hypothesis. It enters the denominator regardless."""
        if state not in STATES:
            raise ValueError(f"unknown state: {state}")
        sh = spec.spec_hash(self.judge_behaviour_hash)
        key = (spec.stage, sh)
        if key in self._seen:
            raise BudgetExceeded(
                f"duplicate specification {spec.label()} in {spec.stage}; the "
                "same hypothesis counted twice is a denominator error")
        self._seen.add(key)
        row = {"stage": spec.stage, "sleeve": spec.sleeve,
               "family": spec.family, "params": dict(spec.params),
               "label": spec.label(), "spec_hash": sh, "depth": spec.depth,
               "state": state, "is_control": bool(getattr(spec, "is_control",
                                                          False)),
               "judge_behaviour_hash": self.judge_behaviour_hash}
        row.update({k: v for k, v in (result or {}).items()
                    if not k.startswith("_")})
        self.rows.append(row)
        # The PERSISTED row carries no private keys - a frozen artifact should
        # not contain a raw return path. The row handed back to the caller does,
        # because the frontier needs those paths to compute the correlation map
        # and the latent risk clusters. Returning the stripped row made both of
        # those silently empty, which reads as "no sleeves are related" rather
        # than "the relationship was never measured".
        return dict(row, **{k: v for k, v in (result or {}).items()
                            if k.startswith("_")})

    def run(self, spec, execute) -> Optional[dict]:
        """Check the budget, execute, and record - in that order.

        Recording AFTER execution but unconditionally is what keeps a failed
        hypothesis in the denominator. Execution that raises is recorded as
        FAILED rather than disappearing.
        """
        self.check(spec)
        try:
            result = execute(spec)
        except Exception as exc:  # noqa: BLE001
            self.record(spec, {"error": f"{type(exc).__name__}: {str(exc)[:300]}"},
                        state=STATE_FAILED)
            return None
        if not result or not result.get("scored"):
            self.record(spec, result or {}, state=STATE_NO_PANEL)
            return None
        if int(result.get("n") or 0) < _contract.MIN_SCORED_DECISIONS:
            self.record(spec, result, state=STATE_INSUFFICIENT_DECISIONS)
            return None
        return self.record(spec, result, state=STATE_OK)

    # -------------------------------------------------------------- artifact #
    def summary(self) -> dict:
        by_stage, by_sleeve, by_state = {}, {}, {}
        for r in self.rows:
            by_stage[r["stage"]] = by_stage.get(r["stage"], 0) + 1
            by_sleeve[r["sleeve"]] = by_sleeve.get(r["sleeve"], 0) + 1
            by_state[r["state"]] = by_state.get(r["state"], 0) + 1
        return {"denominator": self.denominator, "by_stage": by_stage,
                "by_sleeve": by_sleeve, "by_state": by_state,
                "lockbox_accesses": dict(self._lockbox_access),
                "lockbox_opened": self._lockbox_frozen}

    def build(self) -> dict:
        payload = {"calculation_owner": CALCULATION_OWNER,
                   "campaign_id": self.campaign_id,
                   "judge_behaviour_hash": self.judge_behaviour_hash,
                   "budgets": _contract.BUDGETS,
                   "denominator_counts_all_executed":
                       _contract.DENOMINATOR_COUNTS_ALL_EXECUTED,
                   "summary": self.summary(),
                   "rows": self.rows}
        body = r32.artifact_body(REGISTRY_SCHEMA, payload)
        body["registry_hash"] = r32.sha(payload)
        return body

    def path_for(self):
        return r32.campaign_dir(self.campaign_id) / ARTIFACT_NAME

    def freeze(self):
        return r32.write_json(self.path_for(), self.build())
