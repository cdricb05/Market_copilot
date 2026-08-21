"""alpha_agent.r32.information_state - the ONE canonical InformationState owner.

Phase 2. Every observation this project uses carries FOUR timestamps, and
collapsing any two of them is how look-ahead enters a system that believes it is
careful:

``observed_at``
    when the underlying thing happened or was measured. The reference period.
``published_at``
    when the observation first became public. For a market print this equals
    ``observed_at``; for a statistical release it is weeks later; for a revision
    it is later still.
``effective_at``
    the date the observation DESCRIBES. CPI published on 12 March describes
    February, so ``effective_at`` is February and ``published_at`` is March.
``eligible_for_decision_at``
    the first decision timestamp that may legitimately use it. Never earlier
    than ``published_at``, and pushed later by any collection or settlement lag
    this project actually has.

The rule that makes it useful is one line long, and it is enforced here rather
than remembered:

    a decision struck at time T may read an observation only if
    ``eligible_for_decision_at <= T``.

Release 32 measured why this matters. The owned macro database stamps CPI for
month M on the first business day of month M - the value appears roughly six
weeks before it was published, and it is the current revised vintage rather than
the number that was printed. Both defects are invisible if a series is described
only by "its date".
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Optional

from .. import r32
from . import contract as _contract
from . import sources as _sources

CALCULATION_OWNER = "alpha_agent.r32.information_state"
STATE_SCHEMA = "r32_information_state_contract/1"
ARTIFACT_NAME = "information_state_contract.json"

#: The four timestamps, in the order they can occur.
TIMESTAMPS = ("observed_at", "published_at", "effective_at",
              "eligible_for_decision_at")


class LookAheadViolation(RuntimeError):
    """Raised when an observation would be read before it could be known."""


class Observation:
    """One point-in-time observation with its full timestamp set."""

    __slots__ = ("source_id", "key", "value", "observed_at", "published_at",
                 "effective_at", "eligible_for_decision_at", "admissibility",
                 "vintage")

    def __init__(self, *, source_id: str, key: str, value,
                 observed_at: str, published_at: Optional[str] = None,
                 effective_at: Optional[str] = None,
                 eligible_for_decision_at: Optional[str] = None,
                 admissibility: str = _sources.PIT_MARKET_OBSERVABLE,
                 vintage: Optional[str] = None):
        self.source_id = source_id
        self.key = key
        self.value = value
        self.observed_at = str(observed_at)[:10]
        # A market print is published when it happens. Defaulting the other way
        # - assuming a publication lag of zero for everything - is what makes a
        # statistical release look like a market observable.
        self.published_at = str(published_at or observed_at)[:10]
        self.effective_at = str(effective_at or observed_at)[:10]
        self.eligible_for_decision_at = str(
            eligible_for_decision_at or self.published_at)[:10]
        self.admissibility = admissibility
        self.vintage = vintage
        if self.eligible_for_decision_at < self.published_at:
            raise LookAheadViolation(
                f"{source_id}/{key}: eligible_for_decision_at "
                f"{self.eligible_for_decision_at} precedes published_at "
                f"{self.published_at}; nothing becomes usable before it exists")

    def readable_at(self, decision_at: str) -> bool:
        return str(decision_at)[:10] >= self.eligible_for_decision_at

    def as_dict(self) -> dict:
        return {"source_id": self.source_id, "key": self.key,
                "value": self.value, "observed_at": self.observed_at,
                "published_at": self.published_at,
                "effective_at": self.effective_at,
                "eligible_for_decision_at": self.eligible_for_decision_at,
                "admissibility": self.admissibility, "vintage": self.vintage}


class InformationState:
    """Everything legitimately knowable at one decision timestamp."""

    def __init__(self, *, decision_at: str):
        self.decision_at = str(decision_at)[:10]
        self._observations: list = []

    def add(self, obs: Observation) -> "InformationState":
        if obs.admissibility not in _sources.ADMISSIBLE_FOR_HISTORY:
            raise LookAheadViolation(
                f"{obs.source_id}/{obs.key} is {obs.admissibility} and may not "
                "enter an information state used as history")
        if not obs.readable_at(self.decision_at):
            raise LookAheadViolation(
                f"{obs.source_id}/{obs.key} becomes eligible on "
                f"{obs.eligible_for_decision_at}, after the decision at "
                f"{self.decision_at}")
        self._observations.append(obs)
        return self

    def try_add(self, obs: Observation) -> bool:
        """Add if legitimate; report False instead of raising.

        Used by collectors that walk a whole history: an observation that is not
        yet eligible is a normal fact about time, not an error.
        """
        try:
            self.add(obs)
            return True
        except LookAheadViolation:
            return False

    def get(self, source_id: str, key: str):
        for o in reversed(self._observations):
            if o.source_id == source_id and o.key == key:
                return o.value
        return None

    @property
    def observations(self) -> list:
        return list(self._observations)

    def state_hash(self) -> str:
        return r32.sha([o.as_dict() for o in self._observations])

    def as_dict(self) -> dict:
        return {"decision_at": self.decision_at,
                "n_observations": len(self._observations),
                "sources": sorted({o.source_id for o in self._observations}),
                "state_hash": self.state_hash()}


def publication_lag_days(admissibility: str) -> Optional[int]:
    """The lag this project is willing to assume, by admissibility class.

    ``None`` means the class may not be assumed into eligibility at all. That is
    deliberate for ``REVISED_NOT_PIT``: there is no lag that repairs a value
    stamped at the start of the period it measures AND carrying today's
    revisions. The fix is a vintage source, or the honest admission that the
    period is unmeasurable.
    """
    return {_sources.PIT_MARKET_OBSERVABLE: 0,
            _sources.PIT_VINTAGE_DATED: 0}.get(admissibility)


def build_contract(*, campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "timestamps": list(TIMESTAMPS),
        "rule": "a decision struck at time T may read an observation only if "
                "eligible_for_decision_at <= T",
        "defaults": {
            "published_at": "observed_at when the source is a market print",
            "eligible_for_decision_at": "published_at plus any real collection "
                                        "lag; never earlier than published_at",
        },
        "admissible_for_history": list(_sources.ADMISSIBLE_FOR_HISTORY),
        "inadmissible_for_history":
            [s for s in _sources.ADMISSIBLE_STATES_ALL
             if s not in _sources.ADMISSIBLE_FOR_HISTORY],
        "revised_not_pit_cannot_be_repaired_by_a_lag": True,
        "enforcement": "InformationState.add raises LookAheadViolation",
    }
    body = r32.artifact_body(STATE_SCHEMA, payload)
    body["information_state_hash"] = r32.sha(payload)
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r32.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r32.write_json(path_for(body["campaign_id"]), body)
