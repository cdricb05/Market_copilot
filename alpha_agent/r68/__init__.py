r"""alpha_agent.r68 - R68_FORWARD_EVIDENCE_REPAIR_AND_ALPHA_ACTIVATION.

R67 measured, correctly, that four registered forward challengers had no code
path able to produce their next decision, and then declined to wire one because
"attaching a cadence producer to an identity that was registered when no such
producer existed changes what that identity means".

R68 examined that claim against the records and found it false in this case. The
frozen construction of all four R58 challengers declares
``rebalance_cadence_sessions = 21``. The registrar copied that construction. The
accrual owner's state at every boundary since has been
``AWAITING_NEW_GOVERNED_FREEZE`` - a sentence that means "the originating owner
has not frozen yet", not "this identity does not rebalance". A cadence producer
does not change what these identities mean; it is the capability they were
declared to have and never had. The evidence for that determination, and the
exact procedure for reversing it, are written to
``R58_PRODUCER_ACTIVATION_DECISION.json`` by
:mod:`alpha_agent.r68.r58_cadence_runtime`.

RESEARCH ONLY. Nothing in this package creates an order, a fill, a proposal or
an approval, promotes a model, activates a sleeve, allocates capital, changes a
holding, cash or NAV, or writes to any operational store.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

RELEASE = "R68_FORWARD_EVIDENCE_REPAIR_AND_ALPHA_ACTIVATION"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R68_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\r68_forward_activation")

REPO_ROOT = Path(__file__).resolve().parents[2]

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "backfill_allowed": False,
    "records_are_immutable": True,
    "first_write_wins": True,
    "promotes_model": False,
    "activates_sleeve": False,
    "allocates_capital": False,
    "creates_orders": False,
    "creates_fills": False,
    "creates_proposals": False,
    "approves_anything": False,
    "mutates_operational_store": False,
    "registers_forward_challenger": False,
    "manual_review_required": True,
    "safety_badges": ["RESEARCH ONLY", "PREVIEW ONLY", "MANUAL REVIEW",
                      "NO ORDERS", "ORDERS DISABLED", "AUTOMATION OFF",
                      "NO MODEL PROMOTION"],
}


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


__all__ = ["RELEASE", "RESEARCH_ROOT_ENV", "DEFAULT_RESEARCH_ROOT", "REPO_ROOT",
           "SAFETY", "research_root", "now_iso", "stable_hash"]
