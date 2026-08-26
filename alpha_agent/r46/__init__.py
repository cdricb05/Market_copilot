"""Release 46 - the prospective alpha tournament.

Fourteen releases have run the same loop: search history, find a candidate
that looks impressive, tell an economic story about it, meet the first
genuinely untouched evidence, and watch it collapse. Release 45 made the
mechanism explicit - it re-ran Release 44's entire 60-cell screen separately
on each of three event zones and found a DIFFERENT winner every time, the
last one larger than the headline. The premium was the search, seen from the
inside.

Release 46 stops nominating winners from history.

From here, history may only NOMINATE a challenger. A challenger is crowned by
one thing: predictions it put on the record before the outcome existed, scored
against the correct control, after costs, without retuning.

What this release found on arrival, and what it is really fixing
----------------------------------------------------------------
Five releases (R39, R40, R41, R42, R43/R45) each froze a prospective shadow
registry. Together they hold SEVEN frozen shadows. Between them they hold
ZERO forward observations. Not one row. The forward clock was started five
times and never ticked once, because:

* the R39/R40 shadows decide at month-end or on VX Fridays and nothing ever
  called their capture owner again;
* the R41/R42 BTC shadows read a public archive that publishes MONTHLY with a
  24-day lag, from a venue whose REST API answers HTTP 451 here - R42 wrote
  that defect down and it was never acted on;
* R43 and R45 froze nothing at all.

So the estate did not have a prospective-evidence problem. It had FIVE
prospective-evidence implementations, none of them running, and no single
place that would have shown an operator that the number of forward
observations was zero.

Release 46 therefore builds exactly one of each thing, and adopts the seven
orphans into it by reference rather than adding a sixth registry:

===============================  =============================================
concern                          owner
===============================  =============================================
release contract                 :mod:`alpha_agent.r46.contract`
shell policy + disclosure        :mod:`alpha_agent.r46.shell_policy`
emission clock / calendars       :mod:`alpha_agent.r46.clock`
owned live market-data seam      :mod:`alpha_agent.r46.marketdata`
CAN-THIS-STREAM-ACCRUE gate      :mod:`alpha_agent.r46.feasibility`
frozen challenger specifications :mod:`alpha_agent.r46.challengers`
challenger registry + versioning :mod:`alpha_agent.r46.registry`
THE prediction / outcome ledger  :mod:`alpha_agent.r46.ledger`
idempotent forward emission      :mod:`alpha_agent.r46.emit`
THE outcome judge                :mod:`alpha_agent.r46.judge`
evidence maturity + gates        :mod:`alpha_agent.r46.evidence`
THE leaderboard                  :mod:`alpha_agent.r46.leaderboard`
historical vs prospective burden :mod:`alpha_agent.r46.burden`
orchestration                    :mod:`alpha_agent.r46.campaign`
===============================  =============================================

Ledger mechanics REUSE the canonical chain-hash primitives from
``api.paper_trading_desk`` - the same append-only, rewrite-detectable
convention every desk ledger has used since Phase 27. No second forward
ledger implementation is created and no prior release's artifact is written.

The one discipline that makes this different from every prior release: R46
CHOOSES NO PARAMETER. Every seed challenger is a canonical, literature-
standard parameterisation declared in the frozen contract before a single
bar was read. Nothing was swept, screened or ranked on this estate's data to
select it. That is the direct answer to R45's selection-premium finding, and
it is why this release charges essentially no new historical search burden.

RESEARCH ONLY. Paper only. No orders, no promotion, no automation, no
portfolio mutation, no purchase.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

CAMPAIGN_ID = "r46_prospective_alpha_tournament_v1"
RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\prospective_alpha_tournament_r46")

__all__ = [
    "contract", "shell_policy", "clock", "marketdata", "feasibility",
    "challengers", "registry", "ledger", "emit", "judge", "evidence",
    "leaderboard", "burden", "campaign", "advance",
    "velocity", "planner", "intraday",
    "CAMPAIGN_ID", "RESEARCH_ROOT", "campaign_dir", "sha", "read_json",
    "write_json", "artifact_body",
]


def campaign_dir(campaign_id: str = CAMPAIGN_ID) -> Path:
    d = RESEARCH_ROOT / campaign_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def sha(obj) -> str:
    """Stable sha256 over any JSON-serialisable object."""
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def read_json(path: Path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def write_json(path: Path, body) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, default=str), encoding="utf-8")
    tmp.replace(p)
    return p


def artifact_body(schema: str, owner: str, **extra) -> dict:
    from . import contract as _c
    body = {
        "schema": schema,
        "release": _c.RELEASE,
        "campaign_id": CAMPAIGN_ID,
        "calculation_owner": owner,
        "safety_block": dict(_c.SAFETY_BLOCK),
    }
    body.update(extra)
    return body
