"""alpha_agent.r37.unlock - the ONE Release-37 cell-unlock owner.

How many of Release 36's 95 blocked cells would a dataset actually open?

That number is the whole purchase case, so it is DERIVED rather than typed. The
blocked frontier is read from the frozen Release-36 coverage matrix on disk, or
- if that artifact is absent - reconstructed from the released
:mod:`alpha_agent.r36.coverage` market table. Either way the arithmetic comes
from the release that owns the frontier, and Release 37 never re-states it.

Three rules are enforced here rather than trusted:

* **A claimed market must actually be blocked.** A dataset that names a market
  Release 36 already tested is not unlocking anything, and the claim is recorded
  as a defect rather than silently counted.
* **A proxy may not unlock a native cell.** Release 36 spent a whole release
  establishing that ``USO`` is not the WTI curve. A dataset whose implementation
  level is PROXY or SIGNAL gets zero full unlocks, by construction, no matter
  what its coverage table says.
* **Partial is not full.** A market whose coverage is plausible but unconfirmed
  is reported separately and never enters the headline count, because
  ``PARTIAL_UNLOCK_COUNTS_IN_HEADLINE`` is False.
"""
from __future__ import annotations

from collections import Counter
from typing import Optional

from .. import r37
from ..r36 import contract as _r36_contract
from ..r36 import coverage as _r36_coverage
from . import contract as _contract
from . import providers as _providers

CALCULATION_OWNER = "alpha_agent.r37.unlock"
SCHEMA = "r37_r36_cell_unlock_map/1"
ARTIFACT_NAME = "r36_cell_unlock_map.json"

C = _contract

#: The Release-36 states that mean "there is no instrument here at any price we
#: were allowed to pay". Imported by name from the release that declared them.
BLOCKED_STATES = (
    _r36_contract.STATE_BLOCKED_ENTITLEMENT,
    _r36_contract.STATE_BLOCKED_COST,
    _r36_contract.STATE_BLOCKED_LICENSING,
    _r36_contract.STATE_BLOCKED_PIT,
    _r36_contract.STATE_BLOCKED_SURVIVORSHIP,
    _r36_contract.STATE_BLOCKED_HISTORY,
    _r36_contract.STATE_BLOCKED_MAPPING,
)

SOURCE_FROZEN = "R36_FROZEN_COVERAGE_MATRIX"
SOURCE_DERIVED = "R36_COVERAGE_MARKET_TABLE"


def _from_frozen(campaign_id: str) -> Optional[dict]:
    """The blocked frontier as Release 36 actually froze it."""
    body = _r36_coverage.load(campaign_id)
    if not body or not body.get("cells"):
        return None
    blocked = {}
    for cell in body["cells"]:
        if cell.get("state") not in BLOCKED_STATES:
            continue
        row = blocked.setdefault(cell["market_key"], {
            "market_key": cell["market_key"],
            "asset_class": cell.get("asset_class"),
            "sub_asset": cell.get("sub_asset"),
            "native_instrument": cell.get("native_instrument"),
            "blocker": cell.get("state"),
            "blocker_reason": cell.get("blocker_reason"),
            "families": []})
        row["families"].append(cell.get("strategy_family"))
    for row in blocked.values():
        row["families"] = sorted(f for f in row["families"] if f)
        row["n_cells"] = len(row["families"])
    return {"source": SOURCE_FROZEN,
            "campaign_id": body.get("campaign_id"),
            "markets": blocked}


def _from_market_table() -> dict:
    """The blocked frontier reconstructed from the released market table.

    Used when the frozen artifact is not on the machine running this - a fresh
    clone, or CI. The market table carries each market's blocker and its
    applicable families, which is exactly what the frozen matrix expands.
    """
    blocked = {}
    for key, market in _r36_coverage.MARKETS.items():
        if market.get("blocker") not in BLOCKED_STATES:
            continue
        blocked[key] = {
            "market_key": key,
            "asset_class": market.get("asset_class"),
            "sub_asset": market.get("sub_asset"),
            "native_instrument": market.get("native_instrument"),
            "blocker": market.get("blocker"),
            "blocker_reason": market.get("blocker_reason"),
            "families": sorted(market.get("families") or ()),
            "n_cells": len(market.get("families") or ()),
        }
    return {"source": SOURCE_DERIVED, "campaign_id": None, "markets": blocked}


def blocked_frontier(*, r36_campaign_id: str = _r36_contract.CAMPAIGN_ID
                     ) -> dict:
    """Every blocked Release-36 market, its blocker and its blocked families."""
    frozen = _from_frozen(r36_campaign_id)
    frontier = frozen if frozen else _from_market_table()
    markets = frontier["markets"]
    frontier["n_markets"] = len(markets)
    frontier["n_blocked_cells"] = sum(m["n_cells"] for m in markets.values())
    frontier["by_blocker"] = dict(
        Counter(m["blocker"] for m in markets.values()))
    frontier["cells_by_asset_class"] = {
        ac: sum(m["n_cells"] for m in markets.values()
                if m["asset_class"] == ac)
        for ac in sorted({m["asset_class"] for m in markets.values()})}
    return frontier


def _level_allows_full_unlock(level: Optional[str]) -> bool:
    """Only a native implementation may close a native cell."""
    if _contract.PROXY_MAY_CLOSE_A_NATIVE_FRONTIER:
        return True
    return level == _contract.LEVEL_NATIVE


def for_dataset(row: dict, frontier: dict) -> dict:
    """The cells one dataset would unlock, fully and partially."""
    markets = frontier["markets"]
    claimed_full = list(row.get("markets_covered") or [])
    claimed_partial = list(row.get("markets_partial") or [])

    not_blocked = sorted(m for m in claimed_full + claimed_partial
                         if m not in markets)
    level_ok = _level_allows_full_unlock(row.get("implementation_level"))
    dated_ok = row.get("dated_contracts_available") is not False

    full = [] if not level_ok else [m for m in claimed_full if m in markets]
    partial = [m for m in claimed_partial if m in markets]
    if not level_ok:
        # A proxy or signal dataset keeps its claims as PARTIAL so the reader
        # can see what it covers, and receives no headline credit for any of it.
        partial = sorted(set(partial) | {m for m in claimed_full
                                         if m in markets})

    detail = []
    for key in sorted(set(full) | set(partial)):
        m = markets[key]
        detail.append({
            "market_key": key,
            "asset_class": m["asset_class"],
            "sub_asset": m["sub_asset"],
            "native_instrument": m["native_instrument"],
            "blocker": m["blocker"],
            "cells": m["n_cells"],
            "families": list(m["families"]),
            "unlock": "FULL" if key in full else "PARTIAL",
        })

    cells_full = sum(markets[m]["n_cells"] for m in full)
    cells_partial = sum(markets[m]["n_cells"] for m in partial)
    classes_full = sorted({markets[m]["asset_class"] for m in full})
    return {
        "dataset_id": row["dataset_id"],
        "provider": row["provider"],
        "implementation_level": row.get("implementation_level"),
        "dated_contracts_available": row.get("dated_contracts_available"),
        "proxy_credit_refused": not level_ok,
        "continuous_only_penalty_applies": not dated_ok,
        "markets_unlocked_full": sorted(full),
        "markets_unlocked_partial": sorted(partial),
        "markets_claimed_but_not_blocked": not_blocked,
        "cells_unlocked_full": cells_full,
        "cells_unlocked_partial": cells_partial,
        "cells_unlocked_ceiling": cells_full + cells_partial,
        "native_markets_unlocked_full": len(full),
        "asset_classes_unlocked_full": classes_full,
        "n_asset_classes_unlocked_full": len(classes_full),
        "share_of_blocked_frontier": (
            round(cells_full / frontier["n_blocked_cells"], 4)
            if frontier["n_blocked_cells"] else 0.0),
        "cells_by_asset_class": {
            ac: sum(markets[m]["n_cells"] for m in full
                    if markets[m]["asset_class"] == ac)
            for ac in classes_full},
        "detail": detail,
        "headline_counts_partial": _contract.PARTIAL_UNLOCK_COUNTS_IN_HEADLINE,
    }


def build(*, r36_campaign_id: str = _r36_contract.CAMPAIGN_ID) -> dict:
    """The whole unlock map: one row per candidate dataset."""
    frontier = blocked_frontier(r36_campaign_id=r36_campaign_id)
    rows = [for_dataset(row, frontier) for row in _providers.rows()]
    ranked = sorted(rows, key=lambda r: (-r["cells_unlocked_full"],
                                         -r["cells_unlocked_ceiling"],
                                         r["dataset_id"]))
    covered_full = sorted({m for r in rows for m in r["markets_unlocked_full"]})
    covered_any = sorted({m for r in rows
                          for m in (r["markets_unlocked_full"]
                                    + r["markets_unlocked_partial"])})
    unreachable = sorted(m for m in frontier["markets"] if m not in covered_any)
    return {
        "frontier": frontier,
        "rows": rows,
        "ranked_by_cells_unlocked": [r["dataset_id"] for r in ranked],
        "best_single_dataset": ranked[0]["dataset_id"] if ranked else None,
        "best_single_dataset_cells": (ranked[0]["cells_unlocked_full"]
                                      if ranked else 0),
        "markets_reachable_fully_by_some_candidate": covered_full,
        "markets_reachable_at_all_by_some_candidate": covered_any,
        "markets_no_candidate_reaches": unreachable,
        "cells_no_candidate_reaches": sum(
            frontier["markets"][m]["n_cells"] for m in unreachable),
        "claims_without_a_blocked_market": sorted(
            {"%s::%s" % (r["dataset_id"], m)
             for r in rows for m in r["markets_claimed_but_not_blocked"]}),
    }


def artifact(built: dict, *, campaign_id: str, created_at: str) -> dict:
    frontier = built["frontier"]
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "frontier_source": frontier["source"],
        "r36_campaign_id": frontier.get("campaign_id"),
        "blocked_markets": frontier["n_markets"],
        "blocked_cells": frontier["n_blocked_cells"],
        "blocked_by_blocker": frontier["by_blocker"],
        "blocked_cells_by_asset_class": frontier["cells_by_asset_class"],
        "rows": built["rows"],
        "ranked_by_cells_unlocked": built["ranked_by_cells_unlocked"],
        "best_single_dataset": built["best_single_dataset"],
        "best_single_dataset_cells": built["best_single_dataset_cells"],
        "markets_no_candidate_reaches": built["markets_no_candidate_reaches"],
        "cells_no_candidate_reaches": built["cells_no_candidate_reaches"],
        "claims_without_a_blocked_market":
            built["claims_without_a_blocked_market"],
        "unlock_requires_declared_instrument_mapping":
            _contract.UNLOCK_REQUIRES_DECLARED_INSTRUMENT_MAPPING,
        "proxy_may_close_a_native_frontier":
            _contract.PROXY_MAY_CLOSE_A_NATIVE_FRONTIER,
        "partial_unlock_counts_in_headline":
            _contract.PARTIAL_UNLOCK_COUNTS_IN_HEADLINE,
    }
    return r37.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r37.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r37.read_json(path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "BLOCKED_STATES", "SOURCE_FROZEN",
           "SOURCE_DERIVED", "blocked_frontier", "for_dataset", "build",
           "artifact", "freeze", "load", "path_for"]
