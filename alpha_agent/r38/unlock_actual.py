"""alpha_agent.r38.unlock_actual - Phase 5: expected unlocks vs DELIVERED.

Release 37's unlock map credited the Norgate futures package with ~53 expected
full unlocks. That number was derived from the frozen Release-36 matrix and
the vendor's DECLARED instrument list, and Release 37 said so:
``EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS``. This module replaces the
expectation with measurement, cell by cell, from the delivered registries -
and where actual disagrees with expected it says WHY, in both directions.

Two honesty rules bind here:

* a cell whose STRATEGY needs an information leg the futures package does not
  carry (USDA supply/demand, issuer fundamentals) is NOT fully unlocked by
  delivered prices, however good they are - it is PARTIALLY_UNLOCKED, even
  though Release 37 expected it FULL;
* a cell the package opened BEYOND expectation (international government
  bonds, emerging index futures, the CME crypto curve) is credited, with the
  same evidence standard, even though crediting it makes the "53" wrong in
  the other direction.

This module reads the frozen R36 matrix through ``alpha_agent.r36.coverage``
and the frozen R37 expectation through ``alpha_agent.r37.unlock``. It defines
no coverage authority and recomputes neither input.
"""
from __future__ import annotations

import datetime as _dt
from typing import Optional

from .. import r38
from ..r36 import contract as _r36_contract
from ..r36 import coverage as _r36_coverage
from ..r37 import unlock as _r37_unlock
from . import contract as C
from . import enumeration as EN
from . import experiments as EX
from . import quality as Q

CALCULATION_OWNER = "alpha_agent.r38.unlock_actual"
SCHEMA = "r38_r37_expected_vs_r38_actual_unlocks/1"
COVERAGE_SCHEMA = "r38_updated_global_multi_asset_coverage/1"
ARTIFACT_NAME = C.ARTIFACT_NAMES["r37_expected_vs_r38_actual_unlocks"]
COVERAGE_ARTIFACT = C.ARTIFACT_NAMES["updated_global_multi_asset_coverage"]

NORGATE_DATASET_ID = "norgate_futures_package"

#: R36 blocked market -> the delivered evidence that could open it. Declared
#: from metadata; a market key absent here is untouched by this entitlement.
R36_MARKET_TO_DELIVERED = {
    "CMDTY_GRAINS": {"groups": ("GRAINS_AND_OILSEEDS",)},
    "CMDTY_SOFTS": {"groups": ("SOFTS",)},
    "CMDTY_LIVESTOCK": {"groups": ("LIVESTOCK",)},
    "CMDTY_PRECIOUS": {"groups": ("PRECIOUS_METALS",)},
    "CMDTY_INDUSTRIAL": {"groups": ("INDUSTRIAL_METALS",)},
    "INTL_EQUITY_DEVELOPED": {"groups": ("INTL_INDEX_FUTURES",)},
    "INTL_EQUITY_EMERGING": {"groups": ("INTL_INDEX_FUTURES_EMERGING",)},
    "RATES_TREASURY_FUTURES": {"groups": ("TREASURY_FUTURES",)},
    "RATES_INTERNATIONAL": {"groups": ("INTERNATIONAL_GOVERNMENT",)},
    "VOL_VIX_FUTURES": {"markets": ("VX",)},
    "CRYPTO_BASIS_FUNDING": {"markets": ("BTC", "ETH")},
}

#: Families whose research needs an INFORMATION leg beyond delivered prices.
#: ``owned`` names the estate's evidence; None means the leg is absent.
FAMILY_INFORMATION_LEG = {
    "POSITIONING": {
        "requirement": "CFTC Commitments of Traders history",
        "owned": "R35 deacot archive 1986-2026 + declared code mapping",
        "satisfied_for": "US-listed markets in EX.COT_CODE_MAPPING",
    },
    "FUNDAMENTAL_SUPPLY_DEMAND": {
        "requirement": "PIT physical supply/demand data (USDA or equivalent)",
        "owned": None,
        "note": "EIA covers energy only, and R36 already tested energy native",
    },
    "VALUE": {
        "requirement": "issuer/index fundamentals (earnings, book)",
        "owned": None,
    },
    "MACRO_CONDITIONAL": {
        "requirement": "point-in-time macro series",
        "owned": "R33/R35 ALFRED vintages + FRED with declared lags",
    },
}

#: Cross-sectional families need a cross-section.
XS_FAMILIES = ("CROSS_SECTIONAL", "RELATIVE_VALUE")


def _history_years(row: dict) -> float:
    fq = row.get("first_quoted_date")
    if not fq:
        return 0.0
    first = _dt.date.fromisoformat(str(fq)[:10])
    return (_dt.date.today() - first).days / 365.25


def _delivered_for(spec: dict, registry: dict) -> list:
    markets = []
    for market, row in registry["markets"].items():
        if market in C.DUPLICATE_UNDERLYING_EXCLUSIONS:
            continue
        if "groups" in spec and row["economic_group"] in spec["groups"]:
            markets.append(market)
        elif "markets" in spec and market in spec["markets"]:
            markets.append(market)
    return sorted(markets)


def _judge_cell(market_key: str, family: str, delivered_rows: list,
                quality_states: dict) -> dict:
    """One blocked R36 cell judged against delivered evidence."""
    n = len(delivered_rows)
    if n == 0:
        return {"status": C.CELL_STILL_BLOCKED_ENTITLEMENT,
                "why": "no delivered market implements this native "
                       "instrument"}
    histories = sorted(_history_years(r) for r in delivered_rows)
    deep = [h for h in histories if h >= C.MIN_VERIFIED_HISTORY_YEARS]
    contracts = [r.get("contract_count_primary_session") or 0
                 for r in delivered_rows]
    quality_failed = [r["market"] for r in delivered_rows
                      if quality_states.get(r["market"]) == "FAIL"]
    if quality_failed:
        return {"status": C.CELL_STILL_BLOCKED_METADATA,
                "why": "delivered but failed structural validation: %s"
                       % ", ".join(quality_failed)}

    needs_curve = family in ("CARRY", "CURVE_TERM_STRUCTURE", "ROLL",
                             "MEAN_REVERSION", "SEASONALITY")
    if needs_curve and max(contracts) < 24:
        return {"status": C.CELL_STILL_BLOCKED_HISTORY,
                "why": "fewer than 24 dated contracts delivered - no curve "
                       "to research"}

    if family in XS_FAMILIES and n < C.MIN_VERIFIED_MARKETS_PER_GROUP:
        return {"status": C.CELL_PARTIALLY_UNLOCKED,
                "why": "a cross-section needs >= %d delivered markets in the "
                       "group; %d delivered"
                       % (C.MIN_VERIFIED_MARKETS_PER_GROUP, n)}

    leg = FAMILY_INFORMATION_LEG.get(family)
    if leg is not None and leg.get("owned") is None:
        return {"status": C.CELL_PARTIALLY_UNLOCKED,
                "why": "the price leg is delivered and verified, and the "
                       "strategy also needs %s, which this estate does not "
                       "own" % leg["requirement"]}
    if family == "POSITIONING":
        mapped = [r["market"] for r in delivered_rows
                  if r["market"] in EX.COT_CODE_MAPPING]
        if len(mapped) < min(2, n):
            return {"status": C.CELL_PARTIALLY_UNLOCKED,
                    "why": "delivered markets lack a CFTC COT mapping "
                           "(non-US listings)"}

    if not deep:
        return {"status": C.CELL_STILL_BLOCKED_HISTORY,
                "why": "no delivered market reaches %.0f years of history "
                       "(max %.1f)" % (C.MIN_VERIFIED_HISTORY_YEARS,
                                       histories[-1])}

    why = ("%d delivered market(s), %d with >= %.0fy history, "
           "%d-%d dated contracts, structural validation passed"
           % (n, len(deep), C.MIN_VERIFIED_HISTORY_YEARS,
              min(contracts), max(contracts)))
    return {"status": C.CELL_NATIVE_VERIFIED, "why": why}


def build(*, campaign_id: str = C.CAMPAIGN_ID,
          created_at: Optional[str] = None) -> dict:
    created = created_at or _dt.datetime.now(_dt.timezone.utc).isoformat()
    registry = EN.load_market_registry(campaign_id)
    if registry is None:
        raise RuntimeError("Phase-2 market registry not frozen yet")
    quality = Q.load(campaign_id) or {"markets": {}}
    quality_states = {m: row["state"]
                      for m, row in quality.get("markets", {}).items()}

    matrix = _r36_coverage.load(_r36_contract.CAMPAIGN_ID)
    if matrix is None:
        raise RuntimeError("frozen R36 coverage matrix unavailable")
    expectation = _r37_unlock.load()
    if expectation is None:
        raise RuntimeError("frozen R37 unlock map unavailable")
    norgate_row = next(r for r in expectation["rows"]
                       if r["dataset_id"] == NORGATE_DATASET_ID)
    expected_full = {d["market_key"]: d for d in norgate_row["detail"]
                     if d["unlock"] == "FULL"}
    expected_partial = {d["market_key"]: d for d in norgate_row["detail"]
                        if d["unlock"] == "PARTIAL"}

    blocked_states = set(_r37_unlock.BLOCKED_STATES)
    cells = []
    for cell in matrix["cells"]:
        if cell["state"] not in blocked_states:
            continue
        market_key = cell["market_key"]
        family = cell["strategy_family"]
        cell_id = "%s::%s" % (market_key, family)
        spec = R36_MARKET_TO_DELIVERED.get(market_key)
        if spec is None:
            judged = {"status": C.CELL_STILL_BLOCKED_ENTITLEMENT,
                      "why": "this entitlement does not touch the market; "
                             "the Release-36 blocker stands"}
            delivered = []
        else:
            delivered = _delivered_for(spec, registry)
            delivered_rows = [dict(registry["markets"][m], market=m)
                              for m in delivered]
            judged = _judge_cell(market_key, family, delivered_rows,
                                 quality_states)
        if market_key in expected_full:
            expected = "FULL" if family in expected_full[market_key]["families"] \
                else "NOT_CLAIMED"
        elif market_key in expected_partial:
            expected = ("PARTIAL"
                        if family in expected_partial[market_key]["families"]
                        else "NOT_CLAIMED")
        else:
            expected = "NOT_CLAIMED"
        cells.append({
            "cell_id": cell_id,
            "market_key": market_key,
            "asset_class": cell["asset_class"],
            "strategy_family": family,
            "r36_state": cell["state"],
            "r36_blocker_reason": cell.get("blocker_reason"),
            "r37_expected": expected,
            "delivered_markets": delivered,
            "r38_status": judged["status"],
            "why": judged["why"],
            "survivorship_note": (
                "the delivered market universe contains only currently "
                "active markets; terminated markets are absent, so a "
                "cross-market universe is current-composition even though "
                "each market's own contract series is complete"
                if family in XS_FAMILIES else None),
        })

    verified = [c for c in cells if c["r38_status"] == C.CELL_NATIVE_VERIFIED]
    partial = [c for c in cells
               if c["r38_status"] == C.CELL_PARTIALLY_UNLOCKED]
    expected_full_cells = [c for c in cells if c["r37_expected"] == "FULL"]
    agreements = [c for c in expected_full_cells
                  if c["r38_status"] == C.CELL_NATIVE_VERIFIED]
    downgrades = [c for c in expected_full_cells
                  if c["r38_status"] != C.CELL_NATIVE_VERIFIED]
    upgrades = [c for c in verified if c["r37_expected"] != "FULL"]

    by_status: dict = {}
    for c in cells:
        by_status[c["r38_status"]] = by_status.get(c["r38_status"], 0) + 1

    payload = {
        "campaign_id": campaign_id,
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "frontier_source": "R36_FROZEN_COVERAGE_MATRIX",
        "r36_campaign_id": _r36_contract.CAMPAIGN_ID,
        "r37_expectation_source": _r37_unlock.SCHEMA,
        "expected_full_unlocks_r37": norgate_row["cells_unlocked_full"],
        "expected_partial_unlocks_r37": norgate_row["cells_unlocked_partial"],
        "blocked_cells_judged": len(cells),
        "r38_actual_native_verified": len(verified),
        "r38_actual_partially_unlocked": len(partial),
        "by_status": dict(sorted(by_status.items())),
        "expected_full_confirmed": len(agreements),
        "expected_full_downgraded": [
            {"cell_id": c["cell_id"], "r38_status": c["r38_status"],
             "why": c["why"]} for c in downgrades],
        "unlocked_beyond_expectation": [
            {"cell_id": c["cell_id"], "r37_expected": c["r37_expected"],
             "why": c["why"]} for c in upgrades],
        "cells": cells,
        "truth_wins_over_expectation": C.TRUTH_WINS_OVER_EXPECTATION,
    }
    return r38.artifact_body(SCHEMA, payload)


def coverage_overlay(actual: dict, *, created_at: Optional[str] = None) -> dict:
    """The R38 OVERLAY on the frozen R36 matrix - not a second authority."""
    created = created_at or _dt.datetime.now(_dt.timezone.utc).isoformat()
    matrix = _r36_coverage.load(_r36_contract.CAMPAIGN_ID)
    summary = dict(matrix["summary"])
    changed = {c["cell_id"]: c for c in actual["cells"]}
    payload = {
        "campaign_id": actual["campaign_id"],
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "coverage_authority": "alpha_agent.r36.coverage (frozen matrix, "
                              "never recomputed here)",
        "r36_summary_unchanged": summary,
        "r38_overlay": {
            "cells_rejudged": len(changed),
            "native_verified_researchable":
                actual["r38_actual_native_verified"],
            "partially_unlocked": actual["r38_actual_partially_unlocked"],
            "by_status": actual["by_status"],
            "cells": {cid: {"r38_status": c["r38_status"], "why": c["why"]}
                      for cid, c in sorted(changed.items())},
        },
    }
    return r38.artifact_body(COVERAGE_SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r38.campaign_dir(campaign_id) / ARTIFACT_NAME


def coverage_path(campaign_id: str = C.CAMPAIGN_ID):
    return r38.campaign_dir(campaign_id) / COVERAGE_ARTIFACT


def freeze(body: dict, overlay: dict) -> None:
    path = path_for(body["campaign_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    r38.write_json(path, body)
    r38.write_json(coverage_path(overlay["campaign_id"]), overlay)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    path = path_for(campaign_id)
    if not path.exists():
        return None
    return r38.read_json(path)
