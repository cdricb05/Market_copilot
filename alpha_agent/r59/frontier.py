"""alpha_agent.r59.frontier - the measured multi-asset research frontier.

Section F of the release brief: the queue must not collapse back to equities,
and "futures trend failed" must not be recorded as "futures exhausted". That
requires the frontier to be MEASURED from what is actually on disk, per asset
class, and to distinguish an exhausted ECONOMIC FAMILY from an exhausted
SCOPE.

What is measured, per class:

* the substrate - which owned panel supplies it, how many instruments, how many
  sessions, and the first/last date actually present in the file;
* the burden - how many hypotheses this class has already absorbed;
* the outcome mix - how many were prosecuted to a negative verdict;
* the economic families already prosecuted, so a scope is only EXHAUSTED when
  its READY families are gone, not when one of them failed.

Nothing here fabricates readiness: a panel that is missing produces BLOCKED
with the path that was looked for.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from .. import r59
from . import memory as M

CALCULATION_OWNER = "alpha_agent.r59.frontier"

# Families R59 knows how to execute, per class. Two properties decide how a
# family behaves on the frontier, and getting either wrong produces a false
# EXHAUSTED:
#
#   min_instruments   a cross-sectional family needs a cross-section; a
#                     time-series family does not. Requiring two markets for
#                     every family declared the single-market volatility scope
#                     BLOCKED even though a term-carry book on one market is
#                     perfectly executable.
#   generative        an ENUMERABLE family is one fixed economic idea and is
#                     spent once it has been prosecuted. A GENERATIVE family is
#                     a search space - the machine-representation grammar can
#                     emit unlimited distinct hypotheses inside it - so it is
#                     never spent by a single verdict; it is bounded by search
#                     budget and by multiple-testing burden instead. Treating
#                     R39's one negative verdict as closing the whole
#                     machine-representation space is precisely how a scope
#                     gets falsely retired.
_XS = "CROSS_SECTIONAL"
_TS = "TIME_SERIES"

# Families that require the R38 DATED-CONTRACT layer rather than the R57
# back-adjusted panel. Carry, curve relative value and term structure moved
# here after ``close_b`` was measured to be a second back-adjustment of the
# SAME front contract rather than the deferred one - a back-adjusted panel
# simply cannot express a term-structure hypothesis, so declaring one against
# it was declaring a family that could never be honestly tested.
NATIVE_FAMILIES = {
    "CALENDAR_TERM_STRUCTURE": {"group": None, "scopes": ("CROSS_ASSET",)},
    "RATES_CURVE_RV": {"group": "TREASURY_FUTURES",
                       "scopes": ("RATES_FUTURES",)},
    "INTER_COMMODITY_RV": {"group": "MULTI", "scopes": ("COMMODITY_FUTURES",)},
    "AGRICULTURAL_SEASONALITY": {"group": "MULTI",
                                 "scopes": ("COMMODITY_FUTURES",)},
    "ROLL_STATE": {"group": None, "scopes": ("CROSS_ASSET",)},
}

#: Economic groups an INTER_COMMODITY_RV / seasonality mandate may address.
COMMODITY_RV_GROUPS = ("GRAINS_AND_OILSEEDS", "SOFTS", "PRECIOUS_METALS",
                       "ENERGY", "LIVESTOCK")
AG_RV_GROUPS = ("GRAINS_AND_OILSEEDS", "SOFTS", "LIVESTOCK")

FAMILY_SPECS = {
    r59.AC_US_EQUITY: (
        ("CROSS_SECTIONAL_MOMENTUM", _XS, 20, False),
        ("SHORT_HORIZON_REVERSAL", _XS, 20, False),
        ("LOW_RISK_ANOMALY", _XS, 20, False),
        ("LIQUIDITY_PREMIUM", _XS, 20, False),
        ("IDIOSYNCRATIC_VOLATILITY", _XS, 20, False),
        ("52_WEEK_HIGH_PROXIMITY", _XS, 20, False),
        ("RESIDUAL_MOMENTUM", _XS, 20, False),
        ("SECTOR_RELATIVE_MOMENTUM", _XS, 20, False),
        ("MACHINE_REPRESENTATION:AUTO", _XS, 20, True),
        ("MACHINE_REPRESENTATION:SYMBOLIC", _XS, 20, True),
    ),
    r59.AC_EQUITY_INDEX: (
        ("TIME_SERIES_TREND", _TS, 1, False),
        ("CROSS_SECTIONAL_MOMENTUM", _XS, 4, False),
        ("CHANNEL_BREAKOUT", _TS, 1, False),
        ("RETURN_SEASONALITY", _TS, 1, False),
        ("VOLATILITY_SCALED_TREND", _TS, 1, False),
        ("MACHINE_REPRESENTATION:AUTO", _XS, 4, True),
        ("MACHINE_REPRESENTATION:SYMBOLIC", _XS, 4, True),
    ),
    r59.AC_RATES: (
        ("TIME_SERIES_TREND", _TS, 1, False),
        ("CROSS_SECTIONAL_MOMENTUM", _XS, 4, False),
        ("VOLATILITY_SCALED_TREND", _TS, 1, False),
        ("RATES_CURVE_RV", _XS, 4, False),
        ("MACHINE_REPRESENTATION:AUTO", _XS, 4, True),
        ("MACHINE_REPRESENTATION:SYMBOLIC", _XS, 4, True),
    ),
    r59.AC_COMMODITY: (
        ("TIME_SERIES_TREND", _TS, 1, False),
        ("CROSS_SECTIONAL_MOMENTUM", _XS, 4, False),
        ("RETURN_SEASONALITY", _TS, 1, False),
        ("INTER_COMMODITY_SPREAD", _XS, 4, False),
        ("VOLATILITY_SCALED_TREND", _TS, 1, False),
        ("INTER_COMMODITY_RV", _XS, 3, False),
        ("AGRICULTURAL_SEASONALITY", _XS, 3, False),
        ("MACHINE_REPRESENTATION:AUTO", _XS, 4, True),
        ("MACHINE_REPRESENTATION:SYMBOLIC", _XS, 4, True),
    ),
    r59.AC_FX: (
        ("TIME_SERIES_TREND", _TS, 1, False),
        ("CROSS_SECTIONAL_MOMENTUM", _XS, 4, False),
        ("VOLATILITY_SCALED_TREND", _TS, 1, False),
        ("MACHINE_REPRESENTATION:AUTO", _XS, 4, True),
        ("MACHINE_REPRESENTATION:SYMBOLIC", _XS, 4, True),
    ),
    r59.AC_VOLATILITY: (
        ("TIME_SERIES_TREND", _TS, 1, False),
        ("VOLATILITY_SCALED_TREND", _TS, 1, False),
    ),
    r59.AC_CREDIT: (
        ("CREDIT_SPREAD_MOMENTUM", _TS, 1, False),
        ("CREDIT_REGIME_TIMING", _TS, 1, False),
    ),
    r59.AC_CROSS_ASSET: (
        ("CROSS_ASSET_RELATIVE_VALUE", _XS, 8, False),
        ("CROSS_ASSET_LEAD_LAG", _XS, 8, False),
        ("CROSS_ASSET_REGIME_CONDITIONING", _XS, 8, False),
        ("CALENDAR_TERM_STRUCTURE", _XS, 8, False),
        ("ROLL_STATE", _XS, 8, False),
        ("MACHINE_REPRESENTATION:AUTO", _XS, 8, True),
        ("MACHINE_REPRESENTATION:SYMBOLIC", _XS, 8, True),
    ),
}

# Backwards-compatible view: the family names alone.
EXECUTABLE_FAMILIES = {ac: tuple(s[0] for s in specs)
                       for ac, specs in FAMILY_SPECS.items()}

GENERATIVE_FAMILIES = frozenset(
    s[0] for specs in FAMILY_SPECS.values() for s in specs if s[3])


#: A generative family is spent when its generator stops producing books the
#: estate has not already measured. Below this novelty yield, after at least
#: MIN_GENERATIVE_BATCHES batches in the scope, the space is EXHAUSTED - which
#: is what allows the autonomous loop to reach a real terminal state instead of
#: running until a clock stops it.
GENERATIVE_YIELD_FLOOR = 0.15
MIN_GENERATIVE_BATCHES = 8
MIN_GENERATIVE_CANDIDATES = 60


def generative_exhausted(mem, asset_class: str, family: str) -> dict:
    """Has this generative family stopped yielding new books in this scope?"""
    kind = "SYMBOLIC" if family.endswith("SYMBOLIC") else "AUTO"
    rows = {r["kind"]: r for r in mem.generator_yield(asset_class=asset_class)}
    r = rows.get(kind)
    if not r:
        return {"exhausted": False, "reason": "generator never run here"}
    enough = (int(r["batches"]) >= MIN_GENERATIVE_BATCHES
              and int(r["generated"]) >= MIN_GENERATIVE_CANDIDATES)
    spent = enough and float(r["novelty_yield"]) < GENERATIVE_YIELD_FLOOR
    return {
        "exhausted": bool(spent),
        "novelty_yield": r["novelty_yield"],
        "generated": r["generated"], "novel": r["novel"],
        "duplicates": r["duplicates"], "batches": r["batches"],
        "reason": ("generator produced %d candidates over %d batches and only "
                   "%.1f%% were books the estate had not already measured"
                   % (r["generated"], r["batches"],
                      100.0 * float(r["novelty_yield"]))) if spent
        else "still yielding new books",
    }


def family_specs(asset_class: str, *, instruments: int) -> list:
    """The families this scope can actually run given its instrument count."""
    out = []
    for name, mode, min_inst, generative in FAMILY_SPECS.get(asset_class, ()):
        if instruments >= min_inst:
            out.append({"family": name, "mode": mode,
                        "min_instruments": min_inst,
                        "generative": generative})
    return out


def _meta(path: Path) -> Optional[dict]:
    return r59.read_json(path)


def _futures_classes() -> dict:
    """Group the owned Norgate futures panel by R59 asset class.

    &VX is moved out of Norgate's "Stock Index" bucket by symbol: a volatility
    future prices variance, not an equity index, and letting it sit in the
    equity-index cross-section would contaminate that scope's cross-sectional
    ranking with an instrument whose sign convention is opposite.
    """
    meta = _meta(r59.FUTURES_PANEL_META)
    if not meta:
        return {}
    out: dict = {}
    for i, m in enumerate(meta.get("markets") or []):
        sym = str(m.get("symbol"))
        cls = (r59.AC_VOLATILITY if sym in r59.VOLATILITY_SYMBOLS
               else r59.NORGATE_CLASS_MAP.get(str(m.get("classification")),
                                              r59.AC_COMMODITY))
        out.setdefault(cls, []).append(
            {"index": i, "symbol": sym, "name": m.get("name"),
             "sessions": m.get("sessions"), "first": m.get("first"),
             "last": m.get("last"),
             "norgate_classification": m.get("classification")})
    return out


def measure_substrates() -> dict:
    """What research substrate is actually on disk right now."""
    subs: dict = {}

    eq = _meta(r59.EQUITY_PANEL_META)
    subs["EQUITY_PIT_PANEL"] = {
        "present": bool(eq) and r59.EQUITY_PANEL.exists(),
        "path": str(r59.EQUITY_PANEL),
        "instruments": (eq or {}).get("n_symbols"),
        "sessions": (eq or {}).get("n_dates"),
        "first": ((eq or {}).get("dates") or [None])[0],
        "last": ((eq or {}).get("dates") or [None])[-1],
        "survivorship_safe": True,
        "note": "Norgate S&P 500 Current & Past with PIT membership; "
                "total-return series",
    }

    fut = _meta(r59.FUTURES_PANEL_META)
    subs["FUTURES_PANEL"] = {
        "present": bool(fut) and r59.FUTURES_PANEL.exists(),
        "path": str(r59.FUTURES_PANEL),
        "instruments": (fut or {}).get("n_markets"),
        "sessions": (fut or {}).get("n_dates"),
        "first": (fut or {}).get("date_start"),
        "last": (fut or {}).get("date_end"),
        "survivorship_safe": True,
        "note": "Norgate continuous futures, front and second contract, "
                "with roll flags",
    }

    fnd = _meta(r59.FUNDAMENTAL_PANEL_META)
    subs["PIT_FUNDAMENTAL_PANEL"] = {
        "present": bool(fnd) and r59.FUNDAMENTAL_PANEL.exists(),
        "path": str(r59.FUNDAMENTAL_PANEL),
        "instruments": (fnd or {}).get("n_symbols"),
        "decisions": (fnd or {}).get("n_decisions"),
        "features": (fnd or {}).get("features"),
        "delisted": (fnd or {}).get("n_joined_delisted"),
        "survivorship_safe": True,
        "note": "SEC companyfacts joined to the Norgate PIT universe",
    }

    f4 = r59.FORM4_RAW_DIR
    rows = sorted(f4.glob("form4_rows_*.json")) if f4.exists() else []
    subs["FORM4_PARSED_ROWS"] = {
        "present": bool(rows),
        "path": str(f4),
        "day_files": len(rows),
        "note": "R46 daily Form-4 parse with acquired/disposed direction",
    }
    return subs


def measure(mem: Optional[M.ResearchMemory] = None) -> dict:
    """Measure the frontier and persist it into research memory."""
    mem = mem or M.open_memory()
    subs = measure_substrates()
    fut_classes = _futures_classes()
    burden = mem.burden()

    eq_ok = subs["EQUITY_PIT_PANEL"]["present"]
    fut_ok = subs["FUTURES_PANEL"]["present"]

    rows: dict = {}
    for ac in r59.ASSET_CLASSES:
        if ac == r59.AC_US_EQUITY:
            present, n_inst = eq_ok, subs["EQUITY_PIT_PANEL"]["instruments"]
            substrate = "EQUITY_PIT_PANEL"
            sessions = subs["EQUITY_PIT_PANEL"]["sessions"]
        elif ac == r59.AC_CROSS_ASSET:
            present = fut_ok and eq_ok
            n_inst = subs["FUTURES_PANEL"]["instruments"]
            substrate = "FUTURES_PANEL+EQUITY_PIT_PANEL"
            sessions = subs["FUTURES_PANEL"]["sessions"]
        elif ac == r59.AC_CREDIT:
            # No owned credit instrument panel: the R46 credit lane reads FRED
            # NFCI / BAML series, which is a macro conditioner rather than a
            # tradable credit cross-section. Saying so is the honest answer.
            present, n_inst, substrate, sessions = False, 0, "NONE", 0
        else:
            members = fut_classes.get(ac) or []
            present = fut_ok and len(members) >= 1
            n_inst = len(members)
            substrate = "FUTURES_PANEL"
            sessions = subs["FUTURES_PANEL"]["sessions"]

        specs = family_specs(ac, instruments=int(n_inst or 0))
        executable = [s["family"] for s in specs]
        prosecuted = {h["economic_family"] for h in
                      mem.list_hypotheses(asset_class=ac)
                      if h.get("outcome") in
                      (r59.HO_REJECTED, r59.HO_NO_ALPHA_EVIDENCE)}
        # A generative family is a search space, not one idea: a prior verdict
        # inside it never removes it from the remaining work. It IS removed
        # once its generator stops producing books the estate has not already
        # measured - measured exhaustion, not assumed.
        gen_state = {}
        remaining = []
        for s in specs:
            if not s["generative"]:
                if s["family"] not in prosecuted:
                    remaining.append(s["family"])
                continue
            g = generative_exhausted(mem, ac, s["family"])
            gen_state[s["family"]] = g
            if not g["exhausted"]:
                remaining.append(s["family"])
        n_hyp = len(mem.list_hypotheses(asset_class=ac))
        n_frozen = len(mem.list_hypotheses(asset_class=ac,
                                           outcome=r59.HO_FORWARD_FROZEN))
        declared = EXECUTABLE_FAMILIES.get(ac, ())

        if not present:
            state = r59.FS_BLOCKED
            reason = "no owned substrate: %s absent" % substrate
        elif not executable:
            state = r59.FS_BLOCKED
            reason = ("substrate present with %d instrument(s) - below the "
                      "minimum every declared family for this scope needs"
                      % n_inst)
        elif not remaining:
            state = r59.FS_EXHAUSTED
            reason = ("every executable family for this scope has been "
                      "prosecuted to a verdict (%d families)" % len(executable))
        elif n_hyp == 0:
            state = r59.FS_DATA_READY
            reason = "substrate present, nothing prosecuted in this scope yet"
        else:
            state = r59.FS_RESEARCH_READY
            reason = ("%d of %d executable families remain open"
                      % (len(remaining), len(executable)))

        detail = {
            "substrate": substrate,
            "substrate_present": present,
            "instruments": n_inst,
            "sessions": sessions,
            "declared_families": list(declared),
            "executable_families": executable,
            "family_specs": specs,
            "excluded_for_instrument_floor":
                [f for f in declared if f not in executable],
            "prosecuted_families": sorted(prosecuted),
            "remaining_families": remaining,
            "generative_families": [s["family"] for s in specs
                                    if s["generative"]],
            "generative_state": gen_state,
            "hypotheses_recorded": n_hyp,
            "forward_frozen": n_frozen,
            "search_burden": int((burden.get("by_asset_class") or {}).get(ac, 0)),
            "measured_by": CALCULATION_OWNER,
        }
        if ac in fut_classes:
            detail["markets"] = [m["symbol"] for m in fut_classes[ac]]
        if n_frozen and state in (r59.FS_RESEARCH_READY, r59.FS_DATA_READY):
            detail["also_forward_shadow"] = True

        mem.set_frontier(ac, state=state, reason=reason, detail=detail)
        rows[ac] = {"state": state, "reason": reason, "detail": detail}

    ready = [a for a, r in rows.items()
             if r["state"] in (r59.FS_DATA_READY, r59.FS_RESEARCH_READY,
                               r59.FS_ACTIVE_SEARCH)]
    non_eq_ready = [a for a in ready if a != r59.AC_US_EQUITY]
    mem.event("FRONTIER_MEASURED", subject="multi_asset",
              detail={"ready": ready, "non_equity_ready": non_eq_ready})
    return {
        "calculation_owner": CALCULATION_OWNER,
        "substrates": subs,
        "asset_classes": rows,
        "ready": ready,
        "non_equity_ready": non_eq_ready,
        "generated_at": r59.now_iso(),
    }


def futures_members(asset_class: str) -> list:
    """Row indices into the owned futures panel for one research scope.

    CROSS_ASSET is the WHOLE panel, not a bucket in the classification map:
    the point of the scope is a cross-section taken ACROSS the asset classes,
    so restricting it to a named Norgate classification would leave it with no
    members at all. That is exactly what happened in the first full session -
    the frontier reported CROSS_ASSET as READY with 103 instruments while the
    engine received an empty member list and blocked every cross-asset job
    with NO_MEMBERS.
    """
    if asset_class == r59.AC_CROSS_ASSET:
        meta = _meta(r59.FUTURES_PANEL_META) or {}
        return [{"index": i, "symbol": m.get("symbol"), "name": m.get("name"),
                 "sessions": m.get("sessions"), "first": m.get("first"),
                 "last": m.get("last"),
                 "norgate_classification": m.get("classification")}
                for i, m in enumerate(meta.get("markets") or [])]
    return (_futures_classes().get(asset_class) or [])
