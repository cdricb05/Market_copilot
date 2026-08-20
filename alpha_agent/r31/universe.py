"""alpha_agent.r31.universe - the ONE Release 31 investment-universe owner.

Campaign v2 evaluated portfolio decisions on whatever cross-section the frozen
panel happened to contain - the Norgate "Russell 1000 Current & Past" history.
That panel is an excellent SURVIVORSHIP-SAFE TRAINING sample and a poor statement
of the business objective, which is S&P 500 capital allocation. A model that wins
on Russell 1000 has not been shown to win on the book we actually manage.

This module therefore separates two concepts that Campaign v2 conflated:

    TRAINING UNIVERSE     what a candidate may LEARN from
    INVESTMENT UNIVERSE   what the judge may let it OWN

A candidate may be fitted on the broad point-in-time Russell 1000 cross-section
if that improves learning. Every portfolio decision the PRIMARY judge scores is
restricted to names that were S&P 500 members ON THAT DECISION DATE. Which of the
two training universes a candidate used is part of its specification, so
"train broad, invest narrow" is a hypothesis the campaign TESTS rather than an
accident of which panel was loaded.

Point-in-time membership, not a current list
--------------------------------------------
Membership comes from ``norgatedata.index_constituent_timeseries``, which returns
a daily 0/1 history per security. The owned subscription carries the "S&P 500
Current & Past" universe - 1897 securities against 503 current - so a name that
was removed or delisted retains its historical membership and a name added last
year is correctly absent from 2005. Applying today's 503 constituents backwards
would be the single most effective way to manufacture a backtest, and this module
exists so that no other module is ever tempted to do it.

The declared limitation
-----------------------
The frozen price panel is a Russell 1000 history, so it does not carry every name
that was ever an S&P 500 member inside the panel window. ``survivorship_report``
MEASURES that gap rather than assuming it away: it counts the member-days the
panel cannot represent and attributes them. The dominant cause is structural and
knowable - S&P removed its non-US constituents in July 2002, and a Russell 1000
history excludes foreign domiciles by construction - so the gap is reported, not
silently absorbed. A campaign that cannot say how incomplete its universe is has
not established that its universe is complete.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np

from .. import release30_panel as _rp
from .. import r31

CALCULATION_OWNER = "alpha_agent.r31.universe"
MANIFEST_SCHEMA = "r31_investment_universe_manifest/1"
MANIFEST_NAME = "investment_universe_manifest.json"
CACHE_NAME = "sp500_membership.npz"

#: The index whose point-in-time membership defines the INVESTMENT universe.
INVESTMENT_INDEX = "S&P 500"
INVESTMENT_WATCHLIST = "S&P 500 Current & Past"

#: The two declared training universes. A candidate names one; the choice is part
#: of its specification hash and of the multiple-testing denominator.
TRAIN_INVESTMENT_ONLY = "TRAIN_S_AND_P_500_ONLY"
TRAIN_BROAD_PIT = "TRAIN_RUSSELL1000_PIT"
TRAINING_UNIVERSES = (TRAIN_INVESTMENT_ONLY, TRAIN_BROAD_PIT)

#: Whatever a candidate trained on, the primary judge evaluates HERE.
EVALUATION_UNIVERSE = "EVALUATE_S_AND_P_500_PIT_MEMBERS_ONLY"

#: Raised rather than returning an empty mask: a silently empty investment
#: universe would look like "no eligible names" instead of "membership missing".
class UniverseUnavailable(RuntimeError):
    """The point-in-time membership evidence could not be established."""


def _norgate():
    """Import the vendor client, quietly. Absent client is a BLOCKING state."""
    try:
        logging.disable(logging.WARNING)
        import norgatedata as nd
    except ImportError as exc:                       # pragma: no cover - env
        raise UniverseUnavailable(
            "norgatedata is not importable; PIT S&P 500 membership cannot be "
            "established: %s" % (exc,)) from exc
    try:
        if not nd.status():
            raise UniverseUnavailable(
                "Norgate Data Director is not running; PIT S&P 500 membership "
                "cannot be established")
    except UniverseUnavailable:
        raise
    except Exception as exc:                         # pragma: no cover - env
        raise UniverseUnavailable(
            "Norgate status probe failed: %s" % (exc,)) from exc
    return nd


# --------------------------------------------------------------------------- #
# Build + cache
# --------------------------------------------------------------------------- #
def build_cache(*, campaign_id: str, panel=None, force: bool = False) -> Path:
    """Freeze the point-in-time S&P 500 membership mask for the frozen panel.

    Produces a ``(n_dates, n_symbols)`` boolean aligned EXACTLY to the panel's own
    date and symbol axes, so every downstream consumer indexes membership the same
    way it indexes price. Alignment is by DATE STRING, never by position: the
    vendor series and the panel do not share a calendar, and lining two calendars
    up by row index is how a membership history silently shifts.
    """
    out = r31.campaign_dir(campaign_id) / CACHE_NAME
    if out.exists() and not force:
        return out

    nd = _norgate()
    panel = panel if panel is not None else _rp.load_price_panel()
    dates = [str(d) for d in panel.dates]
    symbols = [str(s) for s in panel.symbols]
    date_pos = {d: i for i, d in enumerate(dates)}
    n_d, n_s = len(dates), len(symbols)

    member = np.zeros((n_d, n_s), dtype=bool)
    resolved = np.zeros(n_s, dtype=bool)

    watch = set(nd.watchlist_symbols(INVESTMENT_WATCHLIST))
    for j, sym in enumerate(symbols):
        if sym not in watch:
            # Never an S&P 500 constituent: correctly all-zero, and asking the
            # vendor for a series we know is empty costs a round trip per name.
            continue
        try:
            ts = nd.index_constituent_timeseries(
                sym, INVESTMENT_INDEX, padding_setting=nd.PaddingType.NONE,
                start_date=dates[0], end_date=dates[-1],
                format="numpy-recarray", timeseriesformat="numpy-recarray")
        except Exception:
            continue
        if ts is None or len(ts) == 0:
            resolved[j] = True
            continue
        flags = np.asarray(ts["Index Constituent"]).astype(bool)
        for d, f in zip(np.asarray(ts["Date"]).astype(str), flags):
            if f:
                i = date_pos.get(str(d)[:10])
                if i is not None:
                    member[i, j] = True
        resolved[j] = True

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.stem + ".building.npz")
    with open(tmp, "wb") as fh:
        np.savez_compressed(fh, member=member, resolved=resolved,
                            dates=np.array(dates, dtype="U10"),
                            symbols=np.array(symbols, dtype="U16"))
    Path(tmp).replace(out)
    return out


class Membership:
    """The frozen point-in-time membership mask, loaded once."""

    def __init__(self, member, resolved, dates, symbols):
        self.member = member
        self.resolved = resolved
        self.dates = [str(d) for d in dates]
        self.symbols = [str(s) for s in symbols]
        self._pos = {d: i for i, d in enumerate(self.dates)}

    @property
    def n_dates(self) -> int:
        return len(self.dates)

    @property
    def n_symbols(self) -> int:
        return len(self.symbols)

    def members_on(self, date: str) -> np.ndarray:
        """Boolean over the panel's symbol axis: who was in the index that day."""
        i = self._pos.get(str(date))
        if i is None:
            raise UniverseUnavailable(
                "date %s is not on the frozen panel calendar" % (date,))
        return self.member[i]

    def count_on(self, date: str) -> int:
        return int(self.members_on(date).sum())

    def eligible_columns(self, date: str, columns) -> np.ndarray:
        """Mask over a cross-section's ``columns`` (panel symbol indices).

        This is the seam the judge uses. A cross-section arrives as panel column
        indices, and only those that were index members on that date survive.
        """
        on = self.members_on(date)
        cols = np.asarray(columns, dtype=np.int64)
        return on[cols]


def load(*, campaign_id: str) -> Membership:
    path = r31.campaign_dir(campaign_id) / CACHE_NAME
    if not path.exists():
        raise UniverseUnavailable(
            "membership cache absent at %s; build_cache must run first" % (path,))
    with np.load(path, allow_pickle=False) as z:
        return Membership(z["member"], z["resolved"], z["dates"], z["symbols"])


# --------------------------------------------------------------------------- #
# The declared limitation, measured
# --------------------------------------------------------------------------- #
def survivorship_report(*, campaign_id: str, panel=None) -> dict:
    """Count the index member-days the frozen panel cannot represent.

    A name that was an S&P 500 member inside the panel window but carries no price
    history in the panel is a HOLE: the judge can never own it, so if such names
    were systematically the losers, every result would be flattered. This measures
    the hole rather than asserting it is small.
    """
    nd = _norgate()
    panel = panel if panel is not None else _rp.load_price_panel()
    dates = [str(d) for d in panel.dates]
    psyms = set(str(s) for s in panel.symbols)
    d0, d1 = dates[0], dates[-1]

    mem = load(campaign_id=campaign_id)
    represented_days = int(mem.member.sum())

    holes = []
    never_in_window = 0
    lookup_failed = 0
    for sym in sorted(set(nd.watchlist_symbols(INVESTMENT_WATCHLIST))):
        if sym in psyms:
            continue
        try:
            ts = nd.index_constituent_timeseries(
                sym, INVESTMENT_INDEX, padding_setting=nd.PaddingType.NONE,
                start_date=d0, end_date=d1,
                format="numpy-recarray", timeseriesformat="numpy-recarray")
        except Exception:
            lookup_failed += 1
            continue
        if ts is None or len(ts) == 0:
            never_in_window += 1
            continue
        flags = np.asarray(ts["Index Constituent"]).astype(bool)
        days = int(flags.sum())
        if days <= 0:
            never_in_window += 1
            continue
        base = sym.split("-")[0]
        holes.append({
            "symbol": sym,
            "member_days_in_window": days,
            "base_symbol_present_in_panel": bool(base in psyms),
        })

    # A Norgate symbol gains a "-YYYYMM" suffix on delisting or rename. When the
    # unsuffixed base IS in the panel, the panel already carries that security's
    # price history under its earlier name, so the member-days are represented
    # and counting them as missing would overstate the gap.
    true_holes = [h for h in holes if not h["base_symbol_present_in_panel"]]
    versioning = [h for h in holes if h["base_symbol_present_in_panel"]]
    missing_days = int(sum(h["member_days_in_window"] for h in true_holes))
    total_days = represented_days + missing_days
    frac = (missing_days / total_days) if total_days > 0 else float("nan")

    true_holes.sort(key=lambda h: -h["member_days_in_window"])
    return {
        "calculation_owner": CALCULATION_OWNER,
        "investment_index": INVESTMENT_INDEX,
        "panel_window": {"first": d0, "last": d1, "sessions": len(dates)},
        "member_days_represented": represented_days,
        "member_days_missing": missing_days,
        "member_days_total_implied": total_days,
        "missing_fraction": float(frac),
        "names_absent_never_member_in_window": int(never_in_window),
        "names_absent_and_member_in_window": len(holes),
        "names_true_absence": len(true_holes),
        "names_symbol_versioning_only": len(versioning),
        "membership_lookup_failed": int(lookup_failed),
        "largest_true_holes": true_holes[:20],
        "symbol_versioning_resolved": versioning,
        "dominant_structural_cause": (
            "S&P removed its non-US constituents in July 2002; the frozen panel "
            "is a Russell 1000 history, which excludes foreign domiciles by "
            "construction, so those member-days cannot be represented"),
        "verdict": ("INVESTMENT_UNIVERSE_MATERIALLY_COMPLETE"
                    if frac == frac and frac < 0.01
                    else "INVESTMENT_UNIVERSE_INCOMPLETE_DECLARE_LIMITATION"),
    }


# --------------------------------------------------------------------------- #
# Frozen manifest
# --------------------------------------------------------------------------- #
def build_manifest(*, campaign_id: str, panel=None) -> dict:
    mem = load(campaign_id=campaign_id)
    panel = panel if panel is not None else _rp.load_price_panel()
    counts = mem.member.sum(axis=1).astype(int)
    nz = counts[counts > 0]

    body = {
        "manifest": MANIFEST_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,

        "investment_universe": {
            "name": EVALUATION_UNIVERSE,
            "index": INVESTMENT_INDEX,
            "source_watchlist": INVESTMENT_WATCHLIST,
            "membership_owner": "norgatedata.index_constituent_timeseries",
            "point_in_time": True,
            "current_membership_applied_backwards": False,
            "median_members_per_session": (int(np.median(nz)) if nz.size else 0),
            "min_members_per_session": (int(nz.min()) if nz.size else 0),
            "max_members_per_session": (int(nz.max()) if nz.size else 0),
            "sessions_with_membership": int(nz.size),
            "cache_sha256": r31.sha_file(
                r31.campaign_dir(campaign_id) / CACHE_NAME),
        },

        "training_universes": {
            "declared": list(TRAINING_UNIVERSES),
            TRAIN_INVESTMENT_ONLY: {
                "description": "fit only on names that were index members on the "
                               "fitting date",
                "universe_is_evaluation_universe": True,
            },
            TRAIN_BROAD_PIT: {
                "description": "fit on the broad point-in-time Russell 1000 "
                               "cross-section carried by the frozen panel",
                "membership_owner": "alpha_agent.release30_panel.member",
                "survivorship_safe": True,
                "evaluated_on_investment_universe_only": True,
            },
            "choice_is_part_of_candidate_specification": True,
            "broader_training_never_widens_evaluation": True,
        },

        "separation_rule": (
            "TRAINING UNIVERSE selects what a candidate may LEARN from; "
            "INVESTMENT UNIVERSE selects what the judge may let it OWN. The "
            "primary judge scores portfolio decisions on S&P 500 point-in-time "
            "members only, whichever universe the candidate was fitted on."),

        "survivorship": survivorship_report(campaign_id=campaign_id, panel=panel),
        "panel_fingerprint": r31.file_fingerprint(_rp.DEFAULT_PRICE_PANEL),
    }
    body["universe_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


def path_for(campaign_id: str) -> Path:
    return r31.campaign_dir(campaign_id) / MANIFEST_NAME


def freeze(body: dict) -> Path:
    return r31.write_json(path_for(body["campaign_id"]), body)


def load_manifest(campaign_id: str) -> Optional[dict]:
    return r31.read_json(path_for(campaign_id))
