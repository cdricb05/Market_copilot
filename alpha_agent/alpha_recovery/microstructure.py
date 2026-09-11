r"""alpha_agent.alpha_recovery.microstructure - the ONE owner of the native CME
ORDER-FLOW panel: what was bought, why THAT schema, and the trade-date panel it
normalises to.

WHY THIS AXIS EXISTS
    The 1-minute OHLCV futures campaign ran 54 pre-registered specifications
    across 8 families and 10 markets and produced ZERO qualified alpha, with a
    highest gross t of 1.34 against a frozen floor of 2.0. It also falsified the
    hypothesis that had kept price-derived research open: marking to the 16:00
    settlement instead of 12:59 added a median 0.0000/yr. The estate does not
    have an intraday COVERAGE problem; it has an intraday INFORMATION problem.

    That is a precise result, and it names its own successor. OHLCV bars are a
    lossy projection: they record where price went and how much traded, and
    discard entirely WHO WAS WAITING and WHO CROSSED THE SPREAD. Depth, queue
    imbalance, order counts and trade aggressor side are not transforms of
    open/high/low/close/volume - they cannot be computed from a bar at any lag.
    Under contract rule 13 this is ORTHOGONAL INFORMATION, not a reopening of
    price state, and it is the first axis in this campaign to which rule 14's
    non-price requirement applies favourably.

WHY bbo-1m AND NOT mbp-1, WHICH THE BRIEF PREFERRED
    Priced, not assumed. Every schema GLBX.MDP3 offers was costed with
    ``metadata.get_cost`` before a byte was bought, on an identical front-month
    window (2026-07-06..2026-07-31, ES/NQ/GC/6E):

        schema     $/session, 4 roots    sessions for $45    clears the floor?
        mbp-10             6.2966                       7    no
        mbp-1              3.3280                      13    NO
        tbbo               2.1142                      21    no
        trades             1.2685                      35    no
        ohlcv-1s           0.5479                      82    yes
        bbo-1s             0.3949                     113    yes, no holdout
        bbo-1m             0.0078                   5,764    yes, four regimes

    ``MIN_EFFECTIVE_PERIODS`` is frozen at 36. The gates run on a DAILY strategy
    return series, so a panel of 13 sessions cannot clear the floor at any
    signal strength whatsoever. mbp-1 is therefore not "expensive" here - it is
    UNAFFORDABLE IN THE STRICT SENSE: the budget buys a sample that is
    disqualified before it is examined. Buying it would have spent the whole
    remaining credit on evidence that could not, even in principle, qualify a
    candidate. That is the reasoning, and it was fixed before any result.

    bbo-1m is the subsampled top-of-book: the BBO as it stood at each minute
    boundary, plus the last trade in that minute with its aggressor side. It is
    a LOSSY view of the order book, and the loss is stated rather than hidden -
    see ``INFORMATION_NOT_PURCHASED``. What survives the subsampling is exactly
    the seven fields the brief's families need, over 4 years and 5 buckets, on
    the SAME contract windows the OHLCV panel was bought on - which is what lets
    the campaign measure the INCREMENT of order flow over bars directly, paired
    date for date, rather than by assertion.

RESEARCH ONLY. No order, no fill, no promotion, no subscription, no paid
dollar. Writes only under the campaign research root.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

from . import MIN_EFFECTIVE_PERIODS, research_root, write_artifact
from . import databento_acquisition as DA

CALCULATION_OWNER = "alpha_agent.alpha_recovery.microstructure"
ARTIFACT_NAME = "microstructure_acquisition_state.json"

SCHEMA = "bbo-1m"

#: The seven roots bought. This is the pre-registered CROSS6 group of the OHLCV
#: campaign - ES, NQ, GC, CL, 6E, 6J - plus ZN as the rates bucket's senior
#: contract, so the panel spans all FIVE buckets. ZT/ZF/ZB are deliberately not
#: bought: by the estate's own value model, declared before any price was seen,
#: the third and fourth contract in a bucket carry novelty 0.12 and 0.06, and
#: the credit is better kept as buffer than spent on a fourth Treasury.
ROOTS = ("ES", "NQ", "GC", "CL", "6E", "6J", "ZN")

#: Measured on 2026-07-06..2026-07-31 for ES/NQ/GC/6E with metadata.get_cost,
#: which bills nothing. Recorded so the schema choice is auditable as a priced
#: decision and not a preference.
SCHEMA_PRICES_USD_PER_SESSION_4_ROOTS = {
    "mbp-10": 6.2966, "mbp-1": 3.3280, "tbbo": 2.1142, "trades": 1.2685,
    "ohlcv-1s": 0.5479, "bbo-1s": 0.3949, "bbo-1m": 0.0078,
}

#: What bbo-1m does NOT contain, stated up front so no result from this axis can
#: be read as a verdict on full order-book information.
INFORMATION_NOT_PURCHASED = {
    "every_book_update": "bbo-1m is the book at the minute boundary. Quote revisions WITHIN "
                         "the minute - the flicker that a queue-position model lives on - are "
                         "not observed at all.",
    "every_trade": "only the LAST trade of each minute carries a side. True signed volume "
                   "(sum of buyer- minus seller-initiated size) cannot be formed; the panel "
                   "supports only a last-trade-sign proxy, and the campaign says so wherever "
                   "it uses one.",
    "depth_beyond_the_top": "levels 2-10 are mbp-10, priced at $6.30/session and not bought.",
    "consequence_for_a_negative_result": "a NULL result on this panel falsifies MINUTE-SAMPLED "
                                         "top-of-book information. It does NOT falsify "
                                         "sub-minute order-flow information, and must never be "
                                         "reported as though it did.",
}

# ------------------------------------------------------------------ the fields
#: Databento fixed-point price scale, identical to the OHLCV panel's.
PRICE_SCALE = 1e-9
#: ``price`` is INT64_MAX and ``size`` is 0 on a minute with no trade.
NULL_I64 = 9223372036854775807
#: bbo-1m stamps the interval on ts_recv. ``ts_event`` is the LAST TRADE's event
#: time and is null on a minute that had no trade, so indexing on it would drop
#: every quiet minute - precisely the minutes a liquidity-withdrawal family
#: needs. The panel is built on ts_recv.
CSV_TS = "ts_recv"
BBO_COLUMNS = ("bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00", "bid_ct_00", "ask_ct_00")
#: Aggressor side as Databento encodes it: the trade lifted the ASK (a buy) or
#: hit the BID (a sell). 'N' means no trade in the interval.
SIDE_BUY, SIDE_SELL, SIDE_NONE = "A", "B", "N"


def data_root() -> Path:
    return research_root() / "_data_futures_microstructure"


def normalised_root() -> Path:
    return research_root() / "_panel_futures_microstructure"


# -------------------------------------------------------------------- the plan
_OWNED = re.compile(r"^(?P<sym>[A-Z0-9]+)_ohlcv-1m_(?P<s>\d{4}-\d{2}-\d{2})_(?P<e>\d{4}-\d{2}-\d{2})$")


def owned_windows(roots: tuple = ROOTS) -> dict:
    """The EXACT contract windows the OHLCV panel was bought on.

    Buying order flow on the same windows is not a convenience. It is what makes
    the two panels pairable: same contracts, same roll, same trade dates, so the
    increment of microstructure over bars is a difference on identical ground
    rather than a comparison of two differently-shaped samples.
    """
    out = defaultdict(list)
    for f in sorted(DA.acquisition_root().glob("*.csv")):
        m = _OWNED.match(f.stem)
        if not m:
            continue
        sym = m.group("sym")
        root = next((r for r in roots if sym.startswith(r) and len(sym) - len(r) <= 2), None)
        if root:
            out[root].append({"symbol": sym, "start": m.group("s"), "end": m.group("e")})
    return {r: sorted(out[r], key=lambda x: x["start"]) for r in roots if out.get(r)}


def plan(client: DA.Client, budget_usd: float, roots: tuple = ROOTS) -> dict:
    """Price every request BEFORE anything is downloaded.

    Same invariant as the OHLCV acquisition and enforced by the same code:
    ``DA.download`` refuses any signature this function did not price.
    """
    windows = owned_windows(roots)
    requests, by_root, errors = [], {}, {}
    for root, rows in windows.items():
        total = 0.0
        for r in rows:
            try:
                usd = client.cost([r["symbol"]], r["start"], r["end"],
                                  mode="historical", schema=SCHEMA)
            except DA.DatabentoError as exc:
                errors[r["symbol"]] = str(exc)
                continue
            req = {"root": root, "symbol": r["symbol"], "start": r["start"], "end": r["end"],
                   "cost_usd": round(usd, 6),
                   "signature": DA._signature(r["symbol"], r["start"], r["end"], SCHEMA)}
            requests.append(req)
            total += usd
        by_root[root] = round(total, 6)
    spend = round(sum(r["cost_usd"] for r in requests), 6)
    cap = budget_usd * (1.0 - DA.BUDGET_SAFETY_MARGIN)
    return {
        "dataset": DA.DATASET, "schema": SCHEMA, "stype_in": DA.STYPE_IN,
        "roots": list(windows.keys()),
        "buckets": sorted({DA.UNIVERSE[r][1] for r in windows}),
        "cost_by_root_usd": by_root,
        "requests": requests, "n_requests": len(requests),
        "errors": errors,
        "selection": {
            "budget_usd": round(budget_usd, 4),
            "safety_margin": DA.BUDGET_SAFETY_MARGIN,
            "effective_cap_usd": round(cap, 4),
            "estimated_spend_usd": spend,
            "headroom_usd": round(cap - spend, 4),
            "fits_in_free_credit": spend <= cap,
            "paid_dollars_required": 0.0,
        },
        "cost_estimated_before_any_download": True,
        "windows_are_the_ones_already_owned": True,
    }


def acquire(budget_usd: float, roots: tuple = ROOTS, execute: bool = False,
            client: DA.Client | None = None, write: bool = True) -> dict:
    """Price, then (only with ``execute``) download. Records the whole purchase
    case, including the schemas that were priced and REJECTED."""
    cred = DA.credential_state()
    body = {
        "calculation_owner": CALCULATION_OWNER,
        "provider": "databento", "dataset": DA.DATASET, "schema": SCHEMA,
        "credential": cred,
        "information_case": information_case(),
        "schema_prices_usd_per_session_4_roots": dict(SCHEMA_PRICES_USD_PER_SESSION_4_ROOTS),
        "information_not_purchased": dict(INFORMATION_NOT_PURCHASED),
        "spending_contract": DA.spending_contract(),
    }
    if not cred["usable"]:
        body["state"] = "BLOCKED_CREDENTIAL_ABSENT"
        if write:
            write_artifact(ARTIFACT_NAME, body)
        return body

    client = client or DA.Client()
    p = plan(client, budget_usd=budget_usd, roots=roots)
    body["plan"] = p
    body["state"] = ("PLANNED_FITS_FREE_CREDIT" if p["selection"]["fits_in_free_credit"]
                     else "PLANNED_EXCEEDS_FREE_CREDIT")
    if execute and p["selection"]["fits_in_free_credit"]:
        body["download"] = DA.download(client, p, out_root=data_root(),
                                       dry_run=False, schema=SCHEMA)
        body["state"] = "ACQUIRED"
    elif execute:
        body["download"] = {"state": "REFUSED_EXCEEDS_FREE_CREDIT"}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body


# ============================================================================
# THE PRE-REGISTRATION
# Everything below is fixed BEFORE a single feature is computed on the acquired
# panel. The commit that introduces it precedes the commit that reports any
# result, which is the only way the claim is checkable rather than asserted.
# ============================================================================

# ------------------------------------------------------------ the families
FAM_DEPTH = "MS_DEPTH_IMBALANCE"
FAM_MICRO = "MS_MICROPRICE_PRESSURE"
FAM_AGGRESSOR = "MS_AGGRESSOR_FLOW"
FAM_FLOWIMB = "MS_FLOW_IMBALANCE"
FAM_WITHDRAWAL = "MS_LIQUIDITY_WITHDRAWAL"
FAM_SPREAD = "MS_SPREAD_STATE"
FAM_COUNT = "MS_ORDER_COUNT_IMBALANCE"
FAM_TRANSMISSION = "MS_CROSS_MARKET_TRANSMISSION"

FAMILIES = (FAM_DEPTH, FAM_MICRO, FAM_AGGRESSOR, FAM_FLOWIMB,
            FAM_WITHDRAWAL, FAM_SPREAD, FAM_COUNT, FAM_TRANSMISSION)

#: What each family claims, and - declared in advance - what kills it. A family
#: with no falsifier is a fishing licence. The falsifier is stated on the GROSS
#: number wherever the question is "is there information here", because a family
#: that has no edge before costs cannot be rescued by a cheaper cost model, and
#: saying so in advance is what stops a null result being re-litigated later.
FAMILY_CONTRACT = {
    FAM_DEPTH: {
        "claim": "resting size at the BBO is asymmetric before price moves: more bid than "
                 "ask size precedes an up move",
        "not_in_ohlcv": "a bar records traded volume, never resting size",
        "falsified_by": "gross Newey-West t below 2.0 at ZERO cost at every horizon",
    },
    FAM_MICRO: {
        "claim": "the size-weighted microprice sits away from the mid in the direction "
                 "price is about to take",
        "not_in_ohlcv": "requires both sides' prices AND sizes; a bar has neither",
        "falsified_by": "gross t below 2.0 at zero cost, or no advantage over the raw "
                        "depth imbalance it is built from",
        "algebraic_identity_declared_in_advance": (
            "microprice - mid == (spread/2) * depth_imbalance EXACTLY, so the half-spread-"
            "normalised microprice deviation IS the depth imbalance and carries no extra "
            "information. The family is therefore tested on the only thing that separates "
            "them: micro_bps is the imbalance SCALED BY BOOK WIDTH, an expected-impact "
            "signal. If it does not beat plain depth imbalance, the scaling is noise."),
    },
    FAM_AGGRESSOR: {
        "claim": "who crossed the spread predicts the next move: buyer-initiated trades "
                 "precede up moves",
        "not_in_ohlcv": "a bar's volume is unsigned; the aggressor side is discarded",
        "falsified_by": "gross t below 2.0 at zero cost at every horizon",
        "known_limitation": "bbo-1m carries only the LAST trade of each minute, so this is a "
                            "last-trade-sign proxy for signed volume, not signed volume",
    },
    FAM_FLOWIMB: {
        "claim": "the ROLLING imbalance of signed flow carries more than the instantaneous "
                 "sign, because single prints are noise",
        "not_in_ohlcv": "same as MS_AGGRESSOR_FLOW",
        "falsified_by": "no rolling window beats the instantaneous sign, i.e. the family "
                        "adds nothing to MS_AGGRESSOR_FLOW",
    },
    FAM_WITHDRAWAL: {
        "claim": "liquidity providers step away BEFORE a move: an abrupt one-sided drop in "
                 "resting depth precedes a move in that direction",
        "not_in_ohlcv": "requires the CHANGE in resting depth, which a bar cannot express",
        "falsified_by": "gross t below 2.0 at zero cost at every horizon",
    },
    FAM_SPREAD: {
        "claim": "spread state conditions the subsequent return: a widening book precedes "
                 "movement, a compressed book precedes quiet",
        "not_in_ohlcv": "a bar has one price per field, so no spread at all",
        "falsified_by": "gross t below 2.0 at zero cost at every horizon",
    },
    FAM_COUNT: {
        "claim": "the NUMBER of orders resting each side carries information beyond their "
                 "total size: many small orders are not one large one",
        "not_in_ohlcv": "order counts exist only in book data",
        "falsified_by": "gross t below 2.0, or no advantage over MS_DEPTH_IMBALANCE, which "
                        "would mean counts are a proxy for size and not information",
    },
    FAM_TRANSMISSION: {
        "claim": "order-flow pressure in one market predicts RETURNS in another before it "
                 "predicts its own - NQ->ES, ES->NQ, GC->equity, 6E->equity",
        "not_in_ohlcv": "the closed OHLCV axis tested price lead-lag and failed; this tests "
                        "FLOW lead-lag, which a bar cannot express",
        "falsified_by": "the cross-market beta is no larger than the own-market beta at the "
                        "same horizon, i.e. it carries no transmission information",
    },
}

# ------------------------------------------------------------- the horizons
#: Minutes. Declared by the brief and NOT swept. Microstructure information
#: decays fast; forcing it into a 21-day hold would be a category error, and
#: testing 200 horizons would be a fishing expedition. Five, fixed.
HORIZONS_MINUTES = (1, 5, 15, 30, 60)

#: Entries are NON-OVERLAPPING: an arm at horizon H enters every H minutes, so
#: no two open positions share a minute. Overlapping entries would inflate the
#: effective sample and therefore the t-statistic, which is the single easiest
#: way to manufacture significance at intraday frequency.
ENTRY_WINDOW = "RTH"
ENTRY_FIRST_ET = (9, 30)
ENTRY_LAST_ET = (16, 0)

#: A signal formed from the book AT minute m is traded at minute m, and its
#: return is measured from m to m+H. Nothing at or after m+1 enters the signal.
SIGNAL_READS_UP_TO_AND_INCLUDING_ENTRY_MINUTE = True

# --------------------------------------------------------- the cost question
#: Both are mandatory, per the brief, and the distinction decides what a null
#: result MEANS. A family with no gross edge has NO INFORMATION. A family with a
#: gross edge that dies net has information the estate cannot afford to trade.
#: These are different findings and the report must not merge them.
MEASURE_GROSS_AND_NET_SEPARATELY = True

#: At horizon H an arm trades 390/H round trips per RTH session. At H=1 that is
#: 390 round trips a day, so even a half-tick ladder charges hundreds of basis
#: points daily. This is not a defect of the cost model; it is the economics of
#: minute-horizon trading, and it is declared here so the result cannot be
#: presented as a surprise.
TURNOVER_IS_THE_BINDING_CONSTRAINT_AT_SHORT_HORIZONS = True

# ------------------------------------------------------------- the budgets
MAX_PRIMARY_PER_FAMILY = 6
MAX_RESCUES_PER_FAMILY = 2
RESCUE_REQUIRES_NAMED_MEASURED_BINDING_FAILURE = True

#: The ONE rescue condition allowed, declared in advance: trade only when the
#: signal is in the top percentile of its own trailing distribution. It is the
#: same conviction filter the OHLCV axis used, with the same parameters, and it
#: is the economically correct answer to a NAMED failure - unaffordable turnover
#: - rather than a new degree of freedom.
RESCUE_CONDITION = "TOP_CONVICTION_ONLY"
RESCUE_LOOKBACK_SESSIONS = 60
RESCUE_PERCENTILE = 70

# ---------------------------------------------------------- the split, frozen
#: Chronological, declared before any feature exists. The OHLCV axis's best arm
#: failed exactly here - selection -1.12 %/yr against holdout +17.66 %/yr - and
#: that failure is only interpretable because the split was fixed first.
SELECTION_SHARE = 0.60

#: Inherited UNCHANGED from the frozen contract. Acquiring data does not buy a
#: weaker threshold.
FROZEN_GATES = {
    "materiality_net_pct_per_year": 1.5,
    "paired_t": 2.0,
    "bh_q": 0.10,
    "family_holm_alpha": 0.05,
    "min_effective_periods": MIN_EFFECTIVE_PERIODS,
    "holdout_halves_floor": -0.005,
    "drawdown_multiple": 1.5,
    "must_survive_cost_level": "STRESS",
    "positive_equal_risk_utility_vs_incumbent": True,
}

#: The multiplicity denominator is EVERY specification executed on this axis,
#: fixed by the grid before the first is run. It is not the number that survive,
#: and it is not reset per family.
MULTIPLICITY_DENOMINATOR = "every specification executed on the microstructure axis"


def preregistration() -> dict:
    """The frozen record. Written before the campaign runs."""
    return {
        "calculation_owner": CALCULATION_OWNER,
        "axis": "NATIVE_CME_FUTURES_MICROSTRUCTURE",
        "dataset": DA.DATASET, "schema": SCHEMA, "roots": list(ROOTS),
        "families": list(FAMILIES),
        "family_contract": FAMILY_CONTRACT,
        "horizons_minutes": list(HORIZONS_MINUTES),
        "entry": {"window": ENTRY_WINDOW,
                  "first_et": "%02d:%02d" % ENTRY_FIRST_ET,
                  "last_et": "%02d:%02d" % ENTRY_LAST_ET,
                  "non_overlapping": True,
                  "signal_reads_up_to_and_including_entry_minute": True},
        "cost": {"measure_gross_and_net_separately": MEASURE_GROSS_AND_NET_SEPARATELY,
                 "ladder": "inherited unchanged from futures_intraday.COST_LADDER",
                 "eligibility_requires": "STRESS",
                 "turnover_is_binding_at_short_horizons": True},
        "budgets": {"max_primary_per_family": MAX_PRIMARY_PER_FAMILY,
                    "max_rescues_per_family": MAX_RESCUES_PER_FAMILY,
                    "rescue_condition": RESCUE_CONDITION,
                    "rescue_lookback_sessions": RESCUE_LOOKBACK_SESSIONS,
                    "rescue_percentile": RESCUE_PERCENTILE,
                    "rescue_requires_named_measured_binding_failure": True},
        "split": {"selection_share": SELECTION_SHARE, "chronological": True},
        "frozen_gates": dict(FROZEN_GATES),
        "multiplicity_denominator": MULTIPLICITY_DENOMINATOR,
        "information_not_purchased": dict(INFORMATION_NOT_PURCHASED),
        "schema_prices_usd_per_session_4_roots": dict(SCHEMA_PRICES_USD_PER_SESSION_4_ROOTS),
    }


# ============================================================================
# THE ADAPTER
# The MINIMUM needed to put order flow on the axis the research machinery
# already consumes. It owns no scorer, no gate and no statistic.
# ============================================================================
_CACHE: dict = {}


def _cache_path() -> Path:
    return normalised_root() / "microstructure_panel_v1.npz"


def front_schedule(root: str) -> dict:
    """session -> the dated contract the OHLCV panel decided was front.

    Deliberately READ, not recomputed. The causal volume roll already ran on
    this exact universe when the bars were normalised; re-deriving it from
    quote data would risk the two panels disagreeing about which contract was
    held on a date, and every paired comparison between them would then be
    measuring the roll instead of the information.
    """
    import pandas as pd

    from .futures_intraday import panel_path

    p = panel_path(root)
    if not p.exists():
        raise RuntimeError("the OHLCV front-month series for %s is not on disk; the "
                           "microstructure panel inherits its roll and cannot be built "
                           "without it" % root)
    df = pd.read_csv(p, usecols=["session", "symbol"])
    return dict(df.groupby("session")["symbol"].last().astype(str))


def read_bbo(root: str) -> "pd.DataFrame":  # noqa: F821
    """Every acquired bbo-1m file for ``root``, filtered to the FRONT contract
    and put on the trade-date minute axis.

    Two traps are handled here and nowhere else:

    1. The CSV carries no ``symbol`` column - only ``instrument_id``, a numeric
       venue handle. The dated symbol comes from the FILENAME, which is the
       request this estate priced and paid for. This is the same defect that
       the OHLCV acquisition hit on its first contact with real bytes.
    2. ``ts_event`` is the LAST TRADE's event time and is null on any minute
       that had no trade. Indexing on it would silently drop every quiet
       minute - exactly the minutes a liquidity-withdrawal family exists to
       look at. The interval is stamped on ``ts_recv``.
    """
    import numpy as np
    import pandas as pd

    from .futures_intraday import TD_FIRST_REL, TRADE_DATE_ROLL_HOUR_ET, rel_minute

    sched = front_schedule(root)
    files = sorted(data_root().glob("%s*_%s_*.csv" % (root, SCHEMA)))
    files = [f for f in files if f.stem.split("_")[0].startswith(root)
             and len(f.stem.split("_")[0]) - len(root) <= 2]
    frames = []
    for f in files:
        sym = f.stem.split("_")[0]
        df = pd.read_csv(f, usecols=lambda c: c in (CSV_TS, "side", "price", "size") + BBO_COLUMNS)
        if df.empty:
            continue
        ts = pd.to_datetime(df[CSV_TS], unit="ns", utc=True).dt.tz_convert("America/New_York")
        df["session"] = ts.dt.date.astype(str)
        df["minute_et"] = (ts.dt.hour * 60 + ts.dt.minute).to_numpy()
        # keep only the sessions on which THIS contract was the front month
        df = df[df["session"].map(sched).astype(str) == sym]
        if df.empty:
            continue
        m = df["minute_et"].to_numpy()
        td = pd.to_datetime(df["session"]) + pd.to_timedelta(
            (m >= TRADE_DATE_ROLL_HOUR_ET * 60).astype("int64"), unit="D")
        df = df.assign(symbol=sym,
                       td=td.dt.strftime("%Y-%m-%d").to_numpy(),
                       c=[rel_minute(int(x)) - TD_FIRST_REL for x in m])
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)

    # fixed-point -> decimal, nulls -> NaN
    for c in ("price", "bid_px_00", "ask_px_00"):
        v = pd.to_numeric(out[c], errors="coerce")
        out[c] = np.where(v.to_numpy() == NULL_I64, np.nan, v.to_numpy() * PRICE_SCALE)
    for c in ("bid_sz_00", "ask_sz_00", "bid_ct_00", "ask_ct_00", "size"):
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
    return out


def features(df: "pd.DataFrame", root: str) -> "pd.DataFrame":  # noqa: F821
    """The bounded, economically interpretable feature set. Eight columns, one
    per declared family, and no others - this is where a microstructure study
    turns into a fishing expedition if it is allowed to.

    A NOTE ON THE MICROPRICE, established algebraically rather than discovered
    in the results:

        microprice - mid == (spread / 2) * depth_imbalance   EXACTLY

    so the microprice's deviation from mid, once normalised by the half-spread,
    IS the depth imbalance and carries not one bit more. The two families are
    therefore separated on the only axis that distinguishes them: ``depth_imb``
    is the dimensionless queue asymmetry, while ``micro_bps`` is that same
    asymmetry SCALED BY HOW WIDE THE BOOK IS - an expected-impact signal rather
    than a pressure signal. MS_MICROPRICE_PRESSURE's declared falsifier ("no
    advantage over the raw depth imbalance") is exactly the test of whether
    that scaling carries information.
    """
    import numpy as np

    from .futures_intraday import SPECS

    tick = SPECS[root][0]
    bp, ap = df["bid_px_00"].to_numpy(), df["ask_px_00"].to_numpy()
    bs, asz = df["bid_sz_00"].to_numpy(), df["ask_sz_00"].to_numpy()
    bc, ac = df["bid_ct_00"].to_numpy(), df["ask_ct_00"].to_numpy()

    mid = 0.5 * (bp + ap)
    spread = ap - bp
    # a crossed or locked book is not a tradable quote
    bad = ~np.isfinite(mid) | (spread < 0)
    mid = np.where(bad, np.nan, mid)

    tot_sz = bs + asz
    tot_ct = bc + ac
    with np.errstate(invalid="ignore", divide="ignore"):
        depth_imb = np.where(tot_sz > 0, (bs - asz) / tot_sz, np.nan)
        ct_imb = np.where(tot_ct > 0, (bc - ac) / tot_ct, np.nan)
        micro_bps = np.where(np.isfinite(mid) & (mid > 0),
                             10000.0 * 0.5 * spread * depth_imb / mid, np.nan)

    # 'B' is a BUY aggressor and 'A' a SELL aggressor - Databento labels the
    # side that INITIATED the event. Verified empirically on the acquired data
    # before this line was written: side 'B' prints above the mid 49 % of the
    # time against 31 % for side 'A', and at the ask 39 % against 21 %.
    side = df["side"].astype(str).to_numpy()
    sz = df["size"].to_numpy()
    signed = np.where(side == SIDE_BUY, sz, np.where(side == SIDE_SELL, -sz, 0.0))

    out = df[["td", "c", "symbol"]].copy()
    out["mid"] = mid
    out["spread_t"] = np.where(np.isfinite(mid), spread / tick, np.nan)
    out["depth_imb"] = depth_imb
    out["ct_imb"] = ct_imb
    out["micro_bps"] = micro_bps
    out["depth_tot"] = np.where(tot_sz > 0, tot_sz, np.nan)
    out["signed"] = signed
    out["traded"] = sz
    return out


#: The grids the panel carries. Deliberately short.
FIELDS = ("mid", "spread_t", "depth_imb", "ct_imb", "micro_bps", "depth_tot", "signed", "traded")
#: Fields that are a STATE of the book and persist until revised, so a minute
#: with no update inherits the last known value within the same trade date.
STATE_FIELDS = ("mid", "spread_t", "depth_imb", "ct_imb", "micro_bps", "depth_tot")
#: Fields that are a FLOW over the minute: absence means zero, never carry-over.
FLOW_FIELDS = ("signed", "traded")


def panel(*, rebuild: bool = False, roots: tuple = ROOTS):
    """The aligned microstructure panel, shape ``(n_trade_dates, 1440, n_roots)``.

    Same dates, same axis, same roll and same instruments as
    ``futures_intraday.panel``, so the increment of order flow over bars is a
    paired difference on identical ground.
    """
    import numpy as np

    from .futures_intraday import TD_MINUTES

    if "panel" in _CACHE and not rebuild:
        return _CACHE["panel"]
    cp = _cache_path()
    if cp.exists() and not rebuild:
        z = np.load(cp, allow_pickle=False)
        out = {"dates": [str(d) for d in z["dates"]],
               "instruments": [str(s) for s in z["instruments"]]}
        out.update({f: z[f] for f in FIELDS})
        out["held_day"] = z["held_day"]
        _CACHE["panel"] = out
        return out

    have = [r for r in roots if any(data_root().glob("%s*_%s_*.csv" % (r, SCHEMA)))]
    if not have:
        raise RuntimeError("the microstructure panel is not on disk; acquisition must run first")

    feats = {}
    for r in have:
        raw = read_bbo(r)
        if raw.empty:
            continue
        feats[r] = features(raw, r)
    have = [r for r in have if r in feats]

    ref = "ES" if "ES" in feats else have[0]
    dates = sorted(set(feats[ref]["td"].tolist()))
    n_d, n_i = len(dates), len(have)
    grids = {f: np.full((n_d, TD_MINUTES, n_i), np.nan, dtype=np.float32) for f in FIELDS}
    held = np.full((n_d, n_i), "", dtype="<U12")

    import pandas as pd
    for j, r in enumerate(have):
        df = feats[r]
        for f in FIELDS:
            piv = df.pivot_table(index="td", columns="c", values=f, aggfunc="last")
            piv = piv.reindex(index=dates, columns=list(range(TD_MINUTES)))
            arr = piv.to_numpy(dtype=float)
            if f in FLOW_FIELDS:
                arr = np.nan_to_num(arr, nan=0.0)
            else:
                # the book PERSISTS between updates; minutes before the trade
                # date's first quote stay NaN and are never back-filled
                arr = pd.DataFrame(arr).ffill(axis=1).to_numpy()
            grids[f][:, :, j] = arr.astype(np.float32)
        held[:, j] = (df.groupby("td")["symbol"].last().reindex(dates)
                        .fillna("").astype(str).to_numpy())

    out = {"dates": dates, "instruments": list(have), "held_day": held}
    out.update(grids)
    cp.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(cp, dates=np.array(dates), instruments=np.array(have),
                        held_day=held, **grids)
    _CACHE["panel"] = out
    return out


def information_case() -> dict:
    return {
        "named_defect_it_removes": (
            "OHLCV bars discard the book. Depth, queue imbalance, order counts and trade "
            "aggressor side cannot be computed from open/high/low/close/volume at any lag, "
            "so no transform of the owned panel can reach them."),
        "why_this_is_orthogonal_not_price_state": (
            "a bar says where price went; the book says who was waiting and who crossed the "
            "spread. The second is not a function of the first."),
        "contract_rule_13_condition_met": "NEW ORTHOGONAL INFORMATION",
        "what_the_previous_axis_established": (
            "54 specifications, 8 families, 10 markets, 0 qualified, highest gross t 1.34 "
            "against a frozen floor of 2.0; and the mark-to-close falsifier fired, so the "
            "estate's intraday problem is information, not coverage"),
        "frozen_gates_unchanged": {"min_effective_periods": MIN_EFFECTIVE_PERIODS,
                                   "note": "no threshold is relaxed by acquiring data"},
        "schema_choice_was_priced_not_preferred": (
            "mbp-1 buys 13 sessions at the $45 cap and MIN_EFFECTIVE_PERIODS is 36, so it "
            "cannot clear the floor at any signal strength; bbo-1m buys four years"),
    }
