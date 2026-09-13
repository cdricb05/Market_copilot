"""alpha_agent.alpha_recovery.insider_form4_alpha - the preregistered SEC Form 4
CLUSTERED OPEN-MARKET INSIDER BUYING experiment.

Executes ``research/preregistration/INSIDER_FORM4_PREREGISTRATION.md`` (frozen
BEFORE any return existed) and changes nothing in it.

    hypothesis   two or more distinct officers or directors committing their own
                 capital to the issuer's common stock in open-market purchases
                 within 30 days carries information about the issuer's return
                 AFTER the first legitimate entry, that the estate's owned
                 families - including its own insider block - do not carry
    direction    POSITIVE, pre-specified. A contradicted sign is reported and the
                 cell closes; it is never reversed
    PIT          the SEC ACCEPTANCE INSTANT (UTC -> Eastern); decision session
                 t = first session whose 16:00 close is strictly after it;
                 entry at close t+1 (canonical NEXT_CLOSE). Never the
                 transaction date
    scorer       alpha_agent.r63.sensitivity.run_cell - the ONE scorer
    burden       alpha_agent.r31.multiple_testing over the declared family, AND
                 over the family plus every insider test the estate already ran

REUSED, NOT REWRITTEN: the event study, the panel signal, the residualiser, the
book statistics and the missingness gate are the 13D/G and 8-K experiments'
frozen owners. What this module adds is only what this preregistration adds: the
owned insider block and (when it exists) short positioning as required controls,
the inherited multiplicity burden, the unobservable tail after the archive's
last quarter, and a pre-publication diagnostic.

RESEARCH ONLY. No purchase, no promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no backfill, no live write.
"""
from __future__ import annotations

import numpy as np

from alpha_agent.r31 import multiple_testing as MT
from alpha_agent.r63 import STANDALONE_T_FLOOR
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S

from . import (BH_Q, CONDITIONAL_T_FLOOR, MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS,
               now_iso, write_artifact)
from . import control_block_alpha as CBA
from . import control_block_events as CBE
from . import event_8k_alpha as E8
from . import event_8k_data as E8D
from . import incumbent as INC
from . import insider_form4_data as D
from . import insider_form4_events as EV

CALCULATION_OWNER = "alpha_agent.alpha_recovery.insider_form4_alpha"
ARTIFACT_NAME = "insider_form4.json"

FAMILY_ID = "INSIDER_FORM4_CLUSTERED_PURCHASE_EVENT_V1"
#: The ontology dimension this information belongs to ...
ONTOLOGY_DIMENSION = "INSIDER_BEHAVIOUR"
#: ... and the block key it is scored under. It may NOT reuse the ontology key:
#: that block already exists in the dataset as the owned insider control, and
#: overwriting it would delete the control this family must beat.
BLOCK = "SEC_FORM4_CLUSTER_EVENT"
INSIDER_CONTROL = "INSIDER_BEHAVIOUR"
SHORT_POSITIONING_CONTROL = "SHORT_POSITIONING"

PREREGISTRATION = "research/preregistration/INSIDER_FORM4_PREREGISTRATION.md"
PREREGISTRATION_COMMIT = "944deb6"

HORIZONS_FROZEN = (5, 21, 63)
COST_LADDER_BPS = CBA.COST_LADDER_BPS
DESK_EQUITY_COST_BPS = CBA.DESK_EQUITY_COST_BPS
MIN_DATE_COVERAGE = CBA.MIN_DATE_COVERAGE
MAX_UNCOVERED_SHARE = CBA.MAX_UNCOVERED_SHARE
PPY = CBA.PPY
#: The pre-publication diagnostic window: sessions t-21 .. t-1.
PRE_PUBLICATION_WINDOW = 21

V_QUALIFIED = CBA.V_QUALIFIED
V_NO_EDGE = CBA.V_NO_EDGE
V_NO_INCREMENTAL = CBA.V_NO_INCREMENTAL
V_DATA_HOLD = CBA.V_DATA_HOLD
V_NEED_MORE = CBA.V_NEED_MORE
VERDICTS = (V_QUALIFIED, V_NO_EDGE, V_NO_INCREMENTAL, V_DATA_HOLD, V_NEED_MORE)

_R27 = (r"D:\Stock_Prediction_app_data\alpha_exhaustion_campaign\runs"
        r"\release27_71f5fd62de7c9d0a\insider_transactions_results.json")
_R35 = (r"D:\Stock_Prediction_app_data\orthogonal_information_r35"
        r"\r35_orthogonal_information_v1\predictive_increment.json")
_R39 = (r"D:\Stock_Prediction_app_data\universal_alpha_r39"
        r"\r39_universal_alpha_continuation_v2\continuation_candidate_registry.json")
_R63 = (r"D:\Stock_Prediction_app_data\r63_information_sensitivity\results"
        r"\information_sensitivity_matrix.json")

#: EVERY insider-information test the estate has already run, read from the
#: stored artifacts before this family's preregistration. None succeeded in its
#: own preregistered direction. They enter the INHERITED burden's denominator at
#: p = 1: the burden never resets, and a prior failure can never lower the bar.
PRIOR_INSIDER_TESTS = (
    {"release": "R27", "id": "r27_insider_net_buy_shares", "result": "rank IC t -1.54, REJECTED", "source": _R27},
    {"release": "R27", "id": "r27_insider_buyer_ratio", "result": "rank IC t -0.68, REJECTED", "source": _R27},
    {"release": "R27", "id": "r27_insider_officer_net_buy", "result": "rank IC t -1.50, REJECTED", "source": _R27},
    {"release": "R27", "id": "r27_insider_net_buy_dollar", "result": "rank IC t -1.49, REJECTED", "source": _R27},
    {"release": "R27", "id": "r27_insider_sell_intensity", "result": "rank IC t -1.52, REJECTED", "source": _R27},
    {"release": "R27", "id": "r27_insider_cluster_buy",
     "result": "rank IC t -3.22, SIGN OPPOSITE TO PRE-SPECIFIED, REJECTED", "source": _R27},
    {"release": "R35", "id": "PREDICTIVE::INSIDER_TRANSACTION_INTENSITY::h5", "result": "increment t -0.20", "source": _R35},
    {"release": "R35", "id": "PREDICTIVE::INSIDER_TRANSACTION_INTENSITY::h20", "result": "increment t 0.05", "source": _R35},
    {"release": "R35", "id": "PREDICTIVE::INSIDER_TRANSACTION_INTENSITY::h60", "result": "increment t 0.82", "source": _R35},
    {"release": "R35", "id": "STANDALONE::INSIDER_TRANSACTION_INTENSITY::h20",
     "result": "sector-level standalone rank IC t 2.72, no increment", "source": _R35},
    {"release": "R39", "id": "c39_12c5b63c3464", "result": "SPY insider-breadth timing, after-cost t -1.18", "source": _R39},
    {"release": "R39", "id": "c39_68d75b71ba5c", "result": "EQ ridge + insider, after-cost t -0.47", "source": _R39},
    {"release": "R39", "id": "c39_ad5b60d6aa82", "result": "EQ lightgbm + insider, after-cost t 0.34", "source": _R39},
    {"release": "R63", "id": "US_EQUITY|XS|1|INSIDER_BEHAVIOUR", "result": "conditional t 0.33, NO_CONDITIONAL_VALUE", "source": _R63},
    {"release": "R63", "id": "US_EQUITY|XS|5|INSIDER_BEHAVIOUR", "result": "conditional t -0.06, NO_CONDITIONAL_VALUE", "source": _R63},
    {"release": "R63", "id": "US_EQUITY|XS|21|INSIDER_BEHAVIOUR", "result": "conditional t -2.13, NO_CONDITIONAL_VALUE", "source": _R63},
    {"release": "R63", "id": "US_EQUITY|XS|63|INSIDER_BEHAVIOUR", "result": "conditional t -2.22, NO_CONDITIONAL_VALUE", "source": _R63},
    {"release": "PHASE_11BC", "id": "finnhub_insider_mspr",
     "result": "weak and wrong-signed (292 tickers)", "source": "estate memory record; no artifact located"},
)


def declared_m() -> int:
    return len(EV.CELLS) * len(HORIZONS_FROZEN)


def inherited_m() -> int:
    return declared_m() + len(PRIOR_INSIDER_TESTS)


def control_dims(ds: dict) -> tuple:
    """The owned US_EQUITY baseline PLUS the owned insider block, PLUS short
    positioning whenever the dataset carries it. A clustered-buying event is
    mechanically correlated with the insider block's trailing buy counts, so
    information that block already carries cannot be credited to the event."""
    dims = CBA.baseline_dims(ds)
    for extra in (INSIDER_CONTROL, SHORT_POSITIONING_CONTROL):
        if extra in ds["blocks"] and extra not in dims:
            dims = dims + (extra,)
    return dims


# --------------------------------------------------------------------------- #
# ARM 1 - the event study (the frozen owners) + the pre-publication diagnostic
# --------------------------------------------------------------------------- #
def event_study(E: dict, elig: np.ndarray, events: list, *, h: int,
                vol63: np.ndarray, sign: int) -> dict:
    """The frozen 8-K event study, whose ``market_adj`` (entry at close t+1) is
    the QUALIFICATION input. Its announcement and forgone-session diagnostics
    are inherited unchanged."""
    return E8.event_study(E, elig, events, h=h, vol63=vol63, sign=sign)


def pre_publication_market_adj(E: dict, events: list, *,
                               window: int = PRE_PUBLICATION_WINDOW) -> dict:
    """DIAGNOSTIC, excluded from every gate: the market-adjusted return over
    sessions t-window .. t-1, i.e. strictly BEFORE the session that contains
    publication. A move here happened while the purchases were private, so it
    can never be tradable alpha; it is reported to tell "the market already
    moved" apart from "the information arrived with the filing"."""
    ret1 = CBA._session_returns(E)
    spy1 = E8._spy_session_returns(E)
    lg = np.log1p(np.clip(np.where(np.isfinite(ret1), ret1, 0.0), -0.999999, None))
    cum = np.concatenate([np.zeros((lg.shape[0], 1)), np.cumsum(lg, axis=1)], axis=1)
    ls = np.log1p(np.clip(np.where(np.isfinite(spy1), spy1, 0.0), -0.999999, None))
    cs = np.concatenate([[0.0], np.cumsum(ls)])
    vals, ts = [], []
    for e in events:
        i, t = int(e["row"]), int(e["t"])
        if t - window < 1:
            continue
        name = np.expm1(cum[i, t] - cum[i, t - window])
        mkt = np.expm1(cs[t] - cs[t - window])
        vals.append(float(name - mkt))
        ts.append(t)
    out = E8._clustered(vals, ts, pit.nw_lag(window, 1))
    out["window_sessions"] = [-int(window), -1]
    return out


# --------------------------------------------------------------------------- #
# ARM 2 - the cross-sectional panel, through the canonical scorer
# --------------------------------------------------------------------------- #
def unobservable_from(E: dict) -> int:
    """Index of the first session AFTER the owned archive's last day. From it
    on, "no event" is unobserved rather than observed."""
    end = D.observable_through()
    dates64 = np.asarray(E["dates"], dtype="datetime64[ns]")
    if not end:
        return 0
    return int(np.searchsorted(dates64, np.datetime64(end, "D").astype("datetime64[ns]"),
                               side="right"))


def signal_matrix(E: dict, elig: np.ndarray, events: list, dec: np.ndarray,
                  cadence: int, sign: int, end_ix: int) -> tuple:
    """The 8-K owner's signed event count on the canonical grid; 0 for an
    eligible name without an event, and NaN - never 0 - for a decision session
    whose window reaches past the archive's last observable day."""
    sig, per = E8.signal_matrix(E, elig, events, dec, cadence, sign)
    dropped = 0
    for t in np.asarray(dec, dtype=int):
        if t >= end_ix:
            sig[:, t] = np.nan
            dropped += 1
    return sig, per, dropped


def assemble(h: int, sig: np.ndarray) -> dict:
    base = E8._base_dataset(int(h))
    ds = dict(base)
    ds["blocks"] = dict(base["blocks"])
    ds["blocks"][BLOCK] = sig[:, :, None]
    return ds


def raw_and_orthogonal(ds: dict, sig: np.ndarray, *, h: int,
                       incumbent_score: np.ndarray) -> dict:
    """Per-period rank IC and the canonical long-short book of the signal, raw
    and residualised on THIS family's controls plus the incumbent, in the SAME
    cross-sections. The residualiser, the book and its statistics are the
    frozen 13D/G owners; only the control set differs from the 8-K owner, whose
    released code stays byte-identical."""
    dec = np.asarray(ds["dec"], dtype=int)
    dims = control_dims(ds)
    lag = pit.nw_lag(h, int(ds["cadence"]))
    rows = {"gid": [], "iid": [], "pred_raw": [], "pred_orth": [], "y": [], "vol": []}
    ic_raw, ic_orth, per = [], [], []
    for j, t in enumerate(dec):
        ok = ds["elig"][:, t] & np.isfinite(sig[:, t]) & np.isfinite(ds["y"][:, t])
        if ok.sum() < 50:
            continue
        ix = np.where(ok)[0]
        x, yv = sig[ix, t], ds["y"][ix, t]
        Z = np.column_stack([ds["blocks"][d][ix, t, :] for d in dims]
                            + [incumbent_score[ix, t][:, None]])
        xo = CBA._xs_residual(x, Z)
        a, b = S.spearman(x, yv), S.spearman(xo, yv)
        ic_raw.append(a)
        ic_orth.append(b)
        rows["gid"].append(np.full(ok.sum(), j))
        rows["iid"].append(ix)
        rows["pred_raw"].append(x)
        rows["pred_orth"].append(xo)
        rows["y"].append(yv)
        rows["vol"].append(ds["vol"][ix, t])
        per.append({"date": str(np.asarray(ds["dates"])[t])[:10], "n": int(ok.sum()),
                    "n_with_event": int((x != 0).sum()), "rank_ic_raw": a,
                    "rank_ic_orth": b})
    if not per:
        return {"state": "NO_PERIODS"}
    cat = {k: np.concatenate(v) for k, v in rows.items()}
    out = {"state": "OK", "n_periods": len(per), "nw_lag": lag, "controls": list(dims),
           "per_period": per[:400], "rank_ic_raw": CBA._stat(ic_raw, lag),
           "rank_ic_orthogonalised": CBA._stat(ic_orth, lag)}
    for label, pred in (("raw", cat["pred_raw"]), ("orthogonalised", cat["pred_orth"])):
        out["book_%s" % label] = {}
        for bps in tuple(COST_LADDER_BPS) + (DESK_EQUITY_COST_BPS,):
            bk = S._book_returns(pred, cat["y"], cat["gid"], cat["iid"], cat["vol"],
                                 np.full(len(cat["y"]), bps / 1e4), "XS",
                                 "XS_LONG_SHORT", 1.0)
            out["book_%s" % label]["%.1fbp" % bps] = CBA._book_stats(bk, h, lag)
    return out


def incremental_vs_incumbent(ds: dict, sig: np.ndarray, *, h: int,
                             incumbent_score: np.ndarray, top_n: int = 50) -> dict:
    """The incumbent's top-N book alone against the incumbent blended 50/50 (z
    units, the estate's convention) with the part of the signal the controls
    and the incumbent do NOT explain. Identical rows, dates, costs and
    construction. The blend weight is not a preregistered parameter, so this
    is descriptive and never qualifies a cell on its own."""
    dec = np.asarray(ds["dec"], dtype=int)
    dims = control_dims(ds)
    lag = pit.nw_lag(h, int(ds["cadence"]))
    acc = {k: [] for k in ("gid", "iid", "y", "vol", "inc", "blend")}
    for j, t in enumerate(dec):
        ok = ds["elig"][:, t] & np.isfinite(sig[:, t]) & np.isfinite(ds["y"][:, t]) \
            & np.isfinite(incumbent_score[:, t])
        if ok.sum() < 50:
            continue
        ix = np.where(ok)[0]
        Z = np.column_stack([ds["blocks"][d][ix, t, :] for d in dims]
                            + [incumbent_score[ix, t][:, None]])
        xo = CBA._xs_residual(sig[ix, t], Z)
        zi, zs = CBA._z(incumbent_score[ix, t]), CBA._z(xo)
        acc["gid"].append(np.full(len(ix), j))
        acc["iid"].append(ix)
        acc["y"].append(ds["y"][ix, t])
        acc["vol"].append(ds["vol"][ix, t])
        acc["inc"].append(zi)
        acc["blend"].append(np.where(np.isfinite(zs), 0.5 * zi + 0.5 * zs, zi))
    if not acc["gid"]:
        return {"state": "NO_PERIODS"}
    c = {k: np.concatenate(v) for k, v in acc.items()}
    out = {"state": "OK", "controls": list(dims), "top_n": top_n,
           "blend": "0.5 z(incumbent) + 0.5 z(orthogonalised signed Form 4 cluster signal)",
           "book": "EQ_LONG_ONLY_TOPN vs the equal-weight scored universe"}
    for bps in tuple(COST_LADDER_BPS) + (DESK_EQUITY_COST_BPS,):
        cost = np.full(len(c["y"]), bps / 1e4)
        a = S._book_returns(c["inc"], c["y"], c["gid"], c["iid"], c["vol"], cost,
                            "XS", "EQ_LONG_ONLY_TOPN", 1.0, top_n)
        b = S._book_returns(c["blend"], c["y"], c["gid"], c["iid"], c["vol"], cost,
                            "XS", "EQ_LONG_ONLY_TOPN", 1.0, top_n)
        n = min(len(a["net"]), len(b["net"]))
        d = b["net"][:n] - a["net"][:n]
        st = S.nw_tstat(d, lag=lag)
        out["%.1fbp" % bps] = {
            "incumbent": CBA._book_stats(a, h, lag),
            "incumbent_plus_form4_cluster_event": CBA._book_stats(b, h, lag),
            "incremental_ann_net": float(d.mean() * PPY / float(h)) if n else None,
            "t_incremental": st["t"],
            "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None,
            "periods": int(n)}
    return out


# --------------------------------------------------------------------------- #
# Verdict - only the preregistered gates, in the preregistered order
# --------------------------------------------------------------------------- #
def verdict_for(cell: dict, inc_cell: dict, desc: dict, ev: dict, coverage: dict,
                bias: dict, *, informative_eff: int | None, fdr_pass: bool | None,
                inherited_fdr_pass: bool | None, sign: int,
                short_positioning_controlled: bool) -> dict:
    """Section 10 of the preregistration, in order. Every threshold already
    exists in the estate; none is invented here."""
    if coverage.get("uncovered_share", 0.0) > MAX_UNCOVERED_SHARE:
        return {"verdict": V_DATA_HOLD, "gate": "COVERAGE",
                "why": "%.0f%% of decision sessions resolve under %.0f%% of the universe "
                       "(limit %.0f%%)" % (100 * coverage["uncovered_share"],
                                           100 * MIN_DATE_COVERAGE, 100 * MAX_UNCOVERED_SHARE)}
    if bias.get("biased"):
        return {"verdict": V_DATA_HOLD, "gate": "MISSINGNESS_BIAS",
                "why": "the names that cannot be assessed earn %.2f%%/yr differently from those "
                       "that can (floor %.1f%%)" % (100 * (bias.get("ann_diff") or 0.0),
                                                    100 * MATERIALITY_ANN_NET)}
    eff = cell.get("effective_periods")
    if cell.get("verdict") == S.V_DATA_HOLD or eff is None:
        return {"verdict": V_NEED_MORE, "gate": "SCORER_FLOOR",
                "why": "the canonical scorer returned DATA_HOLD (%s)" % cell.get("why")}
    used = int(min(eff, informative_eff)) if informative_eff is not None else int(eff)
    if used < MIN_EFFECTIVE_PERIODS:
        return {"verdict": V_NEED_MORE, "gate": "EFFECTIVE_PERIODS",
                "why": "%d effective independent periods that carry an event (scorer %d), "
                       "floor %d" % (used, eff, MIN_EFFECTIVE_PERIODS)}
    if ev.get("state") != "OK":
        return {"verdict": V_NEED_MORE, "gate": "EVENT_STUDY_FLOOR",
                "why": "event study state %s" % ev.get("state")}
    m = ev.get("market_adj") or {}
    mean, t_ev = m.get("mean_per_session"), m.get("t")
    if mean is None or float(sign) * float(mean) <= 0.0:
        return {"verdict": V_NO_EDGE, "gate": "FROZEN_SIGN",
                "why": "post-entry market-adjusted return %s has the sign opposite to the "
                       "frozen direction %+d; the sign is not reversed"
                       % (None if mean is None else round(float(mean), 5), sign)}
    if t_ev is None or abs(float(t_ev)) < STANDALONE_T_FLOOR:
        return {"verdict": V_NO_EDGE, "gate": "POST_PUBLICATION_T",
                "why": "post-entry market-adjusted |t| %s below the %.1f floor"
                       % (None if t_ev is None else round(float(t_ev), 2), STANDALONE_T_FLOOR)}
    c = cell.get("conditional") or {}
    t = c.get("t")
    if t is None or float(t) < CONDITIONAL_T_FLOOR:
        return {"verdict": V_NO_EDGE, "gate": "CONDITIONAL_T",
                "why": "conditional t %s below the %.1f floor"
                       % (None if t is None else round(float(t), 2), CONDITIONAL_T_FLOOR)}
    if fdr_pass is False:
        return {"verdict": V_NO_EDGE, "gate": "MULTIPLICITY",
                "why": "does not survive BH q=%.2f over the declared m=%d" % (BH_Q, declared_m())}
    if inherited_fdr_pass is False:
        return {"verdict": V_NO_EDGE, "gate": "INHERITED_MULTIPLICITY",
                "why": "does not survive BH q=%.2f over the family plus the %d insider tests the "
                       "estate already ran (m=%d)" % (BH_Q, len(PRIOR_INSIDER_TESTS), inherited_m())}
    ic_o = (desc.get("rank_ic_orthogonalised") or {}).get("t")
    red = (cell.get("redundancy") or {}).get("redundancy")
    inc_t = ((inc_cell or {}).get("conditional") or {}).get("t")
    if red == "REDUNDANT" or ic_o is None or float(ic_o) < CONDITIONAL_T_FLOOR \
            or inc_t is None or float(inc_t) < CONDITIONAL_T_FLOOR:
        return {"verdict": V_NO_INCREMENTAL, "gate": "INCREMENTAL",
                "why": "does not survive the controls and the incumbent in the frozen direction "
                       "(redundancy=%s, orthogonalised IC t=%s, vs-incumbent t=%s)"
                       % (red, None if ic_o is None else round(float(ic_o), 2),
                          None if inc_t is None else round(float(inc_t), 2))}
    e = cell.get("economics") or {}
    if (e.get("ann_net_increment") or 0.0) < MATERIALITY_ANN_NET:
        return {"verdict": V_NO_EDGE, "gate": "MATERIALITY",
                "why": "net annual increment %.4f below the %.3f floor"
                       % (e.get("ann_net_increment") or 0.0, MATERIALITY_ANN_NET)}
    if not short_positioning_controlled:
        return {"verdict": V_NEED_MORE, "gate": "REQUIRED_CONTROL_SHORT_POSITIONING",
                "why": "every other gate passed, but the canonical equity dataset carries no "
                       "short-positioning block, so the required short-positioning control "
                       "could not be applied"}
    return {"verdict": V_QUALIFIED, "gate": None, "why": "every preregistered gate passed"}


def _bh(pvals: list, extra_nulls: int = 0) -> tuple:
    """BH over ``pvals`` plus ``extra_nulls`` entries at p = 1. A missing
    p-value is also p = 1, so the denominator can never shrink."""
    ps = [1.0 if p is None or not np.isfinite(float(p)) else float(p) for p in pvals]
    res = MT.benjamini_hochberg(ps + [1.0] * int(extra_nulls), BH_Q)
    rej = set(int(i) for i in res["rejected"])
    return res, [i in rej for i in range(len(ps))]


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def run(*, verbose: bool = True, write: bool = True) -> dict:
    E, elig = INC.equity_substrate()
    built = EV.build_events(E, elig, verbose=verbose)
    events, cik2rows = built["events"], built["cik2rows"]
    blocks = INC.incumbent_blocks(E, elig)
    inc_score = blocks[INC.BLOCK_SCORE][:, :, 0]
    vol63 = E8._base_dataset(HORIZONS_FROZEN[0])["vol"]
    end_ix = unobservable_from(E)

    coverage_by_h, pvals, pkeys = {}, [], []
    cells, cells_inc, descs, evs, biases, informative, prepub, controls = ({} for _ in range(8))
    for h in HORIZONS_FROZEN:
        ds0 = E8._base_dataset(h)
        dec = np.asarray(ds0["dec"], dtype=int)
        cadence = int(ds0["cadence"])
        cov = CBE.coverage_report(E, elig, cik2rows, dec)
        cov.pop("per_session", None)
        coverage_by_h[h] = cov
        for cell_name in EV.CELLS:
            sign = EV.CELL_SPECS[cell_name]["sign"]
            key = (cell_name, h)
            evs[key] = event_study(E, elig, events[cell_name], h=h, vol63=vol63, sign=sign)
            prepub[key] = pre_publication_market_adj(E, events[cell_name])
            informative[key] = E8D.oos_sufficiency(E["dates"], [e["t"] for e in events[cell_name]],
                                                   horizons=(h,))[str(h)]
            sig, per, n_unobs = signal_matrix(E, elig, events[cell_name], dec, cadence, sign, end_ix)
            ds = assemble(h, sig)
            dims = control_dims(ds)
            controls[key] = list(dims)
            if verbose:
                print("%s h=%d cadence=%d controls=%d decisions=%d unobservable=%d events=%d "
                      "informative_eff=%d" % (cell_name, h, cadence, len(dims), len(dec), n_unobs,
                                              len(events[cell_name]),
                                              informative[key]["effective_informative_periods"]),
                      flush=True)
            cells[key] = S.run_cell(ds, dims, BLOCK, keep_predictions=False)
            ds_i = assemble(h, sig)
            ds_i["blocks"][INC.BLOCK_SCORE] = blocks[INC.BLOCK_SCORE]
            cells_inc[key] = S.run_cell(ds_i, (INC.BLOCK_SCORE,), BLOCK, keep_predictions=False)
            descs[key] = raw_and_orthogonal(ds, sig, h=h, incumbent_score=inc_score)
            descs[key]["incremental_vs_incumbent"] = incremental_vs_incumbent(
                ds, sig, h=h, incumbent_score=inc_score)
            descs[key]["signal_per_date"] = per[:200]
            descs[key]["decision_sessions_unobservable"] = n_unobs
            biases[key] = CBA.missingness_bias(ds, cik2rows, h=h)
            pvals.append((evs[key].get("market_adj") or {}).get("p_two_sided"))
            pkeys.append(key)

    # ONE declared family (cells x horizons), and the same p-values judged again
    # with every prior insider test in the denominator. Neither is ever reset.
    bh, fdr_list = _bh(pvals)
    bh_inh, inh_list = _bh(pvals, extra_nulls=len(PRIOR_INSIDER_TESTS))
    fdr = dict(zip(pkeys, fdr_list))
    fdr_inh = dict(zip(pkeys, inh_list))

    results = {}
    for key in pkeys:
        cell_name, h = key
        sign = EV.CELL_SPECS[cell_name]["sign"]
        kw = dict(informative_eff=informative[key]["effective_informative_periods"],
                  fdr_pass=fdr[key], inherited_fdr_pass=fdr_inh[key], sign=sign,
                  short_positioning_controlled=SHORT_POSITIONING_CONTROL in controls[key])
        v = verdict_for(cells[key], cells_inc[key], descs[key], evs[key],
                        coverage_by_h[h], biases[key], **kw)
        # DECLARED IN ADVANCE: the same frozen function with both DATA gates
        # satisfied, so the axis is answered on its merits and not only on a gate.
        v_merits = verdict_for(cells[key], cells_inc[key], descs[key], evs[key],
                               {"uncovered_share": 0.0}, {"biased": False}, **kw)
        m = (evs[key].get("market_adj") or {}).get("mean_per_session")
        results["%s|%d" % (cell_name, h)] = {
            "cell": cell_name, "horizon": h, "sign_frozen": sign,
            "verdict": v["verdict"], "gate": v["gate"], "why": v["why"],
            "verdict_if_data_gates_passed": v_merits,
            "sign_observed": None if m is None else ("POSITIVE" if m > 0 else "NEGATIVE"),
            "sign_agrees_with_frozen": None if m is None else bool(float(sign) * m > 0),
            "event_study": evs[key], "pre_publication_market_adj": prepub[key],
            "informative_periods": informative[key], "controls": controls[key],
            "scorer_cell": cells[key], "scorer_cell_vs_incumbent": cells_inc[key],
            "descriptive": descs[key], "missingness_bias": biases[key],
            "fdr_pass": fdr[key], "inherited_fdr_pass": fdr_inh[key]}

    body = {
        "schema": "alpha_recovery_insider_form4/1",
        "calculation_owner": CALCULATION_OWNER, "family_id": FAMILY_ID,
        "ontology_dimension": ONTOLOGY_DIMENSION, "block": BLOCK,
        "preregistration": PREREGISTRATION, "preregistration_commit": PREREGISTRATION_COMMIT,
        "preregistration_altered_after_results": False,
        "direction": "POSITIVE_PRESPECIFIED",
        "sign_policy": "a contradicted sign closes the cell; it is never reversed",
        "cells": {c: dict(s) for c, s in EV.CELL_SPECS.items()},
        "horizons": list(HORIZONS_FROZEN),
        "pit_boundary": "SEC acceptance instant (UTC -> Eastern), else 17:30 ET on the filing "
                        "date as an upper bound; decision session = first session whose 16:00 ET "
                        "close is strictly after it; entry at the NEXT close (canonical "
                        "NEXT_CLOSE). The transaction date is never the availability boundary",
        "event_window_start": EV.EVENT_WINDOW_START,
        "observable_through": D.observable_through(),
        "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
        "desk_single_name_cost_bps_per_side": DESK_EQUITY_COST_BPS,
        "scorer": S.CALCULATION_OWNER, "pit_engine": pit.CALCULATION_OWNER,
        "multiplicity_owner": MT.CALCULATION_OWNER,
        "multiplicity": {"family": FAMILY_ID, "m_declared": declared_m(),
                         "benjamini_hochberg": bh, "q": BH_Q, "denominator_reset": False,
                         "cells_added_after_results": False,
                         "m_inherited": inherited_m(), "benjamini_hochberg_inherited": bh_inh,
                         "prior_insider_tests": list(PRIOR_INSIDER_TESTS),
                         "prior_p_value_policy": "each prior test enters at p = 1"},
        "short_positioning_control_available": any(SHORT_POSITIONING_CONTROL in c
                                                   for c in controls.values()),
        "identity": built["identity"], "event_stats": built["stats"],
        "coverage": {str(h): coverage_by_h[h] for h in HORIZONS_FROZEN},
        "event_counts": {c: len(events[c]) for c in EV.CELLS},
        "results": results, "capital_eligible": False, "generated_at": now_iso()}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
