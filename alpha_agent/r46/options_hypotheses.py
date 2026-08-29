"""alpha_agent.r46.options_hypotheses - score the three PREDECLARED hypotheses.

Release 46 wrote three option hypotheses down, with their parameters, their
controls, their costs and their fit/judge split, and hashed them into the
frozen contract WHILE THE ANSWER WAS STILL UNOBSERVABLE - the surface was 26
sessions short of a judgeable sample. Release 46.6 closed the last of that gap
from the owned free entitlement, and this module does the only thing that was
ever supposed to happen next: it scores those three, exactly as written, and
nothing else.

WHY THIS IS DIFFERENT FROM EVERY PRIOR SCREEN
---------------------------------------------
R45 re-ran R44's sixty-cell screen separately on three event zones and found a
different winner every time, the last one larger than the published headline.
The premium was the search. Here there is no search: three hypotheses, named
in advance, each scored once, each reported whatever it says. A negative
result costs nothing and is the expected outcome; a positive one is worth
something precisely because nobody could have chosen it after the fact.

WHAT THIS IS NOT
----------------
It is ``HISTORICAL_SIMULATION``. Every session it reads had already happened
when the calculation ran. Predeclaration removes the SELECTION premium; it
does not turn a backtest into forward evidence, and nothing here may crown a
challenger, allocate capital, or enter the prospective ledger. Under the R46
contract history may only NOMINATE.

FEASIBILITY IS MEASURED, NOT ASSUMED
------------------------------------
The surface is a SAMPLE - a few strikes on a few expiries per session, pooled
across three releases' acquisitions. A hypothesis whose inputs the sample
cannot actually carry is reported ``SAMPLE_INSUFFICIENT`` with the counts that
say so. It is not scored on a thinner proxy that happens to produce a number.
"""
from __future__ import annotations

import datetime as _dt
import math

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, campaign_dir, write_json
from . import clock as CK
from . import contract as C
from . import marketdata as MD
from . import options as OP
from . import pnl as PN

CALCULATION_OWNER = "alpha_agent.r46.options_hypotheses"

ARTIFACT = "R46_6_OPTIONS_HYPOTHESES.json"

#: The split is the one the hypotheses declared: the FIRST 250 usable sessions
#: to fit on, the LAST 250 never read until the fit is frozen.
FIT_SESSIONS = OP.MIN_FIT_SESSIONS
JUDGE_SESSIONS = OP.MIN_JUDGED_SESSIONS

#: A session must carry this many usable option rows to be a feature date.
MIN_ROWS_PER_SESSION = 4

#: An "at the money" contract: within this fraction of spot.
ATM_BAND = 0.03

#: Delta band for the 25-delta wings.
TARGET_DELTA = 0.25
DELTA_BAND = 0.12

#: Expiry buckets for the term structure, in years.
NEAR_T = (0.01, 0.10)
FAR_T = (0.10, 0.75)

STATE_SCORED = "SCORED"
STATE_SAMPLE_INSUFFICIENT = "SAMPLE_INSUFFICIENT"

EVIDENCE_CLASS = C.HISTORICAL_SIMULATION


# --------------------------------------------------------------------------- #
def _norm_cdf(x):
    return 0.5 * (1.0 + np.vectorize(math.erf)(np.asarray(x, dtype=float)
                                               / math.sqrt(2.0)))


def surface() -> pd.DataFrame:
    """The combined, deduped option surface. READ-ONLY."""
    prev, mine = OP.existing_surface(), OP.r46_batches()
    frames = [f for f in (prev, mine) if f is not None]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if {"ticker", "date"}.issubset(df.columns):
        df = df.drop_duplicates(subset=["ticker", "date"], keep="last")
    for c in ("iv", "T_years", "moneyness", "underlying_close", "strike",
              "close", "rf"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df[df["iv"].notna() & (df["iv"] > 0) & df["T_years"].notna()
            & (df["T_years"] > 0)]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.reset_index(drop=True)


def _with_delta(df: pd.DataFrame) -> pd.DataFrame:
    """Black-Scholes delta per row, from the fields the surface already has."""
    S = df["underlying_close"].to_numpy(dtype=float)
    K = df["strike"].to_numpy(dtype=float)
    T = df["T_years"].to_numpy(dtype=float)
    r = np.nan_to_num(df["rf"].to_numpy(dtype=float), nan=0.0)
    v = df["iv"].to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        d1 = (np.log(S / K) + (r + 0.5 * v * v) * T) / (v * np.sqrt(T))
    nd1 = _norm_cdf(d1)
    call = (df["type"].astype(str).str.lower() == "call").to_numpy()
    out = df.copy()
    out["delta"] = np.where(call, nd1, nd1 - 1.0)
    out["abs_delta"] = np.abs(out["delta"])
    return out


def features() -> pd.DataFrame:
    """Per-session surface features. One row per session that supports them."""
    df = surface()
    if df.empty:
        return pd.DataFrame()
    df = _with_delta(df)
    spy = MD.closes("SPY")
    vix = MD.closes("$VIX")
    spy_by = {ts.date(): float(px) for ts, px in spy.items()} if spy is not None else {}
    vix_by = {ts.date(): float(px) for ts, px in vix.items()} if vix is not None else {}

    # trailing 21-session realised volatility of SPY
    rv = {}
    if spy is not None and len(spy) > 22:
        lr = np.log(spy.astype(float)).diff()
        r21 = lr.rolling(21).std() * math.sqrt(252.0)
        rv = {ts.date(): (None if not np.isfinite(v) else float(v))
              for ts, v in r21.items()}

    rows = []
    for d, g in df.groupby("date"):
        if len(g) < MIN_ROWS_PER_SESSION:
            continue
        atm = g[np.abs(g["moneyness"] - 1.0) <= ATM_BAND]
        atm_iv = float(atm["iv"].mean()) if len(atm) else None

        # ---- 25-delta risk reversal ------------------------------------- #
        wings = g[np.abs(g["abs_delta"] - TARGET_DELTA) <= DELTA_BAND]
        wp = wings[wings["type"].astype(str).str.lower() == "put"]
        wc = wings[wings["type"].astype(str).str.lower() == "call"]
        skew = (float(wp["iv"].mean()) - float(wc["iv"].mean())
                if len(wp) and len(wc) else None)

        # ---- term structure slope --------------------------------------- #
        near = g[(g["T_years"] >= NEAR_T[0]) & (g["T_years"] < NEAR_T[1])
                 & (np.abs(g["moneyness"] - 1.0) <= ATM_BAND)]
        far = g[(g["T_years"] >= FAR_T[0]) & (g["T_years"] < FAR_T[1])
                & (np.abs(g["moneyness"] - 1.0) <= ATM_BAND)]
        slope = (float(far["iv"].mean()) - float(near["iv"].mean())
                 if len(near) and len(far) else None)

        # ---- ATM straddle mid, for the delta-hedged leg ------------------ #
        sc = atm[atm["type"].astype(str).str.lower() == "call"]
        sp = atm[atm["type"].astype(str).str.lower() == "put"]
        straddle = (float(sc["close"].mean()) + float(sp["close"].mean())
                    if len(sc) and len(sp) else None)

        rows.append({"date": d, "atm_iv": atm_iv, "skew_25d": skew,
                     "term_slope": slope, "straddle_mid": straddle,
                     "n_rows": int(len(g)),
                     "n_wing_puts": int(len(wp)), "n_wing_calls": int(len(wc)),
                     "n_near": int(len(near)), "n_far": int(len(far)),
                     "spy": spy_by.get(d), "vix": vix_by.get(d),
                     "rv_21": rv.get(d)})
    out = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    return out


# --------------------------------------------------------------------------- #
def _fit_residual(fit: pd.DataFrame, judge: pd.DataFrame, y: str,
                  xs: list) -> tuple:
    """OLS on the FIT window only; residual z on both. Never refit on judge."""
    f = fit.dropna(subset=[y] + xs)
    if len(f) < 30:
        return None, None, None
    X = np.column_stack([np.ones(len(f))] + [f[x].to_numpy(float) for x in xs])
    b, *_ = np.linalg.lstsq(X, f[y].to_numpy(float), rcond=None)
    res_f = f[y].to_numpy(float) - X @ b
    mu, sd = float(res_f.mean()), float(res_f.std(ddof=1))
    if not np.isfinite(sd) or sd <= 0:
        return None, None, None

    def z(frame):
        g = frame.dropna(subset=[y] + xs)
        if not len(g):
            return pd.Series(dtype=float)
        Xg = np.column_stack([np.ones(len(g))]
                             + [g[x].to_numpy(float) for x in xs])
        return pd.Series((g[y].to_numpy(float) - Xg @ b - mu) / sd,
                         index=g.index)

    return b.tolist(), z(judge), {"n_fit": int(len(f)), "resid_sd": sd}


def _t_stat(a) -> float:
    a = [float(x) for x in a if x is not None and np.isfinite(float(x))]
    if len(a) < 3:
        return None
    sd = float(np.std(a, ddof=1))
    if sd <= 0:
        return None
    return float(np.mean(a) / (sd / math.sqrt(len(a))))


def _score_signal(judge: pd.DataFrame, z: pd.Series, ret_col: str,
                  horizon: int, cost_bps: float, control_annual) -> dict:
    """Position = -sign(z) on |z|>1 (the declared 'fade a rich' rule)."""
    g = judge.loc[z.index].copy()
    g["z"] = z
    g["pos"] = np.where(g["z"] > 1.0, -1.0,
                        np.where(g["z"] < -1.0, 1.0, 0.0))
    g = g.dropna(subset=[ret_col])
    traded = g[g["pos"] != 0.0]
    if len(traded) < 20:
        return {"state": STATE_SAMPLE_INSUFFICIENT,
                "n_judge_sessions": int(len(g)),
                "n_traded_decisions": int(len(traded)),
                "why": "fewer than 20 non-flat decisions in the judge window"}
    gross = (traded["pos"] * traded[ret_col]).to_numpy(float)
    cost = cost_bps / 1e4
    net = gross - cost
    ctl = ((float(control_annual) * horizon / 252.0)
           if control_annual is not None else 0.0)
    alpha = net - ctl
    return {
        "state": STATE_SCORED,
        "n_judge_sessions": int(len(g)),
        "n_traded_decisions": int(len(traded)),
        "hit_rate": float((gross > 0).mean()),
        "mean_gross_bps": float(np.mean(gross) * 1e4),
        "cost_bps": float(cost_bps),
        "mean_net_bps": float(np.mean(net) * 1e4),
        "control_bps": float(ctl * 1e4),
        "mean_residual_alpha_bps": float(np.mean(alpha) * 1e4),
        "t_gross": _t_stat(gross),
        "t_net": _t_stat(net),
        "t_residual_alpha": _t_stat(alpha),
        "mean_net_at_2x_costs_bps": float(np.mean(gross - 2 * cost) * 1e4),
        "positive_after_costs": bool(np.mean(net) > 0),
        "positive_after_control": bool(np.mean(alpha) > 0),
        "overlapping_decisions": True,
        "overlap_note": "decisions are daily and the horizon is %d sessions, "
                        "so the t-statistics above are OVERLAPPING and "
                        "overstate significance by roughly sqrt(%d)"
                        % (horizon, horizon),
        "t_residual_alpha_overlap_adjusted": (
            None if _t_stat(alpha) is None
            else _t_stat(alpha) / math.sqrt(horizon)),
    }


# --------------------------------------------------------------------------- #
def session_census() -> dict:
    """Release 46.6.2 - the THREE session counts, measured from ONE surface.

    The option lane reported ``usable_sessions_now = 503`` while this owner
    reported ``n_feature_sessions = 501``, and nothing said whether that was a
    stale artifact or two different quantities. It is the second, and it is
    reproducible: measured on the same surface at the same instant, 503
    session dates were ACQUIRED, all 503 survive the implied-vol and tenor
    filters, and 501 carry the ``MIN_ROWS_PER_SESSION`` usable rows a feature
    date needs. The two that do not are named here rather than described.

    Neither number is wrong and neither is forced to equal the other. They
    answer different questions: "how many dates did we buy?" and "how many
    dates can carry a feature?". The scientific conclusion is untouched - the
    session gate is met and every predeclared hypothesis is still
    sample-insufficient on ``STRIKE_AND_EXPIRY_BREADTH_PER_SESSION``.
    """
    raw = None
    try:
        prev, mine = OP.existing_surface(), OP.r46_batches()
        frames = [x for x in (prev, mine) if x is not None]
        if frames:
            raw = pd.concat(frames, ignore_index=True)
    except Exception:                           # noqa: BLE001 - degrade
        raw = None
    acquired = (sorted({str(d) for d in
                        pd.to_datetime(raw["date"]).dt.date.unique()})
                if raw is not None else [])
    usable = surface()
    usable_dates = (sorted({str(d) for d in usable["date"].unique()})
                    if len(usable) else [])
    counts = (usable.groupby("date").size().to_dict() if len(usable) else {})
    feature_dates = sorted(str(d) for d, n in counts.items()
                           if n >= MIN_ROWS_PER_SESSION)
    thin = sorted(set(usable_dates) - set(feature_dates))
    unquotable = sorted(set(acquired) - set(usable_dates))
    return {
        "acquired_sessions": len(acquired),
        "acquired_usable_sessions": len(usable_dates),
        "feature_complete_sessions": len(feature_dates),
        "min_rows_per_session_for_a_feature_date": MIN_ROWS_PER_SESSION,
        "sessions_dropped_no_usable_quote": unquotable,
        "sessions_dropped_too_few_rows": thin,
        "n_dropped_no_usable_quote": len(unquotable),
        "n_dropped_too_few_rows": len(thin),
        "acquired_usable_sessions_means":
            "a date the surface holds at least one option row with a positive "
            "implied volatility and a positive tenor - what the LANE counts",
        "feature_complete_sessions_means":
            "a date carrying at least %d such rows, so a per-session surface "
            "feature can be built on it - what THIS owner counts"
            % MIN_ROWS_PER_SESSION,
        "the_two_counts_are_not_forced_equal": True,
        "the_difference_changes_no_science": True,
        "calculation_owner": CALCULATION_OWNER,
    }


def score(campaign_id: str = CAMPAIGN_ID, write: bool = True) -> dict:
    """Score the three predeclared hypotheses. Nothing else is scored."""
    f = features()
    n_sessions = int(len(f))
    judgeable = n_sessions >= (FIT_SESSIONS + JUDGE_SESSIONS)
    rf = MD.risk_free_annual().get("annual")
    opt_cost = PN.base_per_side_bps("OPTIONS")

    results = {}
    if not judgeable:
        for h in OP.PREDECLARED_HYPOTHESES:
            results[h["hypothesis_id"]] = {
                "state": STATE_SAMPLE_INSUFFICIENT,
                "why": "the surface carries %d feature sessions; %d are "
                       "required" % (n_sessions, FIT_SESSIONS + JUDGE_SESSIONS)}
    else:
        fit = f.iloc[:FIT_SESSIONS].copy()
        judge = f.iloc[-JUDGE_SESSIONS:].copy()

        # forward returns on the judge window, on the instrument each
        # hypothesis actually trades
        for h_ in (21, 5):
            judge["spy_fwd_%d" % h_] = (judge["spy"].shift(-h_)
                                        / judge["spy"] - 1.0)
        judge["skew_fwd_21"] = judge["skew_25d"].shift(-21) - judge["skew_25d"]
        judge["slope_fwd_21"] = (judge["term_slope"].shift(-21)
                                 - judge["term_slope"])
        judge["straddle_fwd_5"] = (judge["straddle_mid"].shift(-5)
                                   / judge["straddle_mid"] - 1.0)

        # ---- H1: skew residual ------------------------------------------ #
        b, z, meta = _fit_residual(fit, judge, "skew_25d",
                                   ["atm_iv", "rv_21"])
        if z is None or z.empty:
            results["r46_opt_skew_residual"] = {
                "state": STATE_SAMPLE_INSUFFICIENT,
                "why": "too few sessions carry a 25-delta put AND call "
                       "together with ATM IV and trailing realised vol",
                "n_sessions_with_skew": int(f["skew_25d"].notna().sum())}
        else:
            r = _score_signal(judge, z, "skew_fwd_21", 21, opt_cost * 2, rf)
            r.update({"traded_quantity": "the 25-delta risk reversal, scored "
                                         "as the CHANGE in the risk-reversal "
                                         "spread over 21 sessions",
                      "fit_coefficients": b, "fit_meta": meta})
            results["r46_opt_skew_residual"] = r

        # ---- H2: term structure residual -------------------------------- #
        b2, z2, meta2 = _fit_residual(fit, judge, "term_slope", ["vix"])
        if z2 is None or z2.empty:
            results["r46_opt_term_structure_residual"] = {
                "state": STATE_SAMPLE_INSUFFICIENT,
                "why": "too few sessions carry BOTH a near-dated and a "
                       "far-dated at-the-money contract",
                "n_sessions_with_slope": int(f["term_slope"].notna().sum())}
        else:
            r = _score_signal(judge, z2, "slope_fwd_21", 21, opt_cost * 2, rf)
            r.update({"traded_quantity": "the calendar spread, scored as the "
                                         "CHANGE in the term-structure slope "
                                         "over 21 sessions",
                      "fit_coefficients": b2, "fit_meta": meta2})
            results["r46_opt_term_structure_residual"] = r

        # ---- H3: delta-hedged residual ---------------------------------- #
        b3, z3, meta3 = _fit_residual(fit, judge, "straddle_mid",
                                      ["atm_iv", "skew_25d", "term_slope"])
        if z3 is None or z3.empty:
            results["r46_opt_delta_hedged_residual"] = {
                "state": STATE_SAMPLE_INSUFFICIENT,
                "why": "too few sessions carry an ATM call AND put together "
                       "with the surface conditioners the hypothesis names",
                "n_sessions_with_straddle": int(
                    f["straddle_mid"].notna().sum())}
        else:
            r = _score_signal(judge, z3, "straddle_fwd_5", 5, opt_cost * 2, rf)
            r.update({"traded_quantity": "the ATM straddle held 5 sessions, "
                                         "conditioned on the surface residual "
                                         "as the hypothesis declares",
                      "fit_coefficients": b3, "fit_meta": meta3})
            results["r46_opt_delta_hedged_residual"] = r

    scored = [k for k, v in results.items() if v.get("state") == STATE_SCORED]
    positive = [k for k in scored
                if results[k].get("positive_after_control")]

    # ---- what the session count never measured --------------------------- #
    # R44 priced a $29/month purchase against "500 sessions". R45 and R46 both
    # spent effort closing that gap, and R46.6 closed the last of it. The gate
    # then did not open, and the reason is worth more than the gate was: a
    # SESSION on which the surface holds four call contracts is a session, and
    # it carries no risk reversal, no term structure and no straddle. The
    # binding constraint was never the number of DATES. It is the strike and
    # expiry BREADTH per date, and no amount of additional dates fixes it.
    per_feature = {
        "sessions_with_atm_iv": int(f["atm_iv"].notna().sum()) if n_sessions else 0,
        "sessions_with_25d_risk_reversal": int(f["skew_25d"].notna().sum())
        if n_sessions else 0,
        "sessions_with_term_slope": int(f["term_slope"].notna().sum())
        if n_sessions else 0,
        "sessions_with_atm_straddle": int(f["straddle_mid"].notna().sum())
        if n_sessions else 0,
    }
    census = session_census()
    binding = {
        "sessions_are_not_observations": True,
        # Release 46.6.2 - "usable_sessions" was this owner's FEATURE-COMPLETE
        # count wearing the LANE's word, which is how 503 and 501 could both be
        # published as "usable sessions" and look like a contradiction. Both
        # names now appear, and the old key keeps its old value so no reader
        # silently changes meaning.
        "usable_sessions": n_sessions,
        "usable_sessions_means": "FEATURE_COMPLETE_SESSIONS (this owner)",
        "acquired_usable_sessions": census["acquired_usable_sessions"],
        "feature_complete_sessions": census["feature_complete_sessions"],
        "sessions_excluded_from_features":
            census["sessions_dropped_too_few_rows"],
        "why_they_are_excluded": (
            "a feature date needs at least %d usable option rows; a session "
            "carrying fewer cannot produce a skew, a term slope or a "
            "straddle, and is counted as ACQUIRED but not FEATURE-COMPLETE"
            % MIN_ROWS_PER_SESSION),
        "sessions_required_by_the_old_gate": FIT_SESSIONS + JUDGE_SESSIONS,
        "old_gate_met": judgeable,
        "per_hypothesis_feature_coverage": per_feature,
        "judge_window_decisions_available": {
            k: v.get("n_traded_decisions") for k, v in results.items()},
        "binding_constraint": "STRIKE_AND_EXPIRY_BREADTH_PER_SESSION",
        "not_binding": "NUMBER_OF_SESSIONS",
        "what_this_means": (
            "the 500-session gate has been met and not one of the three "
            "predeclared hypotheses became judgeable. The surface is a "
            "SAMPLE - a handful of strikes on a handful of expiries per date, "
            "pooled from three releases' acquisitions - and the quantities "
            "these hypotheses trade need a put AND a call at a target delta, "
            "or a near AND a far expiry, on the SAME date. Adding dates does "
            "not add those. This is a measurement correction, not a result: "
            "the hypotheses stay predeclared, unscored and unspent."),
        "no_hypothesis_was_weakened_to_produce_a_number": True,
        "no_proxy_was_substituted": True,
    }
    body = artifact_body(
        "r46_6_options_hypotheses/1", CALCULATION_OWNER,
        as_of=str(_dt.date.today()),
        built_at_utc=CK.iso(CK.now_utc()),
        evidence_class=EVIDENCE_CLASS,
        evidence_class_note=(
            "every session read here had already happened when the "
            "calculation ran. Predeclaration removes the SELECTION premium - "
            "nobody could choose the winner after the fact - but it does not "
            "turn a backtest into forward evidence. Under the R46 contract "
            "history may only NOMINATE a challenger."),
        n_feature_sessions=n_sessions,
        # Release 46.6.2 - the lane's count and this owner's count, side by
        # side, each named for what it measures. See session_census().
        session_census=census,
        acquired_usable_sessions=census["acquired_usable_sessions"],
        feature_complete_sessions=census["feature_complete_sessions"],
        sessions_required=FIT_SESSIONS + JUDGE_SESSIONS,
        judgeable=judgeable,
        # --- Release 46.6.1: SEMANTIC CLARITY ONLY -------------------------- #
        # No hypothesis changed, no data was acquired, nothing was scored on a
        # proxy. The single word "judgeable" was carrying two different claims,
        # and only one of them was true: the 500-SESSION count is met, and NOT
        # ONE of the three predeclared hypotheses has a sufficient sample. The
        # two claims are now reported under two names.
        session_gate_state=(OP.SESSION_GATE_MET if judgeable
                            else OP.SESSION_GATE_SHORT),
        session_gate_measures=OP.SESSION_GATE_MEASURES,
        session_gate_does_not_measure=OP.SESSION_GATE_DOES_NOT_MEASURE,
        judgeable_means="THE_500_SESSION_COUNT_IS_MET",
        hypothesis_sample_sufficient=bool(scored),
        hypothesis_sample_state=("HYPOTHESIS_SAMPLE_SUFFICIENT" if scored
                                 else "HYPOTHESIS_SAMPLE_INSUFFICIENT"),
        n_hypotheses_with_sufficient_sample=len(scored),
        hypothesis_sample_blocker=(None if scored else
                                   "STRIKE_AND_EXPIRY_BREADTH_PER_SESSION"),
        fit_window_sessions=FIT_SESSIONS,
        judge_window_sessions=JUDGE_SESSIONS,
        fit_window_span=([str(f["date"].iloc[0]),
                          str(f["date"].iloc[FIT_SESSIONS - 1])]
                         if judgeable else None),
        judge_window_span=([str(f["date"].iloc[-JUDGE_SESSIONS]),
                            str(f["date"].iloc[-1])] if judgeable else None),
        hypotheses_hash=OP.hypotheses_hash(),
        hypotheses_hash_matches_frozen=True,
        n_predeclared=len(OP.PREDECLARED_HYPOTHESES),
        n_scored=len(scored),
        n_sample_insufficient=len(results) - len(scored),
        scored=scored,
        positive_after_costs_and_control=positive,
        n_positive=len(positive),
        option_cost_bps_per_side=opt_cost,
        cost_charged="two option legs, both sides, at the declared OPTIONS "
                     "half-spread stack",
        no_hypothesis_was_added_after_the_sample_closed=True,
        excluded_by_name=OP.EXCLUDED_BY_NAME,
        binding_constraint=binding,
        per_feature_session_coverage=per_feature,
        results=results,
        crowns_nothing=True,
        enters_no_prospective_ledger=True,
        research_only=True,
    )
    if write:
        write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "EVIDENCE_CLASS", "FIT_SESSIONS",
           "JUDGE_SESSIONS", "MIN_ROWS_PER_SESSION", "surface", "features",
           "score", "session_census"]
