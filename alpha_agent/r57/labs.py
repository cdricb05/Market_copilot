"""alpha_agent.r57.labs - the three pre-registered laboratories.

CALIBRATION   score -> expected 21-session excess return, fitted on
              DISCOVERY+VALIDATION, judged on the lockbox. Protocol target:
              the strongest VALIDATION-positive equity family (chosen before
              any lockbox result existed).
CONSTRUCTION  the four registered construction methods on one signal, same
              costs, same calendar. With no HISTORICAL_ALPHA_CANDIDATE this
              runs on the best REJECTED family and is labelled DIAGNOSTIC.
TURNOVER      rank-band hysteresis (K_in=50; K_out in {75,100,150}) judged on
              AFTER-COST lockbox net excess, never on turnover itself.
"""
from __future__ import annotations

import numpy as np

from . import write_artifact
from . import engine as E
from .families import EQUITY_FAMILIES

CAL_ARTIFACT = "calibration_results.json"
CONSTR_ARTIFACT = "construction_results.json"
TURN_ARTIFACT = "turnover_results.json"

SHRINKAGES = (0.25, 0.5, 0.75, 1.0)
N_DECILES = 10


# --------------------------------------------------------------------------- #
# Calibration
# --------------------------------------------------------------------------- #
def _decile_table(panel, fn, cadence, horizon, layers_wanted):
    """Per decision date in the wanted layers: per-decile mean forward excess
    (vs the eligible-universe mean) keyed by score decile."""
    dates = panel["dates"]
    idx = E.decision_indices(dates, cadence, "2006-01-01", horizon)
    lays = E.layer_of(dates, idx, cadence, horizon)
    rows = []
    for j, t in enumerate(idx):
        if lays[j] not in layers_wanted:
            continue
        elig = E.eligibility(panel, t)
        s = fn(panel, t)
        ok = elig & np.isfinite(s)
        if ok.sum() < 100:
            continue
        held = np.where(ok)[0]
        r = E.forward_return(panel, held, t, horizon)
        ex = r - r.mean()
        q = np.searchsorted(np.quantile(s[held], np.linspace(0, 1, N_DECILES + 1)[1:-1]),
                            s[held])
        rows.append(np.array([ex[q == d].mean() if (q == d).any() else np.nan
                              for d in range(N_DECILES)]))
    return np.array(rows)


def run_calibration(panel: dict, family_id: str, variant: str) -> dict:
    spec = EQUITY_FAMILIES[family_id]
    fn = spec["variants"][variant]
    cadence, horizon = spec["cadence"], spec["horizon"]

    fit = _decile_table(panel, fn, cadence, horizon, ("D", "V"))
    val = _decile_table(panel, fn, cadence, horizon, ("V",))
    lock = _decile_table(panel, fn, cadence, horizon, ("L",))

    fit_mean = np.nanmean(fit, axis=0)          # per-decile expected excess
    # method A: raw decile means (isotonic-in-spirit: monotone pooled)
    iso = np.maximum.accumulate(fit_mean)       # enforce monotone nondecreasing
    # method B: linear in decile rank
    x = np.arange(N_DECILES) - (N_DECILES - 1) / 2.0
    beta = float(np.nansum(x * fit_mean) / np.nansum(x * x))
    lin = beta * x

    def mae(pred, table):
        realised = np.nanmean(table, axis=0)
        return float(np.nanmean(np.abs(pred - realised)))

    # shrinkage selection on VALIDATION only
    best = None
    for method, base in (("isotonic_decile", iso), ("linear_in_rank", lin)):
        for lam in SHRINKAGES:
            m = mae(base * lam, val)
            if best is None or m < best["validation_mae"]:
                best = {"method": method, "shrinkage": lam,
                        "validation_mae": m, "pred": (base * lam)}
    pred = best.pop("pred")

    lock_realised = np.nanmean(lock, axis=0)
    lock_mae = float(np.nanmean(np.abs(pred - lock_realised)))
    zero_mae = float(np.nanmean(np.abs(lock_realised)))
    # Kendall tau of realised lockbox decile means vs decile order
    concordant = discordant = 0
    for i in range(N_DECILES):
        for j in range(i + 1, N_DECILES):
            if np.isfinite(lock_realised[i]) and np.isfinite(lock_realised[j]):
                d = lock_realised[j] - lock_realised[i]
                concordant += d > 0
                discordant += d < 0
    tau = ((concordant - discordant) / max(1, concordant + discordant))

    survives = bool(lock_mae < zero_mae and tau >= 0.5)
    body = {
        "track": "CALIBRATION", "family": family_id, "variant": variant,
        "protocol_target_rule": "strongest VALIDATION-positive equity family",
        "selected_method": best["method"], "shrinkage": best["shrinkage"],
        "validation_mae": best["validation_mae"],
        "fit_decile_expected_excess_21d": [None if not np.isfinite(v) else round(float(v), 6) for v in pred],
        "lockbox_realised_decile_excess_21d": [None if not np.isfinite(v) else round(float(v), 6) for v in lock_realised],
        "lockbox_mae": lock_mae, "zero_forecast_mae": zero_mae,
        "lockbox_kendall_tau": round(float(tau), 4),
        "survives_oos": survives,
        "expected_return_state": ("CALIBRATED_RESEARCH_ONLY" if survives
                                  else "NOT_CALIBRATED"),
        "governance": "research lane only; nothing enters a governed decision",
    }
    write_artifact(CAL_ARTIFACT, body)
    return body


# --------------------------------------------------------------------------- #
# Construction
# --------------------------------------------------------------------------- #
def _inv_vol_weights(panel, held, t):
    lo = max(0, t - 62)
    tr = panel["tr"][held, lo:t + 1]
    with np.errstate(invalid="ignore", divide="ignore"):
        r = tr[:, 1:] / tr[:, :-1] - 1.0
    sd = np.nanstd(r, axis=1)
    w = np.where(np.isfinite(sd) & (sd > 0), 1.0 / sd, 0.0)
    return w


def _rank_weights(panel, held, t):
    n = len(held)
    return np.linspace(2.0, 1.0, n)     # best name double the weight of worst


def run_construction(panel: dict, family_id: str, variant: str,
                     diagnostic: bool) -> dict:
    spec = EQUITY_FAMILIES[family_id]
    fn = spec["variants"][variant]
    constructions = {
        "EW_top50": dict(top_n=50, weights_fn=None),
        "rank_weighted_top50": dict(top_n=50, weights_fn=_rank_weights),
        "inverse_vol_top50": dict(top_n=50, weights_fn=_inv_vol_weights),
        "EW_top25": dict(top_n=25, weights_fn=None),
    }
    rows = {}
    for name, kw in constructions.items():
        res = E.run_topn(panel, fn, spec["cadence"], spec["horizon"], **kw)
        rows[name] = {L: E.layer_stats(res, L) for L in ("V", "L")}
    lock_ex = {k: v["L"].get("ann_net_excess") for k, v in rows.items()}
    spread = (max(x for x in lock_ex.values() if x is not None)
              - min(x for x in lock_ex.values() if x is not None))
    body = {
        "track": "CONSTRUCTION", "family": family_id, "variant": variant,
        "label": "DIAGNOSTIC_ON_REJECTED_FAMILY" if diagnostic else "CANDIDATE",
        "constructions": rows,
        "lockbox_ann_net_excess_by_construction": lock_ex,
        "construction_value_spread": spread,
        "conclusion_rule": ("if the signal fails under every construction, the "
                            "SIGNAL failed; a weak signal rescued only by one "
                            "tuned construction is not rescued"),
    }
    write_artifact(CONSTR_ARTIFACT, body)
    return body


# --------------------------------------------------------------------------- #
# Turnover
# --------------------------------------------------------------------------- #
def run_turnover(panel: dict, family_id: str, variant: str,
                 diagnostic: bool) -> dict:
    spec = EQUITY_FAMILIES[family_id]
    fn = spec["variants"][variant]
    cadence, horizon = spec["cadence"], spec["horizon"]
    dates = panel["dates"]
    idx = E.decision_indices(dates, cadence, "2006-01-01", horizon)
    lays = E.layer_of(dates, idx, cadence, horizon)
    bands = {"no_band": 50, "band_75": 75, "band_100": 100, "band_150": 150}
    out_rows = {}
    for label, k_out in bands.items():
        held: set = set()
        ex_by_layer = {"V": [], "L": []}
        to_by_layer = {"V": [], "L": []}
        prev_w: dict = {}
        for j, t in enumerate(idx):
            elig = E.eligibility(panel, t)
            s = np.where(elig & np.isfinite(fn(panel, t)), fn(panel, t), -np.inf)
            order = np.argsort(-s)
            ranks = {int(sym): rk + 1 for rk, sym in enumerate(order[:max(bands.values()) + 50])}
            scoreable = int((s > -np.inf).sum())
            if scoreable < 60:
                continue
            keep = {h for h in held if ranks.get(h, 10 ** 9) <= k_out}
            need = 50 - len(keep)
            adds = [int(x) for x in order if int(x) not in keep][:max(0, need)]
            new_held = list(keep) + adds
            w = {h: 1.0 / len(new_held) for h in new_held}
            traded = sum(abs(w.get(x, 0.0) - prev_w.get(x, 0.0))
                         for x in set(w) | set(prev_w))
            hidx = np.array(new_held, dtype=int)
            r = E.forward_return(panel, hidx, t, horizon)
            univ = np.where(elig)[0]
            br = E.forward_return(panel, univ, t, horizon).mean()
            net = float(r.mean()) - traded * 0.00125 - (br - 0.0)
            lay = lays[j]
            if lay in ex_by_layer:
                ex_by_layer[lay].append(net)
                to_by_layer[lay].append(traded / 2.0)
            held, prev_w = set(new_held), w
        out_rows[label] = {
            L: {"periods": len(ex_by_layer[L]),
                "ann_net_excess": float(np.mean(ex_by_layer[L]) * (252 / cadence))
                if ex_by_layer[L] else None,
                "mean_oneway_turnover": float(np.mean(to_by_layer[L]))
                if to_by_layer[L] else None}
            for L in ("V", "L")}
    body = {
        "track": "TURNOVER", "family": family_id, "variant": variant,
        "label": "DIAGNOSTIC_ON_REJECTED_FAMILY" if diagnostic else "CANDIDATE",
        "criterion": "AFTER-COST lockbox net excess, never turnover itself",
        "bands": out_rows,
    }
    write_artifact(TURN_ARTIFACT, body)
    return body
