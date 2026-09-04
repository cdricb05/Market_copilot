"""alpha_agent.r57.track1 - WHY is the incumbent buy engine weak?

DIAGNOSTIC ONLY. Nothing here issues an alpha verdict, and the protocol
forbids one: the forward ledger holds tens of matured sessions, and the
historical replication measures a legacy leg, not a new hypothesis.

Two measurements:

1. FORWARD LEDGER ASYMMETRY - the operational stack has captured TRUE_FORWARD
   cross-sections for the blend (fundamental_momentum_50_50_v1) AND its two
   legs (composite_sn = fundamental, mom_6_1 = momentum) since 2026-07-24,
   with matured per-member outcomes at h = 1, 5, 20. For each model and
   horizon: the equal-weight forward excess (vs the scored-universe mean) of
   the TOP quartile by that model's own rank (the BUY side) and of the BOTTOM
   quartile (the SELL side, where the skill claim is that low-ranked names
   underperform). Read strictly READ-ONLY from the production desk ledgers.

2. HISTORICAL LEG REPLICATION - a mom_6_1-style momentum leg (126-session
   trailing return, 21-session skip) replicated on the survivorship-safe
   Norgate panel, with the same top/bottom-quartile asymmetry measured on the
   VALIDATION and LOCKBOX layers. This is a LEGACY-COMPONENT diagnostic, not
   a tournament entry (E1 covers the family with its own grid).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from . import write_artifact
from . import engine as E
from .families import mom

DESK_DIR = Path(r"C:\Users\binis\.paper_trader\paper_trading_desk")
SNAPSHOTS = "forward_prediction_snapshots.json"
OUTCOMES = "forward_prediction_outcomes.json"

MODELS = ("fundamental_momentum_50_50_v1", "composite_sn", "mom_6_1")
HORIZONS = (1, 5, 20)
ARTIFACT = "track1_incumbent_diagnosis.json"


def _rows(name: str) -> list:
    return json.loads((DESK_DIR / name).read_text(encoding="utf-8"))["rows"]


def forward_ledger_asymmetry() -> dict:
    """Per (model, horizon): buy-side and sell-side forward excess.

    Uses the outcome owner's OWN matured metrics (computed on the full scored
    universe of ~233 names) rather than the 50-row member subset:
    buy side = top decile minus universe mean, sell side = bottom decile minus
    universe mean, per matured session.
    """
    outs = [r for r in _rows(OUTCOMES) if r.get("kind") == "OUTCOME"
            and r.get("status") == "MATURED"]
    out: dict = {}
    for model in MODELS:
        for h in HORIZONS:
            buys, sells, ics = [], [], []
            for o in outs:
                if o.get("model_id") != model or o.get("horizon") != h:
                    continue
                m = o.get("metrics") or {}
                need = ("top_decile_return_pct", "bottom_decile_return_pct",
                        "universe_avg_return_pct")
                if any(m.get(k) is None for k in need):
                    continue
                buys.append(m["top_decile_return_pct"] - m["universe_avg_return_pct"])
                sells.append(m["bottom_decile_return_pct"] - m["universe_avg_return_pct"])
                if m.get("rank_ic_spearman") is not None:
                    ics.append(m["rank_ic_spearman"])
            if not buys:
                continue
            st_b = E.nw_tstat(np.array(buys), lag=max(0, h // 5))
            st_s = E.nw_tstat(-np.array(sells), lag=max(0, h // 5))
            out["%s_h%d" % (model, h)] = {
                "model": model, "horizon": h, "n_matured_sessions": len(buys),
                "buy_side_top_decile_excess_pct_mean": float(np.mean(buys)),
                "buy_side_t": st_b["t"],
                "sell_side_bottom_decile_excess_pct_mean": float(np.mean(sells)),
                "sell_side_skill_t": st_s["t"],
                "asymmetry_note": ("sell skill = bottom decile UNDERPERFORMS "
                                   "(negative excess); its t is for -excess > 0"),
                "mean_rank_ic": float(np.mean(ics)) if ics else None,
                "sample_warning": "TINY forward sample; diagnostic only",
                "source": "outcome owner's own matured metrics (full universe)",
            }
    return out


def historical_leg_replication(panel: dict) -> dict:
    """mom_6_1-style leg: quartile asymmetry on V and L layers."""
    fn = mom(126, 21)  # 6-month momentum, 1-month skip - the legacy leg's shape
    dates = panel["dates"]
    idx = E.decision_indices(dates, 21, "2006-01-01", 21)
    layers = E.layer_of(dates, idx, 21, 21)
    res = {"V": {"buy": [], "sell": []}, "L": {"buy": [], "sell": []}}
    for j, t in enumerate(idx):
        lay = layers[j]
        if lay not in ("V", "L"):
            continue
        elig = E.eligibility(panel, t)
        s = fn(panel, t)
        ok = elig & np.isfinite(s)
        if ok.sum() < 100:
            continue
        held = np.where(ok)[0]
        r = E.forward_return(panel, held, t, 21)
        sv = s[held]
        mean_r = r.mean()
        top = r[sv >= np.quantile(sv, 0.75)].mean() - mean_r
        bot = r[sv <= np.quantile(sv, 0.25)].mean() - mean_r
        res[lay]["buy"].append(top)
        res[lay]["sell"].append(bot)
    out = {}
    for lay, d in res.items():
        if not d["buy"]:
            continue
        b, sll = np.array(d["buy"]), np.array(d["sell"])
        out[lay] = {
            "periods": len(b),
            "buy_side_ann_excess": float(b.mean() * 12),
            "buy_side_t": E.nw_tstat(b)["t"],
            "sell_side_ann_excess": float(sll.mean() * 12),
            "sell_side_skill_t": E.nw_tstat(-sll)["t"],
        }
    return out


def run(panel: dict) -> dict:
    body = {
        "track": "TRACK1_INCUMBENT_DIAGNOSIS",
        "scope": "DIAGNOSTIC_ONLY_NO_ALPHA_VERDICT",
        "forward_ledger": forward_ledger_asymmetry(),
        "historical_momentum_leg_replication": historical_leg_replication(panel),
        "fundamental_leg_note": (
            "the fundamental leg (composite_sn) cannot be replicated on the "
            "20-year panel (no owned PIT fundamental history before the frozen "
            "Phase 10-L window); its forward-ledger rows above and the Stage "
            "23/24 campaign verdicts are the evidence that exists"),
        "sources_read_only": [str(DESK_DIR / SNAPSHOTS), str(DESK_DIR / OUTCOMES)],
    }
    write_artifact(ARTIFACT, body)
    return body
