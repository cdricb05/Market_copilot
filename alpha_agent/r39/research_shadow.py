"""alpha_agent.r39.research_shadow - Track H: prospective shadow evidence.

Release 39 v1 froze ZERO forward candidates because nothing passed the
qualification gate - correct for PROMOTION, wasteful for CALENDAR TIME.
This module starts the clock without moving anything toward production:

* THREE predeclared, economically distinct research shadows are frozen
  BEFORE any future outcome is observable:
    - ``c39_c9233eccaa74`` (WIDE machine cross-section; the strongest
      candidate the estate has produced),
    - ``c39_8278ddd2d3b9`` (transparent commodity/all-futures carry rule),
    - ``c39_0574796699fa`` (transparent VX term-structure carry rule).
  Every shadow carries ``RESEARCH_SHADOW_ONLY``,
  ``HISTORICAL_QUALIFICATION = FAIL`` and ``PROMOTION_ALLOWED = False``.
  The WIDE model is frozen as BYTES: the A+B-fitted ridge coefficients and
  train-only preprocessing statistics are serialised into the registry and
  hashed - capture never refits anything.

* Ledger mechanics REUSE the canonical chain-hash primitives from
  ``api.paper_trading_desk`` (``_append_ledger`` / ``verify_ledger``) -
  the same append-only, rewrite-detectable convention as every desk
  ledger - pointed at the R39 RESEARCH root. No operational store is
  touched; no second forward-evidence implementation exists. TRUE_FORWARD
  semantics follow Phase 28B: a snapshot is captured only for a decision
  date the capture run can actually observe, appended BEFORE any outcome
  exists, and outcomes mature only after the horizon's sessions have
  passed. Historical observations can never enter these ledgers: capture
  refuses any decision date at or before the registry freeze timestamp.

* The forward evidence clock BEGINS at the first eligible decision AFTER
  the freeze: the first per-market month-end after ``frozen_at`` for the
  futures shadows, the first VX Friday after it for the VX shadow.

Activation into anything operational remains an operator action through
the canonical Phase-28 forward-evidence architecture; this module cannot
promote, allocate, order, or automate.
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

from .. import r39
from . import contract as C
from .continuation import CONTINUATION_CAMPAIGN_ID

CALCULATION_OWNER = "alpha_agent.r39.research_shadow"

REGISTRY_NAME = "research_shadow_registry.json"
SPEC_HASHES_NAME = "forward_specification_hashes.json"
SNAPSHOT_LEDGER = "shadow_forward_snapshots.json"
OUTCOME_LEDGER = "shadow_forward_outcomes.json"
SHADOW_DIRNAME = "research_shadow_forward"

RESEARCH_SHADOW_ONLY = True
PROMOTION_ALLOWED = False
HISTORICAL_QUALIFICATION = "FAIL"

SHADOWS = (
    {"shadow_id": "shadow_wide_xs",
     "candidate_id": "c39_c9233eccaa74",
     "family": "FUT:WIDE", "lane": "FUT",
     "model": "ridge(alpha=10) with FROZEN coefficients",
     "expression": "XS_LONG_SHORT", "scope": "ALL_FUT",
     "cadence": "each market's last session per calendar month",
     "horizon_sessions": 21,
     "economic_hypothesis": "machine-aggregated weak signals across 86 "
                            "wide-representation features"},
    {"shadow_id": "shadow_carry_rule_xs",
     "candidate_id": "c39_8278ddd2d3b9",
     "family": "FUT:HAND_RULE", "lane": "FUT",
     "model": "rule:carry_slope_ann (no parameters)",
     "expression": "XS_LONG_SHORT", "scope": "ALL_FUT",
     "cadence": "each market's last session per calendar month",
     "horizon_sessions": 21,
     "economic_hypothesis": "cross-sectional carry premium"},
    {"shadow_id": "shadow_vx_carry_ts",
     "candidate_id": "c39_0574796699fa",
     "family": "VX:TERM_STRUCTURE", "lane": "VX",
     "model": "rule:carry_slope_ann (no parameters)",
     "expression": "TS_OUTRIGHT", "scope": "VX",
     "cadence": "weekly (every 5th VX session)",
     "horizon_sessions": 5,
     "economic_hypothesis": "volatility term-structure carry, third "
                            "independent measurement in this estate"},
)


def shadow_dir(campaign_id: str = CONTINUATION_CAMPAIGN_ID) -> Path:
    d = r39.campaign_dir(campaign_id) / SHADOW_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _desk():
    from paper_trader.api import paper_trading_desk as desk
    return desk


# --------------------------------------------------------------------------- #
# Registration (freeze BEFORE any future outcome)
# --------------------------------------------------------------------------- #
def _freeze_wide_model(director) -> dict:
    """Fit the confirmed A+B ridge ONCE and freeze it as bytes."""
    from .wide_prosecution import wide_candidate
    cand = wide_candidate()
    p = director._panel(cand)
    fit_rows = p[p["zone"].isin(("ZONE_A", "ZONE_B"))]
    X, y, cols = director._matrices(cand, fit_rows)
    ok = np.isfinite(y)
    model = director._make_model(cand)
    model.fit(X[ok], y[ok])
    inner = model._model
    frozen = {
        "features": cols,
        "impute_median": [float(v) for v in model._med],
        "standardise_mu": [float(v) for v in model._mu],
        "standardise_sd": [float(v) for v in model._sd],
        "coef": [float(v) for v in inner.coef_],
        "intercept": float(inner.intercept_),
        "ridge_alpha": 10.0,
        "fit_zones": ["ZONE_A", "ZONE_B"],
        "n_fit_rows": int(ok.sum()),
    }
    frozen["coefficient_hash"] = r39.sha(frozen)
    return frozen


def apply_frozen_wide(frozen: dict, X: np.ndarray) -> np.ndarray:
    med = np.asarray(frozen["impute_median"], dtype=float)
    mu = np.asarray(frozen["standardise_mu"], dtype=float)
    sd = np.asarray(frozen["standardise_sd"], dtype=float)
    coef = np.asarray(frozen["coef"], dtype=float)
    Xi = np.where(np.isfinite(X), X, med)
    Z = (Xi - mu) / sd
    return Z @ coef + float(frozen["intercept"])


def register(director, campaign_id: str = CONTINUATION_CAMPAIGN_ID) -> dict:
    """Freeze the shadow registry. Idempotent through artifact immutability:
    a changed registry cannot silently replace the frozen one."""
    frozen_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    existing = r39.read_json(r39.campaign_dir(campaign_id) / REGISTRY_NAME)
    if existing:
        return existing
    wide_model = _freeze_wide_model(director)
    fut = director.state["fut"]
    costs = fut.groupby("market_id")["cost_bps_per_side"].median()
    rows = []
    for sh in SHADOWS:
        row = dict(sh)
        row.update({
            "research_shadow_only": RESEARCH_SHADOW_ONLY,
            "promotion_allowed": PROMOTION_ALLOWED,
            "historical_qualification": HISTORICAL_QUALIFICATION,
            "frozen_at": frozen_at,
            "first_eligible_forward_decision":
                "the first %s strictly AFTER frozen_at; captures at or "
                "before frozen_at are refused by code"
                % ("per-market month-end session" if sh["lane"] == "FUT"
                   else "5th-session VX decision"),
            "cost_model": {"base": C.COST_BASE,
                           "state": C.COST_MODEL_STATE,
                           "bps_per_side":
                               {k: float(v) for k, v in costs.items()}
                               if sh["lane"] == "FUT" else
                               {"VX": 15.0}},
            "control": "RISK_MATCHED_CASH"
            if sh["expression"] == "XS_LONG_SHORT"
            else "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE",
            "position_sizing": "equal weight within expression legs; "
                               "gross exposure <= 1; no leverage",
        })
        if sh["shadow_id"] == "shadow_wide_xs":
            row["frozen_model"] = wide_model
            row["spec_hash"] = \
                "cfd2ec367018412d5fb83909ba15f62900bd0e4f2ef974f53e04dea1" \
                "096a361c"
        rows.append(row)
    body = r39.artifact_body("r39_research_shadow_registry/1", {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "frozen_at": frozen_at,
        "shadows": rows,
        "n_shadows": len(rows),
        "ledger_primitives": "api.paper_trading_desk chain-hash ledgers "
                             "(canonical), research-root store",
        "snapshot_ledger": str(shadow_dir(campaign_id) / SNAPSHOT_LEDGER),
        "outcome_ledger": str(shadow_dir(campaign_id) / OUTCOME_LEDGER),
        "historical_observations_can_never_enter": True,
        "activation_owner": "the operator through the canonical Phase-28 "
                            "forward-evidence architecture",
    })
    body["shadow_registry_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / REGISTRY_NAME, body)

    hashes = r39.artifact_body("r39_forward_specification_hashes/1", {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "frozen_at": frozen_at,
        "hashes": {
            r["shadow_id"]: {
                "candidate_id": r["candidate_id"],
                "spec_hash": r.get("spec_hash"),
                "coefficient_hash":
                    (r.get("frozen_model") or {}).get("coefficient_hash"),
            } for r in rows},
        "registry_hash": body["shadow_registry_hash"],
    })
    hashes["forward_specification_hashes_hash"] = r39.sha(hashes)
    r39.write_json(r39.campaign_dir(campaign_id) / SPEC_HASHES_NAME,
                   hashes)
    return body


# --------------------------------------------------------------------------- #
# Capture (append BEFORE outcomes exist; refuses historical dates)
# --------------------------------------------------------------------------- #
def load_registry(campaign_id: str = CONTINUATION_CAMPAIGN_ID):
    return r39.read_json(r39.campaign_dir(campaign_id) / REGISTRY_NAME)


def eligible_new_decisions(registry: dict, panel: pd.DataFrame,
                           captured: set, lane: str) -> list:
    """Decision dates strictly after the freeze, present in the CURRENT
    panel, not yet captured. The freeze cut is the eligibility wall."""
    frozen_at = pd.Timestamp(registry["frozen_at"].replace("Z", ""))
    dates = sorted(pd.to_datetime(panel["decision_date"]).unique())
    return [d for d in dates
            if d > frozen_at and str(d.date()) not in captured]


def capture(director, campaign_id: str = CONTINUATION_CAMPAIGN_ID,
            fresh_state: dict = None) -> dict:
    """Append target snapshots for every newly-eligible decision date.

    ``fresh_state`` is a CURRENT universal state (rebuilt from providers by
    the operator-run capture script); with the frozen R38 layer only, no
    date is eligible yet and the call is a clean no-op - which is exactly
    what makes historical backfill impossible.
    """
    desk = _desk()
    registry = load_registry(campaign_id)
    if not registry:
        raise RuntimeError("register() must freeze the registry first")
    sdir = shadow_dir(campaign_id)
    rows = desk._read_ledger(sdir, SNAPSHOT_LEDGER)
    state = fresh_state or director.state
    appended = []
    for sh in registry["shadows"]:
        captured = {r["decision_date"] for r in rows
                    if r.get("shadow_id") == sh["shadow_id"]}
        panel = state["fut"] if sh["lane"] == "FUT" else state["vx"]
        for d in eligible_new_decisions(registry, panel, captured,
                                        sh["lane"]):
            snap = _target_snapshot(sh, panel, d, director)
            if snap is None:
                continue
            appended.append(snap)
    if appended:
        desk._append_ledger(sdir, SNAPSHOT_LEDGER, appended)
    return {"appended": len(appended),
            "ledger": str(sdir / SNAPSHOT_LEDGER),
            "verify": desk.verify_ledger(sdir, SNAPSHOT_LEDGER)}


def _target_snapshot(sh: dict, panel: pd.DataFrame, d: pd.Timestamp,
                     director):
    rows_d = panel[pd.to_datetime(panel["decision_date"]) == d]
    if rows_d.empty:
        return None
    if sh["shadow_id"] == "shadow_wide_xs":
        frozen = sh["frozen_model"]
        cols = [c for c in frozen["features"] if c in rows_d.columns]
        if len(cols) != len(frozen["features"]):
            return None
        X = rows_d[frozen["features"]].to_numpy(dtype=float)
        pred = apply_frozen_wide(frozen, X)
    elif sh["model"].startswith("rule:carry_slope_ann"):
        pred = rows_d["carry_slope_ann"].to_numpy(dtype=float)
    else:
        return None
    ids = rows_d["market_id"].tolist()
    s = pd.Series(pred, index=ids).dropna()
    if sh["expression"] == "XS_LONG_SHORT":
        if len(s) < 6:
            return None
        ranks = s.rank(pct=True)
        long = ranks[ranks >= 2.0 / 3.0].index.tolist()
        short = ranks[ranks <= 1.0 / 3.0].index.tolist()
        w = {m: 1.0 / (2 * len(long)) for m in long}
        w.update({m: -1.0 / (2 * len(short)) for m in short})
    else:  # TS_OUTRIGHT (VX rule)
        w = {m: float(np.sign(v)) for m, v in s.items()}
    return {"kind": "SHADOW_TARGET_SNAPSHOT",
            "shadow_id": sh["shadow_id"],
            "candidate_id": sh["candidate_id"],
            "decision_date": str(pd.Timestamp(d).date()),
            "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                         time.gmtime()),
            "forward_evidence_type": "TRUE_FORWARD",
            "horizon_sessions": sh["horizon_sessions"],
            "weights": {k: round(float(v), 8) for k, v in w.items()},
            "n_predictions": int(len(s)),
            "promotion_allowed": False}


def build_fresh_state() -> dict:
    """A CURRENT universal state for capture/maturation, rebuilt from the
    live Norgate entitlement through the CANONICAL R38 layer builder and
    the same universal_state functions as the frozen campaigns.

    Macro/COT acquisitions are file caches; if the operator has not
    refreshed them, later decision dates carry NaN there and the frozen
    model imputes its frozen medians - graceful, and recorded per snapshot
    by ``n_predictions`` and the panel's own masks. Representations
    regenerated here are the deterministic families the shadows need
    (spectral, latent, graph, structure); no seeded generator runs.
    """
    from ..r38 import contract as C38
    from ..r38 import enumeration as EN38
    from ..r38 import research_layer as RL38
    from . import representation_factory as R
    from . import universal_state as US

    macro = US.build_macro_overlay()
    meta, cot = US.load_panel_meta()
    registry = EN38.load_contract_registry(C38.CAMPAIGN_ID)
    layer = {}
    for mkt in sorted(meta["market_id"].unique()):
        lists = registry["contract_symbols"].get(mkt, {})
        primary = (sorted(lists, key=lambda s: (len(s), s)) or [None])[0]
        symbols = lists.get(primary, []) if primary else []
        series = RL38.build_market_series(mkt, symbols)
        if series is None or not len(series):
            continue
        df = series.reset_index()
        df = df.rename(columns={df.columns[0]: "Date"})
        # live Norgate series arrive as datetime64[us] under pandas 3; the
        # frozen layer (and every merge key downstream) is datetime64[ns]
        df["Date"] = pd.to_datetime(df["Date"]).astype("datetime64[ns]")
        layer[mkt] = df
    fut = US.build_futures_panel(layer, meta, cot, macro)
    fut, _, _ = R.add_spectral(fut)
    fut, _, _ = R.add_latent_and_graph(fut)
    fut, _, _ = R.add_structure_and_fib(fut, layer)
    fut = fut.sort_values(["decision_date", "market_id"]) \
        .reset_index(drop=True)
    vx = US.build_vx_weekly(layer, meta, macro)
    return {"fut": fut, "vx": vx, "macro": macro, "layer": layer}


def mature(campaign_id: str = CONTINUATION_CAMPAIGN_ID,
           fresh_state: dict = None) -> dict:
    """Append ONE outcome row per matured (shadow, decision date): the
    after-cost book return realised over the snapshot's horizon, computed
    from a CURRENT state and the immutable snapshot weights."""
    desk = _desk()
    if fresh_state is None:
        return {"state": "NO_FRESH_STATE", "appended": 0}
    registry = load_registry(campaign_id)
    sdir = shadow_dir(campaign_id)
    snaps = desk._read_ledger(sdir, SNAPSHOT_LEDGER)
    outs = desk._read_ledger(sdir, OUTCOME_LEDGER)
    done = {(r["shadow_id"], r["decision_date"]) for r in outs}
    by_shadow = {s["shadow_id"]: s for s in registry["shadows"]}
    appended = []
    prev_w: dict = {}
    for snap in snaps:
        key = (snap["shadow_id"], snap["decision_date"])
        sh = by_shadow[snap["shadow_id"]]
        panel = fresh_state["fut"] if sh["lane"] == "FUT" else \
            fresh_state["vx"]
        fwd_col = "fwd_%d" % sh["horizon_sessions"]
        rows_d = panel[pd.to_datetime(panel["decision_date"])
                       == pd.Timestamp(snap["decision_date"])]
        if key in done or rows_d.empty:
            prev_w[snap["shadow_id"]] = snap["weights"]
            continue
        fwd = rows_d.set_index("market_id")[fwd_col]
        w = pd.Series(snap["weights"], dtype=float)
        r = fwd.reindex(w.index)
        if r.isna().all():
            continue  # not matured yet
        gross = float((w * r.fillna(0.0)).sum())
        pw = pd.Series(prev_w.get(snap["shadow_id"], {}), dtype=float)
        dw = (w.reindex(w.index.union(pw.index), fill_value=0.0)
              - pw.reindex(w.index.union(pw.index), fill_value=0.0)).abs()
        bps = sh["cost_model"]["bps_per_side"]
        rate = pd.Series({m: float(bps.get(m, 10.0)) / 1e4
                          for m in dw.index})
        cost = float((dw * rate).sum())
        appended.append({
            "kind": "SHADOW_OUTCOME",
            "shadow_id": snap["shadow_id"],
            "decision_date": snap["decision_date"],
            "matured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                        time.gmtime()),
            "gross_return": round(gross, 8),
            "cost": round(cost, 8),
            "net_return": round(gross - cost, 8)})
        prev_w[snap["shadow_id"]] = snap["weights"]
    if appended:
        desk._append_ledger(sdir, OUTCOME_LEDGER, appended)
    return {"appended": len(appended),
            "verify": desk.verify_ledger(sdir, OUTCOME_LEDGER)}
