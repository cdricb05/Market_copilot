r"""alpha_agent.alpha_recovery.futures_trend_challenger - MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1

THE SECOND NON-EQUITY FORWARD PIPELINE: managed-futures TIME-SERIES TREND across
liquid commodity, rates, FX and equity-index futures, from OWNED Norgate
dated-contract settlements only.

    research definition -> historical point-in-time validation -> frozen challenger
    identity -> prospective emission registration -> canonical forward accrual

WHY THIS FAMILY
    It is economically distinct from the operational US-equity fundamental /
    momentum sleeve: the bet is each market's OWN trailing trend (long or short),
    equal-risk across four asset classes, volatility-targeted. It needs no data
    the estate does not already own (the R41 curve store the FX carry cadence
    producer already refreshes from Norgate), no purchase and no new provider.

THE RULE (frozen; no fitted parameter)
    score_m(t)  = sum of log(1 + ret1) over the market's own sessions in
                  (t - 252, t - 21]   (twelve-minus-one-month trend)
                  / (daily stdev over its last 63 sessions x sqrt(231))
    position    = alpha_agent.r64.construction.ts_unlevered(score, vol, pred_scale=1)
                  = tanh(score) x min(10 % / annualised vol, 3) / n_markets
                  -> equal ex-ante risk across asset classes
                  -> no instrument above 25 % of book risk
                  -> book levered to a 10 % volatility target, gross capped at 5x
                  -> 25 % no-trade band against the previous decision
    cadence     = one decision every 21 realised sessions, held 21 sessions

    Every construction primitive is the ONE R64 risk-controlled book
    (``alpha_agent.r64.construction``); nothing is re-implemented here. The
    scorer is arithmetic over the market's own settlements; there is no ridge,
    no training block and no parameter that a lockbox could have leaked into.

WHAT THIS MODULE MAY NOT DO
    It never registers, never declares a policy, never freezes a decision and
    never touches the frozen R41 store. It measures, writes ONE immutable frozen
    record under the campaign research root, and exposes the adoption row the
    canonical registrar reads. Historical results are never forward evidence.
"""
from __future__ import annotations

import csv
import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from alpha_agent import r63 as _r63
from alpha_agent.r63 import panels as P

from . import research_root, stable_hash
from . import reversed_skew as RS

CALCULATION_OWNER = "alpha_agent.alpha_recovery.futures_trend_challenger"

CHALLENGER_ID = "ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1"
RELEASE = RS.RELEASE
ASSET_CLASS = "MULTI_ASSET_FUTURES"
VENUE = "LISTED_FUTURES_MULTI_VENUE"
SLEEVE = "MANAGED_FUTURES_TREND"
FAMILY = "MANAGED_FUTURES_TS_TREND"
DIMENSION = "PRICE_TREND_12_1"
MARK_OWNER = ("alpha_agent.r41.curve_state (owned Norgate dated-contract settlements, R38 "
              "observable roll rule; the registration's price_mark_owner layer)")
RECORD_SUBDIR = "challengers"
RECORD_SCHEMA = "alpha_recovery_forward_candidate/1"
SPEC_SCHEMA = "alpha_recovery_forward_specification/1"

# --------------------------------------------------------------------------- #
# The frozen specification
# --------------------------------------------------------------------------- #
FROZEN_LOOKBACK_SESSIONS = 252
FROZEN_SKIP_SESSIONS = 21
FROZEN_VOL_SESSIONS = 63
FROZEN_HORIZON = 21              # holding horizon == information label == cadence
FROZEN_TRADE_EVERY = 21
FROZEN_PRED_SCALE = 1.0
FROZEN_CELL_ID = "MULTI_ASSET_FUTURES|TS|21|TREND_12_1|k21"
MODEL_FAMILY = "TS_VOL_TARGET_RISK_CONTROLLED_CADENCE_%d" % FROZEN_TRADE_EVERY
DISCOVERY_START = _r63.FUTURES_DISCOVERY_START          # 1995-01-01
LOCKBOX_START = _r63.LOCKBOX_START                      # 2023-01-01
MIN_MARKETS_PER_DECISION = 10
MIN_OWN_SESSIONS_FOR_SCORE = FROZEN_LOOKBACK_SESSIONS + FROZEN_SKIP_SESSIONS

#: The classes admitted, and what is excluded by declaration. Volatility (VX) is a
#: different bet (term-structure carry) and STIR contracts are a rate-level bet with
#: a different return scale; both are excluded by name, not by result.
FROZEN_CLASSES = (_r63.AC_COMMODITY, _r63.AC_RATES, _r63.AC_FX, _r63.AC_EQUITY_INDEX)
EXCLUDED_CLASSES = (_r63.AC_VOLATILITY,)
EXCLUDED_GROUPS = ("STIR_FUTURES",)

#: The frozen identity has ONE horizon: a decision is held for the label horizon,
#: so every consumer's number is 21 and there is no label/holding split to explain.
HORIZON_CONTRACT = {
    "challenger_id": CHALLENGER_ID,
    "information_label_horizon_sessions": FROZEN_HORIZON,
    "holding_horizon_sessions": FROZEN_HORIZON,
    "rebalance_cadence_sessions": FROZEN_TRADE_EVERY,
    "evaluation_horizon_sessions": FROZEN_HORIZON,
    "evidence_gate_horizon_sessions": FROZEN_HORIZON,
    "forward_observation_unit": "ONE_FROZEN_DECISION_HELD_FOR_THE_HOLDING_HORIZON",
    "label_equals_holding": True,
}

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "promotes_model": False,
    "capital_eligible": False,
    "allocates_capital": False,
    "mutates_portfolio": False,
    "creates_orders": False,
    "creates_fills": False,
    "registers_forward_challenger": False,
    "writes_the_frozen_r41_store": False,
    "backfill_allowed": False,
    "automatic_promotion": False,
    "manual_review_required": True,
    "historical_result_is_forward_evidence": False,
}

EVIDENCE_AT_INCEPTION = {
    "true_forward_observations": 0,
    "backfilled": False,
    "capital_eligible": False,
    "qualified": False,
}

REGISTRATION_OWNER = "scripts/register_futures_trend_challenger.py"
REGISTRATION_ARTIFACT = "futures_trend_registration.json"

# Classification vocabulary (historical; never forward evidence).
CL_CANDIDATE = "HISTORICAL_CANDIDATE_VS_CASH"
CL_WEAK = "HISTORICAL_WEAK_VS_CASH"
CL_NOT_MATERIAL = "HISTORICAL_NOT_MATERIAL"
CL_DEGENERATE = "HISTORICAL_DEGENERATE_UNDER_CONTROLS"
CLASSIFICATIONS = (CL_CANDIDATE, CL_WEAK, CL_NOT_MATERIAL, CL_DEGENERATE)
T_FLOOR = 2.0


def frozen_store_dir() -> Path:
    return Path(_r63.R41_ROOT) / "_data_curves"


def record_dir() -> Path:
    return research_root() / RECORD_SUBDIR


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --------------------------------------------------------------------------- #
# 1. THE UNIVERSE - the R63 mechanical rule, restricted to the frozen classes
# --------------------------------------------------------------------------- #
def daily_returns(market: str, store: Optional[Path] = None) -> tuple:
    """``(dates, ret1)`` as parallel lists of the market's FINITE settlement returns."""
    p = Path(store or frozen_store_dir()) / ("%s_daily.csv" % market)
    dates, rets = [], []
    if not p.exists():
        return dates, rets
    with p.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            v = r.get("ret1")
            if v in (None, ""):
                continue
            try:
                x = float(v)
            except ValueError:
                continue
            if math.isfinite(x):
                dates.append(str(r.get("date") or "")[:10])
                rets.append(x)
    return dates, rets


def scope_universe(store: Optional[Path] = None) -> dict:
    """Markets admitted by the frozen rule, each with class, group and cost."""
    d = Path(store or frozen_store_dir())
    admitted, excluded = [], []
    for p in sorted(d.glob("*_daily.csv")) if d.exists() else []:
        m = p.name[:-len("_daily.csv")]
        if m in P.EXCLUDED_MARKETS:
            excluded.append({"market": m, "reason": P.EXCLUDED_MARKETS[m]})
            continue
        meta = P.futures_market_meta(m)
        if meta is None:
            excluded.append({"market": m, "reason": "NO_HONEST_ASSET_CLASS"})
            continue
        if meta["asset_class"] in EXCLUDED_CLASSES or meta["asset_class"] not in FROZEN_CLASSES:
            excluded.append({"market": m, "reason": "CLASS_EXCLUDED_BY_DECLARATION:%s" % meta["asset_class"]})
            continue
        if meta.get("economic_group") in EXCLUDED_GROUPS:
            excluded.append({"market": m, "reason": "GROUP_EXCLUDED_BY_DECLARATION:%s" % meta["economic_group"]})
            continue
        dates, rets = daily_returns(m, d)
        if len(rets) < P.MIN_SESSIONS:
            excluded.append({"market": m, "reason": "INSUFFICIENT_HISTORY", "sessions": len(rets)})
            continue
        if not dates or dates[-1] < P.MUST_TRADE_ON_OR_AFTER:
            excluded.append({"market": m, "reason": "STALE_MARKET", "last": dates[-1] if dates else None})
            continue
        admitted.append({"market": m, "asset_class": meta["asset_class"],
                         "economic_group": meta["economic_group"],
                         "cost_bps_per_side": float(meta["cost_bps_per_side"]),
                         "sessions": len(rets), "first": dates[0], "last": dates[-1]})
    return {"admitted": admitted, "excluded": excluded,
            "universe_rule": (">=%d finite settlement returns, a bar on/after %s, an honest "
                              "asset class in %s, no STIR / volatility / micro / crypto"
                              % (P.MIN_SESSIONS, P.MUST_TRADE_ON_OR_AFTER, list(FROZEN_CLASSES)))}


# --------------------------------------------------------------------------- #
# 2. THE SCORE - arithmetic over the market's own settlements, as of a session
# --------------------------------------------------------------------------- #
def trend_score(rets: list, p: int) -> Optional[dict]:
    """Score and annualised volatility of ONE market as of its own bar index ``p``.

    Uses only returns at indices <= p. Returns None when the market has fewer
    than ``lookback + skip`` own sessions at that point (never padded).
    """
    if p is None or p < MIN_OWN_SESSIONS_FOR_SCORE - 1:
        return None
    window = rets[p - FROZEN_LOOKBACK_SESSIONS + 1: p - FROZEN_SKIP_SESSIONS + 1]
    if len(window) != FROZEN_LOOKBACK_SESSIONS - FROZEN_SKIP_SESSIONS:
        return None
    cum = 0.0
    for r in window:
        if r <= -1.0:
            return None
        cum += math.log1p(r)
    vol_win = rets[p - FROZEN_VOL_SESSIONS + 1: p + 1]
    if len(vol_win) < FROZEN_VOL_SESSIONS:
        return None
    n = len(vol_win)
    mu = sum(vol_win) / n
    var = sum((x - mu) ** 2 for x in vol_win) / (n - 1)
    sd = math.sqrt(var) if var > 0 else 0.0
    if sd <= 0:
        return None
    score = cum / (sd * math.sqrt(len(window)))
    return {"score": float(score), "vol_annualised": float(sd * math.sqrt(252.0)),
            "cum_log_return_12_1": float(cum), "daily_sd_63": float(sd)}


def _own_index_at_or_before(dates: list, session: str) -> Optional[int]:
    """Index of the market's last own bar at or before ``session`` (bisect)."""
    lo, hi = 0, len(dates)
    while lo < hi:
        mid = (lo + hi) // 2
        if dates[mid] <= session:
            lo = mid + 1
        else:
            hi = mid
    return lo - 1 if lo > 0 else None


def score_markets(scope: list, series: dict, session: str) -> dict:
    """``market -> {score, vol_annualised, ...}`` for every market scorable as of ``session``."""
    out = {}
    for m in scope:
        dates, rets = series.get(m) or ([], [])
        p = _own_index_at_or_before(dates, session)
        s = trend_score(rets, p) if p is not None else None
        if s is not None:
            out[m] = dict(s, own_index=p, own_session=dates[p])
    return out


def unlevered_target(scores: dict, class_of: dict, *, policy: Optional[dict] = None) -> dict:
    """The frozen per-period unlevered target through the ONE R64 construction."""
    from alpha_agent.r64 import construction as B
    pol = dict(B.default_policy())
    if policy:
        pol.update(policy)
    names = sorted(scores)
    if len(names) < MIN_MARKETS_PER_DECISION:
        return {}
    pp = np.array([scores[m]["score"] for m in names], dtype=float)
    vv = np.array([scores[m]["vol_annualised"] for m in names], dtype=float)
    inst = np.arange(len(names))
    w = B.ts_unlevered(pp, vv, inst, FROZEN_PRED_SCALE) or {}
    vol_by = {int(i): float(v) for i, v in zip(inst, vv)}
    cls_by = {int(i): str(class_of.get(names[int(i)])) for i in inst}
    w = B.apply_class_risk_budget(w, vol_by, cls_by)
    w = B.apply_instrument_cap(w, vol_by, float(pol["max_instrument_risk_share"]))
    return {names[int(i)]: float(x) for i, x in w.items() if abs(float(x)) > 1e-12}


def realised_return(series_m: tuple, p: int, horizon: int) -> Optional[float]:
    """Cumulative simple return over the ``horizon`` own bars strictly after index ``p``."""
    dates, rets = series_m
    if p is None or p + horizon >= len(rets):
        return None
    acc = 1.0
    for r in rets[p + 1: p + 1 + horizon]:
        acc *= (1.0 + r)
    return acc - 1.0


# --------------------------------------------------------------------------- #
# 3. HISTORICAL POINT-IN-TIME VALIDATION (research; never forward evidence)
# --------------------------------------------------------------------------- #
def union_grid(series: dict) -> list:
    out: set = set()
    for dates, _ in series.values():
        out.update(dates)
    return sorted(out)


def _slice_book(book: dict, mask: np.ndarray) -> dict:
    out = {}
    for k, v in book.items():
        if isinstance(v, np.ndarray) and len(v) == len(mask):
            out[k] = v[mask]
        else:
            out[k] = v
    return out


def validate(*, store: Optional[Path] = None, start: str = DISCOVERY_START,
             lockbox_start: str = LOCKBOX_START, scope: Optional[list] = None) -> dict:
    """Run the frozen rule over the whole owned history on a 21-session decision grid.

    Every decision uses only returns at or before its session; every outcome is
    the market's own next 21 settlements. The R64 book is built by the ONE
    construction owner; the summary is the ONE R63 economic summary. The lockbox
    (2023+) is reported separately and honestly labelled: the rule has no fitted
    parameter, so the lockbox measures the FAMILY, not a fit.
    """
    from alpha_agent.r64 import construction as B
    d = Path(store or frozen_store_dir())
    uni = scope_universe(d)
    markets = [a["market"] for a in uni["admitted"]]
    if scope is not None:
        markets = [m for m in markets if m in set(scope)]
    meta = {a["market"]: a for a in uni["admitted"]}
    series = {m: daily_returns(m, d) for m in markets}
    grid = [g for g in union_grid(series) if g >= start]
    slots = grid[::FROZEN_TRADE_EVERY]
    pred, y, gid, iid, vol, cost, cls = [], [], [], [], [], [], []
    slot_dates = []
    for g, s in enumerate(slots):
        sc = score_markets(markets, series, s)
        if len(sc) < MIN_MARKETS_PER_DECISION:
            continue
        rows = 0
        for i, m in enumerate(markets):
            x = sc.get(m)
            if x is None:
                continue
            r = realised_return(series[m], x["own_index"], FROZEN_HORIZON)
            if r is None:
                continue
            pred.append(x["score"]); y.append(r); gid.append(g); iid.append(i)
            vol.append(x["vol_annualised"]); cost.append(meta[m]["cost_bps_per_side"] / 1e4)
            cls.append(meta[m]["asset_class"]); rows += 1
        if rows:
            slot_dates.append((g, s))
    if not pred:
        return {"ok": False, "reason": "NO_SCORABLE_DECISIONS", "universe": uni}
    book = B.build_book(np.array(pred), np.array(y), np.array(gid), np.array(iid),
                        np.array(vol), np.array(cost), mode="TS", horizon=FROZEN_HORIZON,
                        pred_scale=FROZEN_PRED_SCALE, class_of=np.array(cls, dtype=object))
    periods = np.unique(np.array(gid))
    date_of = dict(slot_dates)
    period_dates = [date_of[int(g)] for g in periods]
    # build_book skips a period whose unlevered target is empty; align defensively.
    n = len(book["net"])
    if n != len(period_dates):
        period_dates = period_dates[-n:] if n < len(period_dates) else period_dates
    pd_arr = np.array(period_dates)
    full = B.summarise(book, FROZEN_HORIZON, 0)
    sel_mask = pd_arr < lockbox_start
    lock_mask = ~sel_mask
    selection = B.summarise(_slice_book(book, sel_mask), FROZEN_HORIZON, 0) if sel_mask.any() else {"periods": 0}
    lockbox = B.summarise(_slice_book(book, lock_mask), FROZEN_HORIZON, 0) if lock_mask.any() else {"periods": 0}
    by_class = {}
    for c in sorted(set(cls)):
        by_class[c] = int(sum(1 for x in cls if x == c))
    classification, why = classify(full, lockbox)
    return {
        "ok": True,
        "calculation_owner": CALCULATION_OWNER,
        "cell_id": FROZEN_CELL_ID,
        "store": str(d),
        "store_last_session": grid[-1] if grid else None,
        "universe": uni,
        "instruments": markets,
        "asset_class_of": {m: meta[m]["asset_class"] for m in markets},
        "cost_bps_per_side_of": {m: meta[m]["cost_bps_per_side"] for m in markets},
        "rows_scored_by_class": by_class,
        "decisions": int(n),
        "first_decision": period_dates[0] if period_dates else None,
        "last_decision": period_dates[-1] if period_dates else None,
        "full": _jsonable(full),
        "selection": _jsonable(selection),
        "lockbox": _jsonable(lockbox),
        "lockbox_start": lockbox_start,
        "lockbox_sign_agrees": bool((lockbox.get("ann_net") or 0.0) > 0) if lockbox.get("periods") else None,
        "control": "ZERO_RETURN_CASH",
        "classification": classification,
        "why": why,
        "evidence_label": ("NO_FITTED_PARAMETER: the rule is the textbook 12-1 time-series "
                           "trend; the lockbox measures the family, not a fit; the R63/R64 "
                           "lockbox of the futures substrate was viewed by earlier campaigns"),
        "historical_result_is_forward_evidence": False,
    }


def classify(full: dict, lockbox: dict) -> tuple:
    t = full.get("t_net")
    ann = full.get("ann_net")
    degenerate = bool(full.get("degenerate_under_controls"))
    material = ann is not None and ann >= float(_r63.MATERIALITY_ANN_NET)
    significant = t is not None and t >= T_FLOOR
    if degenerate:
        return CL_DEGENERATE, "the risk-controlled book is degenerate (drawdown or leverage cap share)"
    if significant and material:
        return CL_CANDIDATE, ("net of cost vs cash: %.2f%%/yr, t %.2f over %d decisions; lockbox %s"
                              % (100 * ann, t, int(full.get("periods") or 0),
                                 "agrees" if (lockbox.get("ann_net") or 0) > 0 else "disagrees"))
    if significant or material:
        return CL_WEAK, ("only one of significance (t %.2f >= %.1f) and materiality (%.2f%%/yr >= %.1f%%) holds"
                         % (t or 0.0, T_FLOOR, 100 * (ann or 0.0), 100 * _r63.MATERIALITY_ANN_NET))
    return CL_NOT_MATERIAL, "neither significance nor materiality holds net of cost against cash"


def _jsonable(o):
    if isinstance(o, dict):
        return {k: _jsonable(v) for k, v in o.items()}
    if isinstance(o, (np.floating,)):
        return None if not np.isfinite(o) else float(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, np.ndarray):
        return [_jsonable(x) for x in o.tolist()]
    if isinstance(o, float) and not math.isfinite(o):
        return None
    return o


# --------------------------------------------------------------------------- #
# 4. THE FROZEN RECORD - written once, verified on every read
# --------------------------------------------------------------------------- #
def forward_specification(validation: dict) -> dict:
    return {
        "schema": SPEC_SCHEMA,
        "cell_id": FROZEN_CELL_ID,
        "scope": ASSET_CLASS,
        "mode": "TS",
        "horizon_sessions": FROZEN_HORIZON,
        "trade_every_sessions": FROZEN_TRADE_EVERY,
        "lookback_sessions": FROZEN_LOOKBACK_SESSIONS,
        "skip_sessions": FROZEN_SKIP_SESSIONS,
        "vol_sessions": FROZEN_VOL_SESSIONS,
        "pred_scale": FROZEN_PRED_SCALE,
        "construction": "alpha_agent.r64.construction (TS_VOL_TARGET_RISK_CONTROLLED, default policy, "
                        "equal asset-class risk budget, instrument cap, 10% vol target, 5x gross cap, "
                        "25% no-trade band)",
        "classes": list(FROZEN_CLASSES),
        "excluded_classes": list(EXCLUDED_CLASSES),
        "excluded_groups": list(EXCLUDED_GROUPS),
        "instruments": list(validation.get("instruments") or []),
        "asset_class_of": dict(validation.get("asset_class_of") or {}),
        "signal": {"kind": "TIME_SERIES_TREND", "dimension": DIMENSION,
                   "definition": "sum log(1+ret1) over own sessions (t-252, t-21] / (daily sd over "
                                 "last 63 own sessions x sqrt(231)); position tanh(score)"},
        "costs": "R38 per-market cost per side on one-way turnover",
        "emission_rule": ("decision formed on the newest published session whose open interest "
                          "and volume are final, from data <= that session; entered at the "
                          "settlement of the next eligible session; held 21 sessions; rebalanced "
                          "every 21 realised sessions; no backfill"),
        "pit_quality": _r63.PIT_MARKET,
        "lockbox_start": LOCKBOX_START,
        "discovery_start": DISCOVERY_START,
        "evidence_label": validation.get("evidence_label"),
        "horizon_contract": dict(HORIZON_CONTRACT),
    }


def build_record(validation: dict, *, inception: Optional[str] = None) -> dict:
    spec = forward_specification(validation)
    body = {
        "schema": RECORD_SCHEMA,
        "campaign_id": "alpha_recovery_offensive_v1",
        "release": RELEASE,
        "challenger_id": CHALLENGER_ID,
        "asset_class": ASSET_CLASS,
        "kind": "CROSS_DOMAIN_SLEEVE",
        "cell_id": FROZEN_CELL_ID,
        "horizon_sessions": FROZEN_HORIZON,
        "information_identity": {"family": FAMILY, "dimension": DIMENSION},
        "forward_specification": spec,
        "freeze_record_hash": stable_hash(spec),
        "qualification_evidence": {
            "full": validation.get("full"), "selection": validation.get("selection"),
            "lockbox": validation.get("lockbox"), "decisions": validation.get("decisions"),
            "first_decision": validation.get("first_decision"),
            "last_decision": validation.get("last_decision"),
            "lockbox_sign_agrees": validation.get("lockbox_sign_agrees"),
            "control": validation.get("control"),
            "rows_scored_by_class": validation.get("rows_scored_by_class"),
            "store_last_session": validation.get("store_last_session"),
            "fdr": "NOT_APPLICABLE_SINGLE_PREREGISTERED_CELL",
        },
        "classification": validation.get("classification"),
        "why": validation.get("why"),
        "inception": inception or date.today().isoformat(),
        "holdings_changed": False,
        "live_registration_performed": False,
        "promotion_allowed": False,
        "records_are_immutable": True,
        "historical_result_is_forward_evidence": False,
        "safety": dict(SAFETY),
    }
    body["record_hash"] = stable_hash(body)
    body["record_file"] = "%s_%s.json" % (CHALLENGER_ID, body["record_hash"][:12])
    return body


def existing_record_paths(directory: Optional[Path] = None) -> list:
    d = Path(directory or record_dir())
    return sorted(d.glob("%s_*.json" % CHALLENGER_ID)) if d.exists() else []


def write_record(record: dict, *, directory: Optional[Path] = None) -> dict:
    """FIRST WRITE WINS per challenger id: a second, different identity is refused."""
    d = Path(directory or record_dir())
    existing = existing_record_paths(d)
    if existing:
        held = _read(existing[0]) or {}
        same = held.get("record_hash") == record.get("record_hash")
        return {"outcome": "ALREADY_FROZEN" if same else "REFUSED_A_DIFFERENT_IDENTITY_IS_FROZEN",
                "written": False, "path": str(existing[0]), "held_record_hash": held.get("record_hash")}
    d.mkdir(parents=True, exist_ok=True)
    p = d / record["record_file"]
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(record, indent=1, sort_keys=True, default=str), encoding="utf-8")
    tmp.replace(p)
    return {"outcome": "FROZEN", "written": True, "path": str(p)}


def _read(path: Path) -> Optional[dict]:
    try:
        out = json.loads(Path(path).read_text(encoding="utf-8"))
        return out if isinstance(out, dict) else None
    except (OSError, ValueError):
        return None


def verify_record(rec: Optional[dict]) -> list:
    """Every reason this record is not a valid frozen identity. Pure."""
    if not isinstance(rec, dict):
        return ["RECORD_MISSING"]
    problems = []
    body = {k: v for k, v in rec.items() if k not in ("record_hash", "record_file")}
    if stable_hash(body) != rec.get("record_hash"):
        problems.append("RECORD_BYTES_DO_NOT_HASH_TO_THE_RECORDED_HASH")
    spec = rec.get("forward_specification") or {}
    if stable_hash(spec) != rec.get("freeze_record_hash"):
        problems.append("SPECIFICATION_DOES_NOT_HASH_TO_THE_FREEZE_RECORD_HASH")
    if rec.get("challenger_id") != CHALLENGER_ID:
        problems.append("CHALLENGER_ID_MISMATCH: %s" % rec.get("challenger_id"))
    for name, got, want in (("cell_id", spec.get("cell_id"), FROZEN_CELL_ID),
                            ("trade_every_sessions", spec.get("trade_every_sessions"), FROZEN_TRADE_EVERY),
                            ("horizon_sessions", spec.get("horizon_sessions"), FROZEN_HORIZON),
                            ("lookback_sessions", spec.get("lookback_sessions"), FROZEN_LOOKBACK_SESSIONS),
                            ("skip_sessions", spec.get("skip_sessions"), FROZEN_SKIP_SESSIONS)):
        if got != want:
            problems.append("FROZEN_%s_MISMATCH: %r != %r" % (name.upper(), got, want))
    if not spec.get("instruments"):
        problems.append("NO_INSTRUMENTS_IN_THE_FROZEN_SPECIFICATION")
    if rec.get("promotion_allowed") is not False or rec.get("live_registration_performed") is not False:
        problems.append("RECORD_SAFETY_FLAGS_CHANGED")
    return problems


def resolve(*, directory: Optional[Path] = None) -> dict:
    paths = existing_record_paths(directory)
    if not paths:
        return {"ok": False, "problems": ["RECORD_MISSING"], "record": None, "record_path": None}
    rec = _read(paths[0])
    problems = verify_record(rec)
    return {"ok": not problems, "problems": problems, "record": rec, "record_path": str(paths[0])}


def cost_model(rec: dict) -> dict:
    spec = rec.get("forward_specification") or {}
    return {"rule": spec.get("costs"),
            "per_market_bps_per_side_owner": "alpha_agent.r63.panels.futures_market_meta "
                                             "(R38 market meta cost_bps_per_side)",
            "applied": "one-way turnover at every rebalance"}


def freeze_row(resolved: Optional[dict] = None) -> dict:
    """The immutable freeze, in the shape the canonical adoption owner reads."""
    res = resolved or resolve()
    if not res.get("ok"):
        raise ValueError("the frozen futures trend record cannot be registered: %s"
                         % "; ".join(res.get("problems") or []))
    rec = res["record"]
    spec = rec["forward_specification"]
    return {
        "hypothesis_id": rec["record_file"][:-len(".json")],
        "release": RELEASE,
        "asset_class": ASSET_CLASS,
        "economic_family": FAMILY,
        "model_family": MODEL_FAMILY,
        "horizon_sessions": int(rec["horizon_sessions"]),
        "outcome": "FORWARD_FROZEN",
        "invalidated_reason": None,
        "input_data_identity": stable_hash({"record_file": rec["record_file"],
                                            "record_hash": rec["record_hash"],
                                            "cell_id": spec.get("cell_id"),
                                            "store_last_session": (rec.get("qualification_evidence") or {}).get(
                                                "store_last_session")}),
        "spec_json": {**spec, "instrument_scope": list(spec.get("instruments") or []),
                      "venue": VENUE, "sleeve": SLEEVE, "cost_model": cost_model(rec),
                      "feature_snapshot_hash": rec["record_hash"]},
        "forward_challenger": {"challenger_id": CHALLENGER_ID,
                               "record_hash": rec["freeze_record_hash"],
                               "inception": rec.get("inception")},
    }


__all__ = ["CALCULATION_OWNER", "CHALLENGER_ID", "RELEASE", "ASSET_CLASS", "VENUE", "SLEEVE",
           "FAMILY", "MARK_OWNER", "FROZEN_LOOKBACK_SESSIONS", "FROZEN_SKIP_SESSIONS",
           "FROZEN_VOL_SESSIONS", "FROZEN_HORIZON", "FROZEN_TRADE_EVERY", "FROZEN_CELL_ID",
           "MODEL_FAMILY", "FROZEN_CLASSES", "EXCLUDED_CLASSES", "EXCLUDED_GROUPS",
           "HORIZON_CONTRACT", "SAFETY", "EVIDENCE_AT_INCEPTION", "CLASSIFICATIONS",
           "daily_returns", "scope_universe", "trend_score", "score_markets", "unlevered_target",
           "realised_return", "validate", "classify", "forward_specification", "build_record",
           "write_record", "verify_record", "resolve", "cost_model", "freeze_row",
           "frozen_store_dir", "record_dir", "REGISTRATION_OWNER", "REGISTRATION_ARTIFACT"]
