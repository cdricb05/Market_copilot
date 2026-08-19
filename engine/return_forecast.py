"""engine/return_forecast.py - the canonical forward-return forecasting kernel.

Release 30. This module is the ONE owner of what a Paper Trader forward-return
forecast *is*: its target quantity, its horizons, its uncertainty, its
point-in-time integrity claim and its identity hashes. It is pure stdlib and
pure function - it opens no file, calls no service, writes nothing, and holds no
state.

**Why the training code is not here.** Fitting the models needs ``numpy``; the
whole ``api/`` and ``engine/`` layer is deliberately stdlib-only so the backend
imports cheaply and deterministically. Release 30 therefore reuses the pattern
Phase 29D.2 established for the monthly momentum input: the research lane
(``alpha_agent.release30_forecast_research``) owns the mathematics and EMITS a
frozen artifact; this kernel owns validation, application and semantics; and
``api.return_forecast`` owns composition, persistence and the read surface.

**What is forecast.** The investment quantity is a forward *return*, never a
future price level. For every eligible name ``i`` and horizon ``h`` the kernel
produces

    expected_excess_return  = calibration_slope * standardised_model_score
    expected_return         = expected_excess_return + market_baseline

where the target the models were fit on is the forward return MINUS its own
cross-sectional mean. The market's level is not forecast by this layer and is
never silently attributed to it: ``MARKET_BASELINE_POLICY`` says so in one place,
``market_baseline`` is 0.0, and every payload carries the statement.

**What this kernel may never do.** Promote a model, change a weight, retrain,
read a future observation, invent an uncertainty multiplier, size a position, or
create a signal, proposal, decision or order.
"""
from __future__ import annotations

import hashlib
import json
import math
from bisect import bisect_left
from typing import Any, Optional

SCHEMA_VERSION = "return_forecast.v1"
INPUT_SCHEMA_VERSION = "return_forecast.input.v1"
MODEL_CONTRACT = "release30_forecast_model/1"
CALCULATION_OWNER = "engine.return_forecast"
PHASE = "R30"

#: The canonical Release 30 horizons, in TRADING SESSIONS. Sessions rather than
#: calendar days because every owned input is session-indexed and a calendar
#: horizon would drift against the data that has to fill it.
HORIZONS = (5, 20, 60)

#: The forecast target. Stated once, carried by every payload.
TARGET_QUANTITY = "FORWARD_EXCESS_RETURN_VS_CROSS_SECTIONAL_MEAN"
TARGET_DOC = (
    "The modelled quantity is a name's forward TOTAL RETURN minus the equal-"
    "weight mean forward return of the same eligible cross-section. A "
    "cross-sectional model cannot forecast the market's own level, so that level "
    "is removed from the target rather than credited to the model.")

#: The market level is deliberately NOT forecast, so the equity baseline against
#: which cash competes is zero. This is a declared policy, not an omission: a
#: fabricated equity risk premium would be the single easiest way to make a
#: long-only allocator look good on paper.
MARKET_BASELINE_POLICY = "MARKET_LEVEL_NOT_FORECAST"
MARKET_BASELINE = 0.0

#: Forecast states.
STATE_READY = "READY"
STATE_DEGRADED = "DEGRADED"
STATE_BLOCKED = "BLOCKED"
STATE_NOT_ACTIVATED = "NOT_ACTIVATED"
STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED, STATE_NOT_ACTIVATED)

#: Point-in-time integrity vocabulary.
PIT_OK = "POINT_IN_TIME_OK"
PIT_UNVERIFIED = "POINT_IN_TIME_UNVERIFIED"
PIT_VIOLATED = "POINT_IN_TIME_VIOLATED"
PIT_VOCAB = (PIT_OK, PIT_UNVERIFIED, PIT_VIOLATED)

#: --------------------------------------------------------------------------
#: Model identity - Release 30.1
#: --------------------------------------------------------------------------
#: An artifact that represents the CURRENT APPROVED operational model is not a
#: model in its own right: it is an ECONOMIC CALIBRATION of a ranking a human
#: already approved. Its only job is to say what forward return corresponds to
#: that ranking - never to change it.
#:
#: This matters because ``expected_excess_return = slope * standardised_score``
#: and the standardisation of a positive-weight rank blend is strictly
#: monotone. A NEGATIVE slope therefore does not "adjust" the approved model; it
#: reverses it, and the allocator downstream buys the names the approved model
#: ranks worst while the payload still carries the approved model's name. That
#: is exactly what the Release-30 ``operational`` artifact did at 20 sessions
#: (slope -0.000848), and it is why rank identity is a CONTRACT here rather than
#: an outcome anyone is expected to notice.
OPERATIONAL_ACTIVATION = "CURRENT_OPERATIONAL_MODEL"
MODEL_IDENTITY_CONTRACT = "APPROVED_MODEL_RANKING_IS_PRESERVED"

RANK_IDENTITY_PRESERVED = "RANK_IDENTITY_PRESERVED"
RANK_IDENTITY_VIOLATED = "RANK_IDENTITY_VIOLATED"
RANK_IDENTITY_NOT_APPLICABLE = "RANK_IDENTITY_NOT_APPLICABLE"
RANK_IDENTITY_VOCAB = (RANK_IDENTITY_PRESERVED, RANK_IDENTITY_VIOLATED,
                       RANK_IDENTITY_NOT_APPLICABLE)

#: Calibration states an approved-model adapter may declare.
CALIBRATION_CALIBRATED = "CALIBRATED"
CALIBRATION_NOT_CALIBRATED = "NOT_CALIBRATED"

#: Why a horizon of an approved-model adapter can be refused.
SUPPRESSED_RANK_IDENTITY = "APPROVED_MODEL_RANK_ORDER_WOULD_INVERT"
SUPPRESSED_NOT_CALIBRATED = "APPROVED_MODEL_HORIZON_NOT_CALIBRATED"
SUPPRESSED_NO_SLOPE = "APPROVED_MODEL_HORIZON_SUPPLIES_NO_SLOPE"

#: A horizon that fails the contract is SUPPRESSED: it carries its reasons and
#: NO expected return. Emitting a degraded number would be worse than emitting
#: none, because every consumer downstream treats a number as a conclusion.
HORIZON_APPLIED = "APPLIED"
HORIZON_SUPPRESSED = "SUPPRESSED"

#: Learner kinds the kernel can apply. Anything else is refused rather than
#: guessed at, because a silently mis-applied model is worse than no forecast.
KIND_LINEAR = "linear"
KIND_TREE_ENSEMBLE = "tree_ensemble"
KIND_RANK_BLEND = "rank_blend"
KIND_ENSEMBLE = "ensemble"
KINDS = (KIND_LINEAR, KIND_TREE_ENSEMBLE, KIND_RANK_BLEND, KIND_ENSEMBLE)

#: The lower quantile the downside estimate reports.
DOWNSIDE_QUANTILE = 0.05
#: Normal-tail multiplier for the 5 % quantile. It is arithmetic, not a tuned
#: confidence knob: the DISPERSION it multiplies is measured from walk-forward
#: residuals, and that measurement is where the calibration actually lives.
_Z05 = 1.6448536269514722

AUTOMATIC_PROMOTION_ALLOWED = False
SAFETY_BADGES = ["PREVIEW ONLY", "READ ONLY", "NO ORDERS", "NO LIVE PROMOTION",
                 "MANUAL REVIEW", "AUTOMATION OFF"]


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def stable_hash(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"),
                   default=str).encode("utf-8")).hexdigest()[:32]


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


# --------------------------------------------------------------------------- #
# Cross-sectional normalisation - the SAME transform the research lane fit on
# --------------------------------------------------------------------------- #
FEATURE_TRANSFORM = "PER_DATE_RANK_TO_MINUS_HALF_PLUS_HALF_MISSING_ZERO"


def rank_normalise(values: list) -> list:
    """Rank the present values onto [-0.5, +0.5]; a missing value becomes 0.

    This is the operational half of the research transform, and the two must
    agree exactly or the frozen coefficients mean nothing. Ties take the average
    rank, and the transform reads only this cross-section, so it can carry no
    information from any other date.
    """
    n = len(values)
    out = [0.0] * n
    present = [(v, i) for i, v in enumerate(values) if _f(v) is not None]
    m = len(present)
    if m < 2:
        return out
    present.sort(key=lambda p: (float(p[0]), p[1]))
    i = 0
    while i < m:
        j = i
        while j + 1 < m and float(present[j + 1][0]) == float(present[i][0]):
            j += 1
        avg = (i + j) / 2.0
        for k in range(i, j + 1):
            out[present[k][1]] = (avg + 0.5) / m - 0.5
        i = j + 1
    return out


def standardise(scores: list) -> list:
    """Cross-sectional standardisation of one date's raw model output."""
    vals = [v for v in scores if _f(v) is not None]
    n = len(vals)
    if n < 2:
        return [0.0] * len(scores)
    mean = sum(vals) / n
    var = sum((v - mean) ** 2 for v in vals) / (n - 1)
    sd = math.sqrt(var) if var > 0 else 0.0
    if sd <= 0:
        return [0.0] * len(scores)
    return [((float(v) - mean) / sd) if _f(v) is not None else 0.0
            for v in scores]


# --------------------------------------------------------------------------- #
# Model application
# --------------------------------------------------------------------------- #
def _apply_linear(spec: dict, row: list) -> float:
    coef = spec.get("coef") or []
    return sum(float(c) * float(v) for c, v in zip(coef, row))


def _bin_of(value: float, edges: list) -> int:
    return bisect_left(edges, float(value)) if edges else 0


def _apply_tree(tree: dict, bins: list) -> float:
    feat, thr = tree["feat"], tree["thr"]
    left, right, value = tree["left"], tree["right"], tree["value"]
    node = 0
    while feat[node] >= 0:
        node = left[node] if bins[feat[node]] <= thr[node] else right[node]
    return float(value[node])


def _apply_tree_ensemble(spec: dict, row: list) -> float:
    edges = spec["edges"]
    bins = [_bin_of(row[j], edges[j]) for j in range(len(row))]
    trees = spec["trees"]
    scale = (float(spec.get("learning_rate", 1.0))
             if spec.get("shrinkage_applied") else 1.0 / max(1, len(trees)))
    return scale * sum(_apply_tree(t, bins) for t in trees)


def _apply_rank_blend(spec: dict, row: list, feature_names: list) -> float:
    idx = {n: i for i, n in enumerate(feature_names)}
    total = 0.0
    for name, w in (spec.get("weights") or {}).items():
        j = idx.get(name)
        if j is not None:
            total += float(w) * float(row[j])
    return total


def apply_model(spec: dict, matrix: list, feature_names: list) -> list:
    """Apply one frozen model spec to a whole normalised cross-section.

    ``matrix`` is a list of per-name feature rows already put through
    ``rank_normalise``. Returns one raw score per name, in the same order.
    """
    kind = spec.get("kind")
    if kind == KIND_LINEAR:
        return [_apply_linear(spec, row) for row in matrix]
    if kind == KIND_TREE_ENSEMBLE:
        return [_apply_tree_ensemble(spec, row) for row in matrix]
    if kind == KIND_RANK_BLEND:
        return [_apply_rank_blend(spec, row, feature_names) for row in matrix]
    if kind == KIND_ENSEMBLE:
        n = len(matrix)
        total = [0.0] * n
        for member in spec.get("members") or []:
            w = float(member.get("weight") or 0.0)
            if w == 0.0:
                continue
            part = standardise(apply_model(member["spec"], matrix, feature_names))
            for i in range(n):
                total[i] += w * part[i]
        return total
    raise ValueError("unsupported model kind: %r" % (kind,))


def member_scores(spec: dict, matrix: list, feature_names: list) -> dict:
    """Standardised per-member scores, keyed by model id.

    Ensemble members disagreeing with one another is real, measurable evidence
    that a forecast is unreliable, so the members are kept rather than collapsed.
    """
    if spec.get("kind") != KIND_ENSEMBLE:
        return {"model": standardise(apply_model(spec, matrix, feature_names))}
    out = {}
    for member in spec.get("members") or []:
        if float(member.get("weight") or 0.0) == 0.0:
            continue
        out[str(member.get("model_id") or "member_%d" % len(out))] = standardise(
            apply_model(member["spec"], matrix, feature_names))
    return out


# --------------------------------------------------------------------------- #
# Artifact validation
# --------------------------------------------------------------------------- #
def validate_artifact(artifact: Optional[dict]) -> dict:
    """Structural validation of a frozen model artifact.

    Fails CLOSED and by NAME: an artifact that cannot be validated yields
    ``ok=False`` with explicit reason codes, never a partially-applied model.
    """
    problems: list = []
    a = artifact or {}
    if not a:
        return {"ok": False, "reasons": [{"code": "MODEL_ARTIFACT_ABSENT"}]}
    if a.get("contract") != MODEL_CONTRACT:
        problems.append({"code": "MODEL_CONTRACT_MISMATCH",
                         "detail": str(a.get("contract"))})
    if a.get("target") != TARGET_QUANTITY:
        problems.append({"code": "TARGET_QUANTITY_MISMATCH",
                         "detail": str(a.get("target"))})
    if a.get("feature_transform") != FEATURE_TRANSFORM:
        problems.append({"code": "FEATURE_TRANSFORM_MISMATCH",
                         "detail": str(a.get("feature_transform"))})
    if a.get("automatic_promotion_allowed"):
        problems.append({"code": "AUTOMATIC_PROMOTION_DECLARED"})
    names = a.get("feature_names") or []
    if not names:
        problems.append({"code": "NO_FEATURE_NAMES"})
    horizons = a.get("horizons") or {}
    if not horizons:
        problems.append({"code": "NO_HORIZONS"})
    declared_uncalibrated: list = []
    for key, blk in sorted(horizons.items()):
        spec = (blk or {}).get("model") or {}
        if spec.get("kind") not in KINDS:
            problems.append({"code": "UNSUPPORTED_MODEL_KIND", "horizon": key,
                             "detail": str(spec.get("kind"))})
        cal = (blk or {}).get("calibration") or {}
        state = cal.get("state")
        if state == CALIBRATION_NOT_CALIBRATED:
            # Release 30.1: an explicit "this horizon is NOT calibrated" is a
            # valid DECLARATION, not a malformed artifact. It is honoured per
            # horizon by the rank-identity guard in build_forecast, which
            # suppresses that horizon and lets any properly calibrated sibling
            # still be applied. Treating it as a structural defect would force
            # the research lane to omit the horizon instead of stating why it
            # failed - and the reason is the thing an operator needs.
            declared_uncalibrated.append(int(key))
            continue
        if state != CALIBRATION_CALIBRATED:
            problems.append({"code": "HORIZON_NOT_CALIBRATED", "horizon": key})
        elif _f(cal.get("residual_sigma")) is None:
            problems.append({"code": "CALIBRATION_MISSING_DISPERSION",
                             "horizon": key})
    if horizons and len(declared_uncalibrated) == len(horizons):
        problems.append({"code": "NO_CALIBRATED_HORIZON",
                         "declared_uncalibrated": sorted(declared_uncalibrated)})
    return {"ok": not problems, "reasons": problems,
            "declared_uncalibrated_horizons": sorted(declared_uncalibrated),
            "model_spec_hash": a.get("model_spec_hash"),
            "horizons": sorted(int(k) for k in horizons) if horizons else [],
            "feature_names": list(names)}


def validate_input(cross_section: Optional[dict], artifact: dict) -> dict:
    """Validate one forecast-input cross-section against the artifact.

    The input is produced by the research emitter and must state its own as-of
    date, its feature names and its rows. A feature the model was fit on but the
    input does not carry is a BLOCKING mismatch, not a zero-filled guess.
    """
    problems: list = []
    ic = cross_section or {}
    if not ic:
        return {"ok": False, "reasons": [{"code": "FORECAST_INPUT_ABSENT"}]}
    if ic.get("input_schema_version") != INPUT_SCHEMA_VERSION:
        problems.append({"code": "INPUT_SCHEMA_MISMATCH",
                         "detail": str(ic.get("input_schema_version"))})
    if not ic.get("as_of_date"):
        problems.append({"code": "MISSING_AS_OF_DATE"})
    have = list(ic.get("feature_names") or [])
    need = list(artifact.get("feature_names") or [])
    missing = [n for n in need if n not in have]
    if missing:
        problems.append({"code": "FEATURE_SET_MISMATCH", "missing": missing})
    rows = ic.get("rows") or []
    if not rows:
        problems.append({"code": "NO_ELIGIBLE_ROWS"})
    if ic.get("point_in_time_status") not in PIT_VOCAB:
        problems.append({"code": "POINT_IN_TIME_STATUS_UNDECLARED"})
    elif ic.get("point_in_time_status") == PIT_VIOLATED:
        problems.append({"code": "POINT_IN_TIME_VIOLATED"})
    return {"ok": not problems, "reasons": problems,
            "as_of_date": ic.get("as_of_date"), "rows": len(rows)}


def feature_snapshot_hash(cross_section: dict) -> str:
    """Content hash of exactly the inputs a forecast consumed.

    Deliberately excludes generation timestamps: two emissions of the same
    cross-section must hash the same, or a replay could never be proved
    identical.
    """
    ic = cross_section or {}
    names = list(ic.get("feature_names") or [])
    rows = sorted((str(r.get("ticker")),
                   [_r(_f(r.get("features", {}).get(n)), 10) for n in names])
                  for r in (ic.get("rows") or []))
    return stable_hash({"as_of": ic.get("as_of_date"), "names": names,
                        "rows": rows})


# --------------------------------------------------------------------------- #
# The forecast itself
# --------------------------------------------------------------------------- #
def represents_approved_model(artifact: Optional[dict]) -> bool:
    """Does this artifact claim to BE the current approved operational model?

    Two independent declarations, either of which is enough. The Release-30
    ``operational`` artifact carries both, so the guard below applies to it
    retroactively - which is the point: the contract has to bind the artifact
    that broke it, not only artifacts written after the rule existed.
    """
    art = artifact or {}
    if str(art.get("activation") or "") == OPERATIONAL_ACTIVATION:
        return True
    for blk in (art.get("horizons") or {}).values():
        if str((blk or {}).get("weighting_method") or "") == \
                "FROZEN_OPERATIONAL_CHAMPION_NO_FITTING":
            return True
    return False


def rank_identity(*, artifact: Optional[dict], block: Optional[dict]) -> dict:
    """Whether one horizon of an approved-model adapter preserves its ranking.

    Pure and total. For an artifact that is not an approved-model adapter the
    verdict is NOT_APPLICABLE and nothing is suppressed - a genuine research
    candidate is entitled to disagree with the incumbent in either direction,
    because it is not claiming to be the incumbent.
    """
    if not represents_approved_model(artifact):
        return {"applies": False, "verdict": RANK_IDENTITY_NOT_APPLICABLE,
                "disposition": HORIZON_APPLIED, "reasons": [], "slope": None}
    cal = (block or {}).get("calibration") or {}
    slope = _f(cal.get("slope"))
    state = str(cal.get("state") or "")
    reasons: list = []
    if state == CALIBRATION_NOT_CALIBRATED:
        reasons.append(SUPPRESSED_NOT_CALIBRATED)
    if slope is None:
        reasons.append(SUPPRESSED_NO_SLOPE)
    elif slope <= 0.0:
        reasons.append(SUPPRESSED_RANK_IDENTITY)
    verdict = (RANK_IDENTITY_VIOLATED
               if (slope is not None and slope <= 0.0)
               else (RANK_IDENTITY_PRESERVED if slope is not None
                     else RANK_IDENTITY_NOT_APPLICABLE))
    return {
        "applies": True,
        "contract": MODEL_IDENTITY_CONTRACT,
        "verdict": verdict,
        "disposition": HORIZON_SUPPRESSED if reasons else HORIZON_APPLIED,
        "reasons": sorted(set(reasons)),
        "slope": slope,
        "calibration_state": state or None,
        "doc": ("expected_excess_return = slope * standardised_score, and the "
                "standardisation of a positive-weight rank blend is strictly "
                "monotone, so a non-positive slope reverses the approved "
                "model's ranking rather than adjusting it"),
    }


def build_forecast(*, cross_section: dict, artifact: dict,
                   horizons=HORIZONS) -> dict:
    """The canonical forward-return forecast for one decision timestamp.

    Pure and deterministic: the same cross-section and the same artifact always
    produce the same payload, including every hash. Never raises on bad input -
    it returns a BLOCKED payload with named reasons.
    """
    art_check = validate_artifact(artifact)
    if not art_check["ok"]:
        return _blocked(cross_section, artifact, art_check["reasons"])
    in_check = validate_input(cross_section, artifact)
    if not in_check["ok"]:
        return _blocked(cross_section, artifact, in_check["reasons"])

    names = list(artifact["feature_names"])
    rows = list(cross_section.get("rows") or [])
    tickers = [str(r.get("ticker")) for r in rows]
    columns = {n: [_f((r.get("features") or {}).get(n)) for r in rows]
               for n in names}
    coverage = {n: sum(1 for v in columns[n] if v is not None) for n in names}
    normalised = {n: rank_normalise(columns[n]) for n in names}
    matrix = [[normalised[n][i] for n in names] for i in range(len(rows))]

    by_horizon: dict = {}
    warnings: list = []
    suppressed: list = []
    for h in horizons:
        blk = (artifact.get("horizons") or {}).get(str(int(h)))
        if not blk:
            warnings.append({"code": "HORIZON_NOT_IN_ARTIFACT", "horizon": int(h)})
            continue
        # Release 30.1: an approved-model adapter must preserve the approved
        # model's ranking. A horizon that does not carries its reasons and NO
        # expected return, so nothing downstream can allocate against it.
        identity = rank_identity(artifact=artifact, block=blk)
        if identity["disposition"] == HORIZON_SUPPRESSED:
            suppressed.append({"code": "HORIZON_SUPPRESSED", "horizon": int(h),
                               "rank_identity": identity["verdict"],
                               "reasons": identity["reasons"]})
            by_horizon[str(int(h))] = {
                "horizon_sessions": int(h),
                "disposition": HORIZON_SUPPRESSED,
                "rank_identity": identity,
                "member_ids": [], "weights": blk.get("weights") or {},
                "weighting_method": blk.get("weighting_method"),
                "calibration": {"slope": None,
                                "state": identity.get("calibration_state"),
                                "basis": (blk.get("calibration") or {}).get("basis"),
                                "n_rows": (blk.get("calibration") or {}).get("n_rows")},
                "training_cutoff": blk.get("training_cutoff"),
                "forecasts": [],
            }
            continue
        spec = blk["model"]
        cal = blk["calibration"]
        slope = _f(cal.get("slope")) or 0.0
        sigma = _f(cal.get("residual_sigma")) or 0.0
        scores = standardise(apply_model(spec, matrix, names))
        members = member_scores(spec, matrix, names)
        forecasts = []
        for i, tk in enumerate(tickers):
            s = scores[i]
            vals = [m[i] for m in members.values()]
            disagreement = _dispersion(vals)
            # Total uncertainty combines the dispersion the walk-forward
            # residuals actually showed with how much the ensemble's own members
            # disagree about THIS name. Both are measured; neither is a chosen
            # multiplier.
            unc = math.sqrt(sigma ** 2 + (abs(slope) * disagreement) ** 2)
            exp_excess = slope * s
            forecasts.append({
                "ticker": tk,
                "standardised_score": _r(s, 6),
                "expected_excess_return": _r(exp_excess, 6),
                "expected_return": _r(exp_excess + MARKET_BASELINE, 6),
                "forecast_uncertainty": _r(unc, 6),
                "downside_return_q05": _r(exp_excess - _Z05 * unc, 6),
                "member_disagreement": _r(disagreement, 6),
                "feature_coverage": _r(
                    sum(1 for n in names
                        if columns[n][i] is not None) / max(1, len(names)), 4),
            })
        forecasts.sort(key=lambda d: (-(d["expected_excess_return"] or 0.0),
                                      d["ticker"]))
        for rank, row in enumerate(forecasts, 1):
            row["rank"] = rank
        by_horizon[str(int(h))] = {
            "horizon_sessions": int(h),
            "disposition": HORIZON_APPLIED,
            "rank_identity": identity,
            "member_ids": sorted(members),
            "weights": blk.get("weights") or {},
            "weighting_method": blk.get("weighting_method"),
            "calibration": {"slope": _r(slope, 6),
                            "residual_sigma": _r(sigma, 6),
                            "basis": cal.get("basis"),
                            "n_rows": cal.get("n_rows")},
            "training_cutoff": blk.get("training_cutoff"),
            "forecasts": forecasts,
        }

    fsh = feature_snapshot_hash(cross_section)
    # Only an APPLIED horizon counts as a forecast. A payload whose every horizon
    # was suppressed is BLOCKED, not READY with an empty table.
    applied = sorted(int(k) for k, v in by_horizon.items()
                     if v.get("disposition", HORIZON_APPLIED) == HORIZON_APPLIED)
    state = STATE_READY if applied else STATE_BLOCKED
    if applied and (warnings or suppressed):
        state = STATE_DEGRADED
    return {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "state": state,
        "state_vocabulary": list(STATE_VOCAB),
        "decision_timestamp": cross_section.get("generated_at"),
        "eligible_market_date": cross_section.get("as_of_date"),
        "universe_size": len(rows),
        "target_quantity": TARGET_QUANTITY,
        "target_doc": TARGET_DOC,
        "market_baseline": MARKET_BASELINE,
        "market_baseline_policy": MARKET_BASELINE_POLICY,
        "market_baseline_doc": (
            "This layer forecasts cross-sectional differences only. The equity "
            "market's own level is NOT forecast, so the baseline every name is "
            "measured against - and the bar cash has to clear - is zero."),
        "feature_names": names,
        "feature_transform": FEATURE_TRANSFORM,
        "feature_coverage": coverage,
        "horizons": applied,
        "suppressed_horizons": sorted(int(b["horizon"]) for b in suppressed),
        "by_horizon": by_horizon,
        "represents_approved_model": represents_approved_model(artifact),
        "model_identity_contract": (MODEL_IDENTITY_CONTRACT
                                    if represents_approved_model(artifact) else None),
        "model_spec_hash": artifact.get("model_spec_hash"),
        "feature_snapshot_hash": fsh,
        "point_in_time_status": cross_section.get("point_in_time_status"),
        "point_in_time_controls": cross_section.get("point_in_time_controls") or [],
        "input_provenance": cross_section.get("provenance") or {},
        "blockers": suppressed,
        "warnings": warnings,
        "automatic_promotion_allowed": AUTOMATIC_PROMOTION_ALLOWED,
        "safety": {"badges": list(SAFETY_BADGES),
                   "creates_orders": False, "creates_decisions": False,
                   "mutates_holdings": False, "promotes_models": False},
    }


def _dispersion(values: list) -> float:
    vals = [v for v in values if _f(v) is not None]
    n = len(vals)
    if n < 2:
        return 0.0
    mean = sum(vals) / n
    return math.sqrt(sum((v - mean) ** 2 for v in vals) / (n - 1))


def _blocked(cross_section, artifact, reasons) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "state": STATE_BLOCKED,
        "state_vocabulary": list(STATE_VOCAB),
        "eligible_market_date": (cross_section or {}).get("as_of_date"),
        "target_quantity": TARGET_QUANTITY,
        "market_baseline": MARKET_BASELINE,
        "market_baseline_policy": MARKET_BASELINE_POLICY,
        "horizons": [],
        "suppressed_horizons": [],
        "by_horizon": {},
        "represents_approved_model": represents_approved_model(artifact),
        "model_identity_contract": (MODEL_IDENTITY_CONTRACT
                                    if represents_approved_model(artifact) else None),
        "universe_size": len((cross_section or {}).get("rows") or []),
        "model_spec_hash": (artifact or {}).get("model_spec_hash"),
        "feature_snapshot_hash": (feature_snapshot_hash(cross_section)
                                  if cross_section else None),
        "point_in_time_status": (cross_section or {}).get("point_in_time_status"),
        "blockers": list(reasons),
        "warnings": [],
        "automatic_promotion_allowed": AUTOMATIC_PROMOTION_ALLOWED,
        "safety": {"badges": list(SAFETY_BADGES),
                   "creates_orders": False, "creates_decisions": False,
                   "mutates_holdings": False, "promotes_models": False},
    }


def expected_returns(forecast: dict, horizon: int) -> dict:
    """``ticker -> expected excess return`` for one horizon, for the allocator."""
    blk = (forecast.get("by_horizon") or {}).get(str(int(horizon))) or {}
    return {r["ticker"]: r["expected_excess_return"]
            for r in (blk.get("forecasts") or [])
            if _f(r.get("expected_excess_return")) is not None}


def uncertainties(forecast: dict, horizon: int) -> dict:
    blk = (forecast.get("by_horizon") or {}).get(str(int(horizon))) or {}
    return {r["ticker"]: r["forecast_uncertainty"]
            for r in (blk.get("forecasts") or [])
            if _f(r.get("forecast_uncertainty")) is not None}


def downside(forecast: dict, horizon: int) -> dict:
    blk = (forecast.get("by_horizon") or {}).get(str(int(horizon))) or {}
    return {r["ticker"]: r["downside_return_q05"]
            for r in (blk.get("forecasts") or [])
            if _f(r.get("downside_return_q05")) is not None}
