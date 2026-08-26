"""alpha_agent.r46.ledger - THE canonical prospective prediction ledger.

One ledger for predictions, one for outcomes. Both are append-only and
chain-hashed with the canonical primitives from :mod:`api.paper_trading_desk`
- the same convention every desk ledger has used since Phase 27 - pointed at
the R46 research root. No second forward-ledger implementation is created
here, and no operational store is touched.

Three properties the rest of the release depends on:

**Immutable.** A prediction row's forecast fields are never updated. Maturity
and scoring append a row to the OUTCOME ledger keyed by ``prediction_id``;
the original forecast stays byte-identical and any rewrite breaks the sha256
chain.

**Idempotent.** Appending is keyed on
:data:`alpha_agent.r46.contract.PREDICTION_IDENTITY_KEY`. Running emission
twice in one session, or twice in one day, produces the second run's rows
zero times, and says so rather than silently succeeding.

**Complete.** A row missing any field of
:data:`alpha_agent.r46.contract.PREDICTION_RECORD_FIELDS` is refused. A
half-specified prediction cannot be prosecuted later, and a prediction that
cannot be prosecuted is not evidence.
"""
from __future__ import annotations

from pathlib import Path

from . import CAMPAIGN_ID, campaign_dir
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.ledger"

PREDICTION_LEDGER = "r46_forward_predictions.json"
OUTCOME_LEDGER = "r46_forward_outcomes.json"
LEDGERS = (PREDICTION_LEDGER, OUTCOME_LEDGER)

FORWARD_DIRNAME = "prospective_forward"


def _desk():
    from paper_trader.api import paper_trading_desk as desk
    return desk


def forward_dir(campaign_id: str = CAMPAIGN_ID) -> Path:
    d = campaign_dir(campaign_id) / FORWARD_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #
def predictions(campaign_id: str = CAMPAIGN_ID) -> list:
    return _desk()._read_ledger(forward_dir(campaign_id), PREDICTION_LEDGER)


def outcomes(campaign_id: str = CAMPAIGN_ID) -> list:
    return _desk()._read_ledger(forward_dir(campaign_id), OUTCOME_LEDGER)


def verify(campaign_id: str = CAMPAIGN_ID) -> dict:
    desk = _desk()
    d = forward_dir(campaign_id)
    reports = [desk.verify_ledger(d, f) for f in LEDGERS]
    return {"all_intact": all(r["intact"] for r in reports),
            "ledgers": reports,
            "primitives": "api.paper_trading_desk chain-hash ledgers "
                          "(canonical)"}


# --------------------------------------------------------------------------- #
# Keys
# --------------------------------------------------------------------------- #
def prediction_key(row: dict) -> tuple:
    return tuple(str(row.get(k)) for k in C.PREDICTION_IDENTITY_KEY)


def outcome_key(row: dict) -> tuple:
    return tuple(str(row.get(k)) for k in C.OUTCOME_IDENTITY_KEY)


def existing_prediction_keys(campaign_id: str = CAMPAIGN_ID) -> set:
    return {prediction_key(r) for r in predictions(campaign_id)}


def existing_outcome_keys(campaign_id: str = CAMPAIGN_ID) -> set:
    return {outcome_key(r) for r in outcomes(campaign_id)}


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
class LedgerRefusal(Exception):
    """Raised when a row may not enter the ledger. Never caught silently."""


def validate_prediction(row: dict) -> None:
    missing = [f for f in C.PREDICTION_RECORD_FIELDS if f not in row]
    if missing:
        raise LedgerRefusal(
            "prediction is missing required contract fields: %s"
            % ", ".join(sorted(missing)))
    if row.get("forward_evidence_type") != C.TRUE_FORWARD:
        raise LedgerRefusal(
            "this ledger holds TRUE_FORWARD rows only; got %r. Historical "
            "replay belongs in an artifact labelled HISTORICAL_SIMULATION."
            % row.get("forward_evidence_type"))
    if row.get("status") != C.STATUS_PENDING:
        raise LedgerRefusal(
            "a prediction enters the ledger as %s; got %r"
            % (C.STATUS_PENDING, row.get("status")))
    emitted = str(row.get("emitted_at_utc") or "")
    start = str(row.get("outcome_window_start_utc") or "")
    if not emitted or not start:
        raise LedgerRefusal("emitted_at_utc and outcome_window_start_utc are "
                            "both required to prove the ordering")
    if not emitted < start:
        raise LedgerRefusal(
            "REFUSED - not TRUE_FORWARD: emitted_at_utc %s is not strictly "
            "before outcome_window_start_utc %s. This is the one rule the "
            "release exists to enforce." % (emitted, start))
    cutoff = str(row.get("data_cutoff_utc") or "")
    if cutoff and not cutoff <= emitted:
        raise LedgerRefusal(
            "REFUSED - data_cutoff_utc %s is after emitted_at_utc %s"
            % (cutoff, emitted))
    if row.get("point_in_time_status") == C.PIT_VIOLATION:
        raise LedgerRefusal("REFUSED - row carries PIT_VIOLATION")


def validate_outcome(row: dict) -> None:
    for f in ("prediction_id", "challenger_id", "horizon", "scored_at_utc",
              "realised_net_return", "forward_evidence_type"):
        if f not in row:
            raise LedgerRefusal("outcome is missing required field %r" % f)
    if row.get("forward_evidence_type") != C.TRUE_FORWARD:
        raise LedgerRefusal("outcome rows score TRUE_FORWARD predictions only")


# --------------------------------------------------------------------------- #
# Appending
# --------------------------------------------------------------------------- #
def append_predictions(rows: list, campaign_id: str = CAMPAIGN_ID) -> dict:
    """Append new prediction rows. Duplicates are skipped, never overwritten."""
    seen = existing_prediction_keys(campaign_id)
    fresh, duplicates = [], []
    for row in rows:
        validate_prediction(row)
        k = prediction_key(row)
        if k in seen:
            duplicates.append({"key": list(k),
                               "prediction_id": row.get("prediction_id")})
            continue
        seen.add(k)
        fresh.append(row)
    appended = []
    if fresh:
        appended = _desk()._append_ledger(forward_dir(campaign_id),
                                          PREDICTION_LEDGER, fresh)
    return {"n_offered": len(rows), "n_appended": len(appended),
            "n_duplicates_skipped": len(duplicates),
            "duplicates": duplicates, "appended": appended,
            "idempotent": True}


def append_outcomes(rows: list, campaign_id: str = CAMPAIGN_ID) -> dict:
    seen = existing_outcome_keys(campaign_id)
    fresh, duplicates = [], []
    for row in rows:
        validate_outcome(row)
        k = outcome_key(row)
        if k in seen:
            duplicates.append({"prediction_id": row.get("prediction_id")})
            continue
        seen.add(k)
        fresh.append(row)
    appended = []
    if fresh:
        appended = _desk()._append_ledger(forward_dir(campaign_id),
                                          OUTCOME_LEDGER, fresh)
    return {"n_offered": len(rows), "n_appended": len(appended),
            "n_duplicates_skipped": len(duplicates),
            "duplicates": duplicates, "appended": appended,
            "idempotent": True}


# --------------------------------------------------------------------------- #
def summary(campaign_id: str = CAMPAIGN_ID) -> dict:
    preds = predictions(campaign_id)
    outs = outcomes(campaign_id)
    scored = {outcome_key(o)[0] for o in outs}
    by_challenger: dict = {}
    for p in preds:
        cid = p.get("challenger_id")
        e = by_challenger.setdefault(cid, {"emitted": 0, "scored": 0})
        e["emitted"] += 1
        if str(p.get("prediction_id")) in scored:
            e["scored"] += 1
    return {
        "schema": "r46_ledger_summary/1",
        "calculation_owner": CALCULATION_OWNER,
        "prediction_ledger": PREDICTION_LEDGER,
        "outcome_ledger": OUTCOME_LEDGER,
        "forward_dir": str(forward_dir(campaign_id)),
        "n_predictions": len(preds),
        "n_outcomes": len(outs),
        "n_pending": len(preds) - len(scored),
        "by_challenger": by_challenger,
        "chain": verify(campaign_id),
        "true_forward_only": True,
        "historical_observations_can_never_enter": True,
    }
