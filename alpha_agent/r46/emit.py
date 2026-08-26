"""alpha_agent.r46.emit - put predictions on the record before the outcome exists.

One batch per emission. Each prediction is built from a frozen specification
over data whose cutoff is stated on the row, stamped with the instant it was
emitted, and refused by the ledger unless

    emitted_at_utc  <  outcome_window_start_utc

holds strictly. The entry rule is deliberately conservative (see
:data:`alpha_agent.r46.contract.ENTRY_RULE`): the next trading day after the
emission's Eastern date, for every instrument. Nothing about that ordering
depends on when a particular venue happens to settle.

Emission is idempotent on
``(challenger_id, challenger_version, instrument, effective_as_of, horizon)``.
Running it twice in a session appends nothing the second time and says so.

What a row does NOT claim: these are transparent rules, not calibrated return
forecasts, so ``expected_return`` is ``None`` and ``expected_return_state`` is
``NOT_CALIBRATED``. The cost IS known in advance and is a number. The judge
will supply the realised return; the model does not get to guess it first and
be graded generously against its own guess.
"""
from __future__ import annotations

import datetime as _dt

from . import CAMPAIGN_ID, artifact_body, sha, write_json
from . import campaign_dir
from . import challengers as CH
from . import clock as CK
from . import contract as C
from . import ledger as LG
from . import marketdata as MD
from . import registry as RG

CALCULATION_OWNER = "alpha_agent.r46.emit"

BATCH_ARTIFACT = "R46_FORWARD_BATCHES.json"

#: Release 46.2 - the forward-only sub-second stamp contract. Rows emitted from this
#: release onward carry ``*_precise`` companions to the frozen whole-second fields.
#: Existing rows are never rewritten and never acquire them.
TIMESTAMP_PRECISION_CONTRACT = "r46_2_timestamp_precision/1"

#: Release 46.2 - the stable NON-EMISSION reason vocabulary. Every reason names ONE
#: challenger's condition. None of them is a tournament-level blocker: a challenger
#: that cannot emit today is recorded and skipped, and the others still emit.
REASON_DATA_BLOCKED = "DATA_BLOCKED"
REASON_BUILD_FAILED = "CHALLENGER_BUILD_FAILED"
REASON_FLAT = "FLAT_NO_POSITION"
REASON_NO_CUTOFF = "NO_DATA_CUTOFF"
NON_EMISSION_REASONS = (REASON_DATA_BLOCKED, REASON_BUILD_FAILED, REASON_FLAT,
                        REASON_NO_CUTOFF)


def batch_id(emitted_at: _dt.datetime) -> str:
    return "r46b_" + emitted_at.astimezone(_dt.timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ")


def prediction_id(challenger_id: str, version: str, instrument: str,
                  effective_as_of: str, horizon: int) -> str:
    return "r46p_" + sha({"c": challenger_id, "v": version, "i": instrument,
                          "a": effective_as_of, "h": int(horizon)})[:20]


def _data_cutoff(book: dict, spec: dict):
    """The latest session any of this challenger's inputs actually saw."""
    owner = spec["signal_owner"]
    probe = {
        "_eq_cross_section": ("SPY",),
        "_futures_trend": ("&ES", "&ZN", "&CL"),
        "_fx_cross_section": ("EURUSD",),
        "_vx_carry": ("&VX", "$VIX"),
        "_rates_rv": ("&ZN", "&ZT"),
        "_commodity_cross_section": ("&CL", "&GC"),
        "_index_trend": ("SPY",),
        # Release 46.3 expansion owners.
        "_eq_xs_lottery": ("SPY",),
        "_eq_xs_illiquidity": ("SPY",),
        "_eq_xs_seasonal": ("SPY",),
        "_futures_xs_momentum": ("&ES", "&ZN", "&CL"),
        "_commodity_curve_carry": ("&CL", "&GC"),
        "_rates_macro_curve": ("&ZN", "%10YTCM"),
        "_spx_turn_of_month": ("SPY",),
        "_eq_xs_ensemble": ("SPY",),
        "_ml_eq_cross_section": ("SPY",),
    }.get(owner, ("SPY",))
    seen = [MD.last_session(s) for s in probe]
    seen = [d for d in seen if d is not None]
    return max(seen) if seen else None


def build_batch(campaign_id: str = CAMPAIGN_ID, registry: dict = None,
                emitted_at: _dt.datetime = None, specs=None) -> dict:
    """Construct - but do not persist - one forward prediction batch."""
    now = emitted_at or CK.now_utc()
    reg = registry if registry is not None else RG.load(campaign_id)
    candidates = RG.active_specs(reg, specs)

    entry_date = CK.entry_session_date(now)
    window_start = CK.outcome_window_start_utc(entry_date)
    bid = batch_id(now)

    rows, skipped = [], []
    # Release 46.2 - CHALLENGER ISOLATION. Every challenger is built inside its own
    # try/except. Before this, one rule raising (a missing symbol, a provider hiccup,
    # a division on an empty window) aborted the WHOLE batch, so a single broken
    # competitor could silently stop nine healthy ones from putting anything on the
    # record - and the tournament would look quiet rather than broken. A failure is
    # now that challenger's own reason and nothing else's.
    blocked_ids = {c["challenger_id"] for c in (reg.get("challengers") or ())
                   if c.get("state") == C.DATA_BLOCKED}
    for cid in sorted(blocked_ids):
        entry = RG.entry_for(reg, cid) or {}
        skipped.append({"challenger_id": cid, "reason": REASON_DATA_BLOCKED,
                        "detail": entry.get("blocked_reason")
                                  or "the registry marks this challenger's data path "
                                     "as unable to accrue; other challengers are "
                                     "unaffected"})
    for spec in candidates:
        entry = RG.entry_for(reg, spec["challenger_id"]) or {}
        try:
            book = CH.build(spec)
            cutoff = _data_cutoff(book, spec)
        except Exception as exc:                # noqa: BLE001 - isolation is the point
            skipped.append({"challenger_id": spec["challenger_id"],
                            "reason": REASON_BUILD_FAILED,
                            "detail": "%s: %s" % (type(exc).__name__,
                                                  str(exc)[:180]),
                            "isolated": True})
            continue
        if book.get("state") != "OK":
            skipped.append({"challenger_id": spec["challenger_id"],
                            "reason": book.get("state"),
                            "detail": "the rule produced no book for this "
                                      "session; other challengers are "
                                      "unaffected"})
            continue
        if not book.get("legs"):
            skipped.append({"challenger_id": spec["challenger_id"],
                            "reason": REASON_FLAT,
                            "detail": "the frozen rule says hold nothing "
                                      "today; this is a valid decision, not "
                                      "a failure, and no row is emitted "
                                      "because there is nothing to score"})
            continue
        if cutoff is None:
            skipped.append({"challenger_id": spec["challenger_id"],
                            "reason": REASON_NO_CUTOFF,
                            "detail": "no owned session could be observed for this "
                                      "challenger's inputs, so nothing states what "
                                      "the rule actually saw"})
            continue

        cost_bps = CH.expected_cost_bps(book, spec)
        gross = float(book.get("gross_notional") or 0.0)
        net_notional = float(book.get("net_notional") or 0.0)
        direction = ("LONG" if net_notional > 1e-9 else
                     "SHORT" if net_notional < -1e-9 else "MARKET_NEUTRAL")

        for horizon in spec["horizons"]:
            eff = str(entry_date)
            pid = prediction_id(spec["challenger_id"],
                               spec["challenger_version"],
                               spec["instrument"], eff, horizon)
            rf = MD.risk_free_per_session(horizon)
            rf_state = MD.risk_free_annual().get("state")
            rows.append({
                "prediction_id": pid,
                "batch_id": bid,

                "challenger_id": spec["challenger_id"],
                "challenger_version": spec["challenger_version"],
                "challenger_spec_hash": CH.spec_hash(spec),

                "emitted_at_utc": CK.iso(now),
                "emitted_market_timestamp": str(CK.to_eastern(now)),
                "effective_as_of": eff,
                "outcome_window_start_utc": CK.iso(window_start),
                "data_cutoff_utc": CK.iso(now),
                "data_cutoff_session": str(cutoff),

                # --- Release 46.2 timestamp precision (FORWARD-ONLY) ---------- #
                # The frozen whole-second fields above are unchanged, so every
                # existing row keeps its bytes and every existing reader keeps
                # working. These additions make "the specification was frozen
                # before this prediction was emitted" a NUMERIC comparison rather
                # than an argument about a shared second - the exact ambiguity
                # Release 46.1 disclosed in the first batch. A legacy row simply
                # does not carry them, and is read as WHOLE_SECOND resolution.
                "emitted_at_utc_precise": CK.iso_precise(now),
                "data_cutoff_utc_precise": CK.iso_precise(now),
                "outcome_window_start_utc_precise": CK.iso_precise(window_start),
                "timestamp_precision": "MICROSECOND",
                "timestamp_precision_contract": TIMESTAMP_PRECISION_CONTRACT,
                "freeze_before_emission_evidence": CK.ordering_evidence(
                    entry.get("frozen_at_precise") or entry.get("frozen_at"),
                    CK.iso_precise(now)),

                "asset_class": spec["asset_class"],
                "instrument": spec["instrument"],
                "instrument_identity": {
                    "kind": spec["prediction_type"],
                    "legs": [l["instrument"] for l in book["legs"]],
                    "universe": spec["universe"],
                },
                "venue": "OWNED_NORGATE_CONSOLIDATED",

                "prediction_type": spec["prediction_type"],
                "horizon": int(horizon),
                "horizon_unit": "ELIGIBLE_SESSIONS",
                "horizon_end_expected": str(
                    CK.expected_maturity_date(entry_date, horizon)),

                "direction": direction,
                "expected_return": None,
                "expected_return_state": "NOT_CALIBRATED",
                "expected_residual_return": None,
                "probability": None,
                "confidence": None,

                "benchmark": spec["benchmark"],
                "control": spec["control"],
                "hedge_definition": spec.get("hedge_definition"),

                "expected_cost": round(cost_bps, 6),
                "expected_cost_unit": "BPS_OF_GROSS_NOTIONAL_ENTRY_SIDE",
                "expected_financing": (None if rf is None
                                       else round(rf * 1e4, 6)),
                "expected_financing_state": rf_state,
                "expected_slippage": round(
                    C.SLIPPAGE_BPS_PER_SIDE * gross, 6),
                "expected_net_return": None,

                "position_expression": {
                    "legs": book["legs"],
                    "gross_notional": gross,
                    "net_notional": net_notional,
                    "construction": spec["prediction_type"],
                },
                "n_legs": int(book.get("n_legs") or 0),
                "gross_notional": gross,
                "max_notional": None,

                "model_id": spec["challenger_id"],
                "model_family": spec["family"],
                "model_parameters_hash": CH.parameters_hash(spec),
                "feature_set_hash": CH.feature_set_hash(spec),
                # Release 46.3 - an ML challenger states the last training
                # decision date it fit on; a transparent rule has none.
                "training_data_cutoff": book.get("training_data_cutoff"),

                "market_state_snapshot_hash":
                    book.get("market_state_snapshot_hash"),
                "input_evidence_hash": book.get("input_evidence_hash"),

                "point_in_time_status": C.PIT_OK,
                "forward_evidence_type": C.TRUE_FORWARD,
                "status": C.STATUS_PENDING,
                "provenance": {
                    "calculation_owner": CALCULATION_OWNER,
                    "signal_owner": "%s.%s" % (CH.CALCULATION_OWNER,
                                               spec["signal_owner"]),
                    "registry_frozen_at": entry.get("frozen_at"),
                    "registry_frozen_at_precise": entry.get("frozen_at_precise"),
                    "contract_hash": C.contract_hash(),
                    "entry_rule": C.ENTRY_RULE["id"],
                    "data_source": "owned Norgate local (NDU), adjusted "
                                   "daily bars",
                    "risk_free": MD.risk_free_annual(),
                    "parameters_were_searched": False,
                },
            })

    return {
        "batch_id": bid,
        "emitted_at_utc": CK.iso(now),
        "emitted_market_timestamp": str(CK.to_eastern(now)),
        "entry_session_date": str(entry_date),
        "outcome_window_start_utc": CK.iso(window_start),
        "is_true_forward": CK.is_true_forward(now, entry_date),
        "entry_rule": C.ENTRY_RULE,
        "n_candidates": len(candidates),
        "n_predictions": len(rows),
        "rows": rows,
        "skipped": skipped,
    }


def emit(campaign_id: str = CAMPAIGN_ID, registry: dict = None,
         emitted_at: _dt.datetime = None, specs=None) -> dict:
    """Build a batch and append it. Idempotent; never backdates."""
    batch = build_batch(campaign_id, registry, emitted_at, specs)

    if not batch["is_true_forward"]:
        return dict(batch, state="REFUSED_NOT_TRUE_FORWARD",
                    n_appended=0,
                    reason="the emission instant is not strictly before the "
                           "outcome window start; no row may be written")

    result = LG.append_predictions(batch["rows"], campaign_id)
    horizons = sorted({r["horizon"] for r in batch["rows"]})
    maturities = sorted({r["horizon_end_expected"] for r in batch["rows"]})
    body = {
        "state": "EMITTED" if result["n_appended"] else "NO_NEW_PREDICTIONS",
        "batch_id": batch["batch_id"],
        "emitted_at_utc": batch["emitted_at_utc"],
        "emitted_market_timestamp": batch["emitted_market_timestamp"],
        "entry_session_date": batch["entry_session_date"],
        "outcome_window_start_utc": batch["outcome_window_start_utc"],
        "is_true_forward": True,
        "n_candidates": batch["n_candidates"],
        "n_offered": result["n_offered"],
        "n_appended": result["n_appended"],
        "n_duplicates_skipped": result["n_duplicates_skipped"],
        "duplicates": result["duplicates"],
        "skipped_challengers": batch["skipped"],
        "challengers": sorted({r["challenger_id"] for r in batch["rows"]}),
        "asset_classes": sorted({r["asset_class"] for r in batch["rows"]}),
        "horizons": horizons,
        "earliest_expected_maturity": maturities[0] if maturities else None,
        "latest_expected_maturity": maturities[-1] if maturities else None,
        "idempotent": True,
        "backdated": False,
    }
    _record_batch(campaign_id, body)
    return body


def _record_batch(campaign_id: str, body: dict) -> None:
    from . import read_json
    p = campaign_dir(campaign_id) / BATCH_ARTIFACT
    prior = read_json(p, default=None) or {}
    batches = list(prior.get("batches") or [])
    if body.get("n_appended"):
        batches.append({k: v for k, v in body.items() if k != "duplicates"})
    art = artifact_body("r46_forward_batches/1", CALCULATION_OWNER,
                        n_batches=len(batches),
                        first_batch=batches[0] if batches else None,
                        latest_batch=batches[-1] if batches else None,
                        batches=batches)
    write_json(p, art)


def maturity_schedule(campaign_id: str = CAMPAIGN_ID) -> dict:
    """When every pending prediction is expected to become scoreable."""
    preds = LG.predictions(campaign_id)
    scored = {str(o.get("prediction_id")) for o in LG.outcomes(campaign_id)}
    pending = [p for p in preds if str(p.get("prediction_id")) not in scored]
    by_date: dict = {}
    for p in pending:
        d = str(p.get("horizon_end_expected"))
        e = by_date.setdefault(d, {"expected_maturity_date": d, "n": 0,
                                   "challengers": [], "horizons": []})
        e["n"] += 1
        if p["challenger_id"] not in e["challengers"]:
            e["challengers"].append(p["challenger_id"])
        if p["horizon"] not in e["horizons"]:
            e["horizons"].append(p["horizon"])
    schedule = [by_date[k] for k in sorted(by_date)]
    for e in schedule:
        e["challengers"].sort()
        e["horizons"].sort()
    return artifact_body(
        "r46_maturity_schedule/1", CALCULATION_OWNER,
        n_pending=len(pending),
        n_scored=len(scored),
        earliest_maturity=schedule[0]["expected_maturity_date"]
                          if schedule else None,
        next_material_evidence_time=schedule[0]["expected_maturity_date"]
                                    if schedule else None,
        latest_maturity=schedule[-1]["expected_maturity_date"]
                        if schedule else None,
        schedule=schedule,
        note="expected dates count weekdays; the judge always counts the "
             "instrument's realised sessions, so a holiday moves a maturity "
             "later and never earlier",
    )
