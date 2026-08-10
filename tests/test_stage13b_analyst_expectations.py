"""Targeted Stage 13B tests — prospective analyst forward-evidence program.

Pure/offline/deterministic: no provider contact, no credential read, no network,
no operational ledger. Covers: append-only immutability + idempotency, no
backdating (availability = capture stamp), horizon maturity (never early, never
substituted), PENDING-is-derived, refusal of absent historical data, provider
field-population handling, frozen signal formulas, execution lag, and Stage-13A
frozen-registry integrity via the readiness check.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from paper_trader.alpha_agent import analyst_revisions as AR

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / \
    "stage13b_analyst_expectations.py"
_spec = importlib.util.spec_from_file_location("stage13b_analyst_expectations",
                                               _SCRIPT)
S13B = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(S13B)


# --------------------------------------------------------------------------- #
# ledger append-only semantics
# --------------------------------------------------------------------------- #
def _consensus(sec="ngid:1", cap="2026-08-10T17:54:19+00:00", mean=1.0):
    return {"security_id": sec, "estimate_type": "EPS",
            "fiscal_period_end": "2026-12-31", "fiscal_period_type": "ANNUAL",
            "observation_timestamp": cap, "source_availability_timestamp": cap,
            "mean": mean, "prior_month_consensus": 0.9}


def test_append_many_idempotent_and_immutable(tmp_path):
    store = AR.RawObservationStore(tmp_path)
    r1 = store.append_many("CONSENSUS_SNAPSHOT", [_consensus(), _consensus()])
    assert r1["appended"] == 1 and r1["idempotent"] == 1
    r2 = store.append_many("CONSENSUS_SNAPSHOT", [_consensus()])
    assert r2["appended"] == 0 and r2["idempotent"] == 1
    # a CHANGED provider snapshot is a NEW record id, never an overwrite
    r3 = store.append_many("CONSENSUS_SNAPSHOT", [_consensus(mean=1.1)])
    assert r3["appended"] == 1
    # same id + different content trips the immutability guard
    rec = _consensus()
    rec["record_id"] = r1["record_ids"][0]
    rec["mean"] = 99.0
    with pytest.raises(AR.RevisionImmutabilityError):
        store.append("CONSENSUS_SNAPSHOT", rec)


def test_pending_is_never_storable(tmp_path):
    store = AR.RawObservationStore(tmp_path)
    rec = {"formation_record_id": "psf_x", "security_id": "ngid:1",
           "signal_family": "rev_eps_consensus_revision_magnitude",
           "snapshot_date": "2026-08-10", "horizon_sessions": 5,
           "status": "PENDING", "computed_timestamp": "2026-08-10T20:00:00+00:00"}
    with pytest.raises(ValueError, match="PENDING is derived"):
        store.append("FORWARD_OUTCOME", rec)


# --------------------------------------------------------------------------- #
# ingest: capture-stamp availability, refusal of unstamped/unidentified rows
# --------------------------------------------------------------------------- #
def _snapshot_row(cap, sec="ngid:42", assetid="42", date="2026-12-31",
                  fp="FY", mean=2.0):
    return {"retrieved_at": cap, "payload": {
        "_security_id": sec, "_norgate_assetid": assetid,
        "company": {"id": "com_x", "cik": "0000000042", "ticker": "TST"},
        "date": date, "fiscal_period": fp, "fiscal_year": 2026,
        "mean": mean, "median": mean, "high": mean, "low": mean, "count": 5,
        "mean_30_days_ago": mean - 0.1, "mean_90_days_ago": mean - 0.3}}


def _write_snapshot(root, feed, as_of, rows):
    d = root / "zacks_snapshots" / feed
    d.mkdir(parents=True, exist_ok=True)
    with (d / ("%s.jsonl" % as_of)).open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")


def test_ingest_uses_capture_stamp_and_refuses_unstamped(tmp_path):
    root = tmp_path / "research"
    cap = "2026-08-10T17:54:19+00:00"
    good = _snapshot_row(cap)
    unstamped = _snapshot_row(None)          # no retrieved_at -> refused
    unidentified = _snapshot_row(cap, sec=None)   # no canonical id -> refused
    _write_snapshot(root, "eps_estimates_universe", "2026-08-10",
                    [good, unstamped, unidentified])
    store = AR.RawObservationStore(tmp_path / "ledger")
    out = S13B.ingest_snapshot_day(store, root, "2026-08-10")
    feed = out["feeds"]["eps_estimates_universe"]
    assert feed["appended"] == 1 and feed["refused_rows"] == 2
    rec = store.all("CONSENSUS_SNAPSHOT")[0]
    # availability IS the capture stamp — never the fiscal date, never earlier
    assert rec["source_availability_timestamp"] == cap
    assert rec["fiscal_period_end"] != rec["source_availability_timestamp"]


def test_ingest_second_run_is_idempotent(tmp_path):
    root = tmp_path / "research"
    _write_snapshot(root, "eps_estimates_universe", "2026-08-10",
                    [_snapshot_row("2026-08-10T17:54:19+00:00")])
    store = AR.RawObservationStore(tmp_path / "ledger")
    S13B.ingest_snapshot_day(store, root, "2026-08-10")
    out2 = S13B.ingest_snapshot_day(store, root, "2026-08-10")
    feed = out2["feeds"]["eps_estimates_universe"]
    assert feed["appended"] == 0 and feed["idempotent_repeats"] == 1


# --------------------------------------------------------------------------- #
# frozen signal formulas
# --------------------------------------------------------------------------- #
def test_magnitude_formula_and_floor():
    assert S13B._magnitude({"mean": 1.1, "prior_month_consensus": 1.0}, 0.10) \
        == pytest.approx(0.1)
    # tiny denominator hits the floor instead of exploding
    assert S13B._magnitude({"mean": 0.02, "prior_month_consensus": 0.01}, 0.10) \
        == pytest.approx(0.1)
    assert S13B._magnitude({"mean": 1.0}, 0.10) is None


def test_acceleration_formula():
    rec = {"mean": 1.2, "prior_month_consensus": 1.0,
           "prior_quarter_consensus": 0.7}
    # r_recent = 0.2 ; prior per-30d rate = (1.0-0.7)/2 = 0.15 ; accel = 0.05
    assert S13B._acceleration(rec, 0.10) == pytest.approx(0.05)


def test_dispersion_change_requires_genuine_prior():
    now = {"security_id": "ngid:1", "estimate_type": "EPS",
           "fiscal_period_end": "2026-12-31", "mean": 2.0,
           "standard_deviation": 0.4}
    assert S13B._dispersion_change({}, now, 0.10) is None   # no prior -> None
    prior = dict(now, mean=2.0, standard_deviation=0.2,
                 source_availability_timestamp="2026-07-01T00:00:00+00:00")
    lookup = {("ngid:1", "EPS", "2026-12-31"): prior}
    # dispersion rose 0.1->0.2 => signal = -(0.2-0.1) = -0.1
    assert S13B._dispersion_change(lookup, now, 0.10) == pytest.approx(-0.1)


def test_breadth_family_declared_absent_when_fields_missing(tmp_path):
    root = tmp_path / "research"
    cap = "2026-08-10T17:54:19+00:00"
    _write_snapshot(root, "eps_estimates_universe", "2026-08-10",
                    [_snapshot_row(cap)])
    store = AR.RawObservationStore(tmp_path / "ledger")
    S13B.ingest_snapshot_day(store, root, "2026-08-10")
    sig = S13B.compute_day_signals(store, "2026-08-10")
    assert sig["rev_up_minus_down_breadth"]["status"] == \
        "FIELDS_ABSENT_FROM_ENTITLEMENT"
    assert sig["rev_joint_eps_revenue_confirmation"]["status"] == \
        "AWAITING_BOTH_LEGS"


# --------------------------------------------------------------------------- #
# execution lag + maturation
# --------------------------------------------------------------------------- #
SESSIONS = ["2026-08-10", "2026-08-11", "2026-08-12", "2026-08-13",
            "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19"]


def test_entry_is_strictly_after_availability():
    # availability ON a session date -> entry is the NEXT session
    assert SESSIONS[S13B._entry_index(SESSIONS, "2026-08-10")] == "2026-08-11"
    assert S13B._entry_index(SESSIONS, "2026-08-19") is None


def _formation(store, sec="ngid:7", sat="2026-08-10T17:54:19+00:00"):
    rec = {"security_id": sec,
           "signal_family": "rev_eps_consensus_revision_magnitude",
           "signal_value": 0.5, "formation_rank": 1, "breadth": 30,
           "snapshot_date": "2026-08-10", "formation_timestamp": sat,
           "source_availability_timestamp": sat, "provider": "intrinio_zacks"}
    rec["record_id"] = AR.compute_formation_id(rec)
    store.append("PROSPECTIVE_SIGNAL_FORMATION", rec)
    return rec


def test_no_early_maturation(tmp_path):
    store = AR.RawObservationStore(tmp_path)
    _formation(store)
    # only 6 sessions after entry exist -> even the 5d horizon needs entry+5
    panel = {"7": [(d, 100.0) for d in SESSIONS]}
    counts = S13B.mature_outcomes(store, panel, SESSIONS[:3])
    assert counts.get("matured") is None and counts["pending"] == 3
    assert store.all("FORWARD_OUTCOME") == []          # nothing stored early


def test_maturation_return_and_idempotency(tmp_path):
    store = AR.RawObservationStore(tmp_path)
    _formation(store)
    prices = {d: 100.0 + i for i, d in enumerate(SESSIONS)}
    panel = {"7": list(prices.items())}
    counts = S13B.mature_outcomes(store, panel, SESSIONS)
    # entry 08-11 (101.0), exit 5 sessions later 08-18 (106.0)
    assert counts["matured"] == 1 and counts["pending"] == 2
    out = store.all("FORWARD_OUTCOME")[0]
    assert out["status"] == "MATURED" and out["horizon_sessions"] == 5
    assert out["entry_session"] == "2026-08-11"
    assert out["exit_session"] == "2026-08-18"
    assert out["forward_return"] == pytest.approx(106.0 / 101.0 - 1.0)
    again = S13B.mature_outcomes(store, panel, SESSIONS)
    assert again.get("matured") is None and again["already_stored"] == 1


def test_missing_price_path_is_unavailable_not_substituted(tmp_path):
    store = AR.RawObservationStore(tmp_path)
    _formation(store, sec="ngid:404")          # no prices for this asset
    panel = {"7": [(d, 100.0) for d in SESSIONS]}
    counts = S13B.mature_outcomes(store, panel, SESSIONS)
    assert counts["unavailable"] == 1
    out = [r for r in store.all("FORWARD_OUTCOME") if r["horizon_sessions"] == 5][0]
    assert out["status"] == "UNAVAILABLE" and "forward_return" not in out


def test_status_derives_pending_from_absence(tmp_path):
    store = AR.RawObservationStore(tmp_path)
    _formation(store)
    doc = S13B.derive_status(store, SESSIONS)
    fam = doc["horizon_status_per_family"][
        "rev_eps_consensus_revision_magnitude"]
    assert fam == {"h5_PENDING": 1, "h20_PENDING": 1, "h63_PENDING": 1}


# --------------------------------------------------------------------------- #
# Track B: historical refusal + registry integrity
# --------------------------------------------------------------------------- #
def test_historical_import_refuses_absent_data():
    res = S13B.historical_import(r"C:\nonexistent-x\no_history.json",
                                 "ESTIMATE_REVISION_EVENT")
    assert res["status"] == "HISTORICAL_DATA_ABSENT"


def test_historical_readiness_reports_frozen_registry_intact():
    doc = S13B.historical_readiness()
    assert doc["registry_intact"] is True
    assert doc["registry_problems"] == []
    assert doc["importer_refuses_absent_data"] is True


def test_formation_and_outcome_ids_deterministic_and_distinct():
    base = {"security_id": "ngid:1", "signal_family": "f",
            "snapshot_date": "2026-08-10",
            "source_availability_timestamp": "2026-08-10T17:00:00+00:00"}
    a = AR.compute_formation_id(base)
    assert a == AR.compute_formation_id(dict(base))
    # a SECOND same-day snapshot (different capture stamp) is a NEW observation
    b = AR.compute_formation_id(dict(
        base, source_availability_timestamp="2026-08-10T21:00:00+00:00"))
    assert a != b
    o1 = AR.compute_outcome_id({"formation_record_id": a, "horizon_sessions": 5})
    o2 = AR.compute_outcome_id({"formation_record_id": a, "horizon_sessions": 20})
    assert o1 != o2
