#!/usr/bin/env python
r"""Stage 13B — analyst-expectations prospective forward-evidence program.

TRACK A (prospective): normalizes the immutable Zacks trial snapshots captured by
``scripts/intrinio_trial_acquire.py`` into the RELEASED Stage-13A contracts
(``alpha_agent.analyst_revisions``) inside ONE research-only append-only ledger,
forms the pre-registered analyst signals from provider-embedded prior-consensus
fields, and matures forward outcomes ONLY when enough eligible owned sessions
have actually passed.

TRACK B (historical readiness): exposes the Stage-13A ``LocalTrialImporter`` +
``pit_scan`` + adequacy pipeline behind an explicit command that REFUSES to run
when no historical file exists — genuine historical Zacks revision data does not
exist in this entitlement and is NEVER fabricated, reconstructed, or backdated.

Scientific invariants (enforced, tested):
  * availability of every prospective observation = the actual capture
    timestamp (``retrieved_at``); NEVER the fiscal date, NEVER backdated;
  * repeated same-day identical provider state is idempotent (content-hash
    record ids); changed provider state appends NEW records (immutability
    guard: a same-id different-content append raises);
  * PENDING is DERIVED (absence of an outcome record), never stored;
  * a horizon matures only when the OWNED price panel contains the entry
    session plus ``horizon`` further sessions — no early maturation, no
    horizon substitution (UNAVAILABLE is an explicit terminal, per horizon);
  * TRIAL-licensed rows live ONLY under the research root — this module never
    touches an operational ledger, book, order, target, or NAV store.

Signal operationalizations are FROZEN here BEFORE any forward outcome matures
(see ``FROZEN_PROSPECTIVE_FAMILIES``); families whose required provider fields
are absent from this entitlement are declared UNAVAILABLE, not approximated.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import analyst_revisions as _ar          # noqa: E402
from paper_trader.alpha_agent.historical_identity import IdentityStore  # noqa: E402
from paper_trader.alpha_agent.historical_price_panel import (           # noqa: E402
    build_assetid_price_panel)

IDENTITY_DB = Path(r"D:\Stock_Prediction_app_data\alpha_agent\identity\historical_identity.sqlite")
INGESTION_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion")
HORIZONS = (5, 20, 63)
PROVIDER = "intrinio_zacks"
MIN_BREADTH = 20          # frozen Stage-13A per-cohort breadth floor

# --------------------------------------------------------------------------- #
# FROZEN prospective operationalizations of the Stage-13A hypothesis family.
# Frozen 2026-08-10 BEFORE any forward outcome exists. ``basis`` names the
# provider-embedded prior-consensus fields used — all genuinely available at the
# snapshot timestamp (no reconstruction). Scale floors stop tiny-denominator
# blowups (EPS in dollars; revenue in provider units, typically $MM).
# Period rule (frozen): the nearest FUTURE fiscal period of the required type
# with the required fields populated; ANNUAL is primary (measured population
# 31.6% of members vs 27.0% quarterly on the live trial).
# --------------------------------------------------------------------------- #
FROZEN_PROSPECTIVE_FAMILIES = {
    "rev_eps_consensus_revision_magnitude": {
        "estimate_type": "EPS", "direction": 1, "scale_floor": 0.10,
        "basis": "(mean - prior_month_consensus) / max(|prior_month_consensus|, floor)",
        "requires": ("mean", "prior_month_consensus"),
    },
    "rev_revenue_consensus_revision_magnitude": {
        "estimate_type": "REVENUE", "direction": 1, "scale_floor": 1.0,
        "basis": "(mean - prior_month_consensus) / max(|prior_month_consensus|, floor)",
        "requires": ("mean", "prior_month_consensus"),
    },
    "rev_revision_acceleration": {
        "estimate_type": "EPS", "direction": 1, "scale_floor": 0.10,
        "basis": "30d revision rate minus the prior 30-90d per-30d revision rate, "
                 "same scale (all three consensus vintages provider-embedded)",
        "requires": ("mean", "prior_month_consensus", "prior_quarter_consensus"),
    },
    "rev_joint_eps_revenue_confirmation": {
        "estimate_type": "JOINT", "direction": 1, "scale_floor": None,
        "basis": "rank(d_EPS) + rank(d_REVENUE), zeroed when signs disagree",
        "requires": (),   # derived from the two magnitude families
    },
    "rev_up_minus_down_breadth": {
        "estimate_type": "EPS", "direction": 1, "scale_floor": None,
        "basis": "(up - down) / analyst_count",
        "requires": ("upward_revision_count", "downward_revision_count"),
    },
    "rev_analyst_dispersion_change": {
        "estimate_type": "EPS", "direction": -1, "scale_floor": 0.10,
        "basis": "-d(std/|mean|) vs the latest ledger snapshot >= 21 calendar "
                 "days older (cross-snapshot; needs an accumulated ledger)",
        "requires": ("mean", "standard_deviation"),
    },
}

SNAPSHOT_FEEDS = {
    # feed dir -> estimate_type (estimates feeds only; surprises stay evidence-
    # only in their immutable snapshot files)
    "eps_estimates_universe": "EPS",
    "sales_estimates_universe": "REVENUE",
}


def _load_cfg() -> dict:
    p = _REPO / "configs" / "alpha_agent" / "intrinio_trial.json"
    return json.loads(p.read_text(encoding="utf-8"))


def _research_root(cfg: dict) -> Path:
    return Path(cfg.get("research_root")
                or r"D:\Stock_Prediction_app_data\provider_trials\intrinio")


def ledger_root(cfg: dict | None = None) -> Path:
    return _research_root(cfg or _load_cfg()) / "stage13b_prospective_ledger"


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _fpt(payload: dict) -> str:
    fp = str(payload.get("fiscal_period") or "")
    return "QUARTERLY" if fp.upper().startswith("Q") else "ANNUAL"


# --------------------------------------------------------------------------- #
# INGEST — snapshot JSONL -> CONSENSUS_SNAPSHOT + SECURITY_IDENTITY records.
# --------------------------------------------------------------------------- #
def ingest_snapshot_day(store: _ar.RawObservationStore, research_root: Path,
                        as_of: str) -> dict:
    """Idempotently append the day's captured estimate snapshots to the ledger.
    Availability := the file's real ``retrieved_at`` capture stamp. Refuses any
    row without one (nothing is ever backdated)."""
    out = {"as_of": as_of, "feeds": {}}
    for feed, est_type in SNAPSHOT_FEEDS.items():
        p = research_root / "zacks_snapshots" / feed / ("%s.jsonl" % as_of)
        if not p.is_file():
            out["feeds"][feed] = {"status": "SNAPSHOT_ABSENT"}
            continue
        refused = 0
        recs, identities = [], {}
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            pay = row.get("payload") or {}
            cap = row.get("retrieved_at")
            sec = pay.get("_security_id")
            if not cap or not sec:
                refused += 1      # no capture stamp / no canonical identity
                continue
            comp = pay.get("company") or {}
            rec = {
                "security_id": sec,
                "estimate_type": est_type,
                "fiscal_period_end": pay.get("date"),
                "fiscal_period_type": _fpt(pay),
                "observation_timestamp": cap,
                "source_availability_timestamp": cap,
                "mean": pay.get("mean"), "median": pay.get("median"),
                "high": pay.get("high"), "low": pay.get("low"),
                "standard_deviation": pay.get("standard_deviation"),
                "analyst_count": pay.get("count"),
                "prior_week_consensus": pay.get("mean_7_days_ago"),
                "prior_month_consensus": pay.get("mean_30_days_ago"),
                "prior_quarter_consensus": pay.get("mean_90_days_ago"),
            }
            rec = {k: v for k, v in rec.items() if v is not None}
            if not rec.get("fiscal_period_end"):
                refused += 1
                continue
            recs.append(rec)
            if sec not in identities:
                ident = {
                    "provider_security_id": str(comp.get("id") or ""),
                    "stable_assetid": str(pay.get("_norgate_assetid") or ""),
                    "cik": comp.get("cik"),
                    "historical_ticker": comp.get("ticker"),
                    "effective_start": cap,
                    "status": "ACTIVE",
                    "source_provider": PROVIDER,
                }
                ident = {k: v for k, v in ident.items() if v not in (None, "")}
                if ident.get("provider_security_id"):
                    identities[sec] = ident
        res = store.append_many("CONSENSUS_SNAPSHOT", recs)
        store.append_many("SECURITY_IDENTITY", list(identities.values()))
        out["feeds"][feed] = {"status": "INGESTED", "appended": res["appended"],
                              "idempotent_repeats": res["idempotent"],
                              "refused_rows": refused,
                              "identities": len(identities)}
    return out


# --------------------------------------------------------------------------- #
# FORM — frozen signal formation from the day's consensus records.
# --------------------------------------------------------------------------- #
def _front_period(rows: list, as_of: str, period_type: str) -> dict:
    """{security_id: consensus record} for the nearest FUTURE fiscal period of
    ``period_type`` (frozen period rule)."""
    best: dict = {}
    for r in rows:
        if r.get("fiscal_period_type") != period_type:
            continue
        fpe = str(r.get("fiscal_period_end") or "")
        if fpe[:10] < as_of:
            continue
        sec = r["security_id"]
        if sec not in best or fpe < str(best[sec].get("fiscal_period_end")):
            best[sec] = r
    return best


def _magnitude(rec: dict, floor: float) -> float | None:
    mean = rec.get("mean")
    prior = rec.get("prior_month_consensus")
    if mean is None or prior is None:
        return None
    return (float(mean) - float(prior)) / max(abs(float(prior)), floor)


def _acceleration(rec: dict, floor: float) -> float | None:
    mean, p30, p90 = (rec.get("mean"), rec.get("prior_month_consensus"),
                      rec.get("prior_quarter_consensus"))
    if mean is None or p30 is None or p90 is None:
        return None
    s = max(abs(float(p30)), floor)
    r_recent = (float(mean) - float(p30)) / s
    r_prior_per30 = ((float(p30) - float(p90)) / 2.0) / s
    return r_recent - r_prior_per30


def _prior_snapshot_lookup(all_rows: list, as_of: str) -> dict:
    """{(security, type, period_end): latest consensus captured >= 21 calendar
    days before as_of} — the frozen dispersion-change basis. Prospective-only:
    empty until the ledger has genuinely accumulated old-enough snapshots."""
    cutoff = (_dt.date.fromisoformat(as_of) - _dt.timedelta(days=21)).isoformat()
    prior: dict = {}
    for r in all_rows:
        if (str(r.get("source_availability_timestamp") or "")[:10] <= cutoff
                and r.get("mean") is not None
                and r.get("standard_deviation") is not None):
            k = (r.get("security_id"), r.get("estimate_type"),
                 r.get("fiscal_period_end"))
            if k not in prior or str(r["source_availability_timestamp"]) > \
                    str(prior[k]["source_availability_timestamp"]):
                prior[k] = r
    return prior


def _dispersion_change(prior_lookup: dict, rec: dict, floor: float) -> float | None:
    """-d(std/|mean|) vs the latest ledger snapshot >= 21 calendar days older for
    the SAME (security, period). Never reconstructed."""
    mean, std = rec.get("mean"), rec.get("standard_deviation")
    if mean is None or std is None:
        return None
    prior = prior_lookup.get((rec.get("security_id"), rec.get("estimate_type"),
                              rec.get("fiscal_period_end")))
    if prior is None:
        return None
    disp_now = float(std) / max(abs(float(mean)), floor)
    disp_prior = float(prior["standard_deviation"]) / max(abs(float(prior["mean"])), floor)
    return -(disp_now - disp_prior)


def compute_day_signals(store: _ar.RawObservationStore, as_of: str) -> dict:
    """Cross-sectional signal values per frozen family for one snapshot day.
    Returns {family: {"status": ..., "values": {sec: value}, "records": {sec: rec}}}."""
    all_rows = store.all("CONSENSUS_SNAPSHOT")
    rows = [r for r in all_rows
            if str(r.get("source_availability_timestamp") or "")[:10] == as_of]
    prior_lookup = _prior_snapshot_lookup(all_rows, as_of)
    eps = _front_period([r for r in rows if r.get("estimate_type") == "EPS"],
                        as_of, "ANNUAL")
    rev = _front_period([r for r in rows if r.get("estimate_type") == "REVENUE"],
                        as_of, "ANNUAL")
    out: dict = {}

    def _fam(name, source, fn):
        spec = FROZEN_PROSPECTIVE_FAMILIES[name]
        vals = {}
        for sec, rec in source.items():
            v = fn(rec, spec["scale_floor"])
            if v is not None:
                vals[sec] = v
        status = "FORMED" if len(vals) >= MIN_BREADTH else (
            "INSUFFICIENT_BREADTH" if vals else "NO_COMPUTABLE_INPUT")
        out[name] = {"status": status, "values": vals,
                     "records": {s: source[s] for s in vals}}

    _fam("rev_eps_consensus_revision_magnitude", eps, _magnitude)
    _fam("rev_revenue_consensus_revision_magnitude", rev, _magnitude)
    _fam("rev_revision_acceleration", eps, _acceleration)
    _fam("rev_analyst_dispersion_change", eps,
         lambda rec, floor: _dispersion_change(prior_lookup, rec, floor))

    # breadth family: fields ABSENT from this entitlement (measured) — declared,
    # never approximated.
    has_counts = any(r.get("upward_revision_count") is not None for r in rows)
    out["rev_up_minus_down_breadth"] = {
        "status": "FORMED" if has_counts else "FIELDS_ABSENT_FROM_ENTITLEMENT",
        "values": {}, "records": {}}

    # joint confirmation: requires BOTH magnitude legs on the same day.
    e = out["rev_eps_consensus_revision_magnitude"]
    r = out["rev_revenue_consensus_revision_magnitude"]
    joint: dict = {}
    if e["status"] == "FORMED" and r["status"] == "FORMED":
        def _ranks(vals):
            ordered = sorted(vals, key=lambda s: (-vals[s], s))
            return {s: i + 1 for i, s in enumerate(ordered)}
        re_, rr_ = _ranks(e["values"]), _ranks(r["values"])
        for sec in set(re_) & set(rr_):
            if e["values"][sec] * r["values"][sec] > 0:
                n_e, n_r = len(re_), len(rr_)
                joint[sec] = (1 - re_[sec] / n_e) + (1 - rr_[sec] / n_r)
    out["rev_joint_eps_revenue_confirmation"] = {
        "status": ("FORMED" if len(joint) >= MIN_BREADTH else
                   "AWAITING_BOTH_LEGS" if not joint else "INSUFFICIENT_BREADTH"),
        "values": joint,
        "records": {s: e["records"].get(s) or {} for s in joint}}
    return out


def form_day(store: _ar.RawObservationStore, as_of: str) -> dict:
    """Persist PROSPECTIVE_SIGNAL_FORMATION records for every FORMED family."""
    signals = compute_day_signals(store, as_of)
    summary = {"as_of": as_of, "families": {}}
    for fam, res in signals.items():
        info = {"status": res["status"], "breadth": len(res["values"])}
        if res["status"] == "FORMED":
            ordered = sorted(res["values"], key=lambda s: (-res["values"][s], s))
            n = len(ordered)
            recs = []
            for rank, sec in enumerate(ordered, start=1):
                rec_src = res["records"].get(sec) or {}
                sat = rec_src.get("source_availability_timestamp")
                if not sat:
                    continue   # deterministic stamps only; never invent one
                rec = {
                    "security_id": sec, "signal_family": fam,
                    "signal_value": float(res["values"][sec]),
                    "formation_rank": rank, "breadth": n,
                    "snapshot_date": as_of,
                    "formation_timestamp": sat, "source_availability_timestamp": sat,
                    "fiscal_period_end": rec_src.get("fiscal_period_end"),
                    "fiscal_period_type": rec_src.get("fiscal_period_type"),
                    "provider": PROVIDER,
                }
                rec = {k: v for k, v in rec.items() if v is not None}
                rec["record_id"] = _ar.compute_formation_id(rec)
                recs.append(rec)
            wrote = store.append_many("PROSPECTIVE_SIGNAL_FORMATION", recs)
            info["formations_appended"] = wrote["appended"]
            info["idempotent_repeats"] = wrote["idempotent"]
        summary["families"][fam] = info
    return summary


# --------------------------------------------------------------------------- #
# MATURE — append FORWARD_OUTCOME records only for genuinely elapsed horizons.
# --------------------------------------------------------------------------- #
def _entry_index(dates: list, avail_date: str) -> int | None:
    """Index of the first session STRICTLY AFTER the availability date."""
    for i, d in enumerate(dates):
        if d > avail_date:
            return i
    return None


def mature_outcomes(store: _ar.RawObservationStore, panel: dict,
                    sessions: list, *, now_iso: str | None = None) -> dict:
    """For each formation x horizon without an outcome: MATURED when the owned
    session calendar contains entry + horizon further sessions AND the security
    has prices at entry and exit; UNAVAILABLE when the calendar has matured but
    the security's own price path is missing (e.g. delisting mid-horizon);
    otherwise left PENDING (nothing stored). Never substitutes a horizon."""
    now_iso = now_iso or _now_iso()
    existing = {r["record_id"] for r in store.all("FORWARD_OUTCOME")}
    by_asset_dates: dict = {}
    for aid, series in panel.items():
        by_asset_dates[str(aid)] = dict(series)
    # fallback map for formations not keyed by canonical ngid: ids (identity
    # records key on the PROVIDER security id)
    ident = {r.get("provider_security_id"): r
             for r in store.all("SECURITY_IDENTITY") if r.get("stable_assetid")}
    counts = defaultdict(int)
    new_recs = []
    for f in store.all("PROSPECTIVE_SIGNAL_FORMATION"):
        avail_date = str(f.get("source_availability_timestamp") or "")[:10]
        ei = _entry_index(sessions, avail_date)
        aid = None
        sec = f["security_id"]
        if sec.startswith("ngid:"):
            aid = sec.split(":", 1)[1]
        elif sec in ident:
            aid = ident[sec]["stable_assetid"]
        for h in HORIZONS:
            probe = {"formation_record_id": f["record_id"], "horizon_sessions": h}
            rid = _ar.compute_outcome_id(probe)
            if rid in existing:
                counts["already_stored"] += 1
                continue
            if ei is None or ei + h >= len(sessions):
                counts["pending"] += 1          # calendar not yet elapsed
                continue
            entry_d, exit_d = sessions[ei], sessions[ei + h]
            px = by_asset_dates.get(str(aid) if aid else "", {})
            p0, p1 = px.get(entry_d), px.get(exit_d)
            rec = {
                "record_id": rid,
                "formation_record_id": f["record_id"],
                "security_id": sec, "signal_family": f["signal_family"],
                "snapshot_date": f["snapshot_date"], "horizon_sessions": h,
                "computed_timestamp": now_iso,
            }
            if p0 and p1 and p0 > 0:
                rec.update({"status": "MATURED", "entry_session": entry_d,
                            "exit_session": exit_d,
                            "forward_return": float(p1) / float(p0) - 1.0})
                counts["matured"] += 1
            else:
                rec.update({"status": "UNAVAILABLE", "entry_session": entry_d,
                            "exit_session": exit_d})
                counts["unavailable"] += 1
            new_recs.append(rec)
            existing.add(rid)
    store.append_many("FORWARD_OUTCOME", new_recs)
    return dict(counts)


def derive_status(store: _ar.RawObservationStore, sessions: list) -> dict:
    """Derived (never stored) PENDING/MATURED/UNAVAILABLE census per family and
    horizon, plus ledger record counts."""
    outcomes = {(r["formation_record_id"], r["horizon_sessions"]): r["status"]
                for r in store.all("FORWARD_OUTCOME")}
    per = defaultdict(lambda: defaultdict(int))
    total = defaultdict(int)
    for f in store.all("PROSPECTIVE_SIGNAL_FORMATION"):
        fam = f["signal_family"]
        total[fam] += 1
        for h in HORIZONS:
            st = outcomes.get((f["record_id"], h), "PENDING")
            per[fam]["h%d_%s" % (h, st)] += 1
    last_session = sessions[-1] if sessions else None
    return {
        "generated_at": _now_iso(),
        "ledger_counts": {s: len(store.all(s)) for s in (
            "CONSENSUS_SNAPSHOT", "SECURITY_IDENTITY",
            "PROSPECTIVE_SIGNAL_FORMATION", "FORWARD_OUTCOME")},
        "formations_per_family": dict(total),
        "horizon_status_per_family": {k: dict(v) for k, v in per.items()},
        "owned_calendar_last_session": last_session,
        "note": "PENDING is derived from record absence; horizons mature only "
                "after their eligible owned sessions have actually passed.",
    }


def owned_sessions(date_start: str = "2026-01-01") -> tuple:
    """(sessions, panel) from the owned survivorship-safe MARKET_BAR tree."""
    panel = build_assetid_price_panel(INGESTION_ROOT, date_start=date_start)
    dates = sorted({d for series in panel.values() for d, _ in series})
    return dates, panel


# --------------------------------------------------------------------------- #
# TRACK B — historical import readiness (refuses absent data; fabricates nothing)
# --------------------------------------------------------------------------- #
def historical_import(path: str, schema: str) -> dict:
    """Import + PIT-scan a REAL historical file via the released Stage-13A
    pipeline. Refuses (does not fabricate) when the file does not exist."""
    p = Path(path)
    if not p.is_file():
        return {"status": "HISTORICAL_DATA_ABSENT", "path": str(p),
                "note": "no historical Zacks revision archive exists in the "
                        "current entitlement; nothing was fabricated"}
    imp = _ar.LocalTrialImporter(_ar.IntrinioZacksAdapter())
    res = imp.import_file(p, schema=schema)
    events = res["records"] if schema == "ESTIMATE_REVISION_EVENT" else ()
    snaps = res["records"] if schema == "CONSENSUS_SNAPSHOT" else ()
    res["pit_scan"] = _ar.pit_scan(events=events, consensus=snaps)
    return res


def historical_readiness() -> dict:
    """Offline self-check that the ENTIRE historical evaluation path is ready
    the moment a genuine historical extract arrives."""
    reg_path = _REPO / "configs" / "alpha_agent" / "stage13a_revision_hypotheses.json"
    on_disk = json.loads(reg_path.read_text(encoding="utf-8"))
    rebuilt = _ar.build_revision_registry()
    refusal = historical_import(
        r"C:\nonexistent-stage13b\historical_zacks_sample.json",
        "ESTIMATE_REVISION_EVENT")
    return {
        "generated_at": _now_iso(),
        "frozen_registry_on_disk": on_disk.get("registry_version"),
        "frozen_registry_rebuilt": rebuilt.get("registry_version"),
        "registry_intact": on_disk.get("registry_version") == rebuilt.get("registry_version"),
        "registry_problems": _ar.validate_revision_registry(on_disk),
        "adapter": _ar.IntrinioZacksAdapter().provider,
        "importer_refuses_absent_data": refusal["status"] == "HISTORICAL_DATA_ABSENT",
        "adequacy_gate_callable": bool(_ar.assess_adequacy()),
        "identity_db_present": IDENTITY_DB.is_file(),
        "pipeline": ["LocalTrialImporter.import_file", "pit_scan",
                     "identity/survivorship audit", "assess_adequacy",
                     "frozen 6-hypothesis evaluation", "_bh_fdr(family_size=6)",
                     "cost/robustness/orthogonality", "data_expansion_gate"],
    }


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ingest", action="store_true")
    ap.add_argument("--form", action="store_true")
    ap.add_argument("--mature", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--historical-readiness", action="store_true")
    ap.add_argument("--import-historical", default=None, metavar="PATH")
    ap.add_argument("--schema", default="ESTIMATE_REVISION_EVENT")
    ap.add_argument("--as-of", default=None)
    args = ap.parse_args()

    cfg = _load_cfg()
    root = _research_root(cfg)
    store = _ar.RawObservationStore(ledger_root(cfg))
    as_of = args.as_of or _dt.datetime.now(_dt.timezone.utc).date().isoformat()
    ran = False

    if args.ingest:
        ran = True
        print(json.dumps(ingest_snapshot_day(store, root, as_of), indent=1,
                         sort_keys=True))
    if args.form:
        ran = True
        print(json.dumps(form_day(store, as_of), indent=1, sort_keys=True))
    sessions = panel = None
    if args.mature or args.status:
        sessions, panel = owned_sessions()   # one owned-panel build for both
    if args.mature:
        ran = True
        print(json.dumps(mature_outcomes(store, panel, sessions), indent=1,
                         sort_keys=True))
    if args.status:
        ran = True
        doc = derive_status(store, sessions)
        out = ledger_root(cfg) / "forward_evidence_status.json"
        out.write_text(json.dumps(doc, indent=1, sort_keys=True), encoding="utf-8")
        print(json.dumps(doc, indent=1, sort_keys=True))
    if args.historical_readiness:
        ran = True
        print(json.dumps(historical_readiness(), indent=1, sort_keys=True))
    if args.import_historical:
        ran = True
        res = historical_import(args.import_historical, args.schema)
        res.pop("records", None)
        print(json.dumps(res, indent=1, sort_keys=True))
    if not ran:
        ap.print_help()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
