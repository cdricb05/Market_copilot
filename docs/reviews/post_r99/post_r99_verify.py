r"""post_r99_verify.py - POST-R99 independent recount (READ ONLY).

Recounts the R99 contract counters from the raw campaign artifacts WITHOUT importing
r99_contract_guard.py, cross-checks them against a second source (R99_MEASUREMENT_RESULTS.json,
R99_PREREGISTRATIONS.json, the preregister payloads and the read-only R59 research memory), verifies
the survivor chain and compares the three production fingerprints. Writes POST_R99_VERIFICATION.json
next to this file. Never writes into the R99 campaign folder or any store.

    & .\.venv-win\Scripts\python.exe docs\reviews\post_r99\post_r99_verify.py
"""
from __future__ import annotations

import glob
import hashlib
import json
import sqlite3
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
R99 = REPO / "research" / "agents" / "campaign_r99_multi_asset_alpha_offensive"
MEMORY = Path(r"D:\Stock_Prediction_app_data\r59_autonomous_alpha\research_memory.sqlite")

CLAIMED = {"FRESH_HYPOTHESES_SCREENED": 169, "FORMAL_GATE_ASSESSMENTS": 114, "EXPERIMENTS_PREREGISTERED": 20,
           "EXPERIMENTS_MEASURED": 20, "ASSET_AREAS_MEASURED": 5, "HEDGED_OR_CROSS_ASSET_MEASURED": 19,
           "CURVE_OR_SPREAD_MEASURED": 9, "REACHED_LOCKBOX": 2, "SKEPTIC_KILLED": 2, "SKEPTIC_SURVIVORS": 0,
           "RISK_CLEARED": 0, "ENSEMBLES": 0, "GUARD_ACTIVE_MINUTES_AT_LAST_RUN": 174.7}


def j(p: Path):
    return json.loads(p.read_text(encoding="utf-8-sig"))


def sha(p: Path):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def formal(rec: dict) -> bool:
    # independent re-implementation of the documented rule (gate ledger "formal_rule")
    g = rec.get("gates") or {}
    verdict = lambda k: str((g.get(k) or {}).get("verdict", "")).upper()   # noqa: E731
    if verdict("G1") == "FAIL":
        return bool(rec.get("director_ruling_file"))
    if verdict("G1") != "PASS":
        return False
    return verdict("G2") == "PASS" and verdict("G3") == "PASS" and verdict("G5") in ("PASS", "FAIL") \
        and verdict("G6") in ("PASS", "FAIL")


def main() -> dict:
    queue = j(R99 / "R99_CANDIDATE_QUEUE.json")["candidates"]
    cand = {c["id"]: c for c in queue}
    ledger = j(R99 / "R99_GATE_LEDGER.json")
    spec = j(R99 / "campaign_spec.json")["experiments"]
    prereg = j(R99 / "R99_PREREGISTRATIONS.json")
    meas = j(R99 / "R99_MEASUREMENT_RESULTS.json")
    val = j(R99 / "R99_VALIDATION_RESULTS.json")
    sk = j(R99 / "R99_SKEPTIC_RESULTS.json")["verdicts"]
    rk = j(R99 / "R99_RISK_RESULTS.json")
    ens = j(R99 / "R99_ENSEMBLE_RESULTS.json")

    # --- source A: results_*.json (revealed layers) joined to the spec and the queue
    revealed = {}
    for f in sorted(R99.glob("results_*.json")):
        for r in j(f).get("results") or []:
            if r.get("layers"):
                revealed.setdefault(r["experiment_id"], []).append((f.name, r))
    measured = [e for e in spec if e["experiment_id"] in revealed]
    mc = [cand[e["cell_id"]] for e in measured]
    st = lambda c: set(c.get("structures") or [])   # noqa: E731
    areas = sorted({c["asset_area"] for c in mc if c["asset_area"] != "CROSS_ASSET"})
    lockbox = sorted(e for e, rs in revealed.items() if any("L" in (r.get("layers") or {}) for _, r in rs))
    recount = {
        "FRESH_HYPOTHESES_SCREENED": len(cand),
        "FRESH_HYPOTHESES_SCREENED_queue_n_field": j(R99 / "R99_CANDIDATE_QUEUE.json").get("n"),
        "FORMAL_GATE_ASSESSMENTS": sum(1 for r in ledger["records"].values() if formal(r)),
        "FORMAL_GATE_ASSESSMENTS_ledger_n_formal_field": ledger.get("n_formal"),
        "GATE_LEDGER_RECORDS": len(ledger["records"]),
        "EXPERIMENTS_PREREGISTERED": len(spec),
        "EXPERIMENTS_PREREGISTERED_prereg_artifact": prereg.get("n_preregistered"),
        "PREREGISTER_PIPELINE_OK_TOKENS": sum(1 for p in prereg["preregistrations"] if str(p.get("token", "")).startswith("PIPELINE_OK")),
        "PREREGISTRATION_REFUSALS": prereg.get("n_refused"),
        "EXPERIMENTS_MEASURED": len(measured),
        "EXPERIMENTS_MEASURED_measurement_rows": len(meas["rows"]),
        "ASSET_AREAS_MEASURED": len(areas),
        "asset_areas": areas,
        "by_area": {a: sum(1 for c in mc if c["asset_area"] == a) for a in sorted({c["asset_area"] for c in mc})},
        "HEDGED_OR_CROSS_ASSET_MEASURED": sum(1 for c in mc if st(c) & {"HEDGED_RV", "CROSS_ASSET"}),
        "CURVE_OR_SPREAD_MEASURED": sum(1 for c in mc if "CURVE_SPREAD" in st(c)),
        "DIRECTIONAL_MEASURED": sum(1 for c in mc if "DIRECTIONAL" in st(c)),
        "EVENT_DRIVEN_MEASURED": sum(1 for c in mc if "EVENT_DRIVEN" in st(c)),
        "CROSS_SECTIONAL_MEASURED": sum(1 for c in mc if "CROSS_SECTIONAL" in st(c)),
        "DISTINCT_INFORMATION_FAMILIES_MEASURED": len({c["information_family"] for c in mc}),
        "REACHED_LOCKBOX": len(lockbox),
        "lockbox_experiments": lockbox,
        "VALIDATION_RECORDS": sorted(val["records"]),
        "SKEPTIC_VERDICTS": {k: v.get("verdict") for k, v in sk.items()},
        "SKEPTIC_KILLED": sum(1 for v in sk.values() if str(v.get("verdict")).upper() == "KILLED"),
        "SKEPTIC_SURVIVORS": sum(1 for v in sk.values() if str(v.get("verdict")).upper() == "SURVIVES"),
        "RISK_CLEARED": len(rk.get("verdicts") or {}),
        "RISK_STATUS": rk.get("status"),
        "ENSEMBLES": len(ens.get("ensembles") or []),
        "ENSEMBLE_STATUS": ens.get("status"),
        "experiments_revealed_in_more_than_one_results_file": sorted(e for e, rs in revealed.items() if len(rs) > 1),
    }
    # --- source B: measurement rows must cover exactly the same experiment ids
    meas_ids = sorted(r["experiment_id"] for r in meas["rows"])
    recount["measurement_rows_match_results_files"] = meas_ids == sorted(e["experiment_id"] for e in measured)
    # --- survivor chain: every lockbox experiment carries a canonical gate verdict and a skeptic verdict
    chain = {}
    for e in lockbox:
        v = sk.get(e) or {}
        chain[e] = {"cell": v.get("cell"), "skeptic": v.get("verdict"), "canonical_gate": v.get("canonical_gate"),
                    "failed_attacks": sorted(k for k, a in (v.get("attacks") or {}).items() if a.get("passed") is False)}
    # --- source C: read-only R59 research memory
    mem = {"path": str(MEMORY)}
    try:
        con = sqlite3.connect("file:%s?mode=ro" % MEMORY.as_posix(), uri=True)
        tabs = [r[0] for r in con.execute("select name from sqlite_master where type='table'")]
        mem["tables_with_hypothesis"] = [t for t in tabs if "hypoth" in t.lower()]
        ids = [e["experiment_id"] for e in spec]
        found = {}
        for t in mem["tables_with_hypothesis"]:
            cols = [r[1] for r in con.execute("pragma table_info(%s)" % t)]
            key = next((c for c in ("hypothesis_id", "id", "experiment_id") if c in cols), None)
            if not key:
                continue
            oc = next((c for c in ("outcome", "status", "state") if c in cols), None)
            q = "select %s%s from %s where %s in (%s)" % (key, (", " + oc) if oc else "", t, key, ",".join("?" * len(ids)))
            rows = con.execute(q, ids).fetchall()
            if rows:
                found[t] = {"n": len(rows), "by_outcome": {}}
                for row in rows:
                    o = row[1] if oc else None
                    found[t]["by_outcome"][str(o)] = found[t]["by_outcome"].get(str(o), 0) + 1
        mem["r99_experiment_ids_in_memory"] = found
        con.close()
    except Exception as exc:                                         # noqa: BLE001
        mem["error"] = "%s: %s" % (type(exc).__name__, exc)
    # --- PF4 cell-file hash drift, from the preregister payloads themselves
    def find(o, key):
        if isinstance(o, dict):
            if key in o:
                return o[key]
            o = list(o.values())
        if isinstance(o, list):
            for x in o:
                v = find(x, key)
                if v is not None:
                    return v
        return None

    rec_hashes = {}
    for p in sorted(glob.glob(str(R99 / "payloads" / "*__preregister.json"))):
        h = find(j(Path(p)), "cells_file_sha256")
        rec_hashes.setdefault(h, []).append(Path(p).name.split("__")[0])
    cells_now = sha(R99 / "r99_cells.py")
    pf4 = {"current_r99_cells_sha256": cells_now,
           "recorded_cells_file_sha256": {h: sorted(v) for h, v in rec_hashes.items() if h},
           "distinct_recorded_cells_hashes": len([h for h in rec_hashes if h]),
           "any_equal_current": cells_now in rec_hashes,
           "preregs_without_cells_file_hash": rec_hashes.get(None, [])}
    # R15 (event cell) binds a DIFFERENT file: the frozen event-rules JSON and its event source CSV
    r15 = j(R99 / "payloads" / "R99_R15__preregister.json")
    rules_rec, src_rec = find(r15, "frozen_rules_sha256"), find(r15, "source_sha256")
    src_file = Path(find(r15, "source_file"))
    pf4["R15_event_binding"] = {
        "frozen_rules_file": find(r15, "frozen_rules_file"), "frozen_rules_sha256_recorded": rules_rec,
        "frozen_rules_sha256_now": sha(R99 / "R99_EVENT_RULES.json"),
        "frozen_rules_equal": rules_rec == sha(R99 / "R99_EVENT_RULES.json"),
        "source_sha256_recorded": src_rec, "source_sha256_now": sha(src_file) if src_file.exists() else None,
        "source_equal": src_file.exists() and src_rec == sha(src_file),
        "r15_rule_entry_unchanged": "UNVERIFIABLE (R99_EVENT_RULES.json is untracked; the prior bytes are not retained)"}
    pf4["correction_to_R99_PF4"] = ("PF4 counted 4 distinct hashes for r99_cells.py. Only 3 are cells-file hashes "
                                    "(19 preregistrations); the 4th (3ec25710...) is R15's frozen_rules_sha256 for "
                                    "R99_EVENT_RULES.json, which ALSO drifted (rewritten 12:30 after R15's 11:44 "
                                    "preregistration). R15's event source CSV is unchanged.")
    # --- guard state claims
    before_copy = HERE / "R99_CONTRACT_GUARD.before_closeout.json"
    guard = j(before_copy if before_copy.exists() else R99 / "R99_CONTRACT_GUARD.json")
    finals = sorted(p.name for p in R99.iterdir() if "FINAL" in p.name.upper() and p.suffix in (".md", ".json")
                    and p.name != "r99_finalize.py")
    guard_state = {"last_guard_decision": guard["decision"], "last_guard_timestamp": guard["timestamp_utc"],
                   "last_guard_active_minutes": guard["counters"]["ACTIVE_RESEARCH_MINUTES"],
                   "hard_stop_events_file_exists": (R99 / "R99_HARD_STOP_EVENTS.json").exists(),
                   "final_report_files": finals}
    closeout = None
    if before_copy.exists():
        g2 = j(R99 / "R99_CONTRACT_GUARD.json")
        st = j(R99 / "R99_START_STATE.json")
        closeout = {"guard_decision": g2["decision"], "guard_timestamp": g2["timestamp_utc"],
                    "active_minutes": g2["counters"]["ACTIVE_RESEARCH_MINUTES"], "reason": g2["reason"],
                    "hard_stop_events_valid": g2["hard_stop_events_valid"],
                    "idle_exclusions": st.get("idle_exclusions"),
                    "counters_unchanged_vs_before": {k: g2["counters"][k] == guard["counters"][k]
                                                     for k in g2["counters"] if k != "ACTIVE_RESEARCH_MINUTES"},
                    "final_status": ("R99_COMPLETE" if g2["decision"] == "R99_FINALIZATION_ALLOWED" else
                                     "R99_HARD_STOP" if g2["decision"] == "R99_HARD_STOP_ALLOWED" else
                                     "R99_NOT_COMPLETED")}
    # --- production fingerprints
    b = j(R99 / "R99_PRODUCTION_BEFORE.json")
    a = j(R99 / "R99_PRODUCTION_AFTER.json")
    n = j(HERE / "PRODUCTION_BEFORE.json")

    def cmp(x, y):
        df = sorted(k for k in set(x["files"]) | set(y["files"]) if x["files"].get(k) != y["files"].get(k))
        da = sorted(k for k in x["api"] if k != "/v1/ready" and x["api"][k].get("sha256_stripped") != y["api"].get(k, {}).get("sha256_stripped"))
        dd = sorted(t for t in x.get("db", {}) if x["db"].get(t) != y.get("db", {}).get(t))
        return {"diff_files": df, "diff_api": da, "diff_db": dd, "head_equal": x["git"]["head"] == y["git"]["head"],
                "verdict": "PRESERVED" if not (df or da or dd) else "CHANGED"}
    prod = {"R99_BEFORE_vs_R99_AFTER": cmp(b, a), "R99_AFTER_vs_POST_R99_BEFORE": cmp(a, n),
            "db_rows_now": n.get("db")}
    if (HERE / "PRODUCTION_AFTER.json").exists():
        prod["POST_R99_BEFORE_vs_POST_R99_AFTER"] = cmp(n, j(HERE / "PRODUCTION_AFTER.json"))
    # --- discrepancies (claimed vs recount), never silently repaired
    flat = dict(recount, SKEPTIC_KILLED=recount["SKEPTIC_KILLED"], GUARD_ACTIVE_MINUTES_AT_LAST_RUN=guard_state["last_guard_active_minutes"])
    verified, discrepancies = [], []
    for k, v in CLAIMED.items():
        (verified if flat.get(k) == v else discrepancies).append({"counter": k, "claimed": v, "recounted": flat.get(k)})
    out = {"artifact": "POST_R99_VERIFICATION", "read_only": True, "r99_dir": str(R99),
           "recount": recount, "survivor_chain": chain, "research_memory_crosscheck": mem, "pf4_hash_drift": pf4,
           "guard_state_before_closeout": guard_state, "closeout": closeout, "production": prod,
           "UNVERIFIED_CLAIMS": [
               "whether R15's own entry in R99_EVENT_RULES.json changed after its preregistration "
               "(the file is untracked and its prior bytes are not retained; the source CSV is unchanged)",
               "the narrative reasons given in director rulings for admission/refusal (science judgements, "
               "not recountable from artifacts)"],
           "VERIFIED": verified, "DISCREPANCIES": discrepancies}
    (HERE / "POST_R99_VERIFICATION.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    return out


if __name__ == "__main__":
    o = main()
    print("VERIFIED", len(o["VERIFIED"]))
    print("DISCREPANCIES", json.dumps(o["DISCREPANCIES"]))
    print(json.dumps({k: o["recount"][k] for k in ("by_area", "DIRECTIONAL_MEASURED", "EVENT_DRIVEN_MEASURED", "DISTINCT_INFORMATION_FAMILIES_MEASURED",
                                                    "EXPERIMENTS_PREREGISTERED_prereg_artifact", "PREREGISTER_PIPELINE_OK_TOKENS", "PREREGISTRATION_REFUSALS",
                                                    "measurement_rows_match_results_files", "experiments_revealed_in_more_than_one_results_file", "SKEPTIC_VERDICTS")}))
    print(json.dumps(o["survivor_chain"], indent=0)[:1500])
    print(json.dumps(o["research_memory_crosscheck"]))
    print(json.dumps(o["pf4_hash_drift"]))
    print(json.dumps(o["guard_state_before_closeout"]))
    print(json.dumps({k: v["verdict"] if isinstance(v, dict) and "verdict" in v else v for k, v in o["production"].items()}))
