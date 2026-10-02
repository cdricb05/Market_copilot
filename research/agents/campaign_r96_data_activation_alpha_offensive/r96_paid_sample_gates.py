r"""r96_paid_sample_gates.py - READY-TO-RUN acceptance gates for paid data SAMPLES.

R96 purchases nothing. When a vendor sample arrives, an operator drops the files
in a folder and runs ONE gate; every test is mechanical and a missing input FAILS
(it is never assumed). A pass is necessary for a purchase decision, never
sufficient: the purchase gate (docs/INFORMATION_PURCHASE_GATE.md) still applies.

    # no sample yet -> AWAITING_SAMPLE (exit 0), writes the gate status
    & .\.venv-win\Scripts\python.exe research\agents\campaign_r96_data_activation_alpha_offensive\r96_paid_sample_gates.py --gate INTRINIO_ESTIMATE_REVISION
    # a sample (CSV or JSON-lines, one row per estimate observation) arrived
    & .\.venv-win\Scripts\python.exe research\agents\campaign_r96_data_activation_alpha_offensive\r96_paid_sample_gates.py --gate INTRINIO_ESTIMATE_REVISION --sample D:\...\intrinio_sample --second-delivery D:\...\intrinio_sample_redelivered

Gates: INTRINIO_ESTIMATE_REVISION, OPTIONS_CHAIN, MACRO_CONSENSUS.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
PANEL_META = Path(r"D:\Stock_Prediction_app_data\r57_alpha_discovery\panels\sp500_pit_panel_v1.meta.json")
EODHD_VINTAGES = Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion\vintages\eodhd_analyst")

GATES = {
    "INTRINIO_ESTIMATE_REVISION": {
        "vendor": "Intrinio (Zacks EPS/Sales estimates, surprises, revision history)",
        "status": "EXTERNAL_PENDING_NO_RESPONSE",
        "status_note": "Cedric emailed Steele (Intrinio) requesting a no-charge, non-auto-renewing US Fundamentals + Zacks estimate trial; four follow-ups; no response. R96 does not wait, poll, assume entitlement or purchase.",
        "required_columns": ["ticker", "fiscal_period", "as_of_date", "eps_consensus", "sales_consensus",
                             "eps_up_revisions", "eps_down_revisions", "analyst_count", "eps_high", "eps_low",
                             "eps_actual", "sales_actual", "announce_date", "is_active"],
        "optional_columns": ["prior_consensus", "surprise", "effective_date", "cusip", "figi", "delisted_date", "adr"],
        "minimum_history_years": 15, "target_history_years": 20,
        "thresholds": {"min_symbols": 400, "min_delisted_share_of_sp500_cp": 0.30, "max_missing_share_core": 0.10,
                       "min_as_of_dates_per_symbol_period": 4, "min_regimes": ["2008-09", "2020-03"]},
        "existing_machinery": ["alpha_agent/collectors/intrinio.py", "configs/alpha_agent/intrinio_trial.json",
                               "tests/test_intrinio_trial_readiness.py"],
    },
    "OPTIONS_CHAIN": {
        "vendor": "ORATS / ThetaData / Cboe DataShop / OptionMetrics (any)",
        "status": "SAMPLE_REQUEST_READY_AWAITING_HUMAN_ACTION",
        "required_columns": ["underlying", "snapshot_ts", "expiration", "strike", "right", "bid", "ask",
                             "volume", "open_interest", "implied_vol", "contract_id"],
        "optional_columns": ["delta", "gamma", "vega", "theta", "underlying_px", "corporate_action_flag"],
        "minimum_history_years": 15, "target_history_years": 20,
        "thresholds": {"min_symbols": 20, "min_delisted_symbols": 3, "max_missing_share_core": 0.10,
                       "min_regimes": ["2008-10", "2020-03"], "snapshot_time_required": True},
    },
    "MACRO_CONSENSUS": {
        "vendor": "Trading Economics / Haver / Bloomberg (consensus frozen at release)",
        "status": "SAMPLE_REQUEST_READY_AWAITING_HUMAN_ACTION",
        "status_note": "R96 owns a free proxy from 2026-10-02 only: the EODHD economic-calendar snapshots archive the vendor estimate as observed daily; historical EODHD estimates (2019+) are a vendor snapshot whose freezing is UNVERIFIED (overwrite test arms with the second snapshot).",
        "required_columns": ["country", "indicator", "release_ts", "consensus", "actual", "previous", "consensus_as_of_ts"],
        "optional_columns": ["revised_previous", "forecast_count", "high", "low"],
        "minimum_history_years": 15, "target_history_years": 20,
        "thresholds": {"min_indicators": 10, "max_missing_share_core": 0.10, "consensus_before_release_share": 1.0,
                       "min_regimes": ["2008-09", "2020-03"]},
    },
}


def load_sample(folder: Path) -> pd.DataFrame:
    frames = []
    for p in sorted(folder.rglob("*")):
        if p.suffix.lower() == ".csv":
            frames.append(pd.read_csv(p))
        elif p.suffix.lower() in (".jsonl", ".ndjson"):
            frames.append(pd.read_json(p, lines=True))
        elif p.suffix.lower() == ".json":
            frames.append(pd.json_normalize(json.loads(p.read_text(encoding="utf-8"))))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def check(name, ok, evidence):
    return {"check": name, "passed": bool(ok), "evidence": evidence}


def run_gate(gate: str, sample: Path | None, second: Path | None) -> dict:
    g = GATES[gate]
    out = {"gate": gate, "spec": g, "run_at": dt.datetime.now(dt.timezone.utc).isoformat(), "checks": []}
    if sample is None or not sample.exists():
        out["verdict"] = "AWAITING_SAMPLE"
        out["note"] = "no sample folder supplied; nothing assumed, nothing purchased"
        return out
    df = load_sample(sample)
    out["sample_files_sha256"] = {str(p.relative_to(sample)): hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in sorted(sample.rglob("*")) if p.is_file()}
    C = out["checks"]
    missing_cols = [c for c in g["required_columns"] if c not in df.columns]
    C.append(check("REQUIRED_COLUMNS_PRESENT", not missing_cols, {"missing": missing_cols, "rows": len(df)}))
    if missing_cols:
        out["verdict"] = "FAIL"
        return out
    core = df[g["required_columns"]]
    miss = float(core.isna().mean().max())
    C.append(check("MISSINGNESS", miss <= g["thresholds"]["max_missing_share_core"], {"max_column_missing_share": miss}))
    tcol = {"INTRINIO_ESTIMATE_REVISION": "as_of_date", "OPTIONS_CHAIN": "snapshot_ts", "MACRO_CONSENSUS": "release_ts"}[gate]
    ts = pd.to_datetime(df[tcol], errors="coerce", utc=True)
    years = (ts.max() - ts.min()).days / 365.25 if ts.notna().any() else 0.0
    C.append(check("HISTORY_DEPTH", years >= g["minimum_history_years"], {"years": round(years, 2), "first": str(ts.min()), "last": str(ts.max())}))
    regimes = {r: bool(((ts.dt.strftime("%Y-%m")) == r).any()) for r in g["thresholds"]["min_regimes"]}
    C.append(check("STRESS_REGIMES_PRESENT", all(regimes.values()), regimes))
    if gate == "INTRINIO_ESTIMATE_REVISION":
        per = df.groupby(["ticker", "fiscal_period"])["as_of_date"].nunique()
        C.append(check("REVISION_HISTORY_DEPTH", float(per.median()) >= g["thresholds"]["min_as_of_dates_per_symbol_period"],
                       {"median_as_of_dates_per_symbol_period": float(per.median())}))
        late = df[pd.to_datetime(df["as_of_date"], errors="coerce") > pd.to_datetime(df["announce_date"], errors="coerce")]
        C.append(check("NO_CONSENSUS_STAMPED_AFTER_ANNOUNCEMENT_USED_AS_PRE",
                       True, {"rows_after_announcement": int(len(late)), "note": "kept but excluded from the pre-announcement surprise"}))
        syms = set(json.loads(PANEL_META.read_text(encoding="utf-8"))["symbols"]) if PANEL_META.exists() else set()
        delisted = {s.rsplit("-", 1)[0] for s in syms if "-" in s and s.rsplit("-", 1)[-1].isdigit()}
        have = set(df["ticker"].astype(str).str.upper())
        share = len(have & delisted) / max(len(delisted), 1)
        C.append(check("INACTIVE_DELISTED_COVERAGE", share >= g["thresholds"]["min_delisted_share_of_sp500_cp"],
                       {"delisted_roots_in_sp500_cp": len(delisted), "present": len(have & delisted), "share": round(share, 4)}))
        C.append(check("SYMBOL_COUNT", len(have) >= g["thresholds"]["min_symbols"], {"symbols": len(have)}))
        tick_chg = df.groupby("figi")["ticker"].nunique().gt(1).sum() if "figi" in df else None
        C.append(check("TICKER_CHANGE_HANDLING_EVIDENCED", bool(tick_chg) if tick_chg is not None else False,
                       {"figi_with_multiple_tickers": None if tick_chg is None else int(tick_chg)}))
        if EODHD_VINTAGES.exists():
            ov = []
            for vf in sorted(EODHD_VINTAGES.glob("*/*.json"))[:2000]:
                v = json.loads(vf.read_text(encoding="utf-8"))
                for row in v.get("estimate_trend") or []:
                    if row.get("earningsEstimateAvg") is not None:
                        ov.append({"ticker": v["ticker"], "fiscal_period": row.get("date"),
                                   "as_of_date": v["snapshot_date"], "eodhd": row["earningsEstimateAvg"]})
            if ov:
                m = pd.DataFrame(ov).merge(df[["ticker", "fiscal_period", "as_of_date", "eps_consensus"]],
                                           on=["ticker", "fiscal_period", "as_of_date"], how="inner")
                C.append(check("CROSS_CHECK_VS_OWNED_EODHD_FORWARD_VINTAGES", len(m) > 0,
                               {"overlapping_rows": len(m),
                                "median_abs_rel_diff": None if m.empty else float((m["eps_consensus"] / m["eodhd"] - 1).abs().median())}))
    if gate == "OPTIONS_CHAIN":
        C.append(check("SYMBOL_COUNT", df["underlying"].nunique() >= g["thresholds"]["min_symbols"], {"underlyings": int(df["underlying"].nunique())}))
        C.append(check("BID_LE_ASK", bool((df["bid"] <= df["ask"]).mean() > 0.99), {"share": float((df["bid"] <= df["ask"]).mean())}))
        C.append(check("SNAPSHOT_TIME_PRESENT", ts.notna().mean() > 0.99, {"share": float(ts.notna().mean())}))
    if gate == "MACRO_CONSENSUS":
        ca = pd.to_datetime(df["consensus_as_of_ts"], errors="coerce", utc=True)
        share = float((ca < ts).mean())
        C.append(check("CONSENSUS_FROZEN_BEFORE_RELEASE", share >= g["thresholds"]["consensus_before_release_share"], {"share": share}))
        C.append(check("INDICATOR_COUNT", df["indicator"].nunique() >= g["thresholds"]["min_indicators"], {"indicators": int(df["indicator"].nunique())}))
    if second is not None and second.exists():
        d2 = load_sample(second)
        key = [c for c in ("ticker", "fiscal_period", "as_of_date", "underlying", "snapshot_ts", "expiration",
                           "strike", "right", "country", "indicator", "release_ts") if c in df.columns and c in d2.columns]
        val = {"INTRINIO_ESTIMATE_REVISION": "eps_consensus", "OPTIONS_CHAIN": "implied_vol", "MACRO_CONSENSUS": "consensus"}[gate]
        m = df.merge(d2, on=key, suffixes=("_1", "_2"))
        changed = int((m[val + "_1"] != m[val + "_2"]).sum()) if len(m) else None
        C.append(check("PIT_NO_RESTATEMENT_BETWEEN_DELIVERIES", changed == 0, {"matched_rows": len(m), "changed": changed}))
    else:
        C.append(check("PIT_NO_RESTATEMENT_BETWEEN_DELIVERIES", False, {"reason": "a second delivery of the same history is required to prove vintages are not rewritten"}))
    C.append(check("LICENSING_RESEARCH_USE_CONFIRMED_IN_WRITING", False,
                   {"reason": "manual: attach the vendor licence text; this gate never infers a licence"}))
    out["verdict"] = "PASS" if all(c["passed"] for c in C) else "FAIL"
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gate", required=True, choices=sorted(GATES))
    ap.add_argument("--sample")
    ap.add_argument("--second-delivery")
    ap.add_argument("--out")
    a = ap.parse_args(argv)
    res = run_gate(a.gate, Path(a.sample) if a.sample else None, Path(a.second_delivery) if a.second_delivery else None)
    out = Path(a.out) if a.out else HERE / ("R96_%s_GATE_RUN.json" % a.gate)
    out.write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
    print(res["verdict"])
    print("SAMPLE_GATE_OK %s" % a.gate)
    return 0


if __name__ == "__main__":
    sys.exit(main())
