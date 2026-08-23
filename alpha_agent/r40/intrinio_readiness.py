"""alpha_agent.r40.intrinio_readiness - INTRINIO_SAMPLE_READINESS (Track L).

The operator may separately send the already-prepared five-name historical
sample request (AAPL / MON / META / HTZ / CALM, Release 38) to Steele
Barcomb. Release 40 does not wait for it and does not send it; it prepares
the ingestion / validation path so a sample dropped into
``<campaign>/intrinio_sample_inbox/`` can be inspected immediately.

Purpose of the sample: SCHEMA_AND_PIT_VALIDATION_ONLY. The validator can
PROVE (or fail) historical observation dates, estimate vintages, revision
representation, issuer continuity, delisted handling and available fields.
It can NEVER prove alpha - five names are not a universe - and the
artifact says so in a constant, not a sentence. No purchase, no trial, no
licence.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

from .. import r39 as _r39
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r40.intrinio_readiness"
ARTIFACT_NAME = "intrinio_sample_readiness.json"
INBOX_DIRNAME = "intrinio_sample_inbox"

R38_REQUEST = Path(r"D:\Stock_Prediction_app_data\native_futures_r38"
                   r"\r38_native_futures_information_frontier_v4"
                   r"\intrinio_steele_sample_request.json")
SAMPLE_TICKERS = ("AAPL", "MON", "META", "HTZ", "CALM")
SAMPLE_PURPOSE = C.INTRINIO_SAMPLE_PURPOSE
SAMPLE_CAN_PROVE_ALPHA = False

#: Column families the sample must demonstrate (any of the aliases).
REQUIRED_FIELDS = {
    "observation_date": ("observation_date", "as_of_date", "asof", "date",
                         "estimate_date", "knowable_date"),
    "ticker_or_id": ("ticker", "symbol", "identifier", "figi", "cik",
                     "permanent_id", "company_id"),
    "fiscal_period": ("fiscal_period", "period", "fiscal_year", "fy",
                      "fq", "period_end_date"),
    "consensus_mean": ("mean", "consensus_mean", "eps_mean",
                       "estimate_mean"),
    "analyst_count": ("analyst_count", "n_analysts", "num_estimates",
                      "count"),
    "actual": ("actual", "reported", "eps_actual", "actual_value"),
}
OPTIONAL_FIELDS = {
    "consensus_median": ("median", "consensus_median"),
    "consensus_high": ("high", "max"),
    "consensus_low": ("low", "min"),
    "dispersion": ("std", "stdev", "dispersion", "standard_deviation"),
    "revisions_up": ("revisions_up", "up", "num_up"),
    "revisions_down": ("revisions_down", "down", "num_down"),
    "revenue_mean": ("revenue_mean", "sales_mean"),
}


def inbox(campaign_id: str = CAMPAIGN_ID) -> Path:
    d = campaign_dir(campaign_id) / INBOX_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _find(cols: list, aliases: tuple):
    low = {c.lower(): c for c in cols}
    for a in aliases:
        if a in low:
            return low[a]
    for c in cols:
        if any(a in c.lower() for a in aliases):
            return c
    return None


def validate_sample(path: Path) -> dict:
    """Schema + PIT checks for one dropped file (CSV / JSON lines). Never an
    alpha statement."""
    p = Path(path)
    try:
        if p.suffix.lower() == ".csv":
            df = pd.read_csv(p)
        else:
            df = pd.read_json(p, lines=p.suffix.lower() in (".jsonl",
                                                           ".ndjson"))
    except Exception as e:
        return {"file": str(p), "state": "UNREADABLE", "error": str(e)[:200]}
    cols = list(df.columns)
    found = {k: _find(cols, v) for k, v in REQUIRED_FIELDS.items()}
    optional = {k: _find(cols, v) for k, v in OPTIONAL_FIELDS.items()}
    missing = [k for k, v in found.items() if v is None]
    checks = {"rows": int(len(df)), "columns": cols,
              "required_fields_found": found,
              "optional_fields_found": optional,
              "missing_required": missing}
    # PIT / continuity checks where the columns exist
    obs, tk, per, act = (found["observation_date"], found["ticker_or_id"],
                         found["fiscal_period"], found["actual"])
    if obs:
        d = pd.to_datetime(df[obs], errors="coerce")
        checks["observation_dates"] = {
            "parseable_share": float(d.notna().mean()),
            "first": str(d.min().date()) if d.notna().any() else None,
            "last": str(d.max().date()) if d.notna().any() else None,
            "monotone_within_ticker_period": None}
        if tk and per:
            mono = df.assign(_d=d).sort_values("_d").groupby([tk, per])[
                "_d"].apply(lambda s: bool(s.is_monotonic_increasing))
            checks["observation_dates"]["monotone_within_ticker_period"] = \
                float(mono.mean()) if len(mono) else None
            # vintages: multiple observation dates per (ticker, period)
            n_v = df.groupby([tk, per])[obs].nunique()
            checks["estimate_vintages"] = {
                "mean_vintages_per_period": float(n_v.mean()),
                "periods_with_single_vintage_share":
                    float((n_v <= 1).mean()),
                "revision_representation":
                    "MULTIPLE_VINTAGES_PER_PERIOD" if (n_v > 1).mean() > 0.5
                    else "SNAPSHOT_ONLY_SUSPECTED"}
    if tk:
        present = {t: bool((df[tk].astype(str).str.upper() == t).any())
                   for t in SAMPLE_TICKERS}
        checks["issuer_continuity"] = {
            "tickers_present": present,
            "delisted_MON_present": present.get("MON"),
            "renamed_META_present": present.get("META"),
            "bankrupt_HTZ_present": present.get("HTZ"),
            "id_column": tk}
    if act and per and tk:
        checks["actuals"] = {"share_rows_with_actual":
                             float(df[act].notna().mean())}
    state = "SCHEMA_VALID" if not missing else "SCHEMA_INCOMPLETE"
    return {"file": str(p), "sha256": _r39.sha_file(p), "state": state,
            "checks": checks, "purpose": SAMPLE_PURPOSE,
            "can_prove_alpha": SAMPLE_CAN_PROVE_ALPHA}


def build(campaign_id: str = CAMPAIGN_ID) -> dict:
    req = json.loads(R38_REQUEST.read_text(encoding="utf-8")) \
        if R38_REQUEST.exists() else {}
    box = inbox(campaign_id)
    files = sorted(p for p in box.iterdir() if p.is_file()) \
        if box.exists() else []
    validations = [validate_sample(p) for p in files]
    state = ("SAMPLE_RECEIVED_AND_VALIDATED" if validations and all(
        v["state"] == "SCHEMA_VALID" for v in validations)
        else "SAMPLE_RECEIVED_SCHEMA_INCOMPLETE" if validations
        else "WAITING_FOR_SAMPLE")
    body = artifact_body("r40_intrinio_sample_readiness/1", {
        "calculation_owner": CALCULATION_OWNER,
        "lane": "Intrinio / Steele Barcomb historical analyst sample",
        "request_prepared_by": "release38",
        "request_path": str(R38_REQUEST),
        "request_fingerprint": _r39.file_fingerprint(R38_REQUEST)
        if R38_REQUEST.exists() else None,
        "request_sent_by_claude": False,
        "frozen_sample_tickers": list(SAMPLE_TICKERS),
        "purpose": SAMPLE_PURPOSE,
        "sample_can_prove": ["historical observation dates",
                             "estimate vintages", "revision representation",
                             "issuer continuity", "delisted handling",
                             "available fields"],
        "sample_cannot_prove": ["alpha (five names are not a universe)"],
        "can_prove_alpha": SAMPLE_CAN_PROVE_ALPHA,
        "inbox": str(box),
        "validator": "alpha_agent.r40.intrinio_readiness.validate_sample",
        "required_fields": {k: list(v) for k, v in REQUIRED_FIELDS.items()},
        "optional_fields": {k: list(v) for k, v in OPTIONAL_FIELDS.items()},
        "files_found": len(files),
        "validations": validations,
        "state": state,
        "purchase_allowed": C.INTRINIO_PURCHASE_ALLOWED,
        "trial_started": False, "account_created": False,
        "licence_accepted": False, "money_spent": 0.0,
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "prior_evidence": req.get("prior_evidence"),
    })
    body["readiness_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / ARTIFACT_NAME, body,
                    immutable=False)
    return body
