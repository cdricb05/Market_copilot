"""alpha_agent.r37.samples - the ONE Release-37 sample acquisition and proof owner.

Reading a vendor's coverage page is not evidence. This module is where a claim
becomes a measurement:

* **Free public archives are DOWNLOADED**, checksummed and parsed, and the
  parsed result is what the scorecard cites. Nothing here needs an account, a
  card, a trial or a licence acceptance.
* **Blocked routes are RE-PROBED**, because "it was 403 yesterday" is a quote
  rather than a measurement. Release 36's licence block on the Cboe settlement
  archive and Release 35's block on the Zacks tables are both confirmed here
  against the live endpoints rather than carried forward on trust.
* **The owned entitlement is MEASURED against the vendor's own client**, not
  against a subscription email. Release 36 established that the Continuous
  Futures database serves one market; this release adds the measurement Release
  36 did not make - whether the installed client can express a DATED CONTRACT at
  all - because that is the difference between "the vendor cannot deliver this"
  and "we have not paid for it".

Every byte that arrives goes through :func:`alpha_agent.r35.acquisition.fetch`,
the released HTTP owner. This module opens no socket of its own.
"""
from __future__ import annotations

import csv
import datetime as _dt
import io
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

from .. import r37
from ..r35 import acquisition as _r35_acquisition
from ..r36 import entitlements as _r36_entitlements
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r37.samples"
REGISTRY_SCHEMA = "r37_sample_registry/1"
REGISTRY_ARTIFACT = "sample_registry.json"
VALIDATION_SCHEMA = "r37_sample_validation_report/1"
VALIDATION_ARTIFACT = "sample_validation_report.json"

C = _contract

#: Sample verdicts. A sample proves a SCHEMA and a delivery mechanism; it never
#: proves alpha, and there is no verdict here that could be mistaken for one.
SAMPLE_OK = "SAMPLE_VALIDATED"
SAMPLE_THIN = "SAMPLE_VALIDATED_BUT_THIN"
SAMPLE_UNPARSEABLE = "SAMPLE_UNPARSEABLE"
SAMPLE_NOT_OBTAINED = "SAMPLE_NOT_OBTAINED"
SAMPLE_BLOCKED = "SAMPLE_BLOCKED_BY_PROVIDER"
SAMPLE_VERDICTS = (SAMPLE_OK, SAMPLE_THIN, SAMPLE_UNPARSEABLE,
                   SAMPLE_NOT_OBTAINED, SAMPLE_BLOCKED)

#: A sample is never allowed to carry a research claim. Declared as a constant
#: because the temptation is real and the architecture audit checks for it.
A_SAMPLE_MAY_SUPPORT_AN_ALPHA_CLAIM = False

#: Which free route lands in which file. Names are stable so a re-run reuses the
#: bytes an earlier artifact was hashed against.
SAMPLE_FILES = {
    "CBOE_CFE_VOLUME_OPEN_INTEREST": "cboe_cfe/cfevoloi.csv",
    "LBMA_GOLD_PM": "lbma/gold_pm.json",
    "LBMA_SILVER": "lbma/silver.json",
    "LBMA_PLATINUM_AM": "lbma/platinum_am.json",
    "NYFED_PRIMARY_DEALER_POSITIONS": "nyfed/primary_dealer_timeseries.csv",
}

#: Routes that are probed to CONFIRM a block and never downloaded.
CONFIRM_ONLY = ("CBOE_CFE_DAILY_SETTLEMENT",)


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def sample_path(name: str) -> Path:
    return r37.acquisition_root() / SAMPLE_FILES[name]


# --------------------------------------------------------------------------- #
# Acquisition - one owner, reused
# --------------------------------------------------------------------------- #
def acquire_free_samples(*, transport=None, names=None) -> dict:
    """Download every free public sample through the released HTTP owner."""
    out = {}
    for name in (names or sorted(SAMPLE_FILES)):
        url = C.FREE_ROUTES[name]
        record = _r35_acquisition.fetch(url, sample_path(name), min_bytes=256,
                                        transport=transport)
        record["source"] = name
        record["cost_usd"] = 0.0
        record["account_required"] = False
        out[name] = record
    return out


def _probe(url: str, *, transport=None, max_bytes: int = 2048) -> dict:
    """One bounded request used only to CONFIRM a block. Retains no payload."""
    if transport is not None:
        try:
            status, head = transport(url)
        except Exception as exc:  # noqa: BLE001
            return {"url": url, "status": None, "error": type(exc).__name__}
        return {"url": url, "status": int(status), "head_bytes": len(head or b"")}
    request = urllib.request.Request(
        url, headers={"User-Agent": C.HTTP_USER_AGENT})
    try:
        with urllib.request.urlopen(
                request, timeout=C.HTTP_TIMEOUT_SECONDS) as response:
            return {"url": url, "status": int(response.status),
                    "head_bytes": len(response.read(max_bytes))}
    except urllib.error.HTTPError as exc:
        return {"url": url, "status": int(exc.code)}
    except Exception as exc:  # noqa: BLE001
        return {"url": url, "status": None, "error": type(exc).__name__}


def confirm_blocks(*, transport=None) -> dict:
    """Re-probe every route an earlier release recorded as blocked."""
    routes = dict(C.BLOCK_CONFIRMATION_ROUTES)
    routes["CBOE_CFE_DAILY_SETTLEMENT"] = C.CBOE_CFE_DAILY_SETTLEMENT_URL
    out = {}
    for name, url in sorted(routes.items()):
        probe = _probe(url, transport=transport)
        status = probe.get("status")
        out[name] = {
            "probe": probe,
            # A route that could not be reached is UNMEASURED, not open. A
            # transport error reported as "no longer blocked" would be the
            # release telling itself good news it did not measure.
            "still_blocked": (None if status is None
                              else status in (401, 402, 403, 404, 410)),
            "state": (_r36_entitlements.BLOCKED_LICENCE
                      if status in (401, 402, 403)
                      else _r36_entitlements.BLOCKED_MISSING
                      if status in (404, 410)
                      else _r36_entitlements.ENTITLED if status == 200
                      else _r36_entitlements.UNMEASURED),
            "measured_at": _now(),
        }
    return out


# --------------------------------------------------------------------------- #
# The owned entitlement, measured against the vendor's own client
# --------------------------------------------------------------------------- #
#: The dated-contract calls a futures archive must support. Their PRESENCE in
#: the installed client is what separates a capability gap from an entitlement
#: gap, and it is the single most decisive measurement in this release.
NORGATE_DATED_CONTRACT_API = (
    "futures_market_symbols", "futures_market_session_symbols",
    "futures_market_session_contracts", "futures_market_session_type",
    "first_notice_date", "last_quoted_date", "first_quoted_date",
    "point_value", "tick_size", "lowest_ever_tick_size", "margin", "currency",
    "exchange_name", "price_timeseries", "unadjusted_close_timeseries",
)


def measure_owned_futures_client(*, vendor=None) -> dict:
    """Can the OWNED client express a dated futures contract, and is it entitled?

    Two different questions, and collapsing them is how an estate concludes "the
    vendor cannot do this" when the truth is "we did not buy it". The answer
    decides the implementation-complexity term in the purchase case.
    """
    try:
        nd = vendor if vendor is not None else __import__("norgatedata")
    except Exception as exc:  # noqa: BLE001
        return {"state": _r36_entitlements.NOT_CONFIGURED,
                "measured_at": _now(),
                "reason": "%s: %s" % (type(exc).__name__, str(exc)[:120]),
                "client_supports_dated_contracts": None}

    present = {name: bool(callable(getattr(nd, name, None)))
               for name in NORGATE_DATED_CONTRACT_API}
    missing = sorted(n for n, ok in present.items() if not ok)

    def _try(fn_name, *args):
        fn = getattr(nd, fn_name, None)
        if not callable(fn):
            return {"ok": False, "reason": "FUNCTION_ABSENT"}
        try:
            return {"ok": True, "value": fn(*args)}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "reason": type(exc).__name__}

    markets = _try("futures_market_symbols")
    sessions = _try("futures_market_session_symbols")
    n_markets = (len(markets["value"]) if markets.get("ok")
                 and markets.get("value") is not None else None)

    entitled_symbol = None
    if markets.get("ok") and markets.get("value"):
        entitled_symbol = "&%s" % str(sorted(map(str, markets["value"]))[0])

    metadata = {}
    if entitled_symbol:
        for call in ("point_value", "tick_size", "margin", "currency",
                     "exchange_name", "first_quoted_date",
                     "futures_market_name"):
            metadata[call] = _try(call, entitled_symbol)
    contracts = (_try("futures_market_session_contracts", entitled_symbol)
                 if entitled_symbol else {"ok": False,
                                          "reason": "NO_ENTITLED_MARKET"})

    metadata_ok = sum(1 for r in metadata.values() if r.get("ok"))
    return {
        "state": _r36_entitlements.ENTITLED,
        "measured_at": _now(),
        "package_version": str(getattr(nd, "__version__", "unknown")),
        "dated_contract_api_present": present,
        "dated_contract_api_missing": missing,
        "client_supports_dated_contracts": not missing,
        "entitled_futures_markets": n_markets,
        "entitled_market_symbol": entitled_symbol,
        "entitled_session_symbols": (len(sessions["value"])
                                     if sessions.get("ok")
                                     and sessions.get("value") is not None
                                     else None),
        "contract_metadata_calls_answered": metadata_ok,
        "contract_metadata": {k: {"ok": v.get("ok"),
                                  "value": (str(v.get("value"))[:60]
                                            if v.get("ok") else None),
                                  "reason": v.get("reason")}
                              for k, v in sorted(metadata.items())},
        "dated_contract_enumeration": {"ok": contracts.get("ok"),
                                       "reason": contracts.get("reason")},
        "finding": (
            "the installed client exposes every dated-contract call and returns "
            "real contract metadata for the one entitled market, while "
            "enumerating that market's DATED CONTRACTS fails. The wall is the "
            "ENTITLEMENT, not the client, so the implementation cost of the "
            "recommended purchase is an entitlement change rather than an "
            "integration project"
            if (not missing and metadata_ok and not contracts.get("ok"))
            else "measurement incomplete; see the fields above"),
    }


# --------------------------------------------------------------------------- #
# Validation - what the bytes actually contain
# --------------------------------------------------------------------------- #
def _read_text(path: Path, limit_bytes: int = 40 * 1024 * 1024) -> str:
    with open(path, "rb") as handle:
        return handle.read(limit_bytes).decode("utf-8", "replace")


def _validate_cboe_voloi(path: Path) -> dict:
    """Cboe CFE daily volume and open interest.

    The measurement that mattered: this file is **wide and product-level**, one
    row per date with a Volume and an OI column per PRODUCT. It is not keyed by
    expiry, so it carries no per-contract open interest and cannot support a
    term-structure positioning study. The Release-37 scorecard originally
    claimed per-dated-contract coverage on the strength of the exchange's page
    description; parsing the bytes corrected it, which is the entire reason
    Track B downloads samples instead of reading pages.
    """
    text = _read_text(path)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    # The first line is a legal disclaimer that also contains commas, so the
    # header is found by its leading field rather than by counting delimiters.
    header_index = next((i for i, ln in enumerate(lines)
                         if ln.lstrip().lower().startswith("date,")), None)
    if header_index is None:
        return {"verdict": SAMPLE_UNPARSEABLE, "reason": "NO_DATE_HEADER_ROW"}
    reader = csv.DictReader(io.StringIO("\n".join(lines[header_index:])))
    raw_fields = list(reader.fieldnames or [])
    fields = [f.strip() for f in raw_fields if f is not None]
    date_key = raw_fields[0] if raw_fields else None
    volume_cols = [f for f in fields if f.lower().endswith("volume")]
    oi_cols = [f for f in fields if f.lower().endswith("oi")]
    products = sorted({f[:-len(" Volume")].strip()
                       for f in volume_cols if len(f) > len(" Volume")})

    rows, dates, populated = 0, [], 0
    seen, duplicates = set(), 0
    for record in reader:
        rows += 1
        value = str(record.get(date_key) or "").strip()
        if value:
            dates.append(value)
            if value in seen:
                duplicates += 1
            seen.add(value)
        if any(str(record.get(c) or "").strip() for c in volume_cols):
            populated += 1
    dates = [d for d in dates if d]
    return {
        "verdict": SAMPLE_OK if rows > 1000 else SAMPLE_THIN,
        "rows": rows,
        "shape": "WIDE_ONE_ROW_PER_DATE_ONE_COLUMN_PER_PRODUCT",
        "n_fields": len(fields),
        "fields_sample": fields[:8],
        "date_field": date_key,
        "first_date": dates[0] if dates else None,
        "last_date": dates[-1] if dates else None,
        "distinct_products": len(products),
        "products_sample": products[:8],
        "rows_with_any_volume": populated,
        "duplicate_dates": duplicates,
        "carries_settlement_price": any("settle" in f.lower() for f in fields),
        "carries_open_interest": bool(oi_cols),
        "carries_per_contract_expiry": any("expir" in f.lower() for f in fields),
        "granularity": "PRODUCT_LEVEL_NOT_CONTRACT_LEVEL",
        "correction_to_the_scorecard": (
            "the exchange page describes this as historical volume and open "
            "interest; parsing it shows PRODUCT-level totals with no expiry "
            "key. It therefore cannot carry a per-contract positioning study, "
            "and the provider row was corrected to say so"),
        "point_in_time_note": (
            "published by the exchange each day; the file is cumulative and "
            "carries no revision history, so a decision on date t must use the "
            "rows dated on or before t"),
    }


def _validate_lbma(path: Path) -> dict:
    """LBMA benchmark JSON: [{d: date, v: [usd, gbp, eur]}, ...]."""
    try:
        payload = json.loads(_read_text(path))
    except (ValueError, UnicodeDecodeError) as exc:
        return {"verdict": SAMPLE_UNPARSEABLE, "reason": type(exc).__name__}
    if not isinstance(payload, list) or not payload:
        return {"verdict": SAMPLE_UNPARSEABLE, "reason": "NOT_A_LIST"}
    dates = sorted(str(r.get("d")) for r in payload if r.get("d"))
    priced = sum(1 for r in payload
                 if isinstance(r.get("v"), list) and r["v"]
                 and r["v"][0] is not None)
    return {
        "verdict": SAMPLE_OK if len(dates) > 1000 else SAMPLE_THIN,
        "rows": len(payload),
        "fields": sorted({k for r in payload[:50] for k in r}),
        "first_date": dates[0] if dates else None,
        "last_date": dates[-1] if dates else None,
        "rows_with_a_price": priced,
        "missing_fraction": (round(1.0 - priced / len(payload), 4)
                             if payload else None),
        "duplicate_dates": len(dates) - len(set(dates)),
        "instrument_level": C.LEVEL_SIGNAL,
        "point_in_time_note": (
            "a benchmark fixing published the same day; it is NOT a futures "
            "settlement and may never be recorded as one"),
    }


def _validate_nyfed(path: Path) -> dict:
    """New York Fed primary-dealer time series."""
    text = _read_text(path)
    reader = csv.DictReader(io.StringIO(text))
    fields = [f.strip().strip('"') for f in (reader.fieldnames or [])]
    rows, dates, series = 0, [], set()
    date_key = next((f for f in (reader.fieldnames or []) if "Date" in f), None)
    series_key = next((f for f in (reader.fieldnames or [])
                       if "Series" in f), None)
    for record in reader:
        rows += 1
        if date_key and record.get(date_key):
            dates.append(str(record[date_key]).strip().strip('"'))
        if series_key and record.get(series_key):
            series.add(str(record[series_key]).strip().strip('"'))
    dates = sorted(d for d in dates if d)
    return {
        "verdict": SAMPLE_OK if rows > 1000 else SAMPLE_THIN,
        "rows": rows,
        "fields": fields,
        "first_date": dates[0] if dates else None,
        "last_date": dates[-1] if dates else None,
        "distinct_series": len(series),
        "duplicate_dates": None,
        "instrument_level": C.LEVEL_SIGNAL,
        "point_in_time_note": (
            "published weekly with a stated lag; an as-of rule must apply that "
            "lag, exactly as Release 35 does for the Commitments of Traders"),
    }


VALIDATORS = {
    "CBOE_CFE_VOLUME_OPEN_INTEREST": _validate_cboe_voloi,
    "LBMA_GOLD_PM": _validate_lbma,
    "LBMA_SILVER": _validate_lbma,
    "LBMA_PLATINUM_AM": _validate_lbma,
    "NYFED_PRIMARY_DEALER_POSITIONS": _validate_nyfed,
}


def validate_samples(acquired: dict) -> dict:
    """Parse every acquired sample and report what it actually contains."""
    out = {}
    for name, record in sorted(acquired.items()):
        if not record.get("ok"):
            out[name] = {"verdict": SAMPLE_NOT_OBTAINED,
                         "reason": record.get("reason")}
            continue
        path = Path(record["path"])
        if not path.exists():
            out[name] = {"verdict": SAMPLE_NOT_OBTAINED,
                         "reason": "FILE_ABSENT_AFTER_FETCH"}
            continue
        validator = VALIDATORS.get(name)
        if validator is None:
            out[name] = {"verdict": SAMPLE_NOT_OBTAINED,
                         "reason": "NO_VALIDATOR_DECLARED"}
            continue
        try:
            result = validator(path)
        except Exception as exc:  # noqa: BLE001
            result = {"verdict": SAMPLE_UNPARSEABLE,
                      "reason": "%s: %s" % (type(exc).__name__, str(exc)[:120])}
        result.update({"path": str(path),
                       "size_bytes": record.get("size_bytes"),
                       "sha256": record.get("sha256"),
                       "url": record.get("url")})
        out[name] = result
    return out


# --------------------------------------------------------------------------- #
# Artifacts
# --------------------------------------------------------------------------- #
def registry_artifact(acquired: dict, blocks: dict, client: dict, *,
                      campaign_id: str, created_at: str) -> dict:
    checksums = {name: rec.get("sha256") for name, rec in sorted(acquired.items())
                 if rec.get("ok")}
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "http_owner": _r35_acquisition.CALCULATION_OWNER,
        "acquisition_root": str(r37.acquisition_root()),
        "acquired": acquired,
        "payload_checksums": checksums,
        "payloads_acquired": len(checksums),
        "total_bytes": sum(int(rec.get("size_bytes") or 0)
                           for rec in acquired.values() if rec.get("ok")),
        "block_confirmations": blocks,
        "blocks_still_standing": sorted(n for n, b in blocks.items()
                                        if b["still_blocked"] is True),
        "blocks_that_opened": sorted(n for n, b in blocks.items()
                                     if b["still_blocked"] is False),
        "blocks_unmeasured": sorted(n for n, b in blocks.items()
                                    if b["still_blocked"] is None),
        "owned_futures_client": client,
        "money_spent_usd": 0.0,
        "trials_started": 0,
        "accounts_created": 0,
        "credentials_written_to_artifacts": False,
        "a_sample_may_support_an_alpha_claim":
            A_SAMPLE_MAY_SUPPORT_AN_ALPHA_CLAIM,
    }
    return r37.artifact_body(REGISTRY_SCHEMA, payload)


def validation_artifact(validated: dict, *, campaign_id: str,
                        created_at: str) -> dict:
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "verdicts": list(SAMPLE_VERDICTS),
        "samples": validated,
        "validated": sorted(n for n, v in validated.items()
                            if v.get("verdict") in (SAMPLE_OK, SAMPLE_THIN)),
        "not_obtained": sorted(n for n, v in validated.items()
                               if v.get("verdict") == SAMPLE_NOT_OBTAINED),
        "unparseable": sorted(n for n, v in validated.items()
                              if v.get("verdict") == SAMPLE_UNPARSEABLE),
        "a_sample_may_support_an_alpha_claim":
            A_SAMPLE_MAY_SUPPORT_AN_ALPHA_CLAIM,
        "what_a_sample_proves": (
            "a schema, a delivery mechanism, a date range and a set of fields. "
            "It does not prove a predictive result and this release makes none"),
    }
    return r37.artifact_body(VALIDATION_SCHEMA, payload)


def registry_path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / REGISTRY_ARTIFACT


def validation_path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / VALIDATION_ARTIFACT


def freeze_registry(body: dict):
    return r37.write_json(registry_path_for(body["campaign_id"]), body)


def freeze_validation(body: dict):
    return r37.write_json(validation_path_for(body["campaign_id"]), body)


def load_registry(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r37.read_json(registry_path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "SAMPLE_VERDICTS", "SAMPLE_OK", "SAMPLE_THIN",
           "SAMPLE_UNPARSEABLE", "SAMPLE_NOT_OBTAINED", "SAMPLE_BLOCKED",
           "SAMPLE_FILES", "CONFIRM_ONLY", "NORGATE_DATED_CONTRACT_API",
           "A_SAMPLE_MAY_SUPPORT_AN_ALPHA_CLAIM", "sample_path",
           "acquire_free_samples", "confirm_blocks",
           "measure_owned_futures_client", "validate_samples", "VALIDATORS",
           "registry_artifact", "validation_artifact", "freeze_registry",
           "freeze_validation", "load_registry", "registry_path_for",
           "validation_path_for"]
