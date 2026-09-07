"""alpha_agent.r59.provider_probe - bounded, read-only provider capability probe.

Section 5 asks one question the estate could not answer from its own artifacts:
*are we actually using what we pay EODHD for?*

R58 measured the OWNED store: news for 7 tickers, corporate actions for 5,
earnings for 15. The R59 read of ``configs/alpha_agent/stage2_ingestion.json``
found why - the collector is pointed at ``sample_symbols``, a seven-name smoke
list (AAPL, MSFT, NVDA, AMZN, JPM, XOM, SPY). That is a CONFIGURATION limit,
and the field name says so.

But a configuration limit only matters if the entitlement and the HISTORY are
there. A wider universe with three days of history buys a prospective family,
not a backtestable one, and those are very different things. Only the provider
can answer that, so this module asks it - within strict bounds:

* read-only HTTP GET, no write endpoint, no account or subscription change;
* a hard cap on the number of requests per run (``MAX_REQUESTS``);
* rate limited to the source config's own ``min_interval_seconds``;
* the API key is read from the shell environment and is NEVER written to an
  artifact, a log line or an error message;
* results land under the R59 research root only.

This is inspection of a subscription the operator already holds. It purchases
nothing, starts no trial, changes no tier and creates no account.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from typing import Optional

from .. import r59

CALCULATION_OWNER = "alpha_agent.r59.provider_probe"
SCHEMA = "r59_provider_capability_probe/1"

BASE_URL = "https://eodhd.com/api"
KEY_ENV = "EODHD_API_KEY"
MAX_REQUESTS = 12
TIMEOUT_SECONDS = 20
MIN_INTERVAL_SECONDS = 0.25

#: Names deliberately OUTSIDE the seven-symbol sample list, so a successful
#: response proves the entitlement is not confined to the sampled names.
OFF_SAMPLE = ("KO.US", "CAT.US", "PFE.US")


class Budget:
    """A hard request ceiling. Refuses rather than silently continuing."""

    def __init__(self, limit: int = MAX_REQUESTS):
        self.limit = int(limit)
        self.used = 0

    def take(self) -> None:
        if self.used >= self.limit:
            raise RuntimeError("probe request budget exhausted (%d)"
                               % self.limit)
        self.used += 1


def _key() -> Optional[str]:
    k = os.environ.get(KEY_ENV)
    return k if k else None


def _get(path: str, params: dict, budget: Budget) -> dict:
    """One bounded read-only GET. Never returns or logs the API token."""
    key = _key()
    if not key:
        return {"state": "CREDENTIAL_ABSENT",
                "reason": "%s is not set in this shell" % KEY_ENV}
    budget.take()
    q = dict(params)
    q["api_token"] = key
    q["fmt"] = "json"
    url = "%s/%s?%s" % (BASE_URL, path.lstrip("/"), urllib.parse.urlencode(q))
    safe = url.replace(key, "<REDACTED>")
    req = urllib.request.Request(url, headers={"User-Agent": "paper-trader-r59"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            code = resp.getcode()
    except urllib.error.HTTPError as exc:
        return {"state": "HTTP_%d" % exc.code, "url": safe,
                "reason": exc.reason}
    except Exception as exc:                     # noqa: BLE001
        return {"state": "TRANSPORT_ERROR", "url": safe,
                "reason": "%s" % type(exc).__name__}
    finally:
        time.sleep(MIN_INTERVAL_SECONDS)
    try:
        data = json.loads(body)
    except ValueError:
        return {"state": "NON_JSON", "url": safe, "bytes": len(body)}
    return {"state": "OK", "http": code, "url": safe, "data": data}


def _rows(res: dict) -> list:
    d = res.get("data")
    if isinstance(d, list):
        return d
    if isinstance(d, dict):
        return [d]
    return []


def probe(*, budget: Optional[Budget] = None, write: bool = True) -> dict:
    """Establish, by measurement, what the EODHD subscription actually serves.

    Answers three separate questions that the estate had been conflating:
    entitlement (may we ask?), universe (for names we do not sample?) and
    HISTORY (how far back?). Only the third decides whether widening collection
    produces a backtestable family.
    """
    budget = budget or Budget()
    if not _key():
        return {"calculation_owner": CALCULATION_OWNER, "schema": SCHEMA,
                "state": "CREDENTIAL_ABSENT",
                "reason": "%s not present in this shell; entitlement cannot be "
                          "measured and is NOT assumed" % KEY_ENV,
                "purchases_anything": False}

    today = date.today()
    findings: dict = {}

    # 1. NEWS on an OFF-SAMPLE name, recent.
    sym = OFF_SAMPLE[0]
    r = _get("news", {"s": sym, "limit": 5,
                      "from": str(today - timedelta(days=7)),
                      "to": str(today)}, budget)
    rows = _rows(r)
    findings["news_off_sample_recent"] = {
        "symbol": sym, "state": r["state"], "rows": len(rows),
        "entitled": r["state"] == "OK" and bool(rows),
        "reason": r.get("reason"),
    }

    # 2. NEWS HISTORY - the question that decides backtestability.
    hist_windows = [("2016-01-01", "2016-01-31"), ("2020-01-01", "2020-01-31")]
    hist = {}
    for f, t in hist_windows:
        rh = _get("news", {"s": sym, "limit": 5, "from": f, "to": t}, budget)
        rr = _rows(rh)
        dates = sorted({str(x.get("date"))[:10] for x in rr if x.get("date")})
        hist[f[:4]] = {"state": rh["state"], "rows": len(rr),
                       "dates": dates[:3]}
    findings["news_history"] = hist
    findings["news_history_available"] = any(
        v["state"] == "OK" and v["rows"] > 0 for v in hist.values())

    # 3. CORPORATE ACTIONS (dividends) on an off-sample name, long window.
    sym2 = OFF_SAMPLE[1]
    rd = _get("div/%s" % sym2, {"from": "2010-01-01", "to": str(today)},
              budget)
    rows_d = _rows(rd)
    ddates = sorted({str(x.get("date"))[:10] for x in rows_d if x.get("date")})
    findings["dividends_off_sample_history"] = {
        "symbol": sym2, "state": rd["state"], "rows": len(rows_d),
        "first": ddates[0] if ddates else None,
        "last": ddates[-1] if ddates else None,
        "entitled": rd["state"] == "OK" and bool(rows_d),
        "reason": rd.get("reason"),
    }

    # 4. EARNINGS calendar - does it carry a time of day?
    re_ = _get("calendar/earnings",
               {"from": str(today - timedelta(days=14)), "to": str(today)},
               budget)
    er = re_.get("data") if isinstance(re_.get("data"), dict) else {}
    elist = (er or {}).get("earnings") or []
    before_after = sorted({str(x.get("before_after_market"))
                           for x in elist[:200]
                           if x.get("before_after_market")})
    findings["earnings_calendar"] = {
        "state": re_["state"], "rows": len(elist),
        "distinct_symbols": len({x.get("code") for x in elist}),
        "before_after_market_values": before_after,
        "carries_session_timing": bool(before_after),
        "reason": re_.get("reason"),
    }

    entitled = [k for k, v in findings.items()
                if isinstance(v, dict) and v.get("entitled")]
    body = {
        "calculation_owner": CALCULATION_OWNER,
        "schema": SCHEMA,
        "provider": "EODHD",
        "state": "PROBED",
        "requests_used": budget.used,
        "request_budget": budget.limit,
        "findings": findings,
        "entitled_families": entitled,
        "configured_universe": {
            "config": "configs/alpha_agent/stage2_ingestion.json",
            "field": "sources.eodhd.sample_symbols",
            "symbols": ["AAPL.US", "MSFT.US", "NVDA.US", "AMZN.US", "JPM.US",
                        "XOM.US", "SPY.US"],
            "note": "a seven-name SMOKE list; the field name says sample. This "
                    "is the reason the owned news store has 7 distinct "
                    "tickers, not an entitlement ceiling.",
        },
        "safety": {"read_only": True, "purchases_anything": False,
                   "starts_trial": False, "changes_subscription": False,
                   "creates_account": False,
                   "credential_never_persisted": True},
    }
    if write:
        p = r59.write_artifact("r59_eodhd_capability_probe.json", body,
                               subdir="data")
        body["artifact_path"] = str(p)
    return body


def verdict(probe_result: dict) -> dict:
    """Turn the probe into the frontier's answer, without overclaiming."""
    if probe_result.get("state") != "PROBED":
        return {"state": r59.DO_BLOCKED,
                "reason": probe_result.get("reason") or "probe did not run"}
    f = probe_result["findings"]
    news_ok = f["news_off_sample_recent"]["entitled"]
    news_hist = bool(probe_result["findings"].get("news_history_available"))
    div_ok = f["dividends_off_sample_history"]["entitled"]
    timing = f["earnings_calendar"]["carries_session_timing"]

    if news_ok and news_hist:
        state = r59.DO_FREE_AVAILABLE
        reason = ("entitlement covers off-sample names AND history is served; "
                  "widening the collector's universe would create a "
                  "BACKTESTABLE cross-sectional news family")
    elif news_ok:
        state = r59.DO_ALREADY_OWNED_UNUSED
        reason = ("entitlement covers off-sample names but the provider "
                  "returned no deep history on this endpoint, so widening the "
                  "universe creates a PROSPECTIVE family only - real, but not "
                  "backtestable")
    else:
        state = r59.DO_BLOCKED
        reason = "the subscription did not serve news for an off-sample name"
    return {"state": state, "reason": reason,
            "news_entitled_off_sample": news_ok,
            "news_history_served": news_hist,
            "corporate_actions_history_served": div_ok,
            "earnings_carries_session_timing": timing}
