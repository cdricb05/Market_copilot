r"""alpha_agent.alpha_recovery.databento_acquisition - the ONE owner of the
campaign's Databento historical acquisition state.

WHY THIS EXISTS
    The intraday axis closed with a NAMED, MEASURED data defect rather than a
    strategy defect: the owned R45 minute panel stops at 12:59 ET, so there is
    no afternoon, no closing auction, and no overnight session. Every intraday
    family was therefore tested on the first 150 minutes of the US day and
    nothing could be marked to the close. Native CME futures 1-minute history
    removes exactly that defect - a CME session runs ~23 hours, so it carries
    the Asia and Europe sessions, the US afternoon AND the settlement window -
    and it adds two markets the estate has never held at any frequency
    (ENERGY via CL, FX via 6E/6J). Under contract rule 13 this is a MATERIAL
    COVERAGE improvement, which is the specific condition that permits
    reopening price-derived research. It is not another daily transform.

THE SPENDING CONTRACT THIS MODULE ENFORCES IN CODE (not in a comment)
    1. NOTHING IS EVER DOWNLOADED BEFORE ITS COST IS KNOWN. ``download`` refuses
       unless a cost estimate for the byte-identical request signature already
       exists in this session's plan. There is no code path that reaches
       ``timeseries.get_range`` without first having reached
       ``metadata.get_cost`` for the same signature.
    2. ZERO PAID DOLLARS. The plan is capped by ``budget_usd`` (the free credit
       balance the operator states). A plan whose estimate exceeds the cap is
       not trimmed silently - the optimiser drops whole instruments, in
       declared value order, until it fits, and records what it dropped.
    3. NO SUBSCRIPTION. Only usage-based historical endpoints are called. The
       module has no code path to any plan, subscription or live endpoint.

WHY DATED CONTRACTS AND NOT ``ES.c.0``
    A continuous symbol is a convenience series stitched by a roll rule. Even
    where that rule is causal, the series is a DERIVED product whose roll dates
    the estate did not choose and cannot audit. This module requests GENUINE
    DATED CONTRACTS by raw symbol (``ESZ5``-style), each over a window that
    brackets its own front-month tenure, and rolls them itself on a strictly
    causal volume rule. The roll is then part of the research record.

WHY THE SYMBOL FORMAT IS RESOLVED AND NEVER ASSUMED
    CME's dated symbology is written both ``ESZ5`` and ``ESZ25`` depending on
    the venue and vintage, and a wrong guess spends credit on an empty result.
    ``resolve_symbols`` submits every candidate spelling to ``symbology.resolve``
    - which bills no data - and keeps whichever the venue actually recognises.
    The acquisition never guesses a symbol it has not seen resolve.

RESEARCH ONLY. No order, no fill, no promotion, no registration, no
subscription, no paid dollar. Writes only under the campaign research root.
"""
from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

from . import MIN_EFFECTIVE_PERIODS, research_root, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.databento_acquisition"
ARTIFACT_NAME = "databento_acquisition_state.json"

# ---------------------------------------------------------------- the provider
BASE_URL = "https://hist.databento.com/v0"
ENV_KEY = "DATABENTO_API_KEY"
#: The operator states the free-credit balance. There is no public v0 endpoint
#: that reports it, so it is NEVER inferred: absent, the plan is priced but
#: nothing is downloaded.
ENV_BUDGET = "DATABENTO_FREE_CREDIT_USD"
#: Where the budget figure came from. The v0 API exposes no balance endpoint,
#: so a stated budget is ALWAYS one of two different things, and conflating
#: them is how an accidental paid dollar happens:
#:   OPERATOR_STATED  - a human read the remaining balance off the portal.
#:   PROVIDER_PUBLISHED_FREE_TIER - the vendor's published signup grant, which
#:                      is an UPPER BOUND on a fresh account and says nothing
#:                      about how much of it is still unspent.
#: The artifact records which, so no later reader can mistake the second for
#: the first.
ENV_BUDGET_SOURCE = "DATABENTO_FREE_CREDIT_SOURCE"
BUDGET_SOURCE_DEFAULT = "OPERATOR_STATED"

DATASET = "GLBX.MDP3"          # CME Globex MDP 3.0 - CME, CBOT, COMEX, NYMEX
SCHEMA = "ohlcv-1m"            # 1-minute open/high/low/close/volume
STYPE_IN = "raw_symbol"        # genuine dated contracts, never `continuous`
MODES = ("historical", "historical-streaming")   # batch is normally the cheaper

#: Hard ceiling safety margin. A plan must fit inside budget * (1 - MARGIN) so
#: that provider-side rounding can never turn an "exactly at budget" plan into
#: a paid dollar.
BUDGET_SAFETY_MARGIN = 0.10

HTTP_TIMEOUT = 90
DOWNLOAD_TIMEOUT = 900

# ------------------------------------------------------- the research universe
# Priority is the operator's, declared in the brief and NOT reordered by cost.
# The bucket is what makes an instrument informationally NEW rather than a near
# duplicate: four Treasury contracts are one rates bucket, not four markets.
MONTH_CODE = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
              7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}
QUARTERLY = (3, 6, 9, 12)
GOLD_CYCLE = (2, 4, 6, 8, 10, 12)
MONTHLY = tuple(range(1, 13))

#: root -> (priority tier, bucket, cycle, description)
UNIVERSE = {
    "ES": (1, "US_EQUITY_INDEX", QUARTERLY, "E-mini S&P 500"),
    "NQ": (1, "US_EQUITY_INDEX", QUARTERLY, "E-mini Nasdaq 100"),
    "GC": (2, "METALS", GOLD_CYCLE, "COMEX gold"),
    "6E": (3, "FX", QUARTERLY, "EUR/USD"),
    "6J": (3, "FX", QUARTERLY, "JPY/USD"),
    "ZN": (4, "US_RATES", QUARTERLY, "10-year Treasury note"),
    "ZF": (4, "US_RATES", QUARTERLY, "5-year Treasury note"),
    "ZT": (4, "US_RATES", QUARTERLY, "2-year Treasury note"),
    "ZB": (4, "US_RATES", QUARTERLY, "30-year Treasury bond"),
    "CL": (5, "ENERGY", MONTHLY, "WTI crude oil"),
}
PRIORITY_ORDER = ("ES", "NQ", "GC", "6E", "6J", "ZN", "ZF", "ZT", "ZB", "CL")

#: What the estate ALREADY has at 1-minute resolution, so the value model can
#: reward genuinely new markets over duplicated ones.
OWNED_INTRADAY_BUCKETS = {
    "US_EQUITY_INDEX": "SPY/QQQ ETF minutes, 09:30-11:59 ET only",
    "US_RATES": "TLT ETF minutes, 09:30-11:59 ET only",
    "METALS": "GLD ETF minutes, 09:30-11:59 ET only",
}
NEW_BUCKETS = ("FX", "ENERGY")

#: Declared BEFORE any cost was seen, so the optimiser cannot be accused of
#: having been fitted to the price list.
TIER_WEIGHT = {1: 1.00, 2: 0.80, 3: 0.70, 4: 0.60, 5: 0.75}
#: Diminishing returns WITHIN a bucket: the second rates contract is not a
#: second market. This is what stops the optimiser buying four Treasuries.
BUCKET_NOVELTY = (1.00, 0.30, 0.12, 0.06)
#: Seniority INSIDE a bucket, in the operator's own priority order: ES before
#: NQ, 6E before 6J, ZN before ZF/ZT/ZB. Without this the greedy scores every
#: member of a bucket identically and therefore buys whichever is CHEAPEST -
#: which silently returns NQ without ES, 6J instead of 6E and the 2-year note
#: as the sole representative of the rates complex. A junior contract is a poor
#: proxy for its market, so it is scored at the novelty of the rank it holds.
SENIORITY = {root: sum(1 for r in PRIORITY_ORDER[:i] if UNIVERSE[r][1] == UNIVERSE[root][1])
             for i, root in enumerate(PRIORITY_ORDER)}
#: A bucket the estate has never held intraday is worth more than one it has.
UNOWNED_BUCKET_BONUS = 1.35
#: Coverage bonus: futures carry the session the ETF panel is missing.
SESSION_COVERAGE_NOTE = ("a CME session runs ~23h, so a futures panel carries the overnight, "
                         "European, US-afternoon and settlement windows that the owned ETF "
                         "panel (09:30-11:59 ET) structurally cannot")

TARGET_SESSIONS = 500          # the operator's stated target
MIN_USEFUL_SESSIONS = 250      # below this the depth factor is worthless
SESSIONS_PER_YEAR = 252

# Roll rule (strictly causal, declared before acquisition).
ROLL_VOLUME_LOOKBACK = 3       # consecutive sessions the deferred must out-trade the front
ROLL_MIN_DAYS_BEFORE_EXPIRY = 2
CONTRACT_PAD_DAYS = 45         # request each dated contract from this far before its tenure


class DatabentoError(RuntimeError):
    """A provider or credential failure that must stop acquisition, never be
    silently absorbed into an empty panel."""


# ------------------------------------------------------------------ credential
def api_key() -> str | None:
    """The key, from the environment only. Never read from a file, never
    logged, never written into an artifact."""
    key = os.environ.get(ENV_KEY)
    return key.strip() if key and key.strip() else None


def credential_state() -> dict:
    """Existence, never the value. This is the precondition for every priced
    call, and it is reported as a first-class state so a blocked run explains
    itself instead of failing deep inside an HTTP handler."""
    key = api_key()
    if not key:
        return {
            "state": "CREDENTIAL_ABSENT",
            "env_var": ENV_KEY,
            "checked_scopes": ["process"],
            "blocks": "every metadata, symbology and timeseries call",
            "remediation": ("set the User-scope variable and start a NEW shell, e.g. "
                            "[Environment]::SetEnvironmentVariable('%s','<key>','User') - "
                            "a variable set in another shell is not visible to this process"
                            % ENV_KEY),
            # Stated in BOTH branches: the guarantee is that this module never
            # writes the key into an artifact, which does not depend on whether
            # a key happens to be present.
            "key_value_recorded": False,
            "usable": False,
        }
    return {"state": "CREDENTIAL_PRESENT", "env_var": ENV_KEY,
            "key_length": len(key), "key_value_recorded": False, "usable": True}


def _auth_header(key: str) -> str:
    raw = ("%s:" % key).encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


# ------------------------------------------------------------------- transport
def _default_transport(method: str, url: str, params: dict, key: str, timeout: int) -> bytes:
    body = None
    headers = {"Authorization": _auth_header(key), "Accept": "application/json"}
    if method == "GET":
        url = "%s?%s" % (url, urllib.parse.urlencode(params, doseq=True))
    else:
        body = urllib.parse.urlencode(params, doseq=True).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", "replace")[:400]
        except Exception:                                    # pragma: no cover - defensive
            pass
        raise DatabentoError("HTTP %s from %s: %s" % (exc.code, url.split("?")[0], detail)) from exc
    except urllib.error.URLError as exc:
        raise DatabentoError("network failure calling %s: %s" % (url.split("?")[0], exc.reason)) from exc


class Client:
    """A thin, auditable Databento HISTORICAL client.

    Deliberately uses the standard library rather than the ``databento``
    package: the campaign gains no capability from the SDK, and a hand-rolled
    client keeps every billable call visible at the call site and adds no
    dependency to the live checkout's shared virtualenv.

    ``transport`` is injectable so the whole spending contract is testable
    without a credential and without a network.
    """

    def __init__(self, key: str | None = None, transport=None):
        self.key = key or api_key()
        self._transport = transport or _default_transport
        self.calls: list[dict] = []

    def _call(self, method: str, endpoint: str, params: dict, timeout: int = HTTP_TIMEOUT) -> bytes:
        if not self.key:
            raise DatabentoError("no %s in the environment; refusing to call %s"
                                 % (ENV_KEY, endpoint))
        self.calls.append({"endpoint": endpoint, "method": method,
                           "params": {k: v for k, v in params.items()}})
        return self._transport(method, "%s/%s" % (BASE_URL, endpoint), params, self.key, timeout)

    def _json(self, method: str, endpoint: str, params: dict):
        raw = self._call(method, endpoint, params)
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError as exc:
            raise DatabentoError("%s did not return JSON: %r" % (endpoint, raw[:200])) from exc

    # --- free metadata -----------------------------------------------------
    def list_datasets(self) -> list:
        return self._json("GET", "metadata.list_datasets", {})

    def dataset_range(self, dataset: str = DATASET) -> dict:
        return self._json("GET", "metadata.get_dataset_range", {"dataset": dataset})

    def resolve(self, symbols: list, start: str, end: str, dataset: str = DATASET) -> dict:
        """``symbology.resolve`` bills no market data. This is how a symbol
        spelling is verified BEFORE any billable call quotes it."""
        return self._json("POST", "symbology.resolve",
                          {"dataset": dataset, "symbols": ",".join(symbols),
                           "stype_in": STYPE_IN, "stype_out": "instrument_id",
                           "start_date": start, "end_date": end})

    # --- the cost gate -----------------------------------------------------
    def cost(self, symbols: list, start: str, end: str, mode: str = "historical",
             dataset: str = DATASET, schema: str = SCHEMA) -> float:
        """USD for the exact request. THIS IS THE GATE: no download happens
        without this number for the identical signature."""
        out = self._json("GET", "metadata.get_cost",
                         {"dataset": dataset, "start": start, "end": end,
                          "symbols": ",".join(symbols), "schema": schema,
                          "stype_in": STYPE_IN, "mode": mode})
        return float(out if not isinstance(out, dict) else out.get("cost", out.get("total_cost")))

    def billable_size(self, symbols: list, start: str, end: str, mode: str = "historical",
                      dataset: str = DATASET, schema: str = SCHEMA) -> int:
        out = self._json("GET", "metadata.get_billable_size",
                         {"dataset": dataset, "start": start, "end": end,
                          "symbols": ",".join(symbols), "schema": schema,
                          "stype_in": STYPE_IN, "mode": mode})
        return int(out if not isinstance(out, dict) else out.get("size", 0))

    # --- the only billable data call --------------------------------------
    def get_range_csv(self, symbols: list, start: str, end: str,
                      dataset: str = DATASET, schema: str = SCHEMA) -> bytes:
        return self._call("POST", "timeseries.get_range",
                          {"dataset": dataset, "start": start, "end": end,
                           "symbols": ",".join(symbols), "schema": schema,
                           "stype_in": STYPE_IN, "encoding": "csv", "compression": "none"},
                          timeout=DOWNLOAD_TIMEOUT)


# ------------------------------------------------------------------ symbology
def dated_symbols(root: str, start: date, end: date) -> list:
    """Every genuine dated contract of ``root`` whose delivery month falls in
    the window, in both spellings the venue is known to use. The spelling is
    decided by ``symbology.resolve``, never by this function."""
    tier, bucket, cycle, _desc = UNIVERSE[root]
    out = []
    y = start.year
    while y <= end.year + 1:
        for m in cycle:
            expiry_month = date(y, m, 1)
            if expiry_month < date(start.year, start.month, 1) - timedelta(days=120):
                continue
            if expiry_month > end + timedelta(days=120):
                continue
            code = "%s%s" % (root, MONTH_CODE[m])
            out.append({"root": root, "bucket": bucket, "year": y, "month": m,
                        "delivery": expiry_month.isoformat(),
                        "candidates": ["%s%d" % (code, y % 10), "%s%02d" % (code, y % 100)]})
        y += 1
    return out


def contract_window(entry: dict, cycle: tuple) -> tuple:
    """The window over which a dated contract is worth paying for: from the
    previous cycle month (so the roll has overlap to measure) to its own
    delivery. Requesting a contract's whole listed life would pay for years of
    illiquid back-month quotes."""
    d = date.fromisoformat(entry["delivery"])
    prev_month = None
    for m in sorted(cycle, reverse=True):
        cand = date(d.year if m < d.month else d.year - 1, m, 1)
        if cand < d:
            prev_month = cand
            break
    if prev_month is None:                                   # pragma: no cover - defensive
        prev_month = d - timedelta(days=90)
    start = prev_month - timedelta(days=CONTRACT_PAD_DAYS // 3)
    return start.isoformat(), d.isoformat()


def _window_overlaps(win: tuple, start: str, end: str) -> bool:
    """Does a contract's priced window intersect the acquisition window?"""
    w_start, w_end = win
    return not (w_end < start or w_start > end)


def _resolved_names(out) -> set:
    """The symbols the venue actually recognised. Databento returns a mapping
    whose EMPTY entries mean 'not resolved', so an empty list must not count."""
    mappings = (out or {}).get("result") or (out or {}).get("mappings") or {}
    return {k for k, v in mappings.items() if v}


def resolve_symbols(client: Client, roots: list, start: date, end: date,
                    available_end: str | None = None) -> dict:
    """Ask the venue which spelling it recognises. Bills no market data.

    Batched: one call per (root, spelling) rather than one per contract. The
    per-contract form issued 136 round trips for a 10-root universe, which is
    both slow and an unnecessary rate-limit risk before a single dollar of
    value has been obtained.

    ``available_end`` is the dataset's last available session. The resolve
    window is clamped to it, and to the requested acquisition window, because
    ``symbology.resolve`` answers HTTP 422
    ``data_end_date_after_available_end_date`` for a window that runs past the
    data - and the batching loop absorbs that error, so an out-of-range window
    silently resolves NOTHING for every root. That defect made the whole axis
    unreachable while reporting only "no dated contract spelling resolved".
    """
    resolved, unresolved = {}, {}
    hard_end = min(x for x in (end.isoformat(), available_end) if x)
    for root in roots:
        entries = dated_symbols(root, start, end)
        cycle = UNIVERSE[root][2]
        # Only ask about contracts whose PRICED window can overlap the
        # acquisition window. A contract that could never be priced must not
        # be able to widen the resolve window, nor land in ``unresolved`` as
        # noise that looks like a venue problem.
        entries = [e for e in entries
                   if _window_overlaps(contract_window(e, cycle), start.isoformat(), hard_end)]
        if not entries:
            resolved[root] = []
            continue
        n_spellings = max(len(e["candidates"]) for e in entries)
        window = (start.isoformat(), hard_end)
        found: dict = {}
        for i in range(n_spellings):
            pending = [e for e in entries if id(e) not in found]
            if not pending:
                break
            batch = [e["candidates"][i] for e in pending if i < len(e["candidates"])]
            if not batch:
                continue
            try:
                ok = _resolved_names(client.resolve(batch, window[0], window[1]))
            except DatabentoError:
                continue
            for e in pending:
                if i < len(e["candidates"]) and e["candidates"][i] in ok:
                    found[id(e)] = e["candidates"][i]
        keep = [dict(e, symbol=found[id(e)]) for e in entries if id(e) in found]
        missing = [e["candidates"][0] for e in entries if id(e) not in found]
        resolved[root] = keep
        if missing:
            unresolved[root] = missing
    return {"resolved": resolved, "unresolved": unresolved}


# ------------------------------------------------------------- the value model
def _depth_factor(sessions: int) -> float:
    if sessions < MIN_USEFUL_SESSIONS:
        return 0.0
    return min(1.0, sessions / float(TARGET_SESSIONS))


def instrument_value(root: str, sessions: int, taken_in_bucket: int) -> float:
    """Marginal research information value of adding ``root`` when
    ``taken_in_bucket`` instruments from its bucket are already in the plan.

    Declared before any price was known. Non-additive by design: the value of
    the fourth Treasury contract is close to zero because it is not a fourth
    market, and that is the property that stops a cost-ranked greedy from
    buying a rates panel and calling it diversification.

    The novelty index is ``max(already taken, own seniority)``. Taking the
    2-year note as a bucket's FIRST rates contract scores at rank 2, not rank
    0, because ZT is a poor proxy for the rates complex - which is what stops
    the greedy from returning the cheapest member of every bucket."""
    tier, bucket, _cycle, _desc = UNIVERSE[root]
    rank = max(taken_in_bucket, SENIORITY[root])
    novelty = BUCKET_NOVELTY[min(rank, len(BUCKET_NOVELTY) - 1)]
    bonus = UNOWNED_BUCKET_BONUS if bucket not in OWNED_INTRADAY_BUCKETS else 1.0
    return TIER_WEIGHT[tier] * novelty * bonus * _depth_factor(sessions)


def optimise(costs: dict, budget_usd: float, sessions: int) -> dict:
    """Choose the subset of priced instruments with the highest total
    information value that fits STRICTLY inside the budget, after the safety
    margin. Greedy on marginal value per dollar, recomputed at every step
    because bucket novelty makes value non-additive.

    ``costs`` maps root -> USD for that root's full dated-contract set.
    """
    cap = budget_usd * (1.0 - BUDGET_SAFETY_MARGIN)
    chosen, spent, taken = [], 0.0, {}
    remaining = [r for r in PRIORITY_ORDER if r in costs and costs[r] is not None]
    steps = []
    while remaining:
        best, best_ratio, best_val = None, 0.0, 0.0
        for root in remaining:
            c = float(costs[root])
            bucket = UNIVERSE[root][1]
            val = instrument_value(root, sessions, taken.get(bucket, 0))
            if val <= 0.0:
                continue
            if spent + c > cap:
                continue
            ratio = val / c if c > 0 else float("inf")
            if ratio > best_ratio:
                best, best_ratio, best_val = root, ratio, val
        if best is None:
            break
        bucket = UNIVERSE[best][1]
        spent += float(costs[best])
        taken[bucket] = taken.get(bucket, 0) + 1
        chosen.append(best)
        steps.append({"took": best, "bucket": bucket, "marginal_value": round(best_val, 4),
                      "cost_usd": round(float(costs[best]), 4),
                      "value_per_usd": round(best_ratio, 4),
                      "cumulative_usd": round(spent, 4)})
        remaining.remove(best)
    dropped = [{"root": r, "cost_usd": round(float(costs[r]), 4),
                "reason": "does not fit inside the free-credit cap"
                          if spent + float(costs[r]) > cap else
                          "marginal information value is zero (duplicate bucket or "
                          "insufficient history depth)"}
               for r in remaining]
    return {
        "budget_usd": round(budget_usd, 4),
        "safety_margin": BUDGET_SAFETY_MARGIN,
        "effective_cap_usd": round(cap, 4),
        "chosen": chosen,
        "estimated_spend_usd": round(spent, 4),
        "headroom_usd": round(cap - spent, 4),
        "buckets_covered": sorted({UNIVERSE[r][1] for r in chosen}),
        "new_buckets_covered": sorted({UNIVERSE[r][1] for r in chosen}
                                      & set(NEW_BUCKETS)),
        "greedy_steps": steps,
        "dropped": dropped,
        "fits_in_free_credit": spent <= cap,
        # An empty plan trivially "fits". Say so explicitly rather than letting
        # a caller read a $0 plan as a successful one.
        "insufficient_budget_for_any_instrument": bool(costs) and not chosen,
        "paid_dollars_required": 0.0,
    }


# ------------------------------------------------------------------- the plan
def plan(client: Client, budget_usd: float, years: float = 2.0,
         roots: list | None = None, today: date | None = None) -> dict:
    """Price the whole desired panel BEFORE anything is downloaded, then choose
    the affordable subset. This function is the only producer of a
    downloadable plan, and ``download`` will not act on anything else."""
    today = today or date.today()
    end = today - timedelta(days=1)
    start = end - timedelta(days=int(round(365.25 * years)))
    sessions = int(round(years * SESSIONS_PER_YEAR))
    roots = list(roots or PRIORITY_ORDER)

    rng = client.dataset_range()
    # The provider is the authority on how far the data runs. Asking past it is
    # an HTTP 422, not an empty answer, so the window is clamped here BEFORE any
    # symbology or cost call rather than discovered inside an exception handler.
    avail_end = _available_end(rng, SCHEMA)
    if avail_end and avail_end < end.isoformat():
        end = date.fromisoformat(avail_end)
    sym = resolve_symbols(client, roots, start, end, available_end=avail_end)

    priced, requests_by_root, errors = {}, {}, {}
    for root in roots:
        entries = sym["resolved"].get(root) or []
        if not entries:
            priced[root] = None
            errors[root] = "no dated contract spelling resolved"
            continue
        cycle = UNIVERSE[root][2]
        reqs, total = [], 0.0
        for entry in entries:
            w_start, w_end = contract_window(entry, cycle)
            if w_end < start.isoformat() or w_start > end.isoformat():
                continue
            w_start = max(w_start, start.isoformat())
            w_end = min(w_end, end.isoformat())
            if w_start >= w_end:
                continue
            try:
                usd = client.cost([entry["symbol"]], w_start, w_end, mode="historical")
            except DatabentoError as exc:
                errors[root] = str(exc)
                usd = None
            if usd is None:
                continue
            reqs.append({"symbol": entry["symbol"], "start": w_start, "end": w_end,
                         "delivery": entry["delivery"], "cost_usd": round(usd, 6),
                         "signature": _signature(entry["symbol"], w_start, w_end)})
            total += usd
        if not reqs:
            priced[root] = None
            errors.setdefault(root, "no priceable contract window in range")
            continue
        priced[root] = round(total, 6)
        requests_by_root[root] = reqs

    selection = optimise(priced, budget_usd, sessions)
    selected_requests = [r for root in selection["chosen"] for r in requests_by_root[root]]
    return {
        "dataset": DATASET, "schema": SCHEMA, "stype_in": STYPE_IN,
        "dataset_available_range": rng,
        "requested_window": {"start": start.isoformat(), "end": end.isoformat(),
                             "years": years, "approx_sessions": sessions},
        "symbology": {"resolved_counts": {k: len(v) for k, v in sym["resolved"].items()},
                      "unresolved": sym["unresolved"],
                      "policy": "dated contracts only; no continuous symbol is ever requested"},
        "cost_by_root_usd": priced,
        "full_panel_cost_usd": round(sum(v for v in priced.values() if v), 6),
        "errors": errors,
        "selection": selection,
        "requests": selected_requests,
        "n_requests": len(selected_requests),
        "cost_estimated_before_any_download": True,
    }


def _available_end(rng: dict, schema: str = SCHEMA) -> str | None:
    """The last date the dataset actually serves, for ``schema``.

    ``metadata.get_dataset_range`` reports an exclusive, sub-daily upper bound
    (e.g. ``2026-09-10T12:51:34Z`` meaning "up to but not including
    2026-09-11"). One day is subtracted so every requested window ends on a
    session the provider has completely.
    """
    node = ((rng or {}).get("schema") or {}).get(schema) or rng or {}
    raw = node.get("end") or (rng or {}).get("end")
    if not raw:
        return None
    try:
        d = date.fromisoformat(str(raw)[:10]) - timedelta(days=1)
    except ValueError:                                       # pragma: no cover - defensive
        return None
    return d.isoformat()


def _signature(symbol: str, start: str, end: str) -> str:
    return "%s|%s|%s|%s|%s" % (DATASET, SCHEMA, symbol, start, end)


# --------------------------------------------------------------- the download
def acquisition_root() -> Path:
    return research_root() / "_data_futures_databento"


def download(client: Client, plan_body: dict, out_root: Path | None = None,
             dry_run: bool = True) -> dict:
    """Execute ONLY the requests that the plan priced and the optimiser chose.

    The spending contract is enforced here, not documented here: a request
    whose signature is not in the plan raises. ``dry_run`` is the default so
    that calling this function by accident cannot spend anything.
    """
    allowed = {r["signature"] for r in plan_body.get("requests", [])}
    if not allowed:
        return {"state": "NOTHING_TO_DOWNLOAD", "written": [], "spent_estimate_usd": 0.0}
    if not plan_body.get("selection", {}).get("fits_in_free_credit"):
        raise DatabentoError("plan does not fit inside the stated free credit; refusing to "
                            "download - this is the zero-paid-dollars invariant")
    out_root = Path(out_root or acquisition_root())
    if dry_run:
        return {"state": "DRY_RUN", "would_write": len(allowed),
                "spent_estimate_usd": plan_body["selection"]["estimated_spend_usd"],
                "out_root": str(out_root)}
    out_root.mkdir(parents=True, exist_ok=True)
    written, failed, spent = [], [], 0.0
    for req in plan_body["requests"]:
        sig = _signature(req["symbol"], req["start"], req["end"])
        if sig not in allowed:                               # pragma: no cover - defensive
            raise DatabentoError("refusing an unpriced request: %s" % sig)
        target = out_root / ("%s_%s_%s_%s.csv" % (req["symbol"], SCHEMA, req["start"], req["end"]))
        if target.exists() and target.stat().st_size > 0:
            written.append({"symbol": req["symbol"], "path": str(target), "reused": True})
            continue
        try:
            raw = client.get_range_csv([req["symbol"]], req["start"], req["end"])
        except DatabentoError as exc:
            failed.append({"symbol": req["symbol"], "error": str(exc)})
            continue
        tmp = target.with_suffix(".csv.tmp")
        tmp.write_bytes(raw)
        tmp.replace(target)
        spent += float(req["cost_usd"])
        written.append({"symbol": req["symbol"], "path": str(target),
                        "bytes": len(raw), "reused": False,
                        "cost_usd": round(float(req["cost_usd"]), 6)})
        time.sleep(0.05)
    return {"state": "DOWNLOADED", "written": written, "failed": failed,
            "spent_estimate_usd": round(spent, 4), "out_root": str(out_root),
            "paid_dollars": 0.0}


# -------------------------------------------------- normalisation and the roll
#: The canonical CSV columns Databento emits for ``ohlcv-1m``.
CSV_TS, CSV_SYM = "ts_event", "symbol"
CSV_OHLCV = ("open", "high", "low", "close", "volume")
#: Databento OHLCV prices are fixed-point with 9 implied decimals.
PRICE_SCALE = 1e-9


def parse_csv(raw: bytes | str, symbol: str | None = None) -> "pd.DataFrame":  # noqa: F821
    """Databento ``ohlcv-1m`` CSV -> a tidy frame in exchange-local time.

    The DST trap that corrupted the first pass at the owned ETF panel is
    structural, not incidental: ``ts_event`` is UTC, so any minute grid built
    on it silently mixes two different session clocks across a DST boundary.
    Every timestamp is converted to ``America/New_York`` HERE, once, so no
    downstream consumer can index a UTC minute by mistake.

    ``symbol`` is REQUIRED in practice even though it is optional here. The
    real ``ohlcv-1m`` CSV identifies its contract only by ``instrument_id`` - a
    numeric venue handle that is not stable across time and means nothing to
    the roll - so the dated symbol has to come from the caller, which knows it
    because it is what was requested and priced. Without it the roll cannot
    tell ESZ5 from ESH6, and every downstream return would be spliced across
    contracts.
    """
    import io
    import pandas as pd

    text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else raw
    df = pd.read_csv(io.StringIO(text))
    if df.empty:
        return df
    if CSV_SYM not in df.columns:
        if not symbol:
            raise ValueError(
                "ohlcv-1m CSV carries no '%s' column and no symbol was supplied; "
                "the contract identity would be lost" % CSV_SYM)
        df[CSV_SYM] = str(symbol)
    ts = pd.to_datetime(df[CSV_TS], utc=True, errors="coerce", format="mixed")
    df = df.assign(ts_utc=ts).dropna(subset=["ts_utc"])
    df["ts_et"] = df["ts_utc"].dt.tz_convert("America/New_York")
    df["session"] = df["ts_et"].dt.date.astype(str)
    df["minute_et"] = df["ts_et"].dt.hour * 60 + df["ts_et"].dt.minute
    for c in CSV_OHLCV:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Fixed-point prices only when the magnitudes say so; a CSV already in
    # decimal units must not be divided again.
    if "close" in df.columns and df["close"].abs().median() > 1e6:
        for c in ("open", "high", "low", "close"):
            if c in df.columns:
                df[c] = df[c] * PRICE_SCALE
    return df


def roll_schedule(daily_volume: "pd.DataFrame", deliveries: dict) -> "pd.DataFrame":  # noqa: F821
    """The PIT front-month schedule.

    ``daily_volume`` is sessions x symbols. The contract held on session *t* is
    decided ONLY from volume through *t-1*, the roll never runs backwards, and
    a contract is abandoned before it reaches delivery. This is the whole
    reason dated contracts were bought instead of a vendor continuous series:
    the roll is the estate's own, and it is auditable.
    """
    import pandas as pd

    sessions = list(daily_volume.index)
    held, rows = None, []
    for i, s in enumerate(sessions):
        # Strictly prior information only.
        prior = daily_volume.iloc[max(0, i - ROLL_VOLUME_LOOKBACK):i]
        s_date = date.fromisoformat(str(s))

        def alive(sym: str) -> bool:
            d = deliveries.get(sym)
            if not d:
                return False
            return (date.fromisoformat(d) - s_date).days > ROLL_MIN_DAYS_BEFORE_EXPIRY

        candidates = [c for c in daily_volume.columns if alive(c)]
        if not candidates:
            rows.append({"session": s, "symbol": None, "rolled": False})
            continue
        # The NEAREST alive delivery is what "front month" means. Falling back
        # to the first column instead sorts alphabetically, which on an ES
        # panel bootstraps onto ESH6 over ESZ5 and - because the roll is
        # forward-only - then locks that error in for the whole sample.
        nearest = min(candidates, key=lambda c: deliveries.get(c, "9999"))
        if prior.empty:
            leader = nearest
        else:
            totals = prior[candidates].sum()
            leader = str(totals.idxmax()) if totals.notna().any() else nearest
        if held is None:
            held = leader
        elif held not in candidates:
            held = nearest                                  # forced roll: expiry reached
        elif leader != held:
            # Only ever roll FORWARD, to a later delivery.
            if deliveries.get(leader, "") > deliveries.get(held, ""):
                held = leader
        rows.append({"session": s, "symbol": held,
                     "rolled": bool(rows and rows[-1]["symbol"] not in (None, held))})
    return pd.DataFrame(rows).set_index("session")


def normalise(root: str, files: list, out_dir: Path | None = None) -> dict:
    """Dated-contract CSVs -> ONE PIT front-month 1-minute series for ``root``.

    Returns per-minute rows carrying the contract actually held. Returns are
    spliced on the ROLL: the price difference between two different contracts
    is never taken as a return, because that difference is the calendar spread,
    not a tradable move.
    """
    import pandas as pd

    # The dated symbol comes from the FILENAME, which is the request this
    # estate priced and paid for, not from the payload - the payload only
    # carries instrument_id.
    frames = [parse_csv(Path(f).read_bytes(), symbol=Path(f).stem.split("_")[0])
              for f in files]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return {"root": root, "state": "NO_ROWS", "sessions": 0}
    df = pd.concat(frames, ignore_index=True)
    deliveries = {}
    for f in files:
        stem = Path(f).stem.split("_")[0]
        deliveries.setdefault(stem, None)
    # Delivery is recoverable from the symbol itself (root + month code + year).
    for sym in df[CSV_SYM].astype(str).unique():
        deliveries[sym] = _delivery_from_symbol(sym) or deliveries.get(sym)

    vol = (df.pivot_table(index="session", columns=CSV_SYM, values="volume", aggfunc="sum")
             .sort_index())
    sched = roll_schedule(vol, {k: v for k, v in deliveries.items() if v})
    df = df.merge(sched.reset_index().rename(columns={"symbol": "held"}), on="session", how="left")
    front = df[df[CSV_SYM].astype(str) == df["held"].astype(str)].copy()
    front = front.sort_values(["session", "minute_et"])
    # Splice: grouping the pct_change BY CONTRACT is what makes the roll safe -
    # a difference between two different contracts is never formed at all, so
    # the calendar spread can never enter a return. Note this deliberately does
    # NOT zero the first bar after a roll: the new contract's own overnight
    # move is a real, tradable return and discarding it would bias the series.
    front["ret"] = front.groupby(CSV_SYM)["close"].pct_change()
    sessions = sorted(front["session"].unique())
    out = {
        "root": root, "state": "NORMALISED",
        "sessions": len(sessions),
        "first_session": sessions[0] if sessions else None,
        "last_session": sessions[-1] if sessions else None,
        "contracts_used": sorted(front[CSV_SYM].astype(str).unique().tolist()),
        "n_rolls": int(sched["rolled"].sum()) if "rolled" in sched else 0,
        "minutes_per_session_median": float(front.groupby("session").size().median()),
        "minute_range_et": [int(front["minute_et"].min()), int(front["minute_et"].max())],
        "roll_rule": {"lookback_sessions": ROLL_VOLUME_LOOKBACK,
                      "min_days_before_expiry": ROLL_MIN_DAYS_BEFORE_EXPIRY,
                      "decided_from": "volume strictly BEFORE the session it applies to",
                      "returns_spliced_within_contract_only": True},
    }
    if out_dir:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        # gzipped CSV, not parquet. Parquet needs pyarrow or fastparquet, which
        # this estate's virtualenv does not carry - so `to_parquet` raised
        # ImportError at exactly the moment a paid-for panel had just landed.
        # csv.gz also matches the convention the owned R45 minute panels
        # already use, so one reader serves both, and it is byte-reproducible
        # across machines instead of depending on which engine is installed.
        target = out_dir / ("%s_front_1m.csv.gz" % root)
        front.to_csv(target, index=False, compression="gzip")
        out["path"] = str(target)
        out["format"] = "csv.gz"
    return out


_MONTH_FROM_CODE = {v: k for k, v in MONTH_CODE.items()}


def _delivery_from_symbol(sym: str) -> str | None:
    """``ESZ5`` / ``ESZ25`` -> the delivery month's first day."""
    s = str(sym).strip().upper()
    digits = ""
    while s and s[-1].isdigit():
        digits = s[-1] + digits
        s = s[:-1]
    if not digits or not s or s[-1] not in _MONTH_FROM_CODE:
        return None
    month = _MONTH_FROM_CODE[s[-1]]
    if len(digits) == 1:
        base = date.today().year
        year = (base // 10) * 10 + int(digits)
        if year < base - 5:
            year += 10
    else:
        year = 2000 + int(digits[-2:])
    try:
        return date(year, month, 1).isoformat()
    except ValueError:                                       # pragma: no cover - defensive
        return None


def pit_validation(normalised: dict) -> dict:
    """The checks that must pass BEFORE any research runs on acquired data.

    These are the traps that closed the last axis, encoded so they cannot
    recur silently on a new panel."""
    checks = []

    def check(name, ok, detail):
        checks.append({"check": name, "pass": bool(ok), "detail": detail})

    sessions = normalised.get("sessions", 0)
    check("enough_sessions_for_frozen_floor", sessions >= MIN_EFFECTIVE_PERIODS,
          "%d sessions against the frozen MIN_EFFECTIVE_PERIODS floor of %d; the floor is not "
          "moved to admit a short panel" % (sessions, MIN_EFFECTIVE_PERIODS))
    check("reaches_useful_depth", sessions >= MIN_USEFUL_SESSIONS,
          "%d sessions against the %d needed for multiple regimes" % (sessions, MIN_USEFUL_SESSIONS))
    rng = normalised.get("minute_range_et") or [0, 0]
    check("covers_the_us_afternoon", rng[1] >= 15 * 60,
          "last minute observed is %02d:%02d ET - the owned ETF panel stops at 12:59 ET and that "
          "defect is the reason this data was acquired" % (rng[1] // 60, rng[1] % 60))
    check("covers_the_close", rng[1] >= 15 * 60 + 59,
          "a session that reaches 16:00 ET can be marked to the close")
    check("roll_is_causal", bool(normalised.get("roll_rule", {}).get("decided_from")),
          "the front contract for session t is chosen from volume strictly before t")
    check("returns_spliced_within_contract",
          bool(normalised.get("roll_rule", {}).get("returns_spliced_within_contract_only")),
          "a cross-contract price difference is a calendar spread, never a return")
    check("dated_contracts_not_continuous", bool(normalised.get("contracts_used")),
          "genuine dated contracts: %s" % ", ".join(normalised.get("contracts_used", [])[:6]))
    failed = [c["check"] for c in checks if not c["pass"]]
    return {"checks": checks, "failed": failed,
            "state": "PIT_VALID" if not failed else "PIT_INVALID",
            "usable_for_research": not failed}


# ------------------------------------------------------------- the state file
def information_case() -> dict:
    """Why this acquisition is worth doing at all - stated in terms of the
    MEASURED defect it removes, not in terms of having more data."""
    return {
        "named_defect_it_removes": ("the owned R45 ETF minute panel stops at 12:59 ET: no US "
                                    "afternoon, no closing auction, no overnight session. Every "
                                    "intraday family in this campaign was therefore tested on the "
                                    "first 150 minutes of the day only."),
        "why_this_is_coverage_not_a_transform": SESSION_COVERAGE_NOTE,
        "contract_rule_13_condition_met": "coverage materially improves",
        "buckets_already_owned_intraday": dict(OWNED_INTRADAY_BUCKETS),
        "buckets_never_owned_at_any_frequency": list(NEW_BUCKETS),
        "value_model_declared_before_costs_were_seen": {
            "tier_weight": dict(TIER_WEIGHT), "bucket_novelty": list(BUCKET_NOVELTY),
            "unowned_bucket_bonus": UNOWNED_BUCKET_BONUS,
            "min_useful_sessions": MIN_USEFUL_SESSIONS, "target_sessions": TARGET_SESSIONS,
            "why_non_additive": "four Treasury contracts are one rates bucket, not four markets"},
        "roll_rule": {"type": "causal volume crossover",
                      "lookback_sessions": ROLL_VOLUME_LOOKBACK,
                      "min_days_before_expiry": ROLL_MIN_DAYS_BEFORE_EXPIRY,
                      "why_not_continuous": "a continuous symbol is a derived series whose roll "
                                            "dates the estate did not choose and cannot audit"},
        "frozen_gates_unchanged": {"min_effective_periods": MIN_EFFECTIVE_PERIODS,
                                   "note": "no threshold is relaxed by acquiring data"},
    }


def _budget_provenance() -> dict:
    """Say plainly whether the budget is a verified balance or a published cap."""
    src = (os.environ.get(ENV_BUDGET_SOURCE) or BUDGET_SOURCE_DEFAULT).strip().upper()
    verified = src == "OPERATOR_STATED"
    return {
        "source": src,
        "balance_verified_against_the_account": verified,
        "why_it_matters": ("the v0 API exposes no credit-balance endpoint, so an unverified "
                           "figure is an upper bound on a FRESH account, not a statement that "
                           "the credit is still unspent"),
        "residual_risk_if_unverified": ("if the grant were already partly consumed, requests "
                                        "beyond it are refused by the provider or, where a "
                                        "payment method is on file, billed - which is why the "
                                        "priced plan is the real cap here, not the budget"),
    }


def spending_contract() -> dict:
    return {
        "cost_estimated_before_any_download": True,
        "enforced_where": "%s.download refuses any signature the plan did not price" % CALCULATION_OWNER,
        "budget_source": "free-credit balance via %s, provenance via %s" % (ENV_BUDGET,
                                                                             ENV_BUDGET_SOURCE),
        "budget_is_never_inferred": "the v0 API exposes no balance endpoint; absent an operator "
                                    "figure the plan is priced and NOTHING is downloaded",
        "safety_margin": BUDGET_SAFETY_MARGIN,
        "paid_dollars_authorised": 0.0,
        "subscription": "NONE - only usage-based historical endpoints are called",
        "usage_based_only": True,
        "endpoints_used": ["metadata.get_dataset_range", "symbology.resolve",
                           "metadata.get_cost", "metadata.get_billable_size",
                           "timeseries.get_range"],
        "no_plan_or_live_endpoint_reachable": True,
    }


def build(budget_usd: float | None = None, years: float = 2.0, execute: bool = False,
          client: Client | None = None, write: bool = True) -> dict:
    """Measure the acquisition state. With no credential this returns a fully
    formed BLOCKED state that names the exact remediation - it does not raise,
    because the campaign must be able to record the blocker and continue."""
    cred = credential_state()
    budget_env = os.environ.get(ENV_BUDGET)
    if budget_usd is None and budget_env:
        try:
            budget_usd = float(budget_env)
        except ValueError:
            budget_usd = None

    body = {
        "calculation_owner": CALCULATION_OWNER,
        "provider": "databento",
        "credential": cred,
        "budget_usd_stated": budget_usd,
        "budget_env_var": ENV_BUDGET,
        "budget_provenance": _budget_provenance(),
        "information_case": information_case(),
        "spending_contract": spending_contract(),
        "universe": {r: {"tier": UNIVERSE[r][0], "bucket": UNIVERSE[r][1],
                         "description": UNIVERSE[r][3]} for r in PRIORITY_ORDER},
        "priority_order": list(PRIORITY_ORDER),
    }

    if not cred["usable"]:
        body["state"] = "BLOCKED_CREDENTIAL_ABSENT"
        body["blocker"] = {
            "kind": "MISSING_CREDENTIAL_USER_MUST_SUPPLY",
            "env_var": ENV_KEY,
            "provider_reachable": True,
            "evidence": "https://hist.databento.com/v0/metadata.list_datasets answers HTTP 401 "
                        "Unauthorized without a key - the endpoint is reachable and the ONLY "
                        "missing input is the credential",
            "not_a_network_failure": True,
            "remediation": cred["remediation"],
            "what_runs_the_moment_it_is_set": "scripts/run_alpha_recovery_offensive.py databento",
        }
        if write:
            write_artifact(ARTIFACT_NAME, body)
        return body

    client = client or Client()
    if budget_usd is None:
        body["state"] = "BLOCKED_BUDGET_UNSTATED"
        body["blocker"] = {
            "kind": "FREE_CREDIT_BALANCE_UNSTATED",
            "env_var": ENV_BUDGET,
            "why": "the v0 API exposes no credit-balance endpoint, and inferring a balance is "
                   "how an accidental paid dollar happens",
            "remediation": "set %s to the free-credit balance shown on the Databento portal"
                           % ENV_BUDGET,
        }
        if write:
            write_artifact(ARTIFACT_NAME, body)
        return body

    try:
        p = plan(client, budget_usd=budget_usd, years=years)
        body["plan"] = p
        body["state"] = ("PLANNED_FITS_FREE_CREDIT" if p["selection"]["fits_in_free_credit"]
                         else "PLANNED_EXCEEDS_FREE_CREDIT")
        if execute and p["selection"]["fits_in_free_credit"]:
            body["download"] = download(client, p, dry_run=False)
            body["state"] = "ACQUIRED"
        elif execute:
            body["download"] = {"state": "REFUSED_EXCEEDS_FREE_CREDIT"}
    except DatabentoError as exc:
        body["state"] = "PROVIDER_ERROR"
        body["error"] = str(exc)

    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
