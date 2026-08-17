"""
api/collection_replay.py — Release 29 HERMETIC COLLECTION-SERVICE HARNESS.

The service-level counterpart to ``api.event_replay``. It drives the REAL
collection orchestrator (``api.information_collection.run_collection_iteration``)
over a FAKE CLOCK and FAKE PROVIDERS, and hands whatever "arrives" to the REAL
Release-28 owners through ``api.event_replay.run_cycle``.

WHAT IS FAKE
    the clock, the provider responses, the synthetic portfolio/price/ranking world,
    and the state root (an isolated temp directory).

WHAT IS REAL
    the cadence policy, the due/window resolution, the market-clock owner, the
    provider budget, the adaptive backoff and circuit, the singleton lock, the
    durable state and restart path, the event contract, novelty, materiality,
    holding opportunity cost, portfolio reassessment and the reallocation
    proposal. No business logic is stubbed.

Nothing here touches a production store, a production task, a real provider or
the backend on port 8001, and no scenario can produce an order.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from ..engine import collection_cadence as cad
from . import event_replay as replay
from . import information_collection as ic

PHASE = "RELEASE29"
COMPOSITION_OWNER = "api.collection_replay"
SCHEMA_VERSION = "1.0.0"

#: Monday 2026-08-17 10:00 ET (EDT, UTC-4) — the regular session is OPEN.
MARKET_OPEN_UTC = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
#: Sunday 2026-08-16 12:00 ET — WEEKEND, every market feed is NOT_DUE.
WEEKEND_UTC = datetime(2026, 8, 16, 16, 0, tzinfo=timezone.utc)
#: Monday 2026-08-17 06:30 ET — a weekday BEFORE the regular session opens.
PREMARKET_UTC = datetime(2026, 8, 17, 10, 30, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- #
# Harness plumbing
# --------------------------------------------------------------------------- #
#: No hermetic scenario does real I/O, so none may run long. Anything past this
#: bound is a blocked call or a runaway loop, and is reported as TEST_TIMEOUT
#: rather than left to hang an acceptance run.
SCENARIO_TIMEOUT_SECONDS = 90.0


class HermeticNetworkViolation(RuntimeError):
    """A scenario attempted a real outbound connection. That is a harness bug."""


class sealed_network:
    """Structurally seal the process against outbound TCP for the simulation.

    Passing fake fetchers is a CONVENTION; sealing the socket is a GUARANTEE.
    Any path that reaches a provider — today, or after a future refactor adds a
    fetcher nobody remembered to inject — fails loudly inside the harness
    instead of quietly spending real provider quota.
    """

    def __init__(self) -> None:
        self.attempts: list[str] = []

    def __enter__(self) -> "sealed_network":
        import socket
        self._connect = socket.socket.connect
        self._connect_ex = socket.socket.connect_ex
        self._create = socket.create_connection
        outer = self

        def _blocked(address):
            outer.attempts.append(str(address))
            raise HermeticNetworkViolation(
                "hermetic simulation blocked an outbound connection to %r; the "
                "scenario must inject a fake fetcher" % (address,))

        socket.socket.connect = lambda self, address, *a, **kw: _blocked(address)
        socket.socket.connect_ex = lambda self, address, *a, **kw: _blocked(address)
        socket.create_connection = lambda address, *a, **kw: _blocked(address)
        return self

    def __exit__(self, *exc) -> bool:
        import socket
        socket.socket.connect = self._connect
        socket.socket.connect_ex = self._connect_ex
        socket.create_connection = self._create
        return False


def _roots(base: Path) -> dict:
    """Fresh, isolated roots for ONE scenario.

    The path is made UNIQUE rather than cleared. A hermetic harness must never
    inherit another run's watermarks, lock or event fabric — leftover state
    silently turns "nothing was collected" into a false pass — and it must never
    delete a directory it did not create.
    """
    base.parent.mkdir(parents=True, exist_ok=True)
    if base.exists():
        base = Path(tempfile.mkdtemp(prefix=base.name + "_", dir=str(base.parent)))
    roots = {name: base / name
             for name in ("service", "fabric", "hoc", "reassess", "realloc")}
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)
    return roots


def _arm(root) -> None:
    ic.set_collection_automation(enabled=True, confirm=ic.ENABLE_CONFIRM_TOKEN,
                                 requested_by="collection_replay", root=root)
    ic.register_worker_start(root=root, instance_id="replay-worker-1", pid=os.getpid())


def _universe(world: dict) -> dict:
    held = sorted({str(p.get("ticker")).upper()
                   for p in (world["portfolio_state"].get("positions") or [])})
    ranked = [str(r.get("ticker")).upper()
              for r in (world["scoring"].get("rankings") or [])]
    candidates = [t for t in ranked if t not in set(held)][:60]
    return {
        "contract_id": "paper_trader.live_attention_universe/1",
        "active_book_id": "REPLAY_BOOK",
        "eligible_market_date": world["eligible"],
        "tiers": {
            cad.TIER_HOLDINGS: {"tickers": held, "count": len(held), "why": ""},
            cad.TIER_CANDIDATES: {"tickers": candidates, "count": len(candidates),
                                  "why": ""},
            cad.TIER_UNIVERSE: {"tickers": ranked[:250], "count": len(ranked[:250]),
                                "why": ""},
            cad.TIER_GLOBAL: {"tickers": [], "count": 0, "why": ""},
        },
        "holdings": held, "candidates": candidates, "warnings": [],
    }


class FakeProviders:
    """Controllable stand-ins for the Stage-2 / Stage-3.5 collection owners.

    Records every call so a scenario can assert that a source in backoff was NOT
    called — the assertion that actually matters for provider safety.
    """

    def __init__(self) -> None:
        self.stage2_calls: list[list[str]] = []
        self.news_calls: int = 0
        self.stage2_new_records = 1
        self.news_new_records = 1
        self.stage2_fail: set[str] = set()
        self.news_fail = False

    def stage2(self, **kwargs) -> dict:
        cfg = kwargs.get("config") or {}
        enabled = sorted(sid for sid, s in (cfg.get("sources") or {}).items()
                         if s.get("enabled"))
        self.stage2_calls.append(enabled)
        per_source = {}
        for sid in enabled:
            if sid in self.stage2_fail:
                per_source[sid] = {"overall_state": "FAILED",
                                   "entitlement_summary": "simulated provider outage"}
            else:
                per_source[sid] = {"overall_state": "HEALTHY",
                                   "records_normalized_new": self.stage2_new_records,
                                   "duplicates_prevented": 0,
                                   "newest_source_event_time": "2026-08-17"}
        return {"status": "ALPHA_AGENT_STAGE2_READY",
                "counts": {"records_new": self.stage2_new_records * len(enabled)},
                "source_states": per_source, "run_id": "replay_run"}

    def news(self, **kwargs) -> dict:
        self.news_calls += 1
        if self.news_fail:
            return {"status": "ALPHA_AGENT_STAGE35_BLOCKED", "counts": {},
                    "reason": "simulated feed outage"}
        return {"status": "ALPHA_AGENT_STAGE35_READY",
                "counts": {"records_new": self.news_new_records}}

    def called_sources(self) -> set[str]:
        out: set[str] = set()
        for batch in self.stage2_calls:
            out.update(batch)
        if self.news_calls:
            out.add("news_rss")
        return out


def fake_quote_fetcher(tickers) -> tuple:
    """Deterministic stand-in for the delayed-quote provider.

    Returns the SAME shape ``engine.market_data.fetch_latest_prices`` returns.
    Without this the harness would fall through to the live provider the moment
    the simulated clock entered the regular session — which is exactly what a
    hermetic harness must never do.
    """
    rows = [{"ticker": str(t).upper(), "price": 100.0 + (i % 7)}
            for i, t in enumerate(tickers or [])]
    return rows, []


def world_quote_fetcher(world: dict) -> Callable:
    """A quote feed that agrees with the world's own price panel.

    The delayed quote must equal the panel's last close unless a scenario asks
    for a move. A fetcher that invents unrelated prices hands every scenario a
    silent price shock, and a shock is material — which would make "immaterial
    information does no decision work" fail for a reason the scenario never
    intended to test.
    """
    series = (world.get("price_panel") or {}).get("series") or {}

    def fetch(tickers) -> tuple:
        rows = []
        for ticker in (tickers or []):
            key = str(ticker).upper()
            adj = ((series.get(key) or {}).get("adj") or [])
            if adj:
                rows.append({"ticker": key, "price": float(adj[-1])})
        return rows, []
    return fetch


def _fake_gdelt_fetcher(_url) -> dict:
    return {"ok": True, "status": 200, "elapsed_seconds": 0.01,
            "json": {"articles": []}}


def _entity_index(world: dict) -> dict:
    """A resolved company name for every synthetic holding.

    ``capture_gdelt_news`` refuses to query a ticker it cannot name — correct in
    production, but in the replay world it means the adapter would report
    "not queried" and the injected fetcher would never run. Naming the synthetic
    holdings is what makes the backoff scenarios exercise the real code path.
    """
    held = [str(p.get("ticker")).upper()
            for p in (world["portfolio_state"].get("positions") or [])]
    return {"by_ticker": {t: ["%s Replay Corporation" % t] for t in held}}


def _cycle_over_world(world: dict, roots: dict, records: Optional[list] = None,
                      **overrides) -> Callable:
    """An event_cycle_fn that drives the REAL Release-28 orchestrator.

    Live-adapter fetchers ALWAYS default to hermetic fakes. A scenario that wants
    a specific provider behaviour (a 429, an outage) passes its own fetcher.
    """
    quote_override = overrides.pop("quote_fetcher", None)
    gdelt_override = overrides.pop("gdelt_fetcher", None)
    overrides.setdefault("entity_index", _entity_index(world))
    quiet_quotes = world_quote_fetcher(world)

    def fn(**kwargs):
        return replay.run_cycle(
            world=world, records=list(records or []), roots=roots,
            # The SIMULATED clock reaches the live adapters. Without this the
            # adapters would stamp event identity with the real wall clock and the
            # verdict of a scenario would depend on which real minute it ran in.
            now_iso=kwargs.get("now_iso"),
            include_market_quotes=bool(kwargs.get("include_market_quotes")),
            quote_fetcher=(quote_override or kwargs.get("quote_fetcher")
                           or quiet_quotes),
            include_gdelt=bool(kwargs.get("include_gdelt")),
            gdelt_fetcher=(gdelt_override or kwargs.get("gdelt_fetcher")
                           or _fake_gdelt_fetcher), **overrides)
    return fn


def iterate(*, world: dict, roots: dict, now: datetime,
            providers: FakeProviders, records: Optional[list] = None,
            instance_id: str = "replay-worker-1", cycle_fn: Optional[Callable] = None,
            max_sources: int = ic.MAX_SOURCES_PER_ITERATION,
            **cycle_overrides) -> dict:
    """One REAL collection iteration at a simulated moment."""
    return ic.run_collection_iteration(
        root=roots["service"], instance_id=instance_id, now=now,
        attention_universe=_universe(world),
        stage2_fn=providers.stage2, news_fn=providers.news,
        event_cycle_fn=(cycle_fn or _cycle_over_world(world, roots, records,
                                                      **cycle_overrides)),
        max_sources=max_sources)


def _check(name, expected, observed) -> dict:
    return replay._check(name, expected, observed)


def _check_true(name, observed, why=None) -> dict:
    return replay._check_true(name, observed, why)


def _result(key: str, title: str, checks: list, detail: Optional[dict] = None) -> dict:
    return replay._result(key, title, checks, detail)


def _row(receipt_or_health: dict, source_id: str) -> dict:
    for r in (receipt_or_health.get("sources") or []):
        if r["source_id"] == source_id:
            return r
    return {}


def _health(roots: dict, now: datetime) -> dict:
    return ic.build_source_runtime_health(root=roots["service"], now=now)


# --------------------------------------------------------------------------- #
# S1-S3 — cadence: a source is collected at ITS OWN frequency
# --------------------------------------------------------------------------- #
def s1_continuous_cadence(base: Path) -> dict:
    """A 15-minute continuous lane is collected every 15 simulated minutes."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    t0 = MARKET_OPEN_UTC

    def collections_of(source_id: str) -> int:
        """How many times THIS source was collected.

        Counting batches instead would count the bounded-catch-up drain of the
        OTHER due lanes as a re-collection of this one.
        """
        return sum(1 for batch in prov.stage2_calls if source_id in batch)

    iterate(world=world, roots=roots, now=t0, providers=prov)
    first = collections_of("sec_edgar")
    iterate(world=world, roots=roots, now=t0 + timedelta(minutes=5), providers=prov)
    after_5min = collections_of("sec_edgar")
    iterate(world=world, roots=roots, now=t0 + timedelta(minutes=16), providers=prov)
    after_16min = collections_of("sec_edgar")
    policy = cad.CADENCE_POLICY_BY_ID["sec_edgar"]
    return _result("S1", "Continuous lane runs on its own 15-minute cadence", [
        _check("sec_edgar's declared interval is 15 minutes", 900.0,
               policy["normal_interval_seconds"]),
        _check_true("it was collected on the first iteration", first >= 1),
        _check("it was NOT re-collected 5 minutes later", first, after_5min),
        _check_true("it WAS re-collected after 16 minutes",
                    after_16min > after_5min),
    ], {"batches": prov.stage2_calls})


def s2_daily_cadence(base: Path) -> dict:
    """A daily publication lane is collected once per day, in its evening window."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    # 10:00 ET Monday — FINRA publishes after the close, so it is NOT due.
    h_open = _health(roots, MARKET_OPEN_UTC)
    # 19:00 ET Monday — inside FINRA's 18:00-23:59 window. Operational lanes are
    # drained first by design, so the research lane is picked up by a following
    # iteration rather than competing with them: run the loop, do not fake it.
    evening = MARKET_OPEN_UTC + timedelta(hours=9)
    passes = 0
    while passes < 4 and "finra" not in prov.called_sources():
        iterate(world=world, roots=roots, now=evening + timedelta(minutes=passes),
                providers=prov)
        passes += 1
    collected_evening = prov.called_sources()
    h_next = _health(roots, evening + timedelta(hours=2))
    policy = cad.CADENCE_POLICY_BY_ID["finra"]
    return _result("S2", "Daily lane runs once, in its own publication window", [
        _check("finra's declared interval is 24 hours", 86400.0,
               policy["normal_interval_seconds"]),
        _check("at 10:00 ET it is NOT due (publishes after the close)", "NOT_DUE",
               _row(h_open, "finra")["runtime_state"]),
        _check_true("inside its evening window it was collected",
                    "finra" in collected_evening),
        _check_true("it was reached within a few bounded iterations", passes <= 4),
        _check("two hours later it is not due again", False,
               _row(h_next, "finra")["collect_now"]),
    ], {"evening_sources": sorted(collected_evening), "iterations": passes})


def s3_event_driven_cadence(base: Path) -> dict:
    """A publisher-driven lane is due at any hour, including a weekend."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    iterate(world=world, roots=roots, now=WEEKEND_UTC, providers=prov)
    weekend_health = _health(roots, WEEKEND_UTC)
    return _result("S3", "Event-driven lane is due at any hour", [
        _check("news_rss requires no market session", cad.SESSION_ANY,
               cad.CADENCE_POLICY_BY_ID["news_rss"]["market_session_requirement"]),
        _check_true("it was collected on a Sunday", prov.news_calls >= 1),
        _check("its window is active on a Sunday", True,
               _row(weekend_health, "news_rss")["due_window_active"]),
    ], {"news_calls": prov.news_calls})


# --------------------------------------------------------------------------- #
# S4-S5 — market-session awareness
# --------------------------------------------------------------------------- #
def s4_market_closed_not_due(base: Path) -> dict:
    """The delayed-quote feed is NOT_DUE while the market is closed — not stale."""
    roots = _roots(base)
    _arm(roots["service"])
    weekend = _health(roots, WEEKEND_UTC)
    premarket = _health(roots, PREMARKET_UTC)
    wq = _row(weekend, "yahoo_delayed_quote")
    pq = _row(premarket, "yahoo_delayed_quote")
    return _result("S4", "Market feed is NOT_DUE while the market is closed", [
        _check("on a Sunday the quote feed is NOT_DUE", "NOT_DUE",
               wq["runtime_state"]),
        _check("it is NOT reported degraded", False,
               wq["runtime_state"] in ("DEGRADED", "FAILED")),
        _check("it is not counted as expected-to-be-current", False,
               wq["due_window_active"]),
        _check("the session phase is WEEKEND", "WEEKEND", wq["session_phase"]),
        _check("pre-market on a weekday is also NOT_DUE", "NOT_DUE",
               pq["runtime_state"]),
        _check("the halt feed is likewise NOT_DUE on a Sunday", "NOT_DUE",
               _row(weekend, "nasdaq_trader")["runtime_state"]),
    ], {"weekend_reason": wq["health_reason"]})


def s5_market_open_becomes_due(base: Path) -> dict:
    """The same feed becomes due the moment the regular session opens."""
    roots = _roots(base)
    _arm(roots["service"])
    closed = _row(_health(roots, WEEKEND_UTC), "yahoo_delayed_quote")
    opened = _row(_health(roots, MARKET_OPEN_UTC), "yahoo_delayed_quote")
    summary_closed = _health(roots, WEEKEND_UTC)["summary"]
    summary_open = _health(roots, MARKET_OPEN_UTC)["summary"]
    return _result("S5", "Market feed becomes due when the session opens", [
        _check("closed -> NOT_DUE", "NOT_DUE", closed["runtime_state"]),
        _check("open -> DUE", "DUE", opened["runtime_state"]),
        _check("open -> it will actually be collected", True, opened["collect_now"]),
        _check("the session phase is REGULAR_OPEN", "REGULAR_OPEN",
               opened["session_phase"]),
        _check_true("more sources are expected to be current during the session",
                    summary_open["due_now"] > summary_closed["due_now"]),
    ], {"due_closed": summary_closed["due_now"],
        "due_open": summary_open["due_now"]})


# --------------------------------------------------------------------------- #
# S6-S9 — backoff, circuit and failure isolation
# --------------------------------------------------------------------------- #
def _gdelt_429(_url):
    return {"ok": False, "status": 429, "json": None, "detail": "HTTP 429"}


def _gdelt_ok(_url):
    return {"ok": True, "status": 200, "elapsed_seconds": 0.01,
            "json": {"articles": []}}


def s6_rate_limit_opens_backoff(base: Path) -> dict:
    """HTTP 429 places the source in BACKOFF with a long, bounded window."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    t0 = MARKET_OPEN_UTC
    iterate(world=world, roots=roots, now=t0, providers=prov,
            cycle_fn=_cycle_over_world(world, roots, [], gdelt_fetcher=_gdelt_429))
    health = _health(roots, t0 + timedelta(minutes=1))
    row = _row(health, "gdelt")
    wait = cad.backoff_seconds(category=cad.ERR_RATE_LIMIT, consecutive_failures=1)
    return _result("S6", "HTTP 429 opens adaptive backoff", [
        _check("the error is classified as a rate limit", cad.ERR_RATE_LIMIT,
               cad.classify_http_error(status=429)),
        _check("the source is in BACKOFF", "BACKOFF", row["runtime_state"]),
        _check("a 429 backoff is 15 minutes, not a retry", 900.0, wait),
        _check_true("a backoff deadline was recorded", bool(row["backoff_until"])),
        _check("the rate-limit counter advanced", 1, row["rate_limit_count"]),
        _check("the service itself is NOT failed", False,
               row["circuit_state"] == cad.CIRCUIT_OPEN),
    ], {"backoff_until": row["backoff_until"]})


def s7_no_calls_during_backoff(base: Path) -> dict:
    """A source in backoff is NOT called again until the window expires."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    t0 = MARKET_OPEN_UTC
    calls = {"n": 0}

    def counting_gdelt(url):
        calls["n"] += 1
        return _gdelt_429(url)

    iterate(world=world, roots=roots, now=t0, providers=prov,
            cycle_fn=_cycle_over_world(world, roots, [],
                                       gdelt_fetcher=counting_gdelt))
    after_first = calls["n"]
    for minutes in (2, 5, 10):
        iterate(world=world, roots=roots, now=t0 + timedelta(minutes=minutes),
                providers=prov,
                cycle_fn=_cycle_over_world(world, roots, [],
                                           gdelt_fetcher=counting_gdelt))
    row = _row(_health(roots, t0 + timedelta(minutes=10)), "gdelt")
    return _result("S7", "No provider call is made during backoff", [
        _check_true("the first iteration called the provider", after_first >= 1),
        _check("three further iterations made NO additional call", after_first,
               calls["n"]),
        _check("the source is still in BACKOFF", "BACKOFF", row["runtime_state"]),
        _check("it is not scheduled for collection", False, row["collect_now"]),
    ], {"total_calls": calls["n"]})


def s8_recovery_after_backoff(base: Path) -> dict:
    """Once the backoff window expires a success resets the failure streak."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    t0 = MARKET_OPEN_UTC
    iterate(world=world, roots=roots, now=t0, providers=prov,
            cycle_fn=_cycle_over_world(world, roots, [], gdelt_fetcher=_gdelt_429))
    during = _row(_health(roots, t0 + timedelta(minutes=5)), "gdelt")
    later = t0 + timedelta(minutes=70)   # past the 15-minute 429 window
    iterate(world=world, roots=roots, now=later, providers=prov,
            cycle_fn=_cycle_over_world(world, roots, [], gdelt_fetcher=_gdelt_ok))
    after = _row(_health(roots, later + timedelta(minutes=1)), "gdelt")
    return _result("S8", "Collection resumes after the backoff window", [
        _check("during the window it is in BACKOFF", "BACKOFF",
               during["runtime_state"]),
        _check("after a success the failure streak is reset", 0,
               after["consecutive_failures"]),
        _check("the backoff deadline is cleared", None, after["backoff_until"]),
        _check("the circuit is closed", cad.CIRCUIT_CLOSED, after["circuit_state"]),
        _check_true("a successful collection was recorded",
                    bool(after["last_success_at"])),
    ], {})


def s9_failure_isolation(base: Path) -> dict:
    """One failing source never blocks the others."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    prov.stage2_fail = {"sec_edgar"}
    t0 = MARKET_OPEN_UTC
    receipt = iterate(world=world, roots=roots, now=t0, providers=prov,
                      cycle_fn=_cycle_over_world(world, roots, [],
                                                 gdelt_fetcher=_gdelt_429))
    collected = {c["source_id"]: c["ok"] for c in receipt["collected"]}
    health = _health(roots, t0 + timedelta(minutes=1))
    healthy_others = [r["source_id"] for r in health["sources"]
                      if r["source_id"] in collected and collected[r["source_id"]]]
    return _result("S9", "One failed source does not block the others", [
        _check("the failing source is recorded as failed", False,
               collected.get("sec_edgar")),
        _check_true("other sources in the same iteration still succeeded",
                    len(healthy_others) >= 1),
        _check_true("the iteration completed", receipt["ran"]),
        _check_true("the rate-limited live adapter is independent of the "
                    "Stage-2 failure",
                    _row(health, "gdelt")["runtime_state"] == "BACKOFF"),
    ], {"collected": collected, "healthy": healthy_others})


# --------------------------------------------------------------------------- #
# S10-S12 — the decision gate: no work without new material information
# --------------------------------------------------------------------------- #
def s10_identical_information_no_decision_work(base: Path) -> dict:
    """Re-collecting identical information runs no opportunity cost or reassessment."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    pub = world["eligible"] + "T13:00:00+00:00"
    recs = [replay.news_record(source_id="news_rss", native_id="dup-1",
                               ticker="H01", title="Routine corporate update",
                               effective_at=world["eligible"], published_at=pub,
                               publisher="Example Wire")]
    t0 = MARKET_OPEN_UTC
    first = iterate(world=world, roots=roots, now=t0, providers=prov, records=recs)
    second = iterate(world=world, roots=roots, now=t0 + timedelta(minutes=20),
                     providers=prov, records=recs)
    c1 = first.get("event_cycle") or {}
    c2 = second.get("event_cycle") or {}
    return _result("S10", "Identical information triggers no decision work", [
        _check_true("the first pass admitted the event",
                    (c1.get("events_admitted") or 0) >= 1),
        _check("the second pass admitted NO new event", 0,
               c2.get("events_admitted") or 0),
        _check("no reassessment ran on the repeat", False,
               bool(c2.get("reassessment_ran"))),
        _check("no proposal was built on the repeat", False,
               bool(c2.get("proposal_built"))),
    ], {"first": c1.get("state"), "second": c2.get("state")})


def s11_immaterial_information_no_decision_work(base: Path) -> dict:
    """New but immaterial information does not re-ask the portfolio question."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    pub = world["eligible"] + "T13:30:00+00:00"
    recs = [replay.news_record(source_id="news_rss", native_id="unrelated-1",
                               ticker=None,
                               title="A company we do not hold opened an office",
                               effective_at=world["eligible"], published_at=pub,
                               publisher="Example Wire")]
    # The BOOTSTRAP pass is not the test. The first cycle ever run sees a first
    # quote for every holding, and a first quote genuinely is new operational
    # risk information. Settle that baseline, THEN deliver the immaterial item.
    _bootstrap = iterate(world=world, roots=roots, now=MARKET_OPEN_UTC,
                         providers=prov, records=[])
    later = MARKET_OPEN_UTC + timedelta(minutes=16)
    receipt = iterate(world=world, roots=roots, now=later,
                      providers=prov, records=recs)
    cyc = receipt.get("event_cycle") or {}
    boot = _bootstrap.get("event_cycle") or {}
    return _result("S11", "Immaterial information triggers no decision work", [
        _check_true("the event cycle ran (bytes did arrive)",
                    receipt["event_cycle_ran"]),
        _check_true("the immaterial item was admitted as a NEW event",
                    (cyc.get("events_admitted") or 0) >= 1),
        _check("no reassessment ran", False, bool(cyc.get("reassessment_ran"))),
        _check("no proposal was built", False, bool(cyc.get("proposal_built"))),
        _check("it named no current holding", [],
               cyc.get("affected_holdings") or []),
    ], {"state": cyc.get("state"), "bootstrap_state": boot.get("state"),
        "admitted": cyc.get("events_admitted")})


def s12_quiet_iteration_calls_nothing(base: Path) -> dict:
    """When no source is due the iteration performs NO decision work at all."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    t0 = MARKET_OPEN_UTC
    # Drain the backlog AT THE SAME SIMULATED INSTANT so no lane's interval can
    # elapse mid-drain: what is left after this is genuinely nothing, not the
    # remainder of a bounded catch-up.
    drained = 0
    while drained < 6:
        receipt = iterate(world=world, roots=roots, now=t0, providers=prov)
        drained += 1
        if not receipt["due_plan"]:
            break
    before = len(prov.stage2_calls), prov.news_calls
    quiet = iterate(world=world, roots=roots, now=t0 + timedelta(minutes=2),
                    providers=prov)
    after = len(prov.stage2_calls), prov.news_calls
    return _result("S12", "A quiet iteration does no work", [
        _check("no source was due", [], quiet["due_plan"]),
        _check("no provider was called", before, after),
        _check("the Release-28 cycle did NOT run", False, quiet["event_cycle_ran"]),
        _check("the state records that nothing new arrived", "NO_NEW_INFORMATION",
               quiet["state"]),
        _check_true("the reason names the absence of new information",
                    "no source produced new information"
                    in quiet["event_cycle_reason"].lower()),
    ], {"reason": quiet["event_cycle_reason"]})


# --------------------------------------------------------------------------- #
# S13-S15 — material information reaches the REAL decision owners
# --------------------------------------------------------------------------- #
def s13_material_event_reassesses_and_holds(base: Path) -> dict:
    """A material 8-K on a holding runs the REAL owners and can end in HOLD."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    pub = world["eligible"] + "T18:05:11+00:00"
    recs = [replay.stage2_record(
        source_id="sec_edgar", record_type="FILING_EVENT", event_type="8-K",
        native_id="sub|H02|8k-collect", ticker="H02",
        effective_at=world["eligible"], published_at=pub,
        payload={"form_type": "8-K", "accession_number": "8k-collect", "cik": "2",
                 "is_8k": True, "acceptance_datetime": pub, "item_202_note": None})]
    receipt = iterate(world=world, roots=roots, now=MARKET_OPEN_UTC,
                      providers=prov, records=recs)
    cyc = receipt.get("event_cycle") or {}
    return _result("S13", "Material event reaches the real owners and holds", [
        _check_true("the Release-28 orchestrator ran", receipt["event_cycle_ran"]),
        _check_true("the event named a current holding",
                    "H02" in (cyc.get("affected_holdings") or [])),
        _check("ONLY the holding the 8-K named is on the attention list", ["H02"],
               cyc.get("holdings_named_by_material_information")),
        _check_true("the reassessment ran through the canonical owner",
                    bool(cyc.get("reassessment_ran"))),
        _check("no target change was proposed", False,
               bool(cyc.get("proposal_built"))),
        _check("the receipt records MATERIAL_INFORMATION", "MATERIAL_INFORMATION",
               receipt["state"]),
        _check("no order was created", False,
               receipt["safety"]["creates_orders"]),
    ], {"state": cyc.get("state"),
        "refresh_scope": cyc.get("affected_holdings"),
        "attention": cyc.get("holdings_named_by_material_information")})


def s14_material_event_produces_target_for_review(base: Path) -> dict:
    """A material improvement produces a COMPLETE target portfolio for MANUAL review."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world(holding_ranks={"H01": 240, "H02": 238})
    world["prior_ranking"] = dict(world["prior_ranking"], H01=18, H02=19)
    prov = FakeProviders()
    receipt = iterate(world=world, roots=roots, now=MARKET_OPEN_UTC,
                      providers=prov, records=[])
    cyc = receipt.get("event_cycle") or {}
    return _result("S14", "Material improvement produces a reviewable target", [
        _check_true("the Release-28 orchestrator ran", receipt["event_cycle_ran"]),
        _check("a complete target portfolio was built", True,
               bool(cyc.get("proposal_built"))),
        _check("the cycle terminates in manual review", True,
               bool(cyc.get("manual_review_required"))),
        _check("the receipt records MATERIAL_INFORMATION", "MATERIAL_INFORMATION",
               receipt["state"]),
        _check("no order was created", False, receipt["safety"]["creates_orders"]),
        _check("no proposal was approved", False,
               receipt["safety"]["approves_proposals"]),
        _check("no target was confirmed", False,
               receipt["safety"]["confirms_targets"]),
    ], {"state": cyc.get("state")})


def s15_no_execution_path_exists(base: Path) -> dict:
    """No scenario, material or otherwise, can produce an execution."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world(holding_ranks={"H01": 240, "H02": 238})
    world["prior_ranking"] = dict(world["prior_ranking"], H01=18, H02=19)
    prov = FakeProviders()
    receipt = iterate(world=world, roots=roots, now=MARKET_OPEN_UTC,
                      providers=prov, records=[])
    safety = receipt["safety"]
    forbidden = ("creates_orders", "confirms_orders", "fills_orders",
                 "cancels_orders", "approves_proposals", "confirms_targets",
                 "runs_controlled_rebalance", "runs_daily_close",
                 "runs_daily_research_cycle", "promotes_models",
                 "mutates_model_weights", "execution_automation_enabled",
                 "broker_execution_enabled",
                 "model_promotion_automation_enabled")
    checks = [_check("%s is false" % key, False, bool(safety[key]))
              for key in forbidden]
    checks.append(_check("information collection automation is the ONLY thing armed",
                         "ON_WHEN_OPERATOR_STARTS_SERVICE",
                         safety["information_collection_automation"]))
    checks.append(_check("manual review is required", "REQUIRED",
                         safety["manual_portfolio_review"]))
    return _result("S15", "No execution path exists from collection", checks, {})


# --------------------------------------------------------------------------- #
# S16-S18 — restart, idempotency and single-flight
# --------------------------------------------------------------------------- #
def s16_restart_preserves_state(base: Path) -> dict:
    """A worker restart preserves watermarks, counters and backoff."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    t0 = MARKET_OPEN_UTC
    iterate(world=world, roots=roots, now=t0, providers=prov,
            instance_id="worker-A",
            cycle_fn=_cycle_over_world(world, roots, [], gdelt_fetcher=_gdelt_429))
    before_health = _health(roots, t0 + timedelta(minutes=1))
    before_state = ic.load_service_state(root=roots["service"])
    before_rows = {r["source_id"]: (r["last_success_at"], r["watermark"],
                                    r["backoff_until"])
                   for r in before_health["sources"]}

    # Restart: release the slot, then start a NEW worker instance.
    ic.release_service_lock(root=roots["service"], instance_id="worker-A")
    ic.register_worker_start(root=roots["service"], instance_id="worker-B",
                             pid=os.getpid())
    after_state = ic.load_service_state(root=roots["service"])
    after_health = _health(roots, t0 + timedelta(minutes=2))
    after_rows = {r["source_id"]: (r["last_success_at"], r["watermark"],
                                   r["backoff_until"])
                  for r in after_health["sources"]}
    return _result("S16", "Restart preserves durable state", [
        _check("the worker identity changed", True,
               before_state["instance_id"] != after_state["instance_id"]),
        _check("the restart was counted", 1,
               int(after_state["restart_count"]) - int(before_state["restart_count"])),
        _check("every source's watermark/success/backoff is unchanged",
               before_rows, after_rows),
        _check_true("the loop count carried over",
                    after_state["loop_count"] == before_state["loop_count"]),
        _check_true("the rate-limited source is still in backoff after the restart",
                    _row(after_health, "gdelt")["runtime_state"] == "BACKOFF"),
    ], {"before_instance": before_state["instance_id"],
        "after_instance": after_state["instance_id"]})


def s17_restart_does_not_duplicate(base: Path) -> dict:
    """After a restart the same information produces NO duplicate event or decision."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    pub = world["eligible"] + "T18:05:11+00:00"
    recs = [replay.stage2_record(
        source_id="sec_edgar", record_type="FILING_EVENT", event_type="8-K",
        native_id="sub|H02|8k-restart", ticker="H02",
        effective_at=world["eligible"], published_at=pub,
        payload={"form_type": "8-K", "accession_number": "8k-restart", "cik": "2",
                 "is_8k": True, "acceptance_datetime": pub, "item_202_note": None})]
    t0 = MARKET_OPEN_UTC
    first = iterate(world=world, roots=roots, now=t0, providers=prov,
                    instance_id="worker-A", records=recs)
    ic.release_service_lock(root=roots["service"], instance_id="worker-A")
    ic.register_worker_start(root=roots["service"], instance_id="worker-B",
                             pid=os.getpid())
    second = iterate(world=world, roots=roots, now=t0 + timedelta(minutes=20),
                     providers=prov, instance_id="worker-B", records=recs)
    c1 = first.get("event_cycle") or {}
    c2 = second.get("event_cycle") or {}
    return _result("S17", "Restart does not duplicate an event or a decision", [
        _check_true("the pre-restart pass admitted the event",
                    (c1.get("events_admitted") or 0) >= 1),
        _check("the post-restart pass admitted NO duplicate", 0,
               c2.get("events_admitted") or 0),
        _check_true("the duplicate was suppressed by content identity",
                    (c2.get("duplicates_suppressed") or 0) >= 1),
        _check("no second reassessment ran", False,
               bool(c2.get("reassessment_ran"))),
        _check("no proposal was built after the restart", False,
               bool(c2.get("proposal_built"))),
    ], {"first_admitted": c1.get("events_admitted"),
        "second_admitted": c2.get("events_admitted"),
        "second_state": c2.get("state")})


def s18_second_worker_rejected(base: Path) -> dict:
    """Starting a second production worker FAILS CLOSED."""
    roots = _roots(base)
    _arm(roots["service"])
    now = MARKET_OPEN_UTC
    first = ic.acquire_service_lock(root=roots["service"], instance_id="worker-A",
                                    pid=os.getpid(), now=now)
    second = ic.acquire_service_lock(root=roots["service"], instance_id="worker-B",
                                     pid=os.getpid() + 1, now=now)
    released = ic.release_service_lock(root=roots["service"],
                                       instance_id="worker-A")
    third = ic.acquire_service_lock(root=roots["service"], instance_id="worker-C",
                                    pid=os.getpid(), now=now)
    return _result("S18", "A second worker is refused", [
        _check("the first worker acquired the slot", True, first["acquired"]),
        _check("the second worker was REFUSED", False, second["acquired"]),
        _check("the refusal names the single-flight gate", "SINGLE_FLIGHT_LOCK_HELD",
               second["reason"]),
        _check_true("the refusal identifies the incumbent",
                    bool((second.get("holder") or {}).get("pid"))),
        _check("the slot was released cleanly", True, released["released"]),
        _check("a later worker may then acquire it", True, third["acquired"]),
    ], {})


# --------------------------------------------------------------------------- #
# S19 — a wake from sleep is a pass, not a request storm
# --------------------------------------------------------------------------- #
def s19_bounded_catch_up(base: Path) -> dict:
    """After a long sleep, ONE iteration collects a bounded number of sources."""
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    # The machine "slept" for three days; every weekday lane is now overdue.
    receipt = iterate(world=world, roots=roots, now=MARKET_OPEN_UTC,
                      providers=prov, max_sources=3)
    health = _health(roots, MARKET_OPEN_UTC + timedelta(minutes=1))
    eligible = [r["source_id"] for r in health["sources"] if r["due_window_active"]]
    return _result("S19", "Bounded catch-up after a long sleep", [
        _check_true("more sources were due than the per-iteration cap",
                    len(receipt["due_plan"]) + len(receipt["due_deferred_for_cap"])
                    > 3),
        _check("the iteration collected at most the cap", True,
               len(receipt["due_plan"]) <= 3),
        _check_true("the deferred sources are NAMED, never silently dropped",
                    len(receipt["due_deferred_for_cap"]) >= 1),
        _check_true("a warning states what was deferred",
                    any("Bounded catch-up" in w for w in receipt["warnings"])),
        _check_true("the deferred sources remain due for the next iteration",
                    all(s in eligible for s in receipt["due_deferred_for_cap"])),
    ], {"due": receipt["due_plan"],
        "deferred": receipt["due_deferred_for_cap"]})


# --------------------------------------------------------------------------- #
# S20-S21 — the quote lane: an observation decides nothing, a MOVE decides
# --------------------------------------------------------------------------- #
def shocked_quote_fetcher(world: dict, *, ticker: str, move: float) -> Callable:
    """The world's own close for every holding, with ONE name moved by ``move``."""
    base = world_quote_fetcher(world)
    tkr = str(ticker).upper()

    def fetch(tickers) -> tuple:
        rows, failures = base(tickers)
        for row in rows:
            if row["ticker"] == tkr:
                row["price"] = round(float(row["price"]) * (1.0 + float(move)), 4)
        return rows, failures
    return fetch


def s20_intraday_shock_reaches_the_review_list(base: Path) -> dict:
    """A same-session collapse on the delayed quote reassesses THAT holding.

    This is the only decision value the 15-minute quote lane has, and it must not
    depend on tomorrow's end-of-day bar existing.
    """
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    receipt = iterate(world=world, roots=roots, now=MARKET_OPEN_UTC, providers=prov,
                      quote_fetcher=shocked_quote_fetcher(world, ticker="H03",
                                                           move=-0.18))
    cyc = receipt.get("event_cycle") or {}
    named = cyc.get("holdings_named_by_material_information") or []
    return _result("S20", "An intraday collapse reaches the review list", [
        _check_true("the Release-28 orchestrator ran", receipt["event_cycle_ran"]),
        _check("the shocked holding is on the attention list", ["H03"], named),
        _check_true("the portfolio was reassessed",
                    bool(cyc.get("reassessment_ran"))),
        _check("the receipt records MATERIAL_INFORMATION", "MATERIAL_INFORMATION",
               receipt["state"]),
        _check("no order was created", False, receipt["safety"]["creates_orders"]),
    ], {"state": cyc.get("state"), "named": named})


def s21_quote_arrival_alone_is_not_material(base: Path) -> dict:
    """A quote that has barely moved is recorded and decides NOTHING.

    Without this rule a service polling the quote lane every 15 minutes would call
    every holding a "material company event" every 15 minutes.
    """
    roots = _roots(base)
    _arm(roots["service"])
    world = replay.build_world()
    prov = FakeProviders()
    receipt = iterate(world=world, roots=roots, now=MARKET_OPEN_UTC, providers=prov,
                      quote_fetcher=shocked_quote_fetcher(world, ticker="H03",
                                                           move=-0.01))
    cyc = receipt.get("event_cycle") or {}
    return _result("S21", "A quote arrival alone is never material", [
        _check_true("the quotes were admitted as evidence",
                    (cyc.get("events_admitted") or 0) >= 1),
        _check("no holding is on the attention list", [],
               cyc.get("holdings_named_by_material_information")),
        _check("no reassessment ran", False, bool(cyc.get("reassessment_ran"))),
        _check("no proposal was built", False, bool(cyc.get("proposal_built"))),
        _check("the receipt records information WITHOUT materiality",
               "NEW_INFORMATION", receipt["state"]),
    ], {"state": cyc.get("state"), "admitted": cyc.get("events_admitted")})


SCENARIOS: dict[str, Callable] = {
    "S1": s1_continuous_cadence,
    "S2": s2_daily_cadence,
    "S3": s3_event_driven_cadence,
    "S4": s4_market_closed_not_due,
    "S5": s5_market_open_becomes_due,
    "S6": s6_rate_limit_opens_backoff,
    "S7": s7_no_calls_during_backoff,
    "S8": s8_recovery_after_backoff,
    "S9": s9_failure_isolation,
    "S10": s10_identical_information_no_decision_work,
    "S11": s11_immaterial_information_no_decision_work,
    "S12": s12_quiet_iteration_calls_nothing,
    "S13": s13_material_event_reassesses_and_holds,
    "S14": s14_material_event_produces_target_for_review,
    "S15": s15_no_execution_path_exists,
    "S16": s16_restart_preserves_state,
    "S17": s17_restart_does_not_duplicate,
    "S18": s18_second_worker_rejected,
    "S19": s19_bounded_catch_up,
    "S20": s20_intraday_shock_reaches_the_review_list,
    "S21": s21_quote_arrival_alone_is_not_material,
}


def run_scenario(key: str, base: Path,
                 timeout_seconds: float = SCENARIO_TIMEOUT_SECONDS) -> dict:
    """Run ONE scenario under a hard wall-clock bound.

    The scenario runs on a daemon thread so a blocked call can never keep the
    acceptance run — or the interpreter — alive. A breach is a REPORTED result
    (``TEST_TIMEOUT``), not a hang: the run continues and names what stalled.
    """
    import threading
    box: dict[str, Any] = {}

    def _run() -> None:
        try:
            box["result"] = SCENARIOS[key](base)
        except HermeticNetworkViolation as exc:
            # A leak is a HARNESS DEFECT, reported as a failing scenario rather
            # than hidden behind a passing suite.
            box["result"] = _result(key, "Hermetic network seal breached", [
                _check("the scenario made no live provider call", True,
                       "LEAKED: %s" % str(exc)[:300])], {"error": "NETWORK_LEAK"})
        except Exception as exc:  # noqa: BLE001 - one failing scenario, not a crash
            import traceback
            box["result"] = _result(key, "Scenario raised", [
                _check("the scenario completed", True,
                       "%s: %s" % (type(exc).__name__, str(exc)[:300]))],
                {"error": "EXCEPTION",
                 "traceback": traceback.format_exc()[-2000:]})

    thread = threading.Thread(target=_run, name="scenario-%s" % key, daemon=True)
    thread.start()
    thread.join(timeout=float(timeout_seconds))
    if thread.is_alive():
        return _result(key, "Scenario exceeded its bound", [
            _check("the scenario finished within its bound",
                   "<= %.0fs" % timeout_seconds, "TEST_TIMEOUT")],
            {"error": "TEST_TIMEOUT", "timeout_seconds": timeout_seconds,
             "root": str(base)})
    return box["result"]


def run_simulation(*, base_dir=None, scenarios: Optional[list] = None,
                   timeout_seconds: float = SCENARIO_TIMEOUT_SECONDS) -> dict:
    """Run the hermetic service simulation. Isolated roots; no production state."""
    keys = [str(k).upper() for k in (scenarios or SCENARIOS.keys())]
    unknown = [k for k in keys if k not in SCENARIOS]
    parent = Path(base_dir) if base_dir else Path(
        tempfile.mkdtemp(prefix="collection_replay_"))
    parent.mkdir(parents=True, exist_ok=True)
    # Every RUN gets its own directory. Reruns never share state with, and never
    # have to delete, an earlier run.
    base = Path(tempfile.mkdtemp(prefix="run_", dir=str(parent)))
    results = []
    with sealed_network() as net:
        for key in keys:
            if key in unknown:
                continue
            results.append(run_scenario(key, base / key.lower(),
                                        timeout_seconds=timeout_seconds))
    failed = [r for r in results if not r["passed"]]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": ic.utc_iso(),
        "base_dir": str(base),
        "scenarios": results,
        "scenario_count": len(results),
        "check_count": sum(r["check_count"] for r in results),
        "failed_scenarios": [r["scenario"] for r in failed],
        "failed_count": len(failed),
        "unknown_scenarios": unknown,
        "passed": not failed and not unknown,
        "hermetic": True,
        "real_owners_used": [
            "engine.collection_cadence", "engine.market_hours",
            "api.information_collection", "api.event_signal_refresh",
            "api.event_fabric", "engine.event_fabric", "engine.event_materiality",
            "api.holding_opportunity_cost", "api.portfolio_reassessment",
            "api.reallocation_proposal"],
        "faked": ["the clock", "provider responses", "the portfolio/price world",
                  "the state root"],
        "network_sealed": True,
        "scenario_timeout_seconds": float(timeout_seconds),
        "timed_out_scenarios": [r["scenario"] for r in results
                                if (r.get("detail") or {}).get("error")
                                == "TEST_TIMEOUT"],
        "blocked_connection_attempts": list(net.attempts),
        "safety": dict(ic.collection_safety_contract(),
                       touched_production_state=False,
                       touched_production_task=False,
                       called_a_real_provider=bool(net.attempts)),
    }


__all__ = ["PHASE", "COMPOSITION_OWNER", "SCHEMA_VERSION", "SCENARIOS",
           "MARKET_OPEN_UTC", "WEEKEND_UTC", "PREMARKET_UTC",
           "SCENARIO_TIMEOUT_SECONDS", "FakeProviders", "fake_quote_fetcher",
           "world_quote_fetcher", "iterate", "run_scenario", "run_simulation",
           "sealed_network", "HermeticNetworkViolation"]
