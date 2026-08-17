# Continuous Governed Information Collection (Release 29)

Release 28 built the processing path — event contract, novelty, materiality,
holding opportunity cost, reassessment, review-only proposal — and proved one
event could flow through it. It did **not** keep information flowing into it.
Every collector remained operator-initiated, so in production the live surface
read:

```
NEW INFORMATION & REASSESSMENT            NOT_RUN
Events recorded 60   Material 42   Naming a holding 38
Sources fresh 1 / 17   Degraded sources 10   Last cycle never
```

Two separate defects are visible in those six numbers:

1. **Nothing was collecting.** Watermarks were stuck between 2026-08-03 and
   2026-08-10 because the last operator-run collection was days earlier.
2. **The denominator was dishonest.** All 17 registry rows were judged against a
   single anchor date, so a *monthly* Treasury series, a *quarterly* BEA lane, a
   *terminally blocked* options feed and a delayed market quote *on a Sunday*
   all read STALE or MISSING and were counted as "degraded".

Release 29 fixes both: it runs the sources continuously at their own cadence, and
it reports health against the set of sources that should actually be current now.

---

## 1. What continuously runs (and what does not)

| Runs continuously | Never runs automatically |
| --- | --- |
| Source collection at source-specific cadence | Daily Close |
| Normalisation onto the Release-28 event contract | The full Daily Research Cycle |
| Dedup / novelty / watermark advance | Controlled rebalance |
| Signal + risk dependency refresh | Order create / confirm / fill / cancel |
| Materiality gate | Proposal approval, target confirmation |
| Holding opportunity cost (only when material) | Model promotion or weight change |
| Portfolio reassessment (only when material) | Any broker call |
| Review-only reallocation proposal | |

The service produces **decision artifacts for a human**, never an execution.

---

## 2. Ownership

```
scripts/run_information_collection_service.py     the ONE long-lived worker
  └── api/information_collection.py               the ONE collection orchestrator
        ├── engine/collection_cadence.py          cadence / due / budget / backoff (pure)
        ├── engine/market_hours.py                market clock (pure, canonical)
        ├── alpha_agent/ingestion.py              Stage-2 collectors        (existing owner)
        ├── alpha_agent/feed_registry.py          Stage-3.5 RSS/Atom        (existing owner)
        ├── api/portfolio_state.py                attention Tier 0          (existing owner)
        ├── api/universe_scoring.py               attention Tier 1/2        (existing owner)
        └── api/event_signal_refresh.py           THE Release-28 event orchestrator
              └── event_fabric / materiality / HOC / reassessment / proposal
```

There is exactly **one** collection orchestrator and exactly **one** event
orchestrator. Release 29 contains no second provider client, no second scoring
engine, no second opportunity cost, no second reassessment, no second proposal
builder and no second market-session resolver.

---

## 3. Source cadence policy

`engine/collection_cadence.py` holds one authoritative policy per source. Every
interval is justified by the source's actual publication behaviour, not a
convenient round number.

| Source | Kind | Window (ET) | Normal interval | Attention | Why |
| --- | --- | --- | --- | --- | --- |
| `norgate_local` | LOCAL_FILE_WATCH | weekday 17:30–23:59 | 30 min | global | Updated by Norgate's own desktop updater; there is no API to call, so the service **watches** the local store after the close. |
| `eodhd` | SESSION_END | weekday 06:00–22:00 | 60 min | Tier 1 | EOD/actions settle once per session; entitled news updates intraday. One bounded pass per hour covers both without burning quota on unchanged rows. |
| `eodhd_analyst` | DAILY_PUBLICATION | weekday 07:00–22:00 | 24 h | Tier 1 | A **daily** prospective snapshot — collecting twice cannot create a new vintage. Research lane (forward-snapshot only). |
| `sec_edgar` | CONTINUOUS_EVENT | weekday 06:00–22:00 | 15 min | global | EDGAR accepts 06:00–22:00 ET and stamps acceptance to the second. A 15-min delta read of the recent index puts an 8-K on the review list the same hour, well inside fair access. |
| `news_rss` | CONTINUOUS_EVENT | any hour | 15 min | global | Regulators publish at any hour incl. weekends. Conditional GETs (ETag/If-Modified-Since) make this mostly 304s. |
| `nasdaq_trader` | CONTINUOUS_EVENT | regular session only | 10 min | global | A halt is only declared while the tape runs. Polling overnight cannot discover a halt that has not happened. |
| `finra` | DAILY_PUBLICATION | weekday 18:00–23:59 | 24 h | global | Reg SHO files publish once, on the **next** business day. Observability lane (signal failed, t=1.56). |
| `fred_alfred` | DAILY_PUBLICATION | weekday 07:00–20:00 | 4 h | global | Market series update at most daily; macro releases follow a schedule. Four passes/day catches a same-day regime input. |
| `us_treasury` | MONTHLY_RELEASE | weekday 08:00–20:00 | 24 h | global | Series are **monthly**; a daily probe is already generous. Research lane (FRED supersedes it). |
| `yahoo_delayed_quote` | INTRADAY_MARKET | **regular session only** | 15 min | Tier 0 | The quote is ~15 min delayed — polling faster re-reads the same tick. Outside the session it is `NOT_DUE`, not stale. |
| `gdelt` | CONTINUOUS_EVENT | any hour | 60 min | Tier 0 | The free endpoint rate-limits bursts and returned HTTP 429 under Release-28 probing. Hourly over ≤8 held names; a 429 opens a long backoff. |
| `bls` | *disabled* | — | — | — | **Redundant.** CPI/unemployment already arrive from FRED *with ALFRED vintages*, which BLS v2 does not provide. |
| `bea` | *disabled* | — | — | — | **Redundant.** Quarterly national accounts restate without vintages and arrive slower than any reassessment cadence. |
| `corporate_actions_registry` | INTERNAL_EVENT | — | — | — | Operator-registered and confirm-gated. There is no provider to poll. |
| `analyst_revision_vendor` | BLOCKED | — | — | — | Blocked on entitlement (Intrinio trial → DO_NOT_BUY). |
| `options_iv` | BLOCKED | — | — | — | Blocked on entitlement; no free substitute. |
| `prediction_service` | NOT_A_DATA_SOURCE | — | — | — | Emits **model output** over inputs the fabric already ingests; polling it would double-count information. |

### Two questions, deliberately separated

* `due_window_active` — *should this source be current right now?* This is the KPI
  denominator and the basis for FRESH vs DEGRADED.
* `collect_now` — *should this iteration call it?* True only when the window is
  open, the minimum interval has elapsed, no backoff is in force and budget
  remains.

A source can be due-window-active and healthy while `collect_now` is false. That
is the normal quiet case, and it is why a service that wakes every 60 seconds
does **not** make 17 provider calls a minute.

`next_due_at` is deliberately **absent** while a source's publication window is
closed. A weekend row that read *"does not publish today — next due 00:13"* stated
two contradictory things and the second one was not true; with no exchange-holiday
calendar available, the honest answer is the reason, not a fabricated timestamp.

---

## 4. The fixed freshness denominator

`engine.collection_cadence.summarize_runtime` partitions every source into
exactly one runtime state and answers the operator's real question.

Measured live on **Sunday 2026-08-16 13:24 ET**:

```
2 of 2 source(s) that should be current now are healthy
  due_now 2      not_due 9       backoff 0
  degraded 0     failed 0        blocked 2      disabled 4
```

Only `news_rss` and `gdelt` publish 24/7, so only they are expected to be current
on a Sunday. `yahoo_delayed_quote` reads `NOT_DUE` — *"The regular session is
closed (WEEKEND), so this source has no new value to publish"* — instead of the
previous `MISSING`. The operational denominator deliberately excludes research /
observability lanes: they are collected on their own cadence and reported under
`RESEARCH_ONLY`, because their health must not dilute the answer to "is the
decision surface being fed?".

---

## 5. Attention universe

Rebuilt every iteration from the authoritative owners — no ticker is hardcoded
and the UI holds no universe of its own.

| Tier | Content | Used by |
| --- | --- | --- |
| Tier 0 | every current operational holding | delayed quotes, GDELT |
| Tier 1 | strongest eligible non-held candidates from the canonical ranking | EODHD |
| Tier 2 | broader eligible universe | low-frequency lanes |
| Global | no per-symbol query needed | SEC index, halts, RSS, macro |

---

## 5A. What the quote lane is allowed to decide

The delayed-quote lane runs every 15 minutes over the held book. Making it
continuous exposed two defects in how a price observation was treated, and both
are now closed.

**A quote identifies the DAY'S MARK, not the minute's read.** `source_event_id` is
keyed on `(ticker, market date)`. Re-reading an unchanged quote is an exact
duplicate and a no-op; a changed price is one immutable `MATERIAL_UPDATE` that
supersedes the prior mark for that day. Keyed on the minute — as it originally was
— every poll of a still market manufactured a "new" event for every holding: 25
fabricated events an hour, ~650 a session.

**A market OBSERVATION is never material on its own.** `market_bar` and
`market_quote` are now suppressed at the event-trigger stage with
`MARKET_OBSERVATION_NOT_MATERIAL_ON_ARRIVAL`. This is the rule already applied to
macro releases — *a new observation is never material on its own; a measured
transition is.* Before this, a routine quote was reported as "a material company
event (market_quote / DELAYED_QUOTE) named NVDA", so a service polling every 15
minutes would have re-run opportunity cost and reassessment on nothing but the
passage of time, and every holding would have appeared on the attention list.

**But the lane still decides something.** Suppressing the arrival without adding a
measurement would have made the quote lane pure cost. `api.event_signal_refresh`
now overlays the quote on the owned close as `ret_intraday`, and the risk lane
raises `HOLDING_PRICE_SHOCK` at the SAME 7% level it already applies to a
one-session move. A same-session collapse reaches the review list while it is
happening instead of waiting for tomorrow's bar. `ret_intraday` is a risk
measurement only: it is never written to the panel, never becomes the portfolio
mark and never moves a score. An intraday shock and its confirmation at the close
collapse onto ONE trigger for that security, so the two are one reason to
reassess, not two.

The materiality policy version is therefore `event_materiality.v2`. No threshold
number changed; the version is part of the trigger fingerprint, so the first cycle
after the change re-asks the portfolio question once rather than inheriting a
verdict reached under the old rule.

**One clock per cycle.** The live adapters previously stamped event identity from
their own ambient `datetime.now()`, which the cycle knew nothing about. The cycle
clock (`now_iso`) is now threaded from the collection iteration into
`capture_market_quotes` and `capture_gdelt_news`. Without it a replay driven by a
simulated clock was stamped with the real one, and the acceptance verdict depended
on which real-world minute the run happened to straddle.

**The read surface is bound to the gate's rule.** `load_event_signal_refresh_status`
excludes the observation families from its `material` list using the gate's own
constant. Otherwise the operator would read "42 material events" on a day when
nothing happened except that the market was open.

---

## 6. Provider budgets

Every external call is bounded. Per source the policy declares
`max_calls_per_iteration`, `max_calls_per_hour`, `max_calls_per_day`,
`max_symbols_per_iteration`, `minimum_call_interval_seconds`, `timeout_seconds`
and `max_retries`. A source that has exhausted its budget is skipped with a
recorded reason rather than being called and failing.

`MAX_SOURCES_PER_ITERATION = 6` bounds catch-up: however long the machine slept,
one iteration collects at most six sources, so a wake-from-sleep is a pass, never
a request storm. Deferred sources are named in the receipt — coverage is never
silently truncated.

---

## 7. Adaptive backoff and circuit breaker

| Error | Base backoff | Ceiling |
| --- | --- | --- |
| HTTP 429 | 15 min | 4 h |
| HTTP 5xx | 2 min | 1 h |
| timeout / network | 1 min | 30 min |
| auth / entitlement (401/402/403) | **6 h** | 24 h |
| other 4xx | 30 min | 6 h |

Backoff doubles per consecutive failure and is clamped. After
`CIRCUIT_OPEN_THRESHOLD = 4` consecutive failures the circuit opens; when the
window expires the source gets exactly one probe (`HALF_OPEN`). A 429 means *stop
asking*, not *retry immediately* — the service must never turn a provider outage
into an attack on the provider. One failed source never blocks another.

---

## 8. Service state, singleton and restart

`collection_service_state.json` holds the service identity, heartbeat, loop
count, restart count, next wake and the last collection / new-information /
material-information / reassessment / target-change timestamps.
`source_runtime_health.json` holds per-source attempt, success, watermark,
failure streak, backoff and counters. `collection_iteration_history.json` is
bounded to the last 500 receipts.

Exactly one production worker may run. `collection_service.lock` records
instance id, PID, host and heartbeat; a second worker is **refused**
(`SINGLE_FLIGHT_LOCK_HELD`). A lock abandoned by a hard kill is reclaimed only
after the heartbeat has been silent past `LOCK_TAKEOVER_SECONDS` **and** the
recorded PID is gone.

On restart the worker reads the durable state, recomputes what is due from each
source's own `last_attempt_at`, applies bounded catch-up and continues. Watermarks,
event ids and the anti-churn trigger fingerprint are untouched, so a restart never
reclassifies old events as new, never duplicates a reassessment and never replays
months of provider history because the PC slept.

---

## 9. Windows runtime

A **user-level** Scheduled Task, `PaperTrader-InformationCollection`, launches
one long-lived worker at logon. It follows the existing `AlphaAgent-*` pattern:
`LogonType Interactive`, `RunLevel Limited`, `MultipleInstances IgnoreNew`,
`StartWhenAvailable`, restart-on-failure. No administrator rights, no Windows
service, no stored password.

Managed only by `scripts/manage_information_collection.ps1`
(`-Action Install|Start|Stop|Restart|Status|Uninstall`). `Status` is read-only and
runs without `-Execute`; every mutating action requires `-Execute`. Uninstall
removes the task and **never** deletes event evidence.

### Credentials in the background

Task Scheduler does not give a process an interactive shell's environment.
Measured on this machine: `EODHD_API_KEY`, `FRED_API_KEY` and
`PAPER_TRADER_FRED_API_KEY` are set at **User** scope in the registry, so a
logon-triggered task running as this user **does** inherit them. The service
checks *presence only*, never a value. A source whose credential is invisible
becomes `DEGRADED_CREDENTIAL` and the other sources continue — it is never
silently reported as active.

---

## 10. Collection automation vs execution automation

These are different switches and the UI must say so.

```
INFORMATION COLLECTION: ON     ← Release 29 turns this on, once, by operator act
PAPER MODE
MANUAL REVIEW
EXECUTION AUTOMATION: OFF      ← unchanged, unreachable, architecture-tested
NO BROKER EXECUTION
NO LIVE ORDERS
```

The operator enables collection **once** (`CONFIRM_ENABLE_INFORMATION_COLLECTION`).
After that the service may collect, persist events, refresh signals and risk, run
opportunity cost, run reassessment and build review-only proposals without another
button press. It may never approve, confirm, order, fill, cancel, rebalance,
close the day, run the full DRC, or promote a model.

---

## 11. UI wireframe (1920×1080, produced before implementation)

The card replaces the Release-28 "New Information & Reassessment" panel in place.
It answers six questions **in this order** and pushes infrastructure counters
below the fold.

One element sits **outside** the card: a `COLLECTION: …` chip in the always-visible
top status bar, beside `MANUAL REVIEW` / `PAPER ORDERS ONLY` / `AUTOMATION OFF`.
The card is in the Today cockpit band, roughly a screen below the fold. A decision
surface that has quietly stopped being fed is the precise failure this release
exists to end, so "is information still arriving?" must be answerable without
scrolling and from any view. The chip is rendered verbatim by the SAME single
loader from the SAME payload — it is not a second status source — and it carries
the backend's own reason as its tooltip.

```
┌─ Information Collection & Portfolio Reassessment ────────────────────────────────────────────┐
│ ● COLLECTION RUNNING            [COLLECTION: ON] [PAPER MODE] [MANUAL REVIEW]                │
│                                 [EXECUTION AUTOMATION: OFF] [NO BROKER EXECUTION]            │
│                                                                                              │
│ No material new information since 12:42 PM. 2 of 2 sources that should be current            │
│ now are healthy.                                                     (1) IS IT RUNNING?      │
│                                                                      (2) DID ANYTHING ARRIVE?│
│ Heartbeat 12s   Last collection 12:45 PM   Next check 12:50 PM   Iterations 41   Restarts 0  │
│ Sources due 2/2 healthy · Not due 9 · Backoff 1 · Blocked 2 · Disabled 4   ← FIXED KPI ROW   │
├──────────────────────────────────┬───────────────────────────────────────────────────────────┤
│ LATEST MATERIAL EVENTS           │ AFFECTED HOLDINGS                    (3) WHICH HOLDINGS?  │
│ NVDA  8-K guidance   1:04 PM     │ NVDA  rank 3→9  risk +2.1%  HOC: REPLACE                  │
│   [TRIGGER ONLY] [sec_edgar]     │ MSFT  rank 5→5  risk  0.0%  HOC: HOLD                     │
│ ...                              │                                                           │
│ (or: "No material new            │ (or: "No current holding is named by a material event")   │
│  information since <ts>.")       │                                                           │
├──────────────────────────────────┴───────────────────────────────────────────────────────────┤
│ PORTFOLIO DECISION                                          (4) REASSESSED? (5) DID IT CHANGE?│
│ Reassessment: COMPLETE     Recommendation: HOLD                                              │
│ Reason: structural rank remains strong; switching improvement below the cost hurdle          │
│ → (when it changes) Recommendation: REPLACE · Best alternative XYZ · net +0.42 · turnover 8% │
│   [Review target portfolio]  ← navigates to the existing proposal panel. No order button.    │
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│ ▸ Source health & diagnostics (collapsed)                   (6) AUDIT / ADVANCED             │
│   source | state | cadence | next due | last success | backoff/circuit | why                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Acceptance criteria**

1. The first line answers *is collection running* — never a raw counter.
2. The quiet case reads `No material new information since <timestamp>`, never a
   blank em-dash.
3. The KPI row shows `due / healthy / not due / backoff / blocked`, never
   `fresh 1 / 17`.
4. `COLLECTION: ON` and `EXECUTION AUTOMATION: OFF` are both visible and visually
   distinct; the execution safety badge is never removed.
5. A source in backoff shows its next retry while the service still reads RUNNING.
6. A market feed with the session closed reads `NOT_DUE`, never STALE.
7. The source table is collapsed by default (Audit / Advanced).
8. No `alert()`, no `confirm()`, no Create Orders, no automation toggle for
   execution.
9. Every value is rendered from the backend payload; the browser derives none of
   it — no date arithmetic, no health classification, no counting.
10. The `COLLECTION: …` header chip is visible without scrolling on every view and
    is set by the same loader, never by a second status source.

### Browser acceptance (1920×1080, measured)

Measured on the Release-29 build with Playwright at 1920×1080: header chip visible
at the top of the viewport on load; headline `COLLECTION NOT INSTALLED` with the
backend's own sentence beneath it; KPI row reading `Sources due 2/2 healthy · Not
due 9 · Backoff 0 · Degraded 0 · Blocked 2 · disabled 4 · Research lanes 0 · Market
session WEEKEND`; the quiet case reading *"No material new information has been
recorded yet."*; `yahoo_delayed_quote` reading `NOT_DUE — The regular session is
closed (WEEKEND), so this source has no new value to publish`; 17 source rows
collapsed under *Audit / Advanced*; **0** rows contradicting a closed window with a
`Next due`; **0** `alert()`, **0** `confirm()`, **0** blank visible buttons, **0**
Create-Order controls, **0** execution-automation toggles, **0** rendered
"Connect to Load" placeholders; no page-level vertical or horizontal overflow.

Validated on a **non-production instance on port 8011** started by the canonical
owner (`scripts/restart_paper_trader_backend.ps1 -Force -Port 8011`), which printed
`LIVE_SMOKE_OK` with `GET /v1/operations/information-collection -> 200` and 25
positions served. The production backend on 8001 was deliberately not restarted, so
it does not yet serve this route.

---

## 11A. Hermetic acceptance — what was actually proven

`api/collection_replay.py` drives the REAL owners over a simulated clock, a
synthetic portfolio/price world and controllable provider stand-ins. The socket
layer is sealed for the duration of a run: any outbound connection attempt is
reported as a FAILING scenario, never silently allowed. Each scenario runs under a
hard wall-clock bound, so a stalled call is a reported `TEST_TIMEOUT` rather than a
hung acceptance run.

| # | Scenario | What it proves |
| --- | --- | --- |
| S1 | Continuous cadence | A 15-minute lane is collected every 15 simulated minutes — not every wake |
| S2 | Daily cadence | FINRA is NOT due at 10:00 and IS collected inside its evening window |
| S3 | Event-driven cadence | A publisher-driven lane is due on a Sunday |
| S4 | Market closed | The quote feed reads NOT_DUE on a weekend and pre-market — never STALE |
| S5 | Market open | The same feed becomes due when the session opens |
| S6 | Rate limit | HTTP 429 opens a long backoff |
| S7 | During backoff | The source is NOT called while the backoff stands |
| S8 | Recovery | It is probed once and recovers when the window expires |
| S9 | Failure isolation | One failing source never blocks another |
| S10 | Identical information | A re-collected identical item does no decision work |
| S11 | Immaterial information | New but immaterial information does not re-ask the portfolio question |
| S12 | Quiet iteration | When nothing is due, NO provider is called and no cycle runs |
| S13 | Material event | An 8-K on a holding runs the real owners and can end in HOLD |
| S14 | Material improvement | A complete target portfolio is produced FOR REVIEW |
| S15 | No execution path | No scenario, material or otherwise, can produce an execution |
| S16 | Restart | Durable state survives a restart |
| S17 | Restart idempotency | A restart duplicates neither an event nor a decision |
| S18 | Singleton | A second worker is refused |
| S19 | Bounded catch-up | A wake from sleep is a pass, and deferred sources are NAMED |
| S20 | Intraday shock | A same-session collapse reaches the review list for THAT holding |
| S21 | Quote arrival | A quote that has barely moved is recorded and decides nothing |

**Result: 21 scenarios, 117 checks, 3 consecutive clean runs, 0 blocked connection
attempts, 0 timeouts.** Three runs rather than one because the defect this suite
originally hid was a *flake*: event identity came from the wall clock, so the same
code passed or failed depending on whether two iterations straddled a real-world
minute boundary. A verdict that is not repeatable is not a verdict.

### Reproducing the evidence (Windows PowerShell)

```powershell
cd C:\Users\binis\paper_trader

# Hermetic acceptance. Run it THREE times: the defect this suite originally hid was
# a flake, and one green run is not a verdict.
python -c "from paper_trader.api import collection_replay as cr; r = cr.run_simulation(); print(r['scenario_count'], r['check_count'], r['passed'], r['failed_scenarios'], r['blocked_connection_attempts'])"

# Targeted regression (NOT the full repository suite).
python -m pytest tests\test_release29_continuous_collection.py tests\test_release28_event_driven_manager.py tests\test_architecture_contracts.py -q

# Strict architecture audit (exit 0 required).
python scripts\audit_architecture.py --strict

# Read-only service status. Neither of these starts anything.
python scripts\collection_service_control.py --action status
.\scripts\manage_information_collection.ps1 -RepoRoot C:\Users\binis\paper_trader -Action Status
```

### Guards

* `tests/test_release29_continuous_collection.py` (52 tests) — cadence policy,
  the freshness denominator, backoff and the circuit breaker, provider budgets,
  singleton/heartbeat/restart lifecycle, governance, the attention universe, the
  quote-lane integration, the read contract and ownership.
* `scripts/audit_architecture.py` → `check_information_collection_ownership`
  (strict-blocking) — one cadence owner, one collection iteration, one worker, one
  manager script; a PURE cadence kernel; no second opportunity cost, reassessment,
  proposal builder, scoring engine, provider client or market-session resolver in
  the orchestrator; a GET-only route that can never start a worker; the governance
  vocabulary; the observation rule bound on BOTH the gate and the read surface; one
  clock per cycle; and a UI that classifies nothing.
* `PAPER_TRADER_COLLECTION_DIR` is registered in
  `api.environment_isolation.CANONICAL_STORE_ENV_VARS` and redirected by the
  hermetic acceptance server, so an acceptance run can never read — or a production
  process be fooled by — the wrong collection state.

---

## 12. Remaining gap to true real time

These are genuine external constraints, not missing architecture:

* **~15-minute market-data delay.** The free delayed quote is the fastest feed
  legitimately available under current entitlements. Polling faster re-reads the
  same tick. A real-time entitlement would only change one policy row
  (`yahoo_delayed_quote` → a live feed) — no redesign.
* **As-was analyst revisions are unavailable.** Blocked on entitlement; the
  `ANALYST_REVISION` adapter contract already exists in the event-family table, so
  adding the data later requires no new portfolio architecture.
* **Options IV / skew** has no owned or free source.
* **Challenger forward evidence needs calendar time.** `s25_operating_profitability`
  matures on the existing forward-evidence workflow; the collection service is not
  its owner and does not touch it.
* **No authoritative exchange-holiday calendar.** A holiday presents as a weekday
  on which the market feeds return nothing new — recorded honestly rather than
  inferred as a fabricated `HOLIDAY` phase.
