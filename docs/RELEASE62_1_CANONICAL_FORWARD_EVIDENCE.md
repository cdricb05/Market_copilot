# Release 62.1 — Canonical Multi-Asset TRUE_FORWARD Registration

- **Branch:** `r62-1-canonical-forward-evidence`
- **Development worktree:** `D:\paper_trader_r62_1_forward_evidence`
- **Base:** `ba0d9a3` (R61, deployed and accepted)
- **Live state during development:** read-only. No backend restart, no Daily
  Close, no Portfolio Cycle, no approval, no promotion, no scheduled-task change
  and no write to any live store.

R62.1 changes no economics, no gate, no threshold and no allocation rule. It
closes the **one named gap R61 shipped with**, repairs a maturity clock that
scheduled a session the exchange never held, and separates two runtime identities
the estate had only one word for.

---

## 1. The gap, stated precisely

R61 built the one governed prospective-adoption operation and then discovered
that adoption had **nowhere to go**. Forward evidence had two owners, and each of
them owns a FROZEN COHORT rather than a registration:

| Owner | What it owns | Why it cannot take a new challenger |
|---|---|---|
| `alpha_agent/r46/registry.py` | the R46 challenger **contract** — a fixed specification list hashed into `contract_hash` | every emitted R46 row cites that hash; adding a challenger changes the contract those rows were emitted under |
| `api/shadow_portfolio_evidence.py` | a **session cohort** of complete paper portfolios | its own refusal says it: *"it registers a session cohort, not a single challenger"* |

So four R58 freezes sat with an exact immutable identity, an inception, a
specification hash, a computable cross-section — and no owner that would accept
them. R61 reported that honestly as `FORWARD_SIGNAL_NO_CANONICAL_REGISTRAR` with
a durable OPEN intent, which is the right behaviour for a gap and the wrong
resting state for an estate.

The four, with the identities the immutable R58 artifact recorded:

| Challenger | Record hash | Role |
|---|---|---|
| `R58_SHORT_VOLUME_PRESSURE_V1` | `332fe398…2d3e` | CANDIDATE |
| `R58_DISCLOSURE_INTENSITY_V1` | `8699962b…63bf` | CANDIDATE |
| `R58_FUND_MOMENTUM_VETO_V1` | `4196a017…c083` | CANDIDATE |
| `R58_FCF_PURE_V1` | `e9ac8289…a31e` | CONTROL |

Inception `2026-09-03`, horizon 21 sessions, cadence 21, long-only equal-weight
top-50 on the R57-floor eligible PIT S&P 500 universe, 12.5 bps per side charged
symmetrically to strategy and benchmark.

The fifth freeze, `R59_CALENDAR_TERM_STRUCTURE_F9BE2426`, is **WITHDRAWN at
inception** with zero valid forward observations. It is never registered, never
adopted and never revived.

---

## 2. The canonical new owner

`api/forward_challenger_registry.py` is THE signal-challenger forward registrar:
the one place a qualified freeze becomes a registered prospective challenger, and
the one owner of the observation clock that registration starts.

    qualified ACTIVE freeze
      -> exact immutable freeze identity
      -> canonical prospective registration      (THIS MODULE)
      -> prospective predictions                 (the accrual owner it names)
      -> canonical maturation                    (alpha_agent.r52.runtime)
      -> TRUE_FORWARD evidence
      -> human-gated review

**It is a generalisation, not a third parallel owner.**
`api/prospective_adoption.py` routes every class through ONE `REGISTRARS` table:

```
FORWARD_PAPER_PORTFOLIO   -> api.shadow_portfolio_evidence     (R56 cohort, by reference)
FORWARD_SIGNAL_R46        -> alpha_agent.r46.registry          (R46 cohort, by reference)
FORWARD_SIGNAL_CANONICAL  -> api.forward_challenger_registry   (everything else)
```

`classify_challenger_class` sends R46 and R56 to their own cohorts and **every
other release** — R58's four, R59's, and whatever a future release freezes — to
the canonical registrar. The two cohort adapters only READ; neither
`alpha_agent/r46/registry.py` nor `api/shadow_portfolio_evidence.py` imports the
new module, and the architecture check fails the build if either ever does.

**The compatibility path.** An R46 challenger adopted through this path resolves
to the R46 board and is reported `already_present`; an R56 challenger resolves to
the shadow records the same way. Nothing is copied, moved or re-hashed. One
lifecycle view, three stores, zero parallel business ownership.

**It is not a second evidence store.** It writes no prediction, no observation,
no outcome and no score, and computes no P&L. It records the registration and
NAMES the owner that will accrue: `engine.shadow_portfolio_evidence` — the one
pure weight-book forward accrual kernel the estate already has — matured by
`alpha_agent.r52.runtime`.

**R61's fail-closed path is retained.** `CLASS_SIGNAL_UNREGISTERED`,
`NO_CANONICAL_REGISTRAR` and the durable resumable OPEN intent all still exist
and are still reachable. If a class is ever mapped to no registrar again,
recording a named gap instead of inventing an owner is the machinery that kept
five orphans from being invisible, and deleting it would be deleting the lesson.

---

## 3. Registration: what is guaranteed

| Property | How |
|---|---|
| exact-freeze-bound | the record carries `freeze_id`, `freeze_record_hash`, `model_spec_hash`, `feature_snapshot_hash` and the identity hash that covers all of them |
| idempotent | the adoption identity hash IS the store key; FIRST WRITE WINS and an existing record is returned untouched |
| crash-safe | one atomic `os.replace`; a crash leaves either nothing or a complete record |
| retry-safe | re-running the governed operation after a partial run resolves to the SAME registration, and the adoption intent commits against it |
| model-spec-bound | the specification hash is part of the identity, so a retuned spec is a DIFFERENT challenger, never an edit |
| horizon-aware | `horizon_sessions` drives the maturity computation |
| asset-class-aware | the observation calendar is resolved per asset class |
| instrument-scope-aware | `instrument_scope` is a LIST, and two asset classes over the same symbol never collide on one identity |

**It refuses independently of its caller.** A freeze whose lifecycle is not
`ACTIVE` is refused, and a registration with **no established lifecycle verdict**
is refused rather than assumed — a registrar that assumed ACTIVE could resurrect
a withdrawn challenger. The withdrawn R59 term-structure freeze is therefore
refused **twice**, by two independent owners, before any store is touched.

---

## 4. Zero backfill

Registration starts a clock. It never manufactures a past.

- `effective_from_session` is the session registration happened in.
- `first_eligible_observation_session` is the first eligible session **strictly
  after** it, resolved by that asset's own calendar owner.
- Every counter — `predictions_emitted`, `matured_observations`,
  `pending_observations`, `effective_independent_observations` — is **0** at
  registration and is only ever advanced by the accrual owner.

The five sessions between the 2026-09-03 inception and a 2026-09-08 registration
are **not synthesised**, an observation clock earlier than the inception is
refused, and the module holds no code path that could write a historical
prediction or a matured observation. The store after a registration contains
exactly one file: the registration.

---

## 5. The multi-asset contract

There is **no `ticker`** anywhere in it. `instrument_scope` is a list of
instrument identifiers whatever they name — a symbol, a contract root, a currency
pair, a rate tenor, a volatility surface point — the asset class travels as data,
and the observation calendar is resolved per class:

| Asset class | Observation calendar owner |
|---|---|
| `US_EQUITY`, `US_ETF` | `engine.exchange_calendar` + `engine.market_session` (authoritative NYSE sessions) |
| `EQUITY_INDEX_FUTURES`, `EQUITY_INDEX` | the instrument's own realised bar calendar |
| `RATES_FUTURES`, `RATES` | the instrument's own realised bar calendar |
| `COMMODITY_FUTURES`, `COMMODITY` | the instrument's own realised bar calendar |
| `FX_FUTURES`, `FX`, `FUTURES`, `MULTI_ASSET_FUTURES` | the instrument's own realised bar calendar |
| `VOLATILITY`, `VOLATILITY_FUTURES` | the instrument's own realised bar calendar |
| `CROSS_ASSET`, `CREDIT` | the instrument's own realised bar calendar |

A class not in that table publishes **no clock** rather than a guessed one.

R62.1 activates **no** non-equity operational capital. This is research and
evidence infrastructure: registering a rates or FX challenger creates a
measurement and no multi-asset NAV, risk state or execution path.

---

## 6. The calendar / maturity fix

### Before

A live R61 read exposed `next_material_maturity = 2026-09-07` — Labor Day, a full
NYSE non-session. `alpha_agent/r46/clock.expected_maturity_date` counted **bare
weekdays**: `next_weekday` skipped Saturday and Sunday and nothing else. The
R60.1 exchange calendar existed and was authoritative, and forward maturity
scheduling had simply never been wired to it.

### After

No second calendar owner was created. `engine/exchange_calendar.py` is still THE
supplier and `engine/market_session.py` still THE interpreter.

- `expected_maturity_date(entry, horizon, non_sessions=None)` and
  `next_weekday(d, non_sessions=None)` now ACCEPT an authoritative closure set.
  Called without one they are **byte-for-byte** the frozen weekday rule, so every
  existing R46 row and every legacy caller is unchanged.
- `alpha_agent/r46/clock.exchange_non_sessions(asset_class, start, end)` is the
  one resolver: it asks `api.forward_challenger_registry.observation_calendar_for`
  whether the class even keeps exchange sessions, and only then asks the supplier
  for that range's closures. It returns `None` for every market on its own
  calendar, and for any range the supplier cannot cover authoritatively — which
  means "keep the weekday estimate", never "assume a holiday".
- `alpha_agent/r46/emit.py` supplies it per row and records
  `horizon_end_expected_calendar_owner`, so a reader never has to guess whether a
  holiday was considered.

Proof: entry `2026-09-04`, horizon 1. Weekday arithmetic → `2026-09-07`.
Calendar-aware → `2026-09-08`. A test asserts both, so the fix cannot silently
become a no-op.

Scoring is unaffected either way: `alpha_agent.r46.judge` has always counted the
instrument's own realised bars, and `horizon_end_expected` is a scheduling
estimate. What changes is that the board can no longer advertise evidence on a
session that does not exist.

---

## 7. Current runtime identity vs event-cycle runtime identity

### The confusion

A LOADED identity belongs to a PROCESS, and processes end. R61 was deployed while
a collection worker started under the previous release was still running; that
worker produced the 2026-09-08 material-event cycle and then exited, and a new
worker started on the deployed release. Both facts are true. The estate had one
word for them, so the historical cycle's older release kept being reported as the
CURRENT service's identity — "Information Collection is stale" about a worker
that was provably running exactly the deployed code.

### The split

`api/runtime_identity.py` publishes two identity KINDS:

- **`CURRENT_RUNTIME_IDENTITY`** — the release the process serving this runtime
  loaded. The ONLY input to current service health and to runtime alignment.
- **`EVENT_CYCLE_RUNTIME_IDENTITY`** — the release a COMPLETED cycle ran under.
  Immutable provenance. It may NEVER decide a current-health verdict.

`classify_event_cycle_provenance()` decides which, from persisted facts only:

- a **recorded commit** on the cycle that differs from the current runtime's —
  the strongest evidence, available for cycles written from R62.1 onward;
- otherwise the cycle's stamp preceding the current process's `started_at` — a
  process cannot have produced a record that predates its own start;
- otherwise `RUNTIME_PROVENANCE_NOT_ESTABLISHED`. It fails closed and never
  claims the current runtime.

Every verdict carries `decides_current_service_health: False`.

From R62.1 the event cycle records its own producing runtime
(`_producing_runtime_release()`, taken from the ONE identity owner's **frozen
capture** — not a fresh source read), so provenance survives the process. Cycles
written earlier simply do not carry it and are read as provenance-not-recorded.
**Nothing is backfilled.**

---

## 8. One current Information Collection state

### The divergence

`api/active_manager_state.py` took its collection facts from the
`information_collection` section of the **Release-50 decision snapshot**. That
snapshot's identity is a fingerprint of the stores that can change a DECISION, so
it does not move when a worker restarts, stops or re-captures its release. The
sticky header chip meanwhile read `/v1/operations/information-collection` live.
Two payloads answered one question and could disagree about a service that was
healthy the whole time.

### The fix

The Active Manager now reads the **same canonical current-runtime call** the
collection route makes (`api.information_collection.resolve_service_lifecycle`
over the service state as it is right now), publishes it once as
`live_information.current_collection`, and states which read produced it
(`CANONICAL_CURRENT_RUNTIME_READ` / `DECISION_SNAPSHOT_SECTION` /
`CURRENT_COLLECTION_STATE_UNAVAILABLE`). The runtime-alignment row takes its
worker facts from the same place and still delegates the verdict to
`api.runtime_identity`.

No JavaScript reconciliation was added and no real failure is hidden: an
unavailable read reports `available: false` with the reason, and the header chip
still says `COLLECTION: UNAVAILABLE` when its own endpoint genuinely did not
answer. What can no longer happen is two backend payloads disagreeing about a
healthy service.

---

## 9. The historical Sep-8 withheld event

The 18:16Z cycle that persisted `HOC_ARTIFACT_IDENTITY_MISMATCH` and
`DUPLICATE_CANDIDATE` was produced **before R61 was deployed**. R62.1 does not
rewrite it, replay it, rebind it or erase it. Its recorded facts travel through
the read model verbatim.

It is **labelled**. Because the cycle's stamp precedes the current worker's start
instant, the provenance owner classifies it `PRODUCED_BY_AN_EARLIER_RUNTIME`, the
read model publishes `is_historical_event_cycle: true` with the backend's own
sentence, and the operator surface renders a `HISTORICAL` chip beside it. The
repaired HOC chain is therefore not accused of having just reproduced the defect.

A future **natural** post-R61 material-event cycle is the legitimate production
proof of the repair. R62.1 forces no such event.

---

## 10. The forward-evidence read model

`api/alphaagent_outcomes.py` publishes the registrar's own rows —
`canonical_forward_registrations` — and the UI renders a *Registered prospective
challengers* table on the existing Research surface. Per challenger:

challenger id · freeze id · lifecycle state · asset class · instrument scope size
· horizon · canonical registrar · registration timestamp · prospective
effective-from boundary · predictions emitted · matured observations · pending
observations · effective independent observations · first eligible observation
session · next legitimate maturity session · evidence status · next legitimate
evidence gate.

Every one of those is computed by the registrar and read verbatim. **No business
logic in JavaScript**: the browser derives no maturity, no lifecycle and no
evidence verdict, and the architecture check fails the build if it starts to.

A registered freeze also stops being reported as an orphan: `_freeze_row` links
it to its registration and its `forward_evidence_link` becomes
`ACCRUING_THROUGH_FORWARD_EVIDENCE_OWNER`.

---

## 11. Safety

Registration starts a **measurement**; it never starts a position.

- **No path from TRUE_FORWARD evidence to automatic promotion.**
  `PROMOTION_READY` is deliberately not one of the registrar's evidence states —
  reaching an evidence gate is the gate owner's verdict, and a registrar that
  could award it would be a promotion path with a softer name. Every record
  carries `promotion_allowed: False`, `automatic_promotion_allowed: False` and
  `manual_review_required: True`.
- **No path from challenger registration to operational allocation.** The module
  imports no allocator, no desk and no portfolio owner; it changes no holding, no
  cash and no NAV.
- **No path from registration to an order or fill.** It contains no order,
  order-plan, execution or broker call, and the audit fails the build on any.
- Manual model review and manual portfolio review remain mandatory. Execution
  automation remains OFF.

---

## 12. What is deliberately NOT in R62.1

- **Driving the accrual.** The registrar names
  `engine.shadow_portfolio_evidence` as the accrual owner for canonical
  weight-book challengers; wiring their scheduled emission and maturation is a
  separate bounded slice. Today the four R58 registrations are correctly
  `REGISTERED_AWAITING_FIRST_ELIGIBLE_SESSION`.
- **Non-equity operational capital.** Research and evidence infrastructure only.
- **Any live action.** No registration was performed against a live store during
  this release; the four R58 adoptions are proven hermetically. Performing the
  live registration is an explicit operator act.

---

## 13. Files changed

| File | Change |
|---|---|
| `api/forward_challenger_registry.py` | **NEW** — the canonical registrar, observation clock and read model |
| `api/prospective_adoption.py` | third canonical class + routing + registry override seam |
| `api/runtime_identity.py` | the two identity kinds + `classify_event_cycle_provenance` |
| `api/active_manager_state.py` | ONE current collection state; alignment reads the current runtime; event-cycle provenance |
| `api/event_signal_refresh.py` | the cycle records its producing runtime (frozen capture) |
| `api/alphaagent_outcomes.py` | publishes the registrations and links each freeze to its own |
| `alpha_agent/r46/clock.py` | calendar-aware maturity estimate + the one non-session resolver |
| `alpha_agent/r46/emit.py` | supplies the authoritative calendar and names it on the row |
| `alpha_agent/r46/harvest.py` | the estimate note now states which calendar applies |
| `api/ui/index.html` | registrations table; one current collection state; HISTORICAL cycle chip |
| `scripts/audit_architecture.py` | `check_release62_1_canonical_forward_evidence` + 31 blocking invariants |
| `docs/architecture/system_inventory.json` | the new owner, and the R62.1 note on the adoption owner |
| `tests/test_release62_1_canonical_forward_evidence.py` | **NEW** — 65 tests |
| `tests/test_release61_decision_research_continuity.py` | two assertions updated to the new canonical fact |
