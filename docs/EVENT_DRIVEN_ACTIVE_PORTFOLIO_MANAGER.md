# Event-Driven Active Portfolio Manager & Live Signal Fabric (Release 28)

> **Status:** LANDED. Operating model unchanged (`fundamental_momentum_50_50_v1`).
> Frozen research challenger unchanged (`s25_operating_profitability`, spec hash
> `67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e`, 0 forward marks).
> No scheduler is armed. No order exists anywhere in this path.

---

## 1. What changed, in one sentence

Before Release 28 the Paper Trader recomputed the same unchanged information on a
timer and reached the same answer. It now reacts when information **actually
arrives** — and, just as importantly, provably does *not* react when it has not.

The complete path is:

```
NEW INFORMATION
  -> POINT-IN-TIME INGESTION        (api.event_fabric, two lanes)
  -> NORMALIZED EVENT               (engine.event_fabric, one contract)
  -> SOURCE / FRESHNESS / QUALITY   (api.event_fabric + api.data_freshness)
  -> SIGNAL AUTHORITY               (engine.event_fabric, one table)
  -> DEPENDENCY UPDATE              (engine.event_fabric, one graph)
  -> SIGNAL / RISK DELTA            (api.price_panel + api.universe_scoring)
  -> MATERIALITY                    (engine.event_materiality, versioned policy)
  -> HOLDING OPPORTUNITY COST       (api.holding_opportunity_cost — EXISTING owner)
  -> PORTFOLIO REASSESSMENT         (api.portfolio_reassessment — EXISTING owner)
  -> TARGET PORTFOLIO / NO CHANGE   (api.reallocation_proposal — EXISTING owner)
  -> EXPLANATION
  -> MANUAL REVIEW
```

Every step after `MATERIALITY` is an **existing** canonical owner. This release added
an information lane, not a second portfolio brain.

---

## 2. The three signal speeds

The system distinguishes three economically different kinds of arriving information.
The distinction is not decorative: it is what stops a 15-minute quote from being
treated like a new 10-Q.

| Speed | Question it answers | Examples | May move a SCORE? |
|---|---|---|---|
| **STRUCTURAL** | What do we fundamentally want to own? | 10-Q / 10-K, filed accounting facts, index membership | Yes, for periodic reports and filed facts — through the canonical scoring owner, at the model's own formation cadence |
| **TACTICAL** | Has something happened that could change the thesis? | 8-K, earnings release, guidance, Form 4, company news, regulator action | **Never.** It can put a holding on the review list |
| **MARKET_RISK** | Even if the thesis is unchanged, is the amount of capital still right? | EOD bars, delayed intraday quotes, halts, volatility/credit regime series | It may move valuation and risk. **Never** a score |

---

## 3. Signal authority — the safety boundary

`engine/event_fabric.py` holds **one** table mapping every event family to a decision
authority, and every family states *why* it has the authority it has.

| Authority | May change score | May change risk | May trigger reassessment | Reaches operational target |
|---|---|---|---|---|
| `OPERATIONAL_ALPHA` | ✔ | ✔ | ✔ | ✔ |
| `OPERATIONAL_RISK` | ✘ | ✔ | ✔ | ✔ |
| `EVENT_TRIGGER_ONLY` | ✘ | ✘ | ✔ | ✔ (attention only) |
| `RESEARCH_ALPHA` | ✘ | ✘ | ✘ | ✘ |
| `OBSERVABILITY_ONLY` | ✘ | ✘ | ✘ | ✘ |
| `BLOCKED` | ✘ | ✘ | ✘ | ✘ |

**The rule that matters:** an unvalidated news headline, 8-K, earnings release or Form
4 is `EVENT_TRIGGER_ONLY`. It can make the manager *look again* at a holding. It can
never add or subtract expected return. `positive headline = buy` is not implemented
and is not permitted, because no historically validated news signal exists.

Classification **fails closed**: a record type the table does not know receives
`OBSERVABILITY_ONLY` and is counted in `UNCLASSIFIED_SIGNAL_AUTHORITY`, which must be
zero to release. A new feed can never acquire decision power by default.

### The 21 event families

Structural: `structural_financial_report` (alpha), `fundamental_fact` (alpha),
`universe_membership` (risk/eligibility), `security_identity` (observability),
`corporate_action` (risk).
Tactical: `material_corporate_event`, `insider_transaction`, `earnings_result`,
`guidance_change`, `company_news`, `company_press_release`, `regulatory_event` (all
trigger-only), `other_filing` (observability).
Market/risk: `market_bar`, `market_quote`, `trading_halt` (risk),
`macro_regime_release` (risk), `macro_context_release` (observability),
`short_volume` (observability).
Research/blocked: `analyst_estimate_snapshot` (research), `analyst_revision_as_was`
(blocked).

---

## 4. Live source capability — what is actually connected today

`api/source_capability.py` is the ONE machine-readable answer. Every source ends in a
**terminal** state. There is deliberately no `AVAILABLE_BUT_NOT_INTEGRATED`.

### Integrated (12)

| Source | Lane | Frequency | Latency | Authority | Terminal state |
|---|---|---|---|---|---|
| `norgate_local` | corpus | daily EOD | same evening | risk | `INTEGRATED_OPERATIONAL` |
| `eodhd` | corpus | daily; news intraday | minutes–hours | alpha (fundamentals) + trigger (news/earnings/insider) | `INTEGRATED_OPERATIONAL` |
| `sec_edgar` | corpus | continuous | minutes after acceptance | alpha (10-Q/10-K) + trigger (8-K/Form 4) | `INTEGRATED_OPERATIONAL` |
| `nasdaq_trader` | corpus | continuous in session | near real time | risk (halts) | `INTEGRATED_OPERATIONAL` |
| `fred_alfred` | corpus | daily market series | same day | risk (regime series only) | `INTEGRATED_OPERATIONAL` |
| `corporate_actions_registry` | internal | event-driven | operator-driven | risk | `INTEGRATED_OPERATIONAL` |
| **`yahoo_delayed_quote`** | **live adapter** | **intraday, ~15 min delayed** | **~15 min** | **risk only** | `INTEGRATED_OPERATIONAL` |
| **`gdelt`** | **live adapter** | **continuous, 15-min cycle** | **15–60 min** | **trigger only** | `INTEGRATED_TRIGGER_ONLY` |
| `news_rss` | corpus | continuous | minutes | trigger only | `INTEGRATED_TRIGGER_ONLY` |
| `eodhd_analyst` | corpus | daily snapshot | one day | research only | `INTEGRATED_RESEARCH_ONLY` |
| `finra` | corpus | daily | next business day | observability | `INTEGRATED_RESEARCH_ONLY` |
| `us_treasury` | corpus | monthly | weeks | observability | `INTEGRATED_RESEARCH_ONLY` |

### Terminally blocked (2)

* `analyst_revision_vendor` — `BLOCKED_ENTITLEMENT`. No as-was revision vintage exists
  in any approved local root. Intrinio's live trial returned `NO_DEFENSIBLE_ALPHA`
  (`DO_NOT_BUY`); Zacks via Nasdaq requires contacting sales; FMP grades failed the
  survivorship check. **The `ANALYST_REVISION` adapter contract exists in the family
  table**, so adding the data later needs no new portfolio architecture.
* `options_iv` — `BLOCKED_ENTITLEMENT`. No owned or free source provides IV/skew history.

### Redundant or not economically useful (3)

* `bls` — `REDUNDANT_WITH_EXISTING_SOURCE`. CPI and unemployment are already collected
  from FRED **with ALFRED vintages**, which BLS v2 does not provide. Registering a BLS
  key would add a second copy of the same numbers with *worse* point-in-time quality.
* `bea` — `REDUNDANT_WITH_EXISTING_SOURCE`. Quarterly national accounts arrive far
  slower than any reassessment cadence and restate without vintages.
* `prediction_service` — `NOT_ECONOMICALLY_USEFUL` **for the event fabric**. It emits
  model output, not new information: its inputs are the same owned price and
  fundamental data the fabric already ingests, and the released operational model does
  not consume it. Treating its output as an arriving event would double-count.

**Terminal audit (measured):** `READY_UNINTEGRATED_USEFUL_SOURCES = 0`,
`UNCLASSIFIED_SIGNAL_AUTHORITY = 0`, `NON_TERMINAL_SOURCE_STATES = 0`,
`DUPLICATE_DECISION_OWNERS = 0`, `DUPLICATE_EVENT_ORCHESTRATORS = 0`.

---

## 5. The normalized event contract

Owner: `engine/event_fabric.py`. Contract id `paper_trader.normalized_event/1`.

Every event carries: immutable `event_id`, `idempotency_key`, source and collector id,
native source id, record type, event type/sub-type, family, signal speed, decision
authority + policy version + *why*, six distinct timestamps (source, published,
accepted, effective, first observed, ingested), resolved entities and identity
confidence, source and event quality, materiality inputs, novelty and its lineage
(`duplicate_of` / `supersedes`), payload fingerprint and reference, extractor version,
point-in-time status, and quality warnings.

**Idempotency.** `sha256(source_id | record_type | source_event_id |
payload_fingerprint)` — deliberately excluding ingestion and observation time, so the
same raw event retrieved twice is one event. Measured against the real corpus:
re-ingesting 12,508 records admitted **0** new events and suppressed **12,508**
duplicates.

**Immutability.** Events are appended to date-partitioned JSONL and never rewritten. A
correction or material update is a **new** event that `supersedes` the earlier one; the
earlier event's payload, timestamps and authority are untouched.

**Point-in-time.** Timestamps are the ones the source stated. A missing publication
time stays `null` and is flagged `AVAILABILITY_TIMESTAMP_UNKNOWN` — never fabricated,
never back-filled from a period end. Of 11,586 events admitted from owned data, 4,453
carry a full authoritative publication/acceptance timestamp and 7,133 honestly report
that their source stated none.

---

## 6. Deduplication and novelty

Precedence, most specific first:

1. identical payload already ingested → `DUPLICATE` (a no-op; not written)
2. same **canonical document** already ingested → `SYNDICATED`
3. same native id for the **same date**, changed payload → `MATERIAL_UPDATE` / `CORRECTION`
4. same story **content** from anywhere → `SYNDICATED` / `FOLLOW_UP` / `RETRACTION`
5. otherwise → `NEW`

Two rules here came directly from measuring the real corpus:

* **Rule 2 exists** because collectors legitimately re-collect one document under
  several scopes — a wire article about five holdings is fetched once per symbol, and
  one SEC accession is seen by both the daily-index and submissions lanes. Without it,
  each copy looked like a "correction" of the last.
* **Rule 3 includes the date** because several collectors reuse a native id across days
  (`nasdaqlisted|ABNB` every session; a daily bar repeats its ticker). Keying
  supersession on the native id alone declared 25 genuinely distinct daily observations
  to be 24 corrections of the first.

Only `NEW`, `CORRECTION`, `MATERIAL_UPDATE` and `RETRACTION` carry new information into
the decision path.

---

## 7. Source freshness — cadence aware

Owner: `api/event_fabric.build_source_freshness`. The **status vocabulary and
classifier are delegated** to `api.data_freshness.classify_source` — the canonical
cadence-aware owner. This release added a watermark model, not a second freshness
model.

Each source carries: watermark, expected cadence, status, lag in sessions and calendar
days, last attempt, last success, event and duplicate counters, last error, circuit
state, connection status, terminal state and processing lag.

A quarterly filing lane is **not** "broken" for publishing nothing today, and a
degraded source is reported degraded rather than back-filled with a fabricated value.
Publisher-driven feeds carry an explicit tolerance so a quiet day is `NOT_DUE`, not
`STALE`.

---

## 8. The dependency graph

`event -> business concepts -> signals -> calculations`, with an execution order so a
downstream calculation never runs before its inputs.

| Arriving information | Concepts invalidated | Calculations refreshed |
|---|---|---|
| EOD bar / delayed quote | mark, return, volatility, liquidity, drawdown, risk contribution, concentration | `MARKET_RISK_STATE`, `PORTFOLIO_VALUATION` |
| 10-Q / 10-K / filed fact | fundamental input, structural alpha | `UNIVERSE_SCORING`, `HOLDING_OPPORTUNITY_COST`, `PORTFOLIO_REASSESSMENT` |
| 8-K / news / earnings / Form 4 | thesis review | `HOLDING_OPPORTUNITY_COST`, `PORTFOLIO_REASSESSMENT` — **no scoring** |
| Corporate action | corporate action, mark | `PORTFOLIO_VALUATION`, `HOLDING_OPPORTUNITY_COST` |
| Regime macro series | regime | `MARKET_RISK_STATE` |
| CPI / payrolls / GDP | *(none)* | *(none)* |
| Analyst snapshot | research challenger | `RESEARCH_EVIDENCE` only |

---

## 9. Materiality and anti-churn

Owner: `engine/event_materiality.py`, policy version `event_materiality.v1`, versioned
**separately** from every alpha-model calibration.

It separates four statements and owns the first three:

```
DATA_CHANGED  ->  SIGNAL_CHANGED  ->  MATERIAL_SIGNAL_CHANGED  ->  PORTFOLIO_DECISION_CHANGED
                                                                    ^ owned by the
                                                                      reassessment /
                                                                      proposal owners
```

Thresholds (each with a stated reason, none fitted to any outcome): 7% one-day move,
15% five-day move, 20% drawdown, 1.75× volatility ratio, $2m liquidity floor, 25-place
rank deterioration, 0.10 score change, 25-place alternative advantage, top-100
candidate depth.

**Anti-churn is structural, not a threshold.** Triggers are collapsed by
`(code, entity, family, event_date)`: twenty re-collected copies of one filing become
one reason carrying `occurrences: 20`. The trigger fingerprint is keyed on *what was
concluded about which security on which day* — never on an individual event id — so
re-collection cannot manufacture a "new" fingerprint. An identical fingerprint against
an identical portfolio state suppresses the reassessment instead of duplicating it, and
tomorrow's genuinely new 8-K on the same name still produces a new fingerprint.

**Triggering is not acting.** Every trigger buys one read-only assessment. Whether a
change survives transaction costs remains entirely with `api.portfolio_reassessment`
and `api.reallocation_proposal`.

---

## 10. Daily mode and event mode are the same system

| | Daily Research Cycle | Event signal refresh |
|---|---|---|
| Owner | `api/daily_research_cycle.py` | `api/event_signal_refresh.py` |
| Scope | **FULL** dependency refresh | **INCREMENTAL** dependency refresh |
| Opportunity cost | `api.holding_opportunity_cost.run_and_persist` | same |
| Reassessment | `api.portfolio_reassessment.run_and_persist` | same |
| Proposal | `api.reallocation_proposal.run_and_persist` | same |
| Proposal gate | `portfolio_reassessment.should_build_proposal` | same |
| Terminates in | manual review | manual review |

The shared owners are declared in
`api.event_signal_refresh.CANONICAL_CALCULATION_DELEGATES` and asserted by test. The
event module is checked by test to contain no `compute_scores`, `compute_combined`,
`build_books`, `build_target`, `place_order` or `create_order` definition.

---

## 11. The event cycle

`POST /v1/operations/event-signal-refresh/run` with
`{"confirmation": "CONFIRM_EVENT_SIGNAL_REFRESH"}`. Optional booleans
`include_market_quotes` and `include_gdelt` opt in to the two live adapters; both are
**off by default** because each makes a bounded outbound request, and neither is ever
called by a GET.

Steps, each timed: `LOAD_PORTFOLIO_CONTEXT`, `RESOLVE_SOURCES_DUE`,
`INGEST_SINCE_WATERMARK`, `DEDUPLICATE_AND_PERSIST`, `RESOLVE_DEPENDENCIES`,
`REFRESH_AFFECTED_INPUTS`, `ADVANCE_WATERMARKS`, `MEASURE_DELTAS`, `MATERIALITY_GATE`,
then — only when material — `HOLDING_OPPORTUNITY_COST`, `PORTFOLIO_REASSESSMENT`,
`REALLOCATION_PROPOSAL`.

Terminal states: `NO_NEW_INFORMATION`, `INFORMATION_NOT_MATERIAL`,
`DUPLICATE_TRIGGER_SUPPRESSED`, `REASSESSED_NO_CHANGE`,
`PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW`, `BLOCKED`, `NOT_RUN`.

`GET /v1/operations/event-signal-refresh` is the read-only operator surface: contract,
dependency graph, capability matrix, terminal audit, per-source freshness, recent
events with family/authority/novelty, material events, affected holdings, and the last
cycle's state and reason.

---

## 12. Latency model

Measured, never modelled. Per source: median / min / max source-to-ingest seconds, over
events whose source stated a publication time. Events without one are counted as
`unmeasurable_events` rather than given an invented timestamp. Per step: duration.
End-to-end: oldest event publication → reassessment persisted.

The binding latency today is **collection cadence, not processing**: the incremental
cycle completes in well under a second over an already-collected window, while the
Stage-2 collectors run on operator-initiated batches.

---

## 13. Scheduling state

**No scheduler is armed by this release.** No OS task, no cron, no cadence flag is
enabled. `scheduler.armed` is `false` in the read contract and is asserted by test.

The cycle is directly callable and idempotent precisely so that a future scheduled
caller invokes this same owner without a redesign. Activating one is a separate,
deliberate operator decision.

---

## 14. Challenger forward continuity

| Property | Value |
|---|---|
| Candidate | `s25_operating_profitability` |
| Freeze-contract spec hash | `67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e` |
| Registry spec hash | `e49053360625a749` |
| Shadow book | `sb_c9_qualityprofi_e490533606`, inception 2026-08-16, $100k, SPY, 50 bps |
| Forward marks observed | **0** — `FORWARD_TIME_NO_MARKS_YET` |

Release 28 performed **no** refit, reset, backfill, retroactive mark or promotion. The
event fabric is *capable* of supplying future canonical formation inputs and is
*forbidden* by the authority table from touching the operational target with them:
`RESEARCH_ALPHA` cannot trigger a reassessment and cannot reach the operational target.

---

## 15. Replay acceptance

`api/event_replay.py` composes a synthetic world and drives the **real** orchestrator
and the **real** opportunity-cost / reassessment / proposal owners over temporary
roots. All ten required scenarios pass (77 checks):

| | Scenario | Outcome |
|---|---|---|
| A | Nothing changed | second cycle admits 0 events, suppresses 1 duplicate, `NO_NEW_INFORMATION` |
| B | Material market move | price-shock trigger with risk authority; reassessment ran; reason recorded |
| C | New material 8-K | trigger-only; acceptance timestamp preserved; scoring **not** refreshed |
| D | New 10-Q | structural alpha invalidated; scoring + opportunity cost refreshed |
| E | Material news | `NEW` novelty, mapped ticker, publication time preserved, trigger-only |
| F | Same story, five wires | 1 informative event, 4 linked as syndicated, 1 trigger |
| G | Macro event | CPI observability-only; regime **transition** triggers, observation does not |
| H | Stale / failed source | degraded reported, error surfaced, no fabricated watermark |
| I | Research challenger | cannot trigger, cannot reach the target, no promotion |
| J | Alternative improvement | complete target portfolio: 23 RETAIN / 2 EXIT / 2 ADD, 8% turnover, net +0.0675 score points, all constraints hold, **manual review** |

---

## 16. Operator surface

One read-only card in **Today**: *New Information & Reassessment*. It shows the last
cycle state, event and material counts, source-fresh counts, latest material events
with an explicit decision-authority badge (`TRIGGER ONLY` for news), cadence-aware
source freshness, affected holdings, and the terminal-audit counters.

It has **no run control**: the operator's single primary action remains the canonical
daily-cycle action owned by `workflow_state`, and the single-primary-mutation invariant
is untouched. All values are rendered verbatim from the backend; the region derives no
date, freshness, materiality, authority or ranking.

---

## 17. What can cause action today

* An owned EOD bar moving a holding ≥7% in a session, ≥15% in a week, past a 20%
  drawdown, past 1.75× its own volatility, or below the liquidity floor.
* A delayed intraday quote doing the same (risk only — never a score).
* A new 10-Q / 10-K or filed accounting fact for a held or top-100 name.
* An 8-K, earnings release, guidance change, Form 4, company news story, regulator
  action or GDELT story naming a held or top-100 name — as a **review trigger**.
* A trading halt in a holding.
* A corporate action outstanding against a holding.
* A market-regime **state transition** (volatility / credit / financial conditions /
  curve).
* A 25-place ranking deterioration, a 0.10 score move, or an alternative ranking 25+
  places better than a holding.

## 18. What still cannot

* Analyst revisions — no as-was vintage exists (blocked on data, not architecture).
* Options-implied information — no owned or free source.
* Any *scored* use of news, 8-K, earnings, guidance, insider or macro information —
  no validated signal exists, and the authority table forbids it.
* Sub-15-minute market reaction — the fastest legitimately available market data under
  current entitlements is a ~15-minute delayed quote.
* Unattended operation — no scheduler is armed.

---

## 19. Exact next major constraint

**Collection cadence, not processing.** The incremental cycle is fast and correct, but
the Stage-2 / Stage-3.5 collectors that feed the corpus lane run as operator-initiated
batches, so most source watermarks sit days behind the eligible session. The event lane
cannot react to an 8-K that has not been collected.

The next major product move is therefore to make **collection itself continuous and
governed** — a bounded, observable, restart-safe collection service that advances each
source's watermark on that source's own cadence and feeds this fabric — rather than any
further work on the decision path, which is complete.

---

## 20. Files, stores and evidence

**Repository**

| Path | Role |
|---|---|
| `engine/event_fabric.py` | PURE kernel: event contract, authority table, novelty, dependency graph |
| `engine/event_materiality.py` | PURE kernel: materiality / anti-churn policy |
| `api/source_capability.py` | Canonical source capability matrix + terminal audit |
| `api/event_fabric.py` | Immutable event store, two ingestion lanes, entity resolution, watermarks, freshness |
| `api/event_signal_refresh.py` | THE event orchestration path + latency observability |
| `api/event_replay.py` | Hermetic replay harness (scenarios A–J) |
| `scripts/run_release28_event_artifacts.py` | Machine-readable evidence emitter |
| `tests/test_release28_event_driven_manager.py` | 76 targeted hermetic tests |

**Stores**

| Path | Contents |
|---|---|
| `PAPER_TRADER_EVENT_FABRIC_DIR` (default `D:\Stock_Prediction_app_data\event_fabric`) | `events/` append-only JSONL, `state/` dedup index + watermarks + anti-churn fingerprint, `runs/` cycle status, `latest.json` |
| `…\event_fabric\release28\` | The 15 release artifacts |

**Related canonical documents:** `PROJECT_CHARTER.md`, `CURRENT_ARCHITECTURE.md`,
`TARGET_ARCHITECTURE.md`, `ARCHITECTURE_DECISIONS.md`, `CONSOLIDATION_ROADMAP.md`.

Prior releases this one builds on: Stage 22 (normal daily cycle), Stage 23–25 (alpha
research and discovery), Stage 26 (challenger launch), Release 27 (free alpha frontier
exhaustion — the reason no new scoring authority was granted here).
