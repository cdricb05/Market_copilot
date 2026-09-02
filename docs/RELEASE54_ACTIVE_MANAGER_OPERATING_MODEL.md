# Release 54 — Active Manager Operating Model Consolidation (Slice 1)

Built over commit `9a73b73` on `stage19-controlled-rebalance`, immediately after
R53.1. Single agent, Windows PowerShell only, no subagents. Paper-only,
preview-first, manual-review and no-automation boundaries unchanged.

## 1. What the operator was seeing before R54

Every fact had a canonical owner, but no single response answered the
active-manager questions. The operator read COLLECTION RUNNING on one surface,
material information on another, holdings/NAV on a third, the zero-base target
and switching economics on a fourth, and research health on a fifth — and had
to infer, across screens, *what just happened, whether it caused a signal
refresh, whether the portfolio was reassessed, why, and whether action is
needed*. Even on Today, the operational mark, the eligible session, the
collection chip and the decision band were mirrors of four different owners
with no explicit statement of which clock each belongs to.

## 2. The traced end-to-end chain (Phase A, from code)

```
PaperTrader-InformationCollection (S4U task, boot + 30-min recovery)
  -> scripts/run_information_collection_service.py (worker; single-flight lock)
  -> api.information_collection  (ONE collection orchestrator; cadence via
     engine.collection_cadence; bounded 6 sources/iteration)
       collectors: Stage-2 ingestion lanes, news RSS, delayed-quote + GDELT
       live adapters (api.event_fabric)
  -> IF new records / live adapter due / bootstrap:
     api.event_signal_refresh.run_event_signal_refresh   (Release 28 — the ONE
     incremental event cycle; ONE clock per cycle; token-gated)
       1  portfolio context            api.portfolio_state
       2  sources due                  api.source_capability + watermarks
       3-5 ingest, dedup, persist      api.event_fabric (immutable evidence)
       6  affected securities/calcs    engine.event_fabric
       7  refresh ONLY affected inputs api.universe_scoring / api.price_panel
       8  advance watermarks           api.event_fabric
       9  measure deltas               risk state + rank deltas vs PERSISTED prior
       10 MATERIALITY GATE             engine.event_materiality (v2; observations
                                       are never material on arrival; anti-churn
                                       trigger fingerprint; conservative
                                       face-material thresholds)
       11 IF material: HOC             api.holding_opportunity_cost
       12 reassessment                 api.portfolio_reassessment (Stage 20)
       13 IF should_build_proposal:    api.reallocation_proposal (Slice 7 kernel
                                       + engine.constrained_reallocation R47)
       -> terminal state persisted (runs/<id> + latest.json) -> MANUAL REVIEW
GOVERNED PATH (operator-run): api.portfolio_cycle (R48, ONE dispatcher)
  -> api.daily_close -> api.daily_research_cycle (FULL refresh; SAME four
     owners, same order) -> governed decision (Class 2 GOVERNED_DRC_TERMINAL)
READ SIDE: api.workflow_state (ONE combined interpretation)
  -> api.decision_snapshot (R50, ONE composition per identity)
  -> api.operator_presentation (R49, Today's reconciled decision surface)
  -> R54: api.active_manager_state (THE operating-state projection) -> Today
```

Phase-A answers (evidence, not assumption):

1. **A material observation today**: the collection worker's next iteration
   ingests it, persists immutable evidence, refreshes only the affected
   calculations and — through the materiality gate — runs HOC → reassessment →
   (gated) proposal in the SAME pass. This is live in production while the
   collection service runs.
2. **Materiality** is owned by `engine.event_materiality` (v2): a bar/quote is
   an observation and is judged by the move it measures against declared risk
   thresholds; face-material triggers (7% shock, 20% drawdown, halt, rank
   deterioration, alternative improvement, regime transition, outstanding CA);
   idempotent by trigger fingerprint.
3. **Materiality → signal refresh**: yes — within the same event cycle
   (step 7 refreshes only invalidated inputs).
4. **Refresh → scoring**: yes — `api.universe_scoring` (mtime-cached kernel
   build) inside the same cycle when scoring was invalidated.
5. **Scoring → reassessment**: yes, gated by materiality, in the same cycle.
6. **Reassessment cadence**: EVENT-DRIVEN inside the collection iteration
   (bounded by detection cadence, ~15-min quote polls) plus the DRC step in the
   operator-run portfolio cycle. There is deliberately NO separate scheduler
   (`cadence_enabled: False` is a declared audit policy) — that activation is
   R54.1.
7. **HOC producer**: `api.holding_opportunity_cost`, invoked by BOTH paths.
8. **Proposal producer**: `api.reallocation_proposal.run_proposal` (kernel
   `engine.reallocation_proposal` + R47 `engine.constrained_reallocation`),
   gated by `api.portfolio_reassessment.should_build_proposal`.
9. **Today reads**: `api.operator_presentation` through the R50 decision
   snapshot — and now also `api.active_manager_state` (this slice).
10. **Where the chain was only nominally linked**: the event path's artifacts
    are Class 1 (LIVE_PRE_DRC_SIGNAL) and never advance the governed workflow —
    correct by design, but previously invisible on Today; the R53.1 intraday
    emission lane has NO API read surface; and no response combined the
    operational and live clocks.
11. **Available but not activated**: near-real-time reassessment SLA (the
    decision chain is ~7.3 s median; NRT event-to-decision ~45 s measured in
    R53.1); a direct scheduler for the event cycle (`scheduler.armed: False`
    in the read contract, by design).
12. **Observation→decision latency**: measured per cycle in the event payload
    (`latency` block: per-source ingest lag, per-step timings,
    `oldest_event_to_reassessment_seconds`). Detection cadence — not the
    engine — is the bottleneck.

## 3. The Active Manager Operating State (Phase B)

`api/active_manager_state.py` — READ-ONLY composition/projection; endpoint
`GET /v1/operations/active-manager-state`. It is NOT a second calculation
engine: it may only copy owner fields verbatim, select among owner-stamped
timestamps, count owner-bounded rows against an owner-published stamp, and
quote each owner's own not-ready vocabulary. Blocks:

| Block | Content | Source owner |
|---|---|---|
| `operational_book` | eligible session, mark, NAV, cash, holdings, hashes, pending orders, consistency | `api.portfolio_state` (+ workflow operational_state) |
| `live_information` | collection lifecycle, last event cycle, last observation, last material event, material-since-reassessment, affected holdings | `api.information_collection` + `api.event_signal_refresh` |
| `signal_state` | last event refresh, daily price/score date, latest TRUE_FORWARD, scoring identity + counts, freshness words | esr + `api.universe_scoring` + workflow research/evidence state |
| `portfolio_reassessment` | state, when, trigger provenance (artifact class), holdings/alternatives evaluated, decision + economics, HOC summary, best replacement, currency | `api.portfolio_reassessment` + workflow lanes |
| `target_proposal` | R47 outcome, feasibility, switching economics, hurdle, turnover, cost, risk, approval state, proposal hash, decision provenance | `api.reallocation_proposal` (R47 read) + workflow |
| `research_governance` | champion/challengers, model review, forward evidence, R52 runtime health, `automatic_promotion_allowed: False` | workflow + `api.universe_scoring` + `api.research_runtime` |
| `execution_safety` | manual review/approval, order-plan state, execution availability, `automation_enabled/broker_enabled: False` | R47 approval/execution + `api.rebalance_execution` + workflow |
| `operator_guidance` | overall state, current task, ONE next action, attention, blockers, warnings — VERBATIM from the workflow owner | `api.workflow_state` |
| `time_state` | the explicit OPERATIONAL vs LIVE/INTRADAY distinction; `operational_mark_advanced_only_by: api.daily_close` | composed |
| `stale_components` | every missing/stale component in its owner's own vocabulary | composed |

Decision-side sections are served through the ONE Release-50 decision snapshot
(one composition per identity); live research reads run fresh because the event
fabric is deliberately outside the snapshot identity.

## 4. Today (Phase C)

One bounded addition, no visual redesign: the **Active Manager operating-state
strip** (`#today-operating-state`, `data-owner="api.active_manager_state"`),
rendered by exactly ONE new loader (`loadActiveManagerState`) inside the marked
`R54_REGION`. Three labeled columns — OPERATIONAL BOOK · last closed session /
LIVE · INTRADAY RESEARCH STATE / LAST PORTFOLIO REASSESSMENT — plus the
workflow owner's own next step and the stale-component list. The region
performs no date arithmetic, no freshness verdict and no decision math (audit-
scanned). The four R49 sections and the ONE canonical dispatcher are untouched.

## 5. Ownership classification (Phase D)

The machine-readable register lives in
`docs/architecture/system_inventory.json` → `r54_ownership_classification`
(plus a new `canonical_concepts` entry and module/route registrations).
Headline: **no TRUE_DUPLICATE_OWNER remains live in the operational decision
path** — the strict audit's multi-writer candidates classify as
RESEARCH_ONLY_CALCULATION (alpha_agent lanes), READ_COMPOSER (route/read
compositions), COMPATIBILITY_WRAPPER (delegating date/close policies),
PROJECTION (CA previews, audit pattern constants) and LEGACY_DEPRECATED (the
equity-era cockpit family: `portfolio_valuation`, `current_operating_state`,
`daily_operating_run`, `command_center`, `daily_workflow_dashboard`, the
`/v1/review/current-workflow-state` stage machine).

## 6. The one real consolidation beyond composition (Phase E)

**The Today operational-mark pill (`cc-status-mark`) had two writers.** The
canonical writer is `renderPortfolioState` (`_psOwnSet`, owner
`api.portfolio_state`; the node is registered in `PS_CANONICAL_NODES` and
`_obSet` hard-refuses it). But the LEGACY command-center renderer also wrote it
through the guard-free `_ccSetText`, with a fallback to the legacy DB
portfolio's `as_of_market_date` — so a late legacy response could overwrite the
operational mark with the dormant legacy book's date (last-writer-wins, the
exact time-state conflation R54 exists to prevent). The legacy write is
removed; the pill now has exactly ONE unguarded writer; the strict audit
(`check_release54_active_manager_state`) and
`tests/test_release54_active_manager_state.py` fail the build if the legacy
writer returns. Chosen because it was the only *live conflicting-writer* found
in the review (selection priority 2/4), it is bounded, and a trustworthy
operational-vs-live header is a precondition for near-real-time reassessment
surfaces.

## 7. R54.1 — the event-driven activation contract (Phase F)

> **STATUS: DELIVERED by R54.1** — see
> `docs/RELEASE54_1_GOVERNED_INTRADAY_DECISION.md`. The activation landed as
> ONE governance gate inside `api.portfolio_decision` rather than as a cadence
> change: the trigger criteria, debounce, idempotency key, stale-data rules,
> point-in-time binding, turnover protection, proposal-regeneration semantics
> and the manual-review boundary below are all UNCHANGED and are now BOUND by
> the gate. Activation steps 1, 2 and 4 (a new cadence policy, a measured
> cadence verdict, raising detection cadence) deliberately did NOT happen:
> detection is still the bottleneck, `cadence_enabled` remains a declared
> False, and raising it waits on the measured `observation_to_governed_seconds`
> series that step 3 now publishes.
>
> **R54.2 completed the precondition** — see
> `docs/RELEASE54_2_SAME_SESSION_REASSESSMENT_VERSIONING.md`. "A portfolio may
> be reassessed many times per trading day" needs the STORE to hold many
> assessments per session, and Stage-20/21 appended a version only on ECONOMIC
> change. `api.portfolio_reassessment` now versions on ASSESSMENT EVIDENCE too
> (`CREATED_ASSESSMENT_VERSION`, append-never-rewrite), the churn input no
> longer reads the session it is assessing, and the governance gate additionally
> requires that a live conclusion actually became an immutable artifact.

The contract as originally written:


Target behaviour: **a portfolio may be REASSESSED many times per trading day;
it is NOT automatically rebalanced because it was reassessed.**

The path to activate is the EXISTING one — no new engine:

```
MATERIAL INFORMATION -> SIGNAL REFRESH -> SCORE/RANK -> PORTFOLIO REASSESSMENT
(engine.event_materiality)   (api.event_signal_refresh steps 7-9)   (step 11-12)
```

Contract for the next slice:

* **Trigger criteria** — exactly the Release-28 materiality gate
  (`assess_materiality`): `reassessment_required` True. No second materiality
  vocabulary may be introduced.
* **Debounce / minimum meaningful change** — the existing trigger fingerprint
  (`duplicate_of_prior_trigger` suppresses repeats) plus the gate's declared
  thresholds; any *additional* debounce (e.g. a minimum inter-cycle interval
  during RTH) must be declared in ONE cadence policy inside
  `engine.collection_cadence`, never in a second scheduler.
* **Idempotency key** — the trigger fingerprint (materiality policy version +
  triggering facts + portfolio_state_hash), already persisted at
  `event_fabric/state/last_trigger.json`; the reassessment artifact remains
  idempotent per (active_book, session, input identity).
* **Stale-data rules** — the event cycle already fails closed on blockers
  (unclassified authority) and reports degraded sources; a reassessment run on
  stale inputs is marked by the owners' own freshness fields and MUST surface
  in `active_manager_state.stale_components`. No fabricated close marks.
* **Point-in-time binding** — ONE clock per cycle (`now_iso` from the
  iteration), event identity stamped from it; the reassessment input contract
  binds HOC hash + ranking identity + portfolio_state_hash (already enforced).
* **Expected latency** — detection (collection cadence, today ~15-min quote
  polls) dominates; the decision chain is ~7.3 s median. R54.1's SLA target:
  material observation → recorded reassessment within ONE collection iteration
  (≤ the iteration budget, ~300 s), with the measured
  `oldest_event_to_reassessment_seconds` published per cycle.
* **Failure recovery / retry** — the collection worker already survives a
  failing event cycle (warning + next iteration); no retry-within-iteration;
  the fingerprint guarantees a retry cannot double-count a trigger.
* **Concurrency / single-flight** — the collection service lock (live-holder
  refused; provably-dead takeover) remains the ONLY writer path; the event
  cycle stays token-gated and is never armed by a second scheduler.
* **Turnover protection** — unchanged owners: Stage-20 churn controls +
  R47 switching hurdle + turnover budget; reassessment frequency must not
  loosen any of them.
* **Proposal regeneration semantics** — unchanged: a proposal from a different
  HOC hash for the same session SUPERSEDES (immutable artifacts); Stage-19
  execution precedence keeps a pending execution ahead of a fresh proposal.
* **Manual-review boundary** — ABSOLUTE and unchanged: the cycle may terminate
  in PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW and never in an approval, an order,
  a fill, or a model promotion.

Activation steps for R54.1 (each behind targeted tests + the strict audit):

1. Declare the intraday reassessment cadence policy in
   `engine.collection_cadence` (RTH-aware; explicit interval floor).
2. Flip the audit's `cadence_enabled` declaration for the reassessment lane to
   a MEASURED verdict bound to that one policy (still False for execution).
3. Surface the SLA measurement (`oldest_event_to_reassessment_seconds`) in
   `active_manager_state.live_information` and alert on breach via
   `stale_components`.
4. Only then consider raising detection cadence (the R53.1 intraday feed) —
   collection remains the ONE trigger source.

## 8. Verification (this slice)

* `tests/test_release54_active_manager_state.py` — 39/39.
* `tests/test_release49_operator_presentation.py` — 52/52 (test_51 evolved
  deliberately to admit the ONE declared R54 region).
* Slice 5/6/7 + Stage 20 suites — green; Release 29 collection — 99/99;
  `tests/test_architecture_contracts.py` — 36/36.
* `tests/test_slice2_workflow_state.py`: 4 PRE-EXISTING failures (proven
  present on the untouched HEAD via stash A/B): the 2026-08-05-pinned fixture
  does not inject the DRC status, so the LIVE research-cycle store leaks into
  the unit expectation on any new session day. Unrelated to R54; left for its
  owning slice.
* `scripts/audit_architecture.py --strict` — exit 0 (new
  `check_release54_active_manager_state` blocking invariants included).
* `git diff --check` — clean.

## 9. R54 finalization (post-deployment semantic hardening, 2026-09-01)

Live deployment surfaced four semantic/authority ambiguities. All were
resolved by PROJECTING facts the owners already publish — no state machine, no
threshold, no decision and no scheduler changed.

### 9.1 The slice-2 workflow tests were not hermetic (repaired)

`tests/test_slice2_workflow_state.py` test_14/16/17/18 failed on any new
session day because the shared fixtures left three injection seams unbound —
`research_cycle`, `reassessment_summary`, `decision_record` — so
`load_workflow_state` read the operator's LIVE Daily Research Cycle store. The
live store answers `governed_research_evidence_current` for the fixture's
eligible session (2026-08-04); once that session's governed run aged out,
`research_cycle_due_after_close` (P4.5) began firing and rewrote the expected
overall states. The fixtures now bind all three seams explicitly
(`_DRC_NOT_STARTED` / `_DRC_COMPLETE` / `_REAS_NOT_RUN` / `{}`), and
`test_00_fixtures_bind_every_live_store_seam` poisons the three live loaders
and proves the composed contract is identical — the leak cannot return.
53/53 green. No production freshness/business rule was weakened.

### 9.2 Decision-state semantics (governed HOLD vs live "proposal")

Traced meanings, all now carried explicitly by the composed state:

* **`PROPOSAL_READY`** (engine.portfolio_reassessment) = the reassessment's
  economic pre-gate cleared, so the Slice-7 owner MUST build (or reuse) the
  reviewable proposal. It records that a target was ASKED FOR — it approves
  nothing and does not say the target recommends a change.
* **`PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW`** (api.event_signal_refresh) = the
  live cycle built a complete priced target ARTIFACT for review. The token
  records artifact existence; the target's own outcome (R47) states whether
  the change clears the switching hurdle.
* **Coexistence with HOLD**: the R47 constrained owner priced the complete
  feasible alternative at net improvement 0.000 vs hurdle 0.050 → outcome
  `HOLD_CURRENT_BOOK`; the Track-B settled-aware presentation
  (api.portfolio_reassessment.build_presentation with the decision lane)
  reports `operator_state = PORTFOLIO_DECISION_SETTLED` — "holding the
  current book IS the decision". The R54 payload now projects
  `operator_state` / `decision_settled` / `settled_decision_state` /
  `operator_task` / `operator_next_action` verbatim, and Today renders the
  settled state (raw kernel token preserved in the hover title).
* **`OVERDUE`**: decided by `api.workflow_state.classify_assessment`; on
  2026-09-01 it fired because the LEGACY daily-action-gate scheduled-review
  date (2026-08-01) is in the past (`review_overdue`), while the assessment
  itself is CURRENT for the eligible session (age 0). The payload now carries
  `reassessment_freshness_detail` (currency owner, schedule owner, the
  schedule date, `current_for_eligible_session`,
  `advanced_by_live_event_cycles: False`) and the stale row's detail states
  it in words. Live event cycles never advance the governed clock — by
  design; R54.1 changes the reassessment cadence, never this authority rule.
* **`decision_authority`** (new payload block): the five-rung ladder — live
  intraday assessment / governed reassessment / governed target /
  manual-review candidate / approved decision — each rung a verbatim owner
  value beside its owner's name, plus the workflow's
  `canonical_portfolio_decision` verbatim. Today's strip footer shows the
  canonical decision headline.

### 9.3 Full-universe vs incremental scoring

`api.universe_scoring` recomputes the FULL eligible universe on every build
(there is no partial scorer), and `ranking_date` is the owned model-input
as-of date — the point-in-time DATA basis, never the wall-clock recompute
time. The event cycle refreshes it through the one owner whenever an input is
invalidated. The payload now separates `scoring_basis` (scope + basis
statement), `last_full_universe_scoring` (basis date, count, input-contract
hash) and `last_incremental_signal_refresh` (cycle stamp, state,
`calculations_refreshed`, `affected_names_refreshed`, held rank-delta rows,
prior-ranking availability — all from the cycle owner's persisted run).
Because `latest.json` is only a 4-field pointer, the event-refresh owner now
also publishes `last_run_summary` built from its OWN persisted run payload.
Declared gaps (`not_persisted_facts`): `affected_names_rescored` (full
universe by contract) and `latest_rank_change_timestamp` (deltas are per
cycle, not a stamped series).

### 9.4 TRUE_FORWARD vs intraday prospective emission

The 16:20 ET PaperTrader-IntradayEmission run was the watcher firing OUTSIDE
a legal slot: the frozen R53 factory refused (`NOT_AN_EMISSION_SLOT`,
0 appended). Today's real emission was the 18:00 UTC slot — 36 predictions
(second invocation idempotently appended 0; ledgers 72 predictions /
72 outcomes / 0 forfeitures). Those rows are `forward_evidence_type
TRUE_FORWARD` with `evidence_class PROSPECTIVE_INTRADAY` in the R53 research
ledger — a DISTINCT identity from the daily governed TRUE_FORWARD bundle
(api.forward_prediction_skill, captured by the Daily Close). "Latest
TRUE_FORWARD = 2026-08-31" on Today correctly referred ONLY to the daily
governed bundle. The lane's missing API read surface was added as
`api.research_runtime.load_intraday_emission_status()` (read-only, no mkdir,
no ledger write; the R52-precedented api↔research bridge), and the payload
now exposes `latest_governed_true_forward_date` and
`latest_intraday_prospective_emission` as two named identities that are never
summed. Today renders them as two rows.

### 9.5 Finalization verification

* `tests/test_slice2_workflow_state.py` — 53/53 (hermeticity guard added).
* `tests/test_release54_active_manager_state.py` — 60/60 (new classes:
  decision authority, scoring semantics, evidence identity, UI
  non-interpretation).
* `tests/test_release28_event_driven_manager.py` — 76/76;
  `tests/test_release49_operator_presentation.py` — 52/52; Slice 5/6/7 +
  Stage 20 + architecture contracts + Release 29 collection — 441 passed.
* `scripts/audit_architecture.py --strict` — exit 0 (two new blocking
  invariants: `decision_authority_declared`,
  `evidence_identities_distinct`).
* Browser acceptance (Playwright, 1920×1080) against the live backend: strip
  renders, graceful fallback on the pre-finalization payload, finalized
  wording verified by direct render, no horizontal scroll, no blank buttons,
  only the pre-existing favicon 404.
