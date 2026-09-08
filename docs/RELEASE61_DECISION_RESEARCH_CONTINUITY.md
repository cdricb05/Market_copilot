# Release 61 — Decision & Research Continuity

- **Branch:** `r61-decision-research-continuity`
- **Development worktree:** `D:\paper_trader_r61_decision_research_continuity`
- **Base:** `dc0ca39` (R60.1, deployed and accepted)
- **Live state during development:** read-only. No backend restart, no Daily
  Close, no Portfolio Cycle, no approval, no scheduled-task change, no write to
  any live store.

R61 is a continuity release. It changes no economics, no threshold, no allocation
rule and no decision policy. Every fix below is about an **identity** or a
**state word** being wrong, and each was diagnosed against the live artifacts
that exhibited it before a line was written.

---

## 1. HOC identity root cause

### Before

A same-session opportunity-cost store may legitimately hold several immutable
assessments (R54.3). When a later cycle re-derives an assessment whose economic
state, evidence and conclusion are all unchanged, the persistence owner returns
`REUSED_EXISTING`: the caller's freshly built document is **not** written, and
the durable evidence stays the artifact already on disk.

R55.2.2 already taught `artifact_binding()` to report the **stored** identity in
that case. But **three** consumers went on re-deriving the same field
independently from the transient in-memory assessment:

| Consumer | Field | Source before R61 |
|---|---|---|
| `api/event_signal_refresh.py::_summarize_hoc` | `assessment_hash` | the recomputed document |
| `api/portfolio_reassessment.py::build_input_contract` | `hoc_assessment_hash` | the recomputed document |
| `api/reallocation_proposal.py::build_input_contract` | `hoc_assessment_hash` | the recomputed document |

All three published that hash **beside the stored `artifact_id`**. The
governance gate then did exactly what R54.3 built it to do — load the artifact
by exact id and compare — and found a hash the artifact does not carry.

The third consumer matters more than it looks. The gate ALSO cross-checks the
proposal's bound hash against the candidate's (`TARGET_BOUND_TO_SAME_HOC`, reason
code `HOC_IDENTITY_MISMATCH`). Repairing only the event cycle and the
reassessment would have left the proposal disagreeing with both — trading
`HOC_ARTIFACT_IDENTITY_MISMATCH` for `HOC_IDENTITY_MISMATCH`, a different
withheld reason for the same non-problem. **One owner, one spelling, three
consumers**, and the cross-consumer agreement is now itself a test
(`test_06b`).

`api/event_replay.py` is the fourth call path, and the easiest one to overlook:
it wires the REAL owners to a synthetic world, so its seams are production code
rather than test doubles. R54.3 already threads the binding into its
reassessment seam; R61 threads it into the proposal seam for exactly the same
reason. Left alone, the harness the estate uses to PROVE the orchestrator's
behaviour would have gone on reproducing the very defect this release removes —
replay and live disagreeing about the one identity under test. Nothing in the
targeted bundle caught that; the full repository gate did
(`test_release28_event_driven_manager`), which is the argument for running one.

### The live evidence

Event cycle `evt_d9a07c4269709b6e` (2026-09-08T13:50:36Z), read-only:

```
hoc.artifact_id             hoc_2026-09-04_alpha_paper_book_1_3db8c5b5ecfd
hoc.persistence_status      REUSED_EXISTING
hoc.assessment_hash         d94f24fd…   <- the RECOMPUTED document
binding.hoc_assessment_hash 3db8c5b5…   <- the STORED artifact  (correct!)
binding.hoc_reused_recomputed_document  True
```

Resolved through the artifact owner with the published claim:

```
hoc_artifact_retrievable        True
hoc_artifact_identity_matches   False
detail  artifact … retrieved; assessment hash MISMATCH
```

The owner's own binding block was correct the whole time. **The artifact was
retrievable, immutable, and exactly the one the cycle depended on.** Only the
consumers' independent re-derivations of its identity were wrong.

All three persisted Sep-4 reassessment versions show the same signature: one
stored `hoc_artifact_id`, three different `hoc_assessment_hash` values, two of
them from `REUSED_EXISTING` runs.

### After

`api/holding_opportunity_cost.py` now owns **one** spelling:

```python
bound_assessment_hash(binding, assessment)      # the STORED hash, always
recomputed_assessment_hash(binding, assessment) # audit only, never a dependency
```

All three consumers delegate to it, and both producers (the live event cycle and
the governed Daily Research Cycle) hand the proposal the same
`hoc_binding` they already handed the reassessment. The re-derivation stays visible beside the
binding (`recomputed_assessment_hash`, `reused_recomputed_document`,
`derived_assessment_hash`) so nothing about a reuse is hidden — it is simply no
longer an identity.

**Second, independent defect in the same chain.**
`portfolio_reassessment.resolve_hoc_binding()` compared the consumed hash
against `load_latest_artifact()` only. A reassessment that legitimately consumed
an **earlier** version of a multi-version session therefore resolved to *"no
opportunity-cost artifact is persisted"* while that version sat retrievable on
disk. R61 adds `load_artifact_by_assessment_hash()` — exact resolution by
content across the whole append-only chain — and the binding uses it. A hash no
version holds still resolves to nothing; it never falls back to the latest.

---

## 2. Canonical identity owners

| Stage | Immutable identity it owns | Owner |
|---|---|---|
| signal / event cycle | `run_id`, `materiality_trigger_fingerprint` | `api/event_signal_refresh.py` |
| scoring snapshot | `universe_scoring_hash`, `universe_input_contract_hash` | `api/universe_scoring.py` |
| HOC assessment | `artifact_id` + `assessment_hash` + `assessment_evidence_hash` + `decision_fingerprint` | `api/holding_opportunity_cost.py` |
| reassessment | `reassessment_id` + `reassessment_hash` + evidence identity | `api/portfolio_reassessment.py` |
| reallocation proposal | `reallocation_proposal_id` + `reallocation_proposal_hash` | `api/reallocation_proposal.py` |
| governed decision | `candidate_identity_hash` + `record_id` | `api/portfolio_decision.py` |

**The authoritative HOC identity is composite**: `artifact_id` *and* the stored
`assessment_hash`, bound to `active_book_id` and `eligible_market_date`. An id
alone cannot detect a substituted body; a hash alone cannot be retrieved.

Answers to the ten diagnostic questions:

3. The reassessment referenced a hash the artifact does not carry because the
   read model re-derived it from the transient document (§1).
4. Yes — HOC was recomputed **semantically identically** (same economic state,
   same evidence, same conclusion) and the document-wide hash differed anyway,
   which is the documented Stage-21 trap. The store correctly reused; the read
   model incorrectly re-identified.
5. Yes, in a second place: `resolve_hoc_binding` resolved "latest HOC" rather
   than the exact bound one. Fixed by exact-by-content resolution.
6. No — governance and the event cycle named the **same** artifact id. Only the
   hash disagreed.
7. Duplicate detection uses content identity (`candidate_identity_hash`, and a
   five-field core-evidence tuple). That part is correct.
8. **No — the Sep-8 candidate was not a duplicate.** See §4.
9. Yes, before R61: a repeated read of the same immutable artifact produced a
   different published identity on every reuse. It cannot now.
10. No second writer was found for any of these identities.

---

## 3. Decision continuity fix

Required invariant, now enforced end to end:

```
reassessment.hoc_binding
    == exact immutable artifact retrievable through api.holding_opportunity_cost
    == the identity governance evaluates
```

- one spelling of the bound hash, owned by the artifact store and used by all
  three consumers (event cycle, reassessment, proposal);
- exact-by-content resolution across the version chain;
- a later "latest" artifact can never validate a binding it does not carry
  (proved by test 10: two real artifacts, one impossible pairing, refused);
- idempotent retries produce identical lineage (test 13);
- an assessment that only ever existed in memory still fails closed (test 11);
- no historical artifact is rewritten or rebound (test 12, byte comparison).

---

## 4. Duplicate-candidate finding

**The Sep-8 `DUPLICATE_CANDIDATE` was a structural false positive, and it would
have suppressed every genuinely new intraday candidate.**

`govern_latest_intraday_assessment` resolved its standing authority as

```python
max(persisted_governed_record, project_governed_daily_cycle_decision(rs, summ, con))
```

`project_governed_daily_cycle_decision` builds its identity **from the
reassessment and proposal it is handed** — and the gate hands it the very ones
the candidate under evaluation is built from. Its core evidence therefore equals
the candidate's *by construction*, so `CANDIDATE_ADDS_NEW_EVIDENCE` could never
pass. Because the projection's `decided_at` is the reassessment artifact's
stamp, it also outranked the real Sep-4 ledger row.

The rule that prevents this already existed. R54.4: *"the moment a real daily
ledger row exists for that book and session, the row IS the decision and the
projection is retired — two descriptions of one decision must never both be
candidates for authority."* `load_governed_portfolio_decision` has always
applied it. The gate **declared parity with that read in a comment and did not
perform it.**

R61 extracts the rule into ONE function,
`portfolio_decision.resolve_standing_governed_decision()`, used by both. Against
the live store the projection is now retired
(`LEGACY_DAILY_PROJECTION_RETIRED_BY_LEDGER_ROW`), the standing decision is the
real `gdec_2026-09-04_…` row bound to reassessment `41e9bb8f…`, and the Sep-8
candidate (`900082d1…`) differs from it — so it adds new evidence.

Fail-closed in the other direction is preserved: a candidate whose core evidence
genuinely equals the standing record is still a duplicate (test 17), and the
projection still stands for a session with no ledger row (test 15).

---

## 5. Blocked research classification

All 17 live blocked jobs, classified by the new canonical taxonomy
`alpha_agent/r59/blockers.py`:

| Canonical reason | Count | Clears on |
|---|---|---|
| `FAMILY_EXHAUSTED` | 14 | INFORMATION |
| `DEPENDENCY_BLOCKED` | 3 | INFORMATION |

By asset class: CROSS_ASSET 7, US_EQUITY 4, RATES_FUTURES 3,
EQUITY_INDEX_FUTURES 2, COMMODITY_FUTURES 1.

The taxonomy is asset-agnostic **by construction** — no ticker, symbol or equity
concept appears anywhere in its logic, and the asset class travels as data. An
unrecognised blocker classifies as `UNCLASSIFIED_BLOCKER` and keeps its original
sentence; folding an unknown blocker into a known bucket is how a real outage
comes to be reported as an exhausted search space.

Each reason carries a **clearance class** — `TIME`, `INFORMATION` or `TERMINAL`
— because that, not the reason itself, decides whether waiting is legitimate.

---

## 6. Multi-asset frontier continuity

**Is independent useful research still available? Yes — and the worker is
already doing it.** The observed `ready=0 / runnable=0 / blocked=17` was a
snapshot between drains, not an idle agent. In the 24 hours to 2026-09-08 the
worker completed **89,077 jobs** and switched across six scopes:

```
r59.data_opportunity.us_equity          70,468
r59.mathematical.cross_asset             6,613
r59.mathematical.us_equity               3,606
r59.mathematical.commodity_futures       2,373
r59.mathematical.equity_index_futures    2,227
r59.mathematical.fx_futures              2,092
r59.mathematical.rates_futures           1,698
```

The governor's non-equity reservation and kind ceiling are working: 21% of
completions are non-equity generative work spanning five asset classes.

**But the volume was not research.** Two closed loops accounted for essentially
all of it, and both are the failure R61 explicitly forbids —
*brute-force meaningless transformations forever*:

**(a) The data-opportunity re-probe loop.** Four `ALREADY_OWNED_UNUSED`
opportunities (`OWNED_NEWS_EVENT`, `OWNED_CORPORATE_ACTION`,
`OWNED_EARNINGS_EVENT`, `EODHD_NEWS_UNIVERSE`) were re-issued and re-executed
**~17,700 times each**, every one returning the identical

```
disposition ALREADY_OWNED_UNUSED   resolved false
detail      blocker unchanged (DATA_INCOMPLETE); no owned reader closes it
            and no purchase is permitted
```

For these families `opportunities.reassess()` reads nothing new — it restates a
stored measurement. Re-running it against an unchanged substrate is
*arithmetically guaranteed* to return what the last run returned.

**Fix:** a **probe watermark** owned by `alpha_agent/r59/opportunities.py`. It
fingerprints exactly what a re-probe would observe (the live Form 4 coverage for
the insider families; the stored row for the restating ones). The governor
issues a probe only when that watermark has moved, and publishes every deferral
in `deferred_probes` with the condition that revives it. Nothing is disabled,
retired or rate-limited by a clock: the moment the substrate moves, the mandate
is issuable again on the next batch.

**(b) The symbolic-generation fixpoint.** 18,621 `HYPOTHESIS_GENERATION` jobs in
24 hours re-proposed the same expression — `('u','abs',('col','mom_252_21'))`,
signal fingerprint `bed1d16de99e224b` — rejected as a duplicate every time. The
seed was `f(asset_class, machine_kind, burden)`, and **a draw rejected as a
duplicate does not count toward burden**, so a scope whose every draw was a
duplicate re-derived the same seed, and therefore the same expression, forever.

**Fix:** the seed now also advances with the generator's own draw count, read
from the existing `generator_yield` ledger. Each batch explores a new region; the
seed stays fully deterministic and replayable from persisted state (never a
clock, never the per-process randomised builtin hash), and an empty ledger
reproduces the pre-R61 seed exactly.

---

## 7. Wait / wake semantics

The live runtime reported `worker_state=RESEARCHING`,
`stop_or_sleep_reason="STARTING"`, `next_planned_wake=null`,
`cycles_completed=0` — after 1,484 iterations and 84 minutes.

Not a lie about the work (it genuinely was executing jobs), but the one field
that says *what the agent is waiting for* said nothing, for the entire life of
the process. `sleep_plan` was initialised to the boot placeholder and only
recomputed at the **end** of a cycle — and a research cycle has no cap by
design, so cycle 1 never ended.

R61:

- a research cycle publishes its own live reason (`EXECUTING_RESEARCH`) with the
  iteration as evidence — a busy worker is not waiting, and saying so is not the
  same as saying nothing;
- four truthful waiting states join the vocabulary: `WAITING_FOR_MARKET_DATA`,
  `WAITING_FOR_FORWARD_EVIDENCE`, `WAITING_FOR_EXTERNAL_SAMPLE`,
  `FRONTIER_EXHAUSTED_UNTIL_NEW_INFORMATION`;
- `plan_sleep` takes the canonical blocker summary and names the dominant
  reason, every reason, and the **wake condition**
  (`AN_ELAPSED_MARKET_SESSION` / `NEW_INFORMATION_OR_AN_OPERATOR_DECISION` /
  `A_RE_CHECK_OF_AN_UNCLASSIFIED_BLOCKER`);
- a blocker only TIME can clear earns a short re-check; one that time can never
  clear sleeps the ceiling instead of polling. An **unclassified** blocker keeps
  the R59 short back-off — the safe answer when the estate cannot say what it is
  waiting for.

The status body and the R60 outcomes read model both carry `wake_condition`,
`blocker_reason`, `blocker_reasons` and `wait_detail`.

---

## 8. Orphan freeze reconciliation

Reconstructed from persisted history alone — 56 freezes, 5 unregistered:

| Challenger | Release | Asset class | Horizon | Inception | Obs @ freeze | Lifecycle |
|---|---|---|---|---|---|---|
| `R58_SHORT_VOLUME_PRESSURE_V1` | R58 | US_EQUITY | 21 | 2026-09-03 | 0 | **ACTIVE** |
| `R58_DISCLOSURE_INTENSITY_V1` | R58 | US_EQUITY | 21 | 2026-09-03 | 0 | **ACTIVE** |
| `R58_FUND_MOMENTUM_VETO_V1` | R58 | US_EQUITY | 21 | 2026-09-03 | 0 | **ACTIVE** |
| `R58_FCF_PURE_V1` | R58 | US_EQUITY | 21 | 2026-09-03 | 0 | **ACTIVE** |
| `R59_CALENDAR_TERM_STRUCTURE_F9BE2426` | R59 | CROSS_ASSET | 63 | 2026-09-04 | 0 | **WITHDRAWN** |

The other 51 (45 R46 signal challengers, 6 R56 forward paper portfolios) are
registered with their canonical owners and were never orphans.

**Not five candidates needing a home — four.** Counting them together is what
made the R60 finding read as five pieces of outstanding work when one of them is
a decision the estate already took.

None of the four has been adopted by this release. Adoption is an operator
action (§21), and their challenger class has no canonical forward-evidence
registrar (§10).

---

## 9. Withdrawn / invalidated freeze handling

`R59_CALENDAR_TERM_STRUCTURE_F9BE2426` was withdrawn at inception after two gate
defects were corrected — the observation floor had been applied to the raw
period count of an overlapping-window book (177 rows at cadence 5 / horizon 63
are ~14 independent observations, below a floor of 36), and the validation check
accepted any positive value, waving through 0.2% against a lockbox of 10.6%. It
held **zero** forward observations at withdrawal.

`classify_lifecycle()` resolves it to `WITHDRAWN` with
`never_resurrectable: True`, and `adopt_prospective_freeze()` refuses it
**before any store is touched** — the registrar is never reached and nothing is
written (test 43 asserts both). Withdrawal outranks every other signal: a
withdrawn freeze that is also superseded and also carries a `MATURED` forward
verdict is still `WITHDRAWN` (test 40), so nothing can launder it back.

---

## 10. Atomic freeze → forward registration

### The defect

Qualifying a challenger and starting its forward evidence were two unrelated
writes in two stores, performed by a handler that only ever did the first.
`alpha_agent/r59/handlers.py::freeze_qualified` wrote the freeze into research
memory and an artifact beside it — and then returned. Nothing registered the
challenger with any forward-evidence owner. Because a freeze that accrues
nothing is indistinguishable from a freeze that is merely young, the gap
survived two releases.

That is not a bug in one handler; it is a **missing operation**.

### The owner

`api/prospective_adoption.py` — one governed operation, one lifecycle
reconstruction. Properties, each tested:

- **idempotent**, keyed by the adoption identity hash — the registrar is called
  exactly once however many times the operation is re-run;
- **exact freeze identity**, model/spec hash, feature-snapshot hash, horizon and
  cost model;
- **asset-class and instrument-scope aware** (§11);
- **crash-safe / retry-safe** by a recoverable protocol, not a pretended atomic
  one: the INTENT is durable **before** the registrar is called and COMMITTED
  after, so every state a crash can leave is a named, resumable record on disk
  and a retry resolves it rather than creating a second registration;
- **no backdating** — an adoption declares the session from which observations
  may legitimately begin and is refused when that precedes the freeze's own
  inception; sessions in between are never synthesised;
- **no promotion, no portfolio write, no order**, and an explicit confirmation
  token distinct from every approval and promotion token in the estate.

Cross-store atomicity does not exist here and is **not claimed**. What is
guaranteed is that no state a crash can leave is silent.

### Not a second registry

The registration itself is delegated to the canonical owner for the challenger's
class:

| Class | Canonical registrar |
|---|---|
| `FORWARD_PAPER_PORTFOLIO` (R56) | `api/shadow_portfolio_evidence.py` |
| `FORWARD_SIGNAL_R46` (R46) | `alpha_agent/r46/registry.py` |
| `FORWARD_SIGNAL_NO_CANONICAL_REGISTRAR` (R58/R59) | **none — refused** |

### Injected, never imported

An R59 invariant (`research_does_not_import_the_app`, enforced by
`scripts/audit_architecture.py`) forbids the research package from importing the
application layer. The adoption owner therefore reaches the freeze path from the
composition root — `scripts/run_research_runtime.py` — exactly as the revision
reader does. An un-wired worker still freezes and reports
`FORWARD_ADOPTION_OWNER_NOT_INJECTED` by name.

---

## 11. Multi-asset forward-evidence proof

The adoption identity carries no equity-only field. There is **no `ticker`**;
`instrument_scope` is a list of instrument identifiers whatever they name, and
the asset class travels as data:

```
challenger_id  release  asset_class  instrument_scope  venue  sleeve
economic_family  model_family  horizon_sessions  freeze_id  freeze_record_hash
model_spec_hash  feature_snapshot_hash  inception  cost_model  price_mark_owner
```

Registration is proved on one contract for **US equity, equity-index futures,
rates futures, commodity futures, FX futures, volatility and cross-asset**
(tests 55–56, parameterised). Two asset classes sharing an instrument string
never collide on one identity (test 57). A declared `price_mark_owner` per asset
class is part of the contract, because a forward observation whose mark owner is
unknown is not evidence.

---

## 12. Research outcomes / UI changes

Extended the **existing** R60 AlphaAgent Outcomes read model
(`GET /v1/research/alphaagent-outcomes`) — no second dashboard, no new route,
no business logic in JavaScript:

- `blocked_research` — every blocked job with its canonical reason, clearance
  class, wake semantics and per-asset-class grouping;
- `runtime.wake_condition` / `blocker_reason` / `blocker_reasons` /
  `wait_detail` / `waiting_state_vocabulary`;
- per-freeze `lifecycle_state`, `lifecycle_evidence`, `adoptable`,
  `never_resurrectable`, `challenger_class`, `canonical_registrar`;
- `orphan_freezes_adoptable` (4) vs `orphan_freezes_closed_by_lifecycle` (1);
- `open_adoption_intents` — freezes taken whose forward half has not started.

The read model delegates: it owns no taxonomy and no lifecycle rule, and
`process_state_is_not_evidence_state` stays explicit beside
`process_health_is_not_research_success`.

---

## 13. Cross-surface consistency (Workstream I)

### A. Session / provider date

R60.1 built `engine/exchange_calendar.py` and taught
`engine/market_session.py` to honour it — but `api/daily_close.py::_expected_session`,
the call that decides which session the owned provider is probed for, still
passed **no calendar**. Its own docstring said so: *"A holiday only makes the
expected date one session too new."*

On 2026-09-08 that published `expected_market_date = 2026-09-07` — Labor Day, a
full-day NYSE closure — beside a session-recovery block that correctly said
2026-09-04. Two calendars, two answers, one payload.

R61 supplies the authoritative calendar on both the live and the injected-date
path. `api/daily_close.py` derives no holiday of its own and carries no holiday
list. Regression coverage: Sep-7 Labor Day (never within a trading day, cutoff
never passes), Sep-8 pre-close (expects 2026-09-04, `within_trading_day` True),
Sep-8 post-cutoff (expects 2026-09-08), and the offline path.

`current_open_or_next_session` was a verbatim alias of
`expected_completed_market_date` and never carried the session it names. It is
**deprecated in place** — value unchanged, promise withdrawn — beside a
correctly named `latest_expected_completed_session`.

### B. Active-manager latency gap

**Finding: a category error over an immutable historical record, plus a writer
that did not forward the producer's own declaration. Not a missing write, and
nothing is backfilled.**

The standing governed decision is a **daily-lane** record
(`provenance = GOVERNED_DAILY_CYCLE`). A session-terminal daily decision
processes no observation and starts no event cycle, so `observation_received_at`
and `event_cycle_started_at` never existed for it. `build_daily_cycle_candidate`
already declared `intraday_latency_applicable: False` — but
`record_governed_decision` never passed that declaration to the latency owner,
so both came back MISSING and the acceptance contract read 9/10 LATENCY MISSING
for a fault that does not exist.

R61 forwards the producer's declaration (prospective; only an *unstamped*
endpoint can be excused, so a stamped one is still measured — test 70), and adds
a read-time **lane scope** over the immutable record naming
`structurally_absent_measurements` vs `measurements_missing_and_expected`. An
intraday gap is still a real gap (test 71).

The acceptance contract gains a third status, `NOT_APPLICABLE_TO_THIS_LANE`.
`present_count` now counts only PRESENT rows, so the live contract reads
**present 9 / accountable 9, not-applicable 1, missing none, complete True** —
neither "9/10 MISSING" (a fault report for no fault) nor "10/10 present" (a
claim the evidence does not support).

### C. Cross-surface identity

Every surface now derives its session date from the one exchange calendar, its
standing governed decision from `resolve_standing_governed_decision`, and its
HOC binding from `bound_assessment_hash`. Research recommendation, reassessment
candidate and governed decision remain three distinct states, and a newer
WITHHELD reassessment coexisting with an older authoritative governed decision
is stated explicitly (`standing_decision_resolution` on the gate result).

### D. Preserved intentional differences

Untouched, by design: `PRE_PROPOSAL_RELEASE_SET_ESTIMATE` (~0.081) vs the
binding constrained proposal improvement (~0.056); research-lane DEPLOY vs
governed-lane NO ECONOMIC PROOF / MANUAL REVIEW; forward observations present vs
zero FORWARD_CONFIRMED challengers.

---

## 14. Remaining multi-asset operational gap (explicit)

**Lifecycle and evidence contracts are asset-agnostic. Operational capital is
still equity-centric, and R61 does not pretend otherwise.**

- `api/holding_opportunity_cost.py` positions and the reallocation target are
  ticker-shaped; `excluded_non_equity_positions()` remains.
- No multi-asset NAV, cross-asset risk state or non-equity execution path was
  added, and none should be inferred from this release.
- **R58/R59-class signal challengers have no canonical forward-evidence
  registrar.** The R46 cohort is a frozen contract whose `contract_hash` binds
  68 existing predictions, and `alpha_agent/r46/adopted_forward.py` is wired to
  the R39/R40 shadow owners. Registering a new class there is a bounded release
  of its own, not a line in this one. Until it exists,
  `adopt_prospective_freeze` refuses with `NO_CANONICAL_REGISTRAR` and writes a
  durable OPEN intent, so the gap is a record the estate can see and act on
  rather than an invisible orphan.

Sequenced as **R62.1** below.

---

## 15. Migration / compatibility

- **No artifact is rewritten, rebound or migrated.** Every historical HOC
  artifact, reassessment, proposal and governed record is byte-identical.
- The first reassessment after deploy will legitimately create a **new version**
  in sessions where a reuse previously published a re-derived hash:
  `hoc_assessment_hash` is an evidence-identity component, and it now carries
  the stored value. That is correct versioning, not a rebind — and it makes the
  identity *stable* across reuses instead of moving on every run.
- `current_open_or_next_session` is retained and unchanged for existing readers.
- The acceptance `status_vocabulary` grows by one value; PRESENT and MISSING keep
  their exact meanings.
- `make_handlers` / `run_session` / `run_forever` gain one keyword-only,
  defaulted parameter each.

---

## 16. Future deprecation work

1. **R62.1 — a forward-evidence registrar for R58/R59-class signal
   challengers** *(the remaining half of Workstream D)*. Four ACTIVE freezes are
   waiting on it. Prospective only, no backdating.
2. **R62.2 — one declared vocabulary across the four forward-evidence
   identities** (already on the roadmap; R61's `challenger_class` is the first
   half of it).
3. **Retire `current_open_or_next_session`** once no reader references it.
4. **Retire the legacy daily-cycle projection** entirely once every session in
   the ledger predates R54.4 or has a real row; `resolve_standing_governed_decision`
   is the single place that will have to change.

---

## 17. Safety

No path exists from an AlphaAgent candidate to a champion promotion, an
operational portfolio mutation, or a paper/live order. Manual promotion and
manual portfolio review remain mandatory; no broker automation exists. The R61
modules call no promotion, order, fill or holdings writer (asserted structurally
over executable source), and every adoption result carries the full negative
safety block.

The standing Sep-4 governed decision is untouched, unapproved and unrepaired.
The withheld Sep-8 reassessment was used for diagnosis only and was never
re-posted.
