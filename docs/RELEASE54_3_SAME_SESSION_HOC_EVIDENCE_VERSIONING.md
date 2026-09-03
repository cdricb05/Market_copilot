# Release 54.3 — Same-Session HOC Evidence Versioning & Retrievable Governance Binding

**Status:** LANDED (uncommitted at time of writing)
**Base:** committed R54.2.4 head `3d2d311`
**Branch:** `stage19-controlled-rebalance`
**Scope:** persistence + identity + downstream binding. No economics changed, no
recommendation changed, no historical artifact rewritten, no order path touched.

---

## 1. What was wrong

The Holding Opportunity-Cost store indexed exactly **one artifact per
(active book, eligible market session)**. `persist_assessment` answered only one
identity question — "is this the same assessment?" — using `assessment_hash`, and
refused anything else that arrived the same session as `CONFLICT_REJECTED`.

That was **safe** (an immutable artifact was never overwritten) and **correct for
a once-a-day governed cycle**. Continuous intraday management broke it.

The book can be economically identical all session — same holdings, same cash,
same NAV — while the evidence behind the opportunity-cost conclusion moves: a
newer ranking artifact, a newer owned price window, a prior-rank snapshot that
only just became available. That is a **new point-in-time assessment of an
unchanged portfolio**, and refusing to persist it stranded the system on the
session's first artifact.

### The live measurement (2026-09-02 / 09-03, read-only)

| cycle | at (UTC) | computed HOC | persisted? |
|---|---|---|---|
| `evt_4456405791b07b2e` | 23:32:45 | `702c599ee5b3…` | ✅ `hoc_2026-09-02_alpha_paper_book_1_702c599ee5b3` |
| `evt_7dece2a4e47fe608` | 01:33:07 | `a162fca969c9…` | ❌ never persisted |
| `evt_efe6a0e34ebe588e` | 02:33:38 | `a162fca969c9…` | ❌ never persisted |

Worse, the R54.2 reassessment version chain had already bound the transient hash:

```
prs_2026-09-02_..._74776dda34ac   hoc=702c599ee5b3…   retrievable=True
prs_2026-09-02_..._029df5cdcda5   hoc=a162fca969c9…   retrievable=FALSE
```

A **persisted, immutable reassessment artifact claimed a dependency that could
never be produced as evidence.** And the event cycle published no HOC persistence
field at all, so nothing downstream could detect it.

A governance gate must never accept an ephemeral hash as immutable evidence. The
system therefore had to fail closed, and the governed intraday decision was
structurally unreachable.

---

## 2. The three HOC identity axes

R54.3 separates the three questions R54.2 already separated for the portfolio
reassessment, **using the same words** — one vocabulary, two stores.

| axis | question | field |
|---|---|---|
| **Economic** | which portfolio is this about? | `economic_state_hash` |
| **Evidence** | which observations produced it? | `assessment_evidence_hash` |
| **Conclusion** | what did it conclude? | `decision_fingerprint` |

### 2.1 Economic identity

Reuses the canonical Stage-21 economic fingerprint published by
`api.portfolio_state` (`economic_state_hash`) — holdings / cash / NAV / orders /
fills / corporate actions. No competing portfolio-state definition is invented.

`portfolio_state_hash` is **never** used for this: that document-wide hash embeds
this assessment's own output (via `api.daily_action_gate`), so it drifts the
moment the artifact is written. That is the Stage-21 trap.

### 2.2 Assessment-evidence identity

`ASSESSMENT_EVIDENCE_IDENTITY_VERSION = "holding_opportunity_cost.assessment_evidence_identity.v1"`

Twelve components, every one an input the kernel demonstrably consumes:

| component | why it is evidence |
|---|---|
| `universe_scoring_hash` | the ranking the comparison used |
| `universe_input_contract_hash` | the model inputs behind that ranking |
| `scoring_ranking_date` | the ranking basis session |
| `corporate_actions_hash` | the registry holdings were projected through |
| `holdings_snapshot_fingerprint` | weights/values drive HHI, risk contribution, name-cap, days-to-liquidate |
| `market_data_fingerprint` | trailing closes, median dollar volume, aligned returns → return, volatility, drawdown, liquidity, covariance |
| `previous_ranking_fingerprint` | rank CHANGE is what the deterioration rule reads |
| `prior_signal_fingerprint` | prior-signal inputs handed to the kernel |
| `policy_fingerprint` | the resolved numeric policy (entry rank, buffers, caps, cost rate) |
| `decision_policy_version` | frozen policy label |
| `cost_policy_version` | frozen cost label |
| `inputs_as_of_eligible_date` | the freshness picture the assessment saw |

**Explicitly excluded**, and declared as `EVIDENCE_EXCLUDED_PROVENANCE` so the
exclusion is testable rather than merely intended:

`generated_at`, `persisted_at`, `now`, `wall_clock`, `request_id`, `run_id`,
`drc_run_id`, `event_cycle_id`, `scheduler_invocation_id`,
`materiality_trigger_fingerprint`, `materiality_event_timestamp`,
`portfolio_state_hash`, `economic_state_hash`, `assessment_hash`, `artifact_id`.

Two different triggers reaching the same conclusion from the same evidence are
**one** assessment. Versioning them twice would be evidence noise — exactly what a
poll-driven cycle generates.

The raw kernel inputs are far too large to persist whole, so their deterministic
fingerprints are written into the compacted input contract. That is what lets the
next persist decide the version question — and an auditor re-derive the identity —
from the compacted contract alone.

### 2.3 Conclusion identity

`decision_fingerprint(result)` hashes the kernel result **minus `provenance` and
`assessment_hash`**. `assessment_hash` covers the whole result including
`provenance`, which carries `portfolio_state_hash`; two runs can therefore differ
in that hash while having reached an identical conclusion from identical evidence.
Comparing the conclusion directly is what separates "the same assessment, re-run"
from "identical evidence produced a different answer".

---

## 3. Persistence contract

| # | condition | outcome |
|---|---|---|
| 0 | assessment state not READY/DEGRADED | `NOT_PERSISTED` |
| 1 | same economic state + same evidence + same conclusion | `REUSED_EXISTING` |
| 2 | same economic state + **different** evidence | `CREATED_ASSESSMENT_VERSION` |
| 3 | economic state itself changed | `CREATED_NEW_VERSION` |
| 4 | same economic state + same evidence + **different** conclusion | `CONFLICT_REJECTED` |
| 5 | the artifact's own parts disagree about session or book | `REJECTED_INCONSISTENT_IDENTITY` |

Evaluation order matters. **Exact idempotency is tested first and independently**:
if the stored `assessment_hash` equals the new one it is the same assessment,
whatever a legacy index entry can or cannot say about the evidence behind it.

Outcome 4 is a genuine determinism failure and still fails closed. Outcome 5 is
impossible evidence and is never written at all. **No artifact is ever rewritten,
in any outcome.**

---

## 4. Immutable version chain

The index entry keeps the newest version at the top level — backward compatible
for every existing reader — and the full append-only chain under `versions`:

```python
index[key] = {**entry, "versions": prior_versions + [entry]}
```

| read | resolves |
|---|---|
| `load_latest_artifact(book, session)` | newest version |
| `load_latest_artifact(..., economic_state_hash=H)` | newest version bound to **that** economic state |
| `load_artifact_versions(book, session)` | the whole chain, oldest first |
| `load_artifact_by_id(artifact_id=…)` | **the artifact file itself**, never the index pointer |

`load_artifact_by_id` is the read the governance retrievability proof depends on:
a caller holding an older `hoc_artifact_id` keeps receiving exactly the assessment
it referenced, however many later versions the session acquired.

### Collision safety

`artifact_id_for` = `hoc_<session>_<book>_<assessment_hash[:12]>` — deterministic,
no wall clock, no UUID. Because the evidence flows into the result, two genuine
versions always differ in `assessment_hash` and therefore in id. Immutability is
nevertheless **enforced** rather than assumed: `_unique_artifact_id` refuses to
land on an existing file whose identity differs, suffixing the (deterministic)
evidence hash.

---

## 5. Legacy compatibility

A pre-R54.3 artifact carries neither `assessment_evidence_hash` nor
`decision_fingerprint`. Rather than reinterpreting or rewriting it,
`_existing_assessment_identity` **recomputes both from what it already persisted**
— its own identity, its own compacted input contract, its own result. What cannot
be derived stays `None`, and `None` is never treated as a match.

Verified against the **real production artifact**
(`hoc_2026-09-02_alpha_paper_book_1_702c599ee5b3`, copied into a temp store; the
production store was only ever read):

- still readable, still exactly retrievable by id, version chain length 1;
- evidence hash and conclusion fingerprint recomputed successfully;
- an identical rerun is still `REUSED_EXISTING`;
- the assessment the live system had been refusing (`a162fca969c9…`) persists as
  `CREATED_ASSESSMENT_VERSION`, and **v1's bytes are unchanged**;
- the new version then resolves as `hoc_persisted=True`,
  `hoc_artifact_retrievable=True`, `hoc_artifact_identity_matches=True`.

No historical store was rewritten. No missing evidence was fabricated. No
TRUE_FORWARD state was backfilled.

---

## 6. Downstream exact binding

`api.holding_opportunity_cost` publishes the binding once; everything else copies
it verbatim.

```
artifact_binding(persistence|artifact)  -> hoc_artifact_id, hoc_assessment_hash,
                                           hoc_assessment_evidence_hash,
                                           hoc_decision_fingerprint,
                                           hoc_persistence_status, hoc_persisted, …
resolve_binding(binding, …, hoc_dir)    -> + hoc_artifact_retrievable,
                                             hoc_artifact_identity_matches,
                                             hoc_binding_detail
```

`resolve_binding` is the **io half**, and it lives in the artifact's owner because
that module is the one owner of the store. The governance gate stays pure and
consumes the result as a fact.

| consumer | binds |
|---|---|
| `api.portfolio_reassessment` input contract + identity + compact contract | `hoc_artifact_id`, `hoc_assessment_evidence_hash`, `hoc_decision_fingerprint`, `hoc_persisted` |
| `proposal_binding` (the proposal's inherited lineage) | `hoc_artifact_id`, `hoc_assessment_evidence_hash`, `hoc_persisted` |
| `build_intraday_candidate` identity | `hoc_artifact_id`, `hoc_assessment_evidence_hash` |
| `api.event_signal_refresh` run summary | `hoc_artifact_id`, `hoc_persisted`, `hoc_persistence_status`, `hoc_assessment_evidence_hash`, `supersedes_hoc_artifact_id`, `hoc_version_index` |

`prs.resolve_hoc_binding` accepts the binding **only** when the retrieved
artifact's `assessment_hash` equals the hash of the assessment actually consumed.
That is precisely the R54.3 failure mode, and the mismatch is now reported as
`hoc_persisted: False` with a detail naming both hashes.

> `hoc_artifact_id` is recorded in the reassessment **identity** but deliberately
> NOT added to `ASSESSMENT_EVIDENCE_COMPONENTS`: the id embeds
> `hoc_assessment_hash`, which is already an evidence component, so binding it
> twice would version the same fact twice.

---

## 7. Event-cycle ordering

```
material event
  -> HOLDING_OPPORTUNITY_COST step   : compute -> persist -> capture binding
  -> PORTFOLIO_REASSESSMENT step     : hoc_binding= handed forward, not re-derived
  -> proposal (when warranted)
  -> GOVERNED_DECISION_GATE          : hoc_dir= handed forward
```

The HOC step's `detail` now records the persistence outcome verbatim
(`"…; persistence=CREATED_ASSESSMENT_VERSION"`), and a refused write is reported
**as** a refused write. `api.event_replay` threads the same binding so a replayed
reassessment records the same provable dependency a live one would.

---

## 8. Governance requirements

**Seven** checks added to the `HOC_IDENTITY` group; the gate moves from **38 to
45** mandatory conditions. Six of the seven fail with one of R54.3's two NEW
reason codes; the seventh binds the new evidence axis and reuses the existing
`HOC_IDENTITY_MISMATCH` code — it is a new check either way, and the count of
checks is seven.

| check | fails with | code is new in R54.3 |
|---|---|---|
| `HOC_ARTIFACT_ID_BOUND` | `HOC_ARTIFACT_NOT_PERSISTED` | yes |
| `HOC_ASSESSMENT_WAS_PERSISTED` | `HOC_ARTIFACT_NOT_PERSISTED` | yes |
| `HOC_ARTIFACT_RETRIEVABLE` | `HOC_ARTIFACT_NOT_PERSISTED` | yes |
| `HOC_ARTIFACT_IDENTITY_MATCHES` | `HOC_ARTIFACT_IDENTITY_MISMATCH` | yes |
| `REASSESSMENT_BOUND_TO_THE_SAME_HOC_ARTIFACT` | `HOC_ARTIFACT_IDENTITY_MISMATCH` | yes |
| `REASSESSMENT_DEPENDENCY_IS_NOT_TRANSIENT` | `HOC_ARTIFACT_NOT_PERSISTED` | yes |
| `HOC_EVIDENCE_IDENTITY_BOUND` | `HOC_IDENTITY_MISMATCH` | no (pre-existing code) |

All seven are enforced by `audit_architecture.R543_GATE_CHECKS`.

**Absence is inadmissible here, deliberately.** Elsewhere the gate uses
`_eq_when_known` — "not comparable" is admissible for a binding that might
legitimately be unknown. It is *not* admissible for "does this artifact exist?",
where the only honest answers are a proof and a refusal. Each persistence fact is
compared to `True` explicitly, so a missing binding fails closed.

No existing R54.1 check was weakened or removed.

---

## 9. Intraday presentation (R54.2.4 Lane B)

Lane B is **not** redesigned. It already renders `governance_state` and the exact
withheld reason codes verbatim from the backend, so it consumes the strengthened
truth without change: `GOVERNED` when every check passes, `WITHHELD` with
`HOC_ARTIFACT_NOT_PERSISTED` / `HOC_ARTIFACT_IDENTITY_MISMATCH` when the
dependency cannot be produced. No governance is computed in JavaScript, and the
UI never infers artifact persistence (audit-enforced: `ui_derives_hoc_persistence`
must be empty).

---

## 10. Authority, churn and cooldown

Authority is untouched from R54.2.3.2. A fully governed intraday decision may
supersede the standing decision under the one canonical ordering; a research-only
or governance-withheld result **never** does, and a governed CHANGE remains a
recommendation requiring manual review.

On churn: the opportunity-cost owner holds **no change history at all** — turnover
and cooldown are counted from the reassessment owner's authoritative
one-row-per-economic-session history, which R54.2 already collapses. Multiple
same-session HOC versions are multiple *assessments*, and an assessment moves no
capital. The full immutable chain stays available for audit; nothing is collapsed
globally. Audit-enforced: the owner defines no `_append_history`, no
`change_history` and no `turnover_event`.

Idempotency: a persistence retry re-derives the same identity and returns
`REUSED_EXISTING` with the **same artifact id**, so nothing downstream sees a new
dependency to build against — no duplicate proposal, no duplicate governed
decision.

---

## 11. Live read-only verdict

The backend was **not restarted** and still runs the committed R54.2.4 code, so
the live store is unchanged (18 artifacts, bytes identical). Evaluating the R54.3
contract read-only against that live state:

```
binding    : hoc_persisted=False, hoc_artifact_retrievable=False
detail     : the assessment consumed (a162fca969c93831) is NOT the persisted
             artifact (702c599ee5b38535); its evidence exists only transiently
gate       : INTRADAY_DECISION_WITHHELD, 30/45 checks passed
HOC codes  : HOC_ARTIFACT_NOT_PERSISTED, HOC_ARTIFACT_IDENTITY_MISMATCH
```

That is the **correct** answer for the current store: the dependency genuinely
does not exist. Before R54.3 the gate had no way to see this at all. Once the
backend runs R54.3, the next cycle's assessment persists as
`CREATED_ASSESSMENT_VERSION` and the same binding resolves clean — demonstrated
above against the real production artifact.

---

## 12. Guards

- **Audit:** `check_release54_3_hoc_evidence_versioning` — 18 strict-blocking
  fields (one writer, one store, append-only, no deletion, uncontaminated
  evidence identity, declared exclusions, outcome vocabulary, inconsistent-identity
  guard, all seven gate checks, reason codes, gate purity, fail-closed comparison,
  owner-held binding resolver, reassessment/proposal exact binding, cycle
  publication, cycle ordering, no UI derivation).
- **Tests:** `tests/test_release54_3_same_session_hoc_versioning.py` — 61 tests
  covering all 50 required proofs.
- **Deliberately replaced:** `tests/test_slice6_holding_opportunity_cost.py::test_52`.
  It pinned "a second same-session assessment differing only in
  `portfolio_state_hash` is `CONFLICT_REJECTED`". That hash is the Stage-21 trap;
  the honest answer is `REUSED_EXISTING`. The scenario is still pinned, only the
  correct answer moved, and the genuine determinism conflict is pinned separately
  in the new `test_52b`.
- **Deliberately widened:** the R54.2 audit rule `duplicate_versioning_owners` now
  permits the shared identity vocabulary (`assessment_evidence_identity`,
  `assessment_evidence_hash`, `decision_fingerprint`, `load_artifact_versions`,
  `load_artifact_by_id`) in the one opportunity-cost owner as well as the one
  reassessment owner — two stores, one vocabulary, and still no third module.
  Reassessment-exclusive definitions (`persist_reassessment`,
  `authoritative_history_rows`) remain unique.

## 13. What R54.3 does not do

It creates no order, no fill, no order plan, no approval, no model promotion and
no sleeve activation; it enables no automation; it advances no operational mark;
it fabricates no TRUE_FORWARD row; and it changes no investment recommendation.
It makes an assessment that was already being computed **durable and provable**.
