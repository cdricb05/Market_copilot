# Release 54.2 — Same-session reassessment versioning

**Make new signal evidence an immutable, governable portfolio assessment.**

Built over the committed R54.1 head `0cff378` ("Add governed intraday portfolio
decision gate (R54.1)"), itself over the committed R54 head `8c040ce`. Windows
PowerShell only, single agent, no subagents. Nothing committed, nothing pushed,
no live backend restart, no production store write.

---

## 1. The rule that was correct, and then was not

Stage 20 asked ONE identity question — *is this the same reassessment?* — and
answered it with `reassessment_hash`. Stage 21 split off a second, ECONOMIC
question — *does the prior artifact still describe the portfolio?* — after
`portfolio_state_hash` proved unusable for it: the document-wide hash embeds
this owner's own output, so every downstream research write invalidated its own
input. Stage 21 introduced `economic_state_hash` for that axis and taught
`persist_reassessment` two cases:

| Stage-21 case | Situation | Outcome |
|---|---|---|
| (a) | same economic state, different research inputs | `CONFLICT_REJECTED` — immutability wins |
| (b) | economic state changed (holdings / cash / NAV / corporate actions) | `CREATED_NEW_VERSION` — append |

That was correct for daily operation. The reassessment ran once per operating
cycle, so "the portfolio has not moved" and "the answer has not moved" were the
same statement in practice.

Continuous intraday management breaks the equivalence. The book can be
economically identical all session — same holdings, same cash, same NAV basis —
while new material information arrives, the ranking moves, the holding
opportunity cost moves, the strongest replacement changes and the priced target
changes. The prior artifact still describes the PORTFOLIO. It no longer
describes the ANSWER. Refusing that write left the live cycle's conclusion with
no immutable artifact behind it, which is exactly why the R54.1 governance gate
scored **37/38** and withheld on `CYCLE_REASSESSMENT_IS_THE_CANDIDATE`.

**Core principle of this release:** the portfolio being economically unchanged
does not mean the investment assessment is unchanged. A materially different
assessment of an unchanged portfolio is a NEW point-in-time conclusion and needs
its own immutable artifact. Append it. Never overwrite the old one.

---

## 2. The third identity — assessment evidence

`api.portfolio_reassessment` now publishes a third, narrow identity beside the
two that already existed.

| Axis | Question | Fingerprint |
|---|---|---|
| Economic | Has the PORTFOLIO changed? | `economic_state_hash` (Stage 21) |
| Assessment evidence | Has the EVIDENCE about it changed? | `assessment_evidence_hash` (**R54.2**) |
| Conclusion | Has the ANSWER changed? | `decision_fingerprint` (**R54.2**) |

`ASSESSMENT_EVIDENCE_COMPONENTS` — all of them already published by the Stage-20
input contract; R54.2 introduces no new evidence source:

```
universe_scoring_hash            universe_input_contract_hash
hoc_assessment_hash             hoc_decision_policy_version
corporate_actions_hash          holdings_snapshot_hash
model_identity                  allocation_policy_version
reassessment_policy_version     churn_policy_version
declared_inputs_fingerprint
```

`declared_inputs_fingerprint` is the freshness picture the assessment actually
saw, hashed from the owner's own `declare_inputs` rows (source, state, usage,
as-of date, required). A required source moving FRESH → STALE changes what the
assessment is entitled to conclude, so it is evidence.

**Deliberately EXCLUDED, and enforced by the build:**

* `portfolio_state_hash` — the Stage-21 trap. It embeds this owner's own output,
  so including it would make every downstream research write look like new
  evidence and manufacture a version on every cycle.
* `economic_state_hash` — that is the OTHER axis, compared separately.
* `reassessment_hash` — the CONCLUSION, not the evidence that produced it.
* wall clock, run id, materiality trigger fingerprint — provenance, never
  identity. Two triggers reaching the same conclusion from the same evidence are
  ONE assessment; versioning them twice is exactly the evidence noise a
  poll-driven cycle would generate.

`decision_fingerprint(result)` is the result with `provenance` and
`reassessment_hash` removed. It exists because `reassessment_hash` covers the
whole result *including* provenance, so two runs can differ in that hash while
having reached an identical conclusion from identical evidence. Comparing the
conclusion directly is what separates "the same assessment, re-run" from
"identical evidence produced a different answer" — and only the second is a
genuine conflict.

---

## 3. The persistence contract

`persist_reassessment` now has four outcomes plus a fail-closed guard. Artifacts
are **never** rewritten in any of them.

| # | Situation | Status | Effect |
|---|---|---|---|
| 1 | same economic state + same evidence + same conclusion | `REUSED_EXISTING` | idempotent; no second artifact, no second history row |
| 2 | same economic state + **materially different evidence** | `CREATED_ASSESSMENT_VERSION` | **new immutable version APPENDED** |
| 3 | economic state changed | `CREATED_NEW_VERSION` | Stage-21 behaviour, preserved exactly |
| 4 | same economic state + same evidence + **different conclusion** | `CONFLICT_REJECTED` | inconsistency, never a version |
| — | the artifact's own parts disagree about session or book | `REJECTED_INCONSISTENT_IDENTITY` | fail closed, nothing written |

Case 3 takes precedence over case 2 (an economic move is the stronger statement).
Case 4 is the *entire* residue of the old rule and keeps its protection: an
immutable artifact is never overwritten and the caller must resolve the
inconsistency.

**Point-in-time guard (`_session_identity_conflicts`).** Before anything is
written, the identity, the input contract and the result must agree about the
eligible session and the active book, and the bound HOC assessment's session and
the declared inputs' as-of session must match the eligible session. A
disagreement is impossible evidence and is refused outright. No point-in-time
rule was relaxed anywhere in this release.

**Collision guard (`_unique_artifact_id`).** `artifact_id_for` embeds
`reassessment_hash`, so a collision means two versions reached an identical
conclusion — but if their identities differ the older file must still not be
rewritten. Immutability is now enforced at the write, not assumed from the id
scheme.

**Backward compatibility without rewriting.** An index entry written before
R54.2 carries neither new field. `_existing_assessment_identity` RECOMPUTES both
from what the artifact already persisted — its own identity, its own compacted
input contract, its own result — so a historical artifact becomes comparable
without a single byte of it changing. Verified live: the production index entry
for 2026-08-31 recomputes to `d03a4d45…`, exactly the value the artifact itself
produces.

---

## 4. Version and supersession semantics

A reassessment version is EVIDENCE, not execution. The chain is append-only.

* the index keeps the newest version at the top level (backward compatible for
  every existing reader) and the full chain under `versions`;
* each version records `assessment_evidence_hash`, `decision_fingerprint`,
  `economic_state_hash`, the ranking / HOC / holdings / corporate-action
  identities, its decision and `supersedes_artifact_id`;
* `load_latest_artifact` resolves the newest version (and, with an
  `economic_state_hash` hint, the newest version bound to exactly that economic
  state — Stage 21's rule, unchanged);
* `load_artifact_versions` exposes the whole chain, oldest first;
* `load_artifact_by_id` resolves the artifact FILE, never the pointer, so a
  caller holding an older `reassessment_id` keeps receiving exactly the
  assessment it referenced however many versions the session later acquired.

---

## 5. Signal-driven versioning — no evidence noise

A poll is not a decision. Versions are created only when canonical assessment
evidence changed.

| Cycle outcome | New version? |
|---|---|
| `NO_NEW_INFORMATION` / `INFORMATION_NOT_MATERIAL` | no — the reassessment never runs |
| `DUPLICATE_TRIGGER_SUPPRESSED` | no — the reassessment never runs |
| material trigger, identical ranking + HOC + freshness | no — `REUSED_EXISTING` |
| only the document-wide `portfolio_state_hash` drifted | no — `REUSED_EXISTING` |
| ranking / HOC / freshness materially moved | **yes** — `CREATED_ASSESSMENT_VERSION` |
| economic state moved | **yes** — `CREATED_NEW_VERSION` |

---

## 6. Downstream readers — what same-session versions could have broken

Multiple versions per session make three readers double-count if left alone.
Each is corrected in the canonical owner, and each correction is a no-op for the
existing one-version-per-session behaviour.

**`authoritative_history_rows(rows)`** — the new single reducer: ONE
authoritative row per (book, session), the last one recorded. The append-only
history keeps every row; superseded versions simply do not vote twice.

1. **Churn control.** `recent_change_rows` now reads authoritative rows AND
   excludes the session being assessed. The second rule matters most: a
   reassessment has never seen its own recommendation (its history row is
   written afterwards), and letting version 2 read version 1's row would make
   the 5-session cooldown self-blocking — the system would be structurally
   unable to repeat at 11:10 what it concluded at 09:45.
2. **Forward attribution.** `build_attribution` measures what the system
   CONCLUDED at each session, once.
3. **Stage-21 outcome observations.** `api.reassessment_outcomes` observes the
   authoritative rows, so a session reassessed three times does not contribute
   three copies of the same recommendation to the forward-outcome evidence.

`load_reassessment_history` still returns the FULL append-only record — hiding a
superseded assessment would be rewriting evidence — and now also reports
`authoritative_row_count`, `superseded_row_count` and
`authoritative_reassessment_ids`.

Latest-state readers (`load_portfolio_reassessment`, `load_reassessment_summary`,
the workflow read, Active Manager State, the DRC, the event cycle) resolve
through `load_latest_artifact` and therefore see the newest version. Explicit-id
readers are immutable by construction. `proposal_is_current_for` compares bound
identities, so a proposal built for an older version is correctly reported as
not reusable and the target is rebuilt.

---

## 7. Governance integration — the gate was tightened, not weakened

The R54.1 gate still has **38 checks in 9 groups**. No rule was relaxed and no
exemption was added. One check was made STRICTER:

`REASSESSMENT_IDENTITY / CYCLE_REASSESSMENT_IS_THE_CANDIDATE` still compares the
cycle's reassessment hash against the candidate's, and now additionally requires
that the cycle's conclusion actually became an immutable artifact. A refused
write (`CONFLICT_REJECTED`, `REJECTED_INCONSISTENT_IDENTITY`) leaves
`reassessment_persisted` False and no id, and an unpersisted assessment is never
governable however current it looks.

To make that checkable, `api.event_signal_refresh` now publishes what the
canonical persistence owner answered — `reassessment_id`,
`reassessment_persistence_status`, `reassessment_persisted`,
`assessment_evidence_hash`, `assessment_evidence_changed`,
`supersedes_reassessment_id` — and Active Manager State projects the same fields
on the live lane, which remains explicitly non-authoritative
(`is_authoritative_decision: false`).

**Hermetic 38/38 proof** (`test_32b`): version 1 is persisted, version 2 is
persisted from changed HOC evidence as `CREATED_ASSESSMENT_VERSION`, the
identities the gate checks are taken from the owner's own `proposal_binding` for
that exact artifact, and the gate returns `GOVERNED_INTRADAY_DECISION_ELIGIBLE`
with 38/38. Version 1 remains on disk and readable by id.

---

## 8. Daily-cycle compatibility

There is ONE reassessment history. The Daily Research Cycle and the live event
cycle both reach it through `api.portfolio_reassessment.run_and_persist`; neither
defines a persistence path of its own. A DRC reassessment later in the same
session participates in the same append-only chain and wins by the same
ordering. There is deliberately no "intraday reassessment store" and no "DRC
reassessment store"; the build fails if one appears.

---

## 9. Safety

Unchanged and re-proved: no order, no fill, no order plan, no broker call, no
approval, no automatic rebalance, no model promotion, no sleeve activation, no
Daily Close, no scheduler. Persisting a version touches only the reassessment
store (`artifacts/`, `index.json`, `recommendation_history.json`). The
operational close mark is advanced by `api.daily_close` and nothing else. The
owner's only execution touchpoint is a READ of the rebalance state, which is how
an in-flight execution keeps operator precedence.

R54.2 changes EVIDENCE PERSISTENCE SEMANTICS only. It does not execute capital
decisions.

---

## 10. Architecture audit

`check_release54_2_same_session_reassessment_versioning` adds **20 BLOCKING
invariants**. The build fails on: a second reassessment store, a second
persistence writer, a second identity calculator, an overwrite-instead-of-append
version chain, any artifact deletion, an intraday-only parallel history, an
assessment-evidence identity contaminated with `portfolio_state_hash`,
`economic_state_hash` or `reassessment_hash`, a missing inconsistency guard, a
missing collision guard, a producer that stops delegating, or a governance gate
that stops requiring a persisted reassessment.

---

## 11. Verification

* **New suite:** `tests/test_release54_2_same_session_reassessment_versioning.py`
  — 55/55.
* **Required regressions, all green on the final code:** stage20 + stage21 + R28
  = 246; R54 + R54.1 + R54.2 + architecture contracts = 245; slice6 + slice7 +
  stage22 + R29.3 + R29.4 + stage18 = 446.
* **`scripts/audit_architecture.py --strict`** — exit 0.
* **`git diff --check`** — exit 0.
* **Live validation** — READ-ONLY. The backend was never restarted, no
  production store was written, no Daily Close / DRC / portfolio cycle /
  approval / order / fill ran.

### The live read-only verdict

Against the production 2026-08-31 state, computed without writing anything:

```
ECONOMIC_STATE_UNCHANGED             True
changed evidence component           hoc_assessment_hash
                                     6de5ece4... (persisted, DRC)
                                  -> 9efb688d... (live event cycle)
PERSISTED_ASSESSMENT_EVIDENCE_HASH   d03a4d45...
LIVE_ASSESSMENT_EVIDENCE_HASH        bd9df440...
RECOMPUTED_FROM_LEGACY_ENTRY         d03a4d45...   (identical; nothing rewritten)
R542_PERSISTENCE_VERDICT             CREATED_ASSESSMENT_VERSION
```

The live cycle's reassessment would be APPENDED as version 2 of the session,
the cycle would bind that artifact, and `CYCLE_REASSESSMENT_IS_THE_CANDIDATE`
would pass — closing the 37/38 that R54.1 documented.

---

## 12. What is still needed before a live governed intraday decision occurs

1. **Deploy.** The live backend is deliberately still running the R54 runtime
   loaded before R54.1 was committed. R54.1 is committed (`0cff378`) but not
   yet loaded; R54.2 is an uncommitted working-tree change. The operator's
   regression + commit gate and a canonical restart are what put both in force.
2. **One material intraday cycle after deployment.** The evidence is already
   there (the live cycle produces a different HOC assessment every run), so the
   first material cycle after restart writes version 2 and offers a governable
   candidate.
3. **Known residual — the HOC store has the same shape of gap.**
   `api.holding_opportunity_cost.persist_assessment` still returns
   `CONFLICT_REJECTED` for a same-session assessment with different evidence, so
   the live HOC `9efb688d…` has no artifact on disk while the persisted
   reallocation proposal already binds it. That is a PRE-EXISTING gap — the
   proposal owner has superseded on changed inputs since Slice 7 — and R54.2
   does not create it, but a governed intraday decision would bind a HOC hash
   whose artifact is not retrievable. Giving `persist_assessment` the same
   two-axis append semantics is the exact next slice (**R54.3**), and it is
   deliberately not folded in here: it changes a second canonical owner's
   immutability contract and its own required regression (`test_52` in
   `tests/test_slice6_holding_opportunity_cost.py` pins the current rule).
4. **Latency.** Unchanged by this release and still dominated by detection /
   corpus age (`oldest_event_to_reassessment_seconds` ≈ 5·10³–3·10⁴ against a
   ~300 s cycle). Cadence was deliberately not touched.
