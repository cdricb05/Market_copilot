# Release 54.4 — One Governed Portfolio Decision Writer

**Daily DRC + intraday authority consolidation.**

Status: implemented, bounded-gate tested, not committed.
Base HEAD: `e563146` (R54.3).
Branch: `stage19-controlled-rebalance`.

---

## 1. The defect this release removes

The governed portfolio decision is **one business concept**. Before R54.4 it had
**two persistence realities**:

| Producer | Persisted? | Identity | Lineage | Read path |
|---|---|---|---|---|
| Intraday event cycle | **Yes** — appended to `governed_decisions.json` + `governed_index.json` | `candidate_identity_hash` | `supersedes_decision_id` | `load_governed_decision_record` |
| Daily Research Cycle | **No** — nothing was ever written | none | none | `project_governed_daily_cycle_decision`, **re-derived on every read** |

The daily session-terminal conclusion lived only inside
`api.daily_research_cycle`'s run manifest and was reconstructed at read time
from three separately mutable inputs: the workflow's `research_cycle_state`, the
current reassessment, and the current proposal summary.

Three consequences followed, and all three are real:

1. **A decision recomputed on read is not a decision the system ever made.** It
   changes retroactively whenever any upstream input moves. There is no record
   of what was decided, only a recipe for re-deciding it.
2. **It had no record id**, so nothing could name it in a supersession lineage.
3. **The intraday writer had to rebuild the projection just to discover what it
   was superseding** (`govern_latest_intraday_assessment` called
   `project_governed_daily_cycle_decision` before every gate evaluation). That
   is the clearest possible signature of parallel ownership.

Evidence from the production store at the time of this work:

```
D:\Stock_Prediction_app_data\portfolio_decisions\
    decisions.json      (manual operator lane, last written 2026-08-12)
    index.json
    # governed_decisions.json — ABSENT. The governed lane had never been written.
```

The daily manifest for 2026-09-02 (`drc_2026-09-02_15abfb01856f`, `COMPLETE`)
carried `portfolio_reassessment_decision = CURRENT_NO_CHANGE` and
`reallocation_proposal_state = NOT_REQUIRED` — a real, terminal, governed
conclusion that existed nowhere as a decision record.

---

## 2. Canonical model after R54.4

```
ONE BUSINESS CONCEPT      governed portfolio decision
ONE AUTHORITATIVE OWNER   api.portfolio_decision
ONE LEDGER                governed_decisions.json + governed_index.json
ONE ORDERING              governed_decision_ordering_key
TWO PRODUCERS             GOVERNED_DAILY_CYCLE | GOVERNED_INTRADAY
```

**Producer is not authority.** `provenance` records which lane produced a row;
the ordering function alone decides which row is authoritative.

```
Daily Research Cycle                    Live event cycle
  scoring -> HOC -> reassessment          signal refresh -> HOC -> reassessment
  -> proposal if warranted                -> proposal if warranted
  -> terminal manifest (durable)          -> 45-check intraday gate
  -> 19-check DAILY gate                        |
        |                                       |
        +---------------> api.portfolio_decision.record_governed_decision
                                    (the ONE writer)
                                          |
                             governed_decisions.json (append-only)
                                          |
                          load_governed_portfolio_decision  ->  Lane A
```

---

## 3. Canonical governed-decision owner

`api.portfolio_decision` owns, and is the only module that may define:

| Surface | Purpose |
|---|---|
| `record_governed_decision` | **THE writer.** Append-only, idempotent, fail-closed. |
| `governed_decision_ordering_key` | **THE ordering.** Total, reproducible. |
| `load_governed_decision_record` | Latest persisted row for a book. |
| `load_persisted_daily_decision` | *(new)* Did the daily producer already write this session? |
| `load_governed_portfolio_decision` | **THE read.** What stands right now. |
| `resolve_decision_authority` | Which decision is authoritative, which proposal reviewable. |
| `build_intraday_candidate` / `evaluate_intraday_governance` / `govern_latest_intraday_assessment` | Intraday producer contract (R54.1, unchanged). |
| `build_daily_cycle_candidate` / `evaluate_daily_cycle_governance` / `govern_daily_cycle_decision` | *(new)* Daily producer contract. |
| `_governed_identity` / `_governed_decision_word` | *(new)* The shared identity + conclusion contract both producers use. |

The architecture audit check
`release54_4_single_governed_decision_writer` fails the build if any of these
appears anywhere else in `api/` or `engine/`, or if a second governed-decision
store or index appears.

---

## 4. Daily producer contract

`api.daily_research_cycle` remains fully responsible for research orchestration,
TRUE_FORWARD daily evidence, daily scoring, and HOC / reassessment / proposal
production. It **delegates only the governed decision write.**

```python
# api/daily_research_cycle.py — after the manifest is durably persisted
rec = _persist(final, ...)
delegated, gd_warnings = _delegate_governed_decision(
    manifest=rec, drc_dir=drc_dir, reassess_subdir=..., realloc_subdir=...,
    hoc_subdir=..., governed_decision_fn=governed_decision_fn)
rec["governed_portfolio_decision"] = delegated
```

Rules:

- **Delegation happens only after the manifest is terminal-COMPLETE and
  read-back verified.** A manifest that is not durable is not governed evidence.
- **The handoff is append-only in one direction.** The decision row names the
  run (`evidence.daily_cycle_run_id`); the manifest is *never* rewritten to name
  the decision. Audit invariant: `manifest_rewritten_with_decision == False`.
- **Governance never breaks research.** A failure in the decision owner is
  reported as a warning; the completed research run stays COMPLETE. The converse
  is fail-*closed*: the gate refuses anything it cannot prove.
- **`rec["governed_portfolio_decision"]` is a REPORT, not the decision's home.**
  It carries `read_the_decision_from: "api.portfolio_decision"`.
- **Hermetic seam.** When `drc_dir` is pinned, the decision store resolves to
  `<drc_dir>/portfolio_decisions` and every downstream owner read stays under
  that root. `run_daily_research_cycle(governed_decision_fn=...)` is the
  injectable seam.

### The daily gate (19 checks)

`evaluate_daily_cycle_governance` asks the **session-terminal** question — *"is
this a validated terminal-COMPLETE DRC manifest whose bound reassessment and
opportunity-cost artifacts actually exist and actually belong to it?"* — not the
intraday freshness question. It decides admissibility only and computes no
economics.

| Group | Checks |
|---|---|
| `DAILY_MANIFEST` | present, terminal-COMPLETE, run identified, session matches, book matches |
| `PORTFOLIO_IDENTITY` | active book present, eligible session present |
| `EVIDENCE` | reassessment bound, reassessment matches manifest, HOC artifact bound, HOC retrievable, HOC identity matches, HOC matches manifest |
| `DECISION` | conclusive decision word, not a TRUE_BLOCKER, proposal binding consistent |
| `SUPERSESSION` | strictly outranks the standing decision (or is a duplicate) |
| `SAFETY` | CHANGE is recommendation-only, no automation/approval/promotion |

One new reason code, because reusing an intraday code would describe a condition
that was never evaluated:

```
WR_DAILY_MANIFEST_NOT_GOVERNED = "DAILY_MANIFEST_NOT_GOVERNED"
```

All other codes are reused verbatim from the canonical taxonomy.

---

## 5. Intraday producer contract — unchanged

R54.1 and R54.3 behaviour is preserved exactly:

- the exact-artifact HOC requirements (`hoc_artifact_retrievable is True`,
  `hoc_artifact_identity_matches`) still fail closed;
- the reassessment exact binding is unchanged;
- the 45-check gate, manual-review requirement, supersession logic, idempotency
  and point-in-time integrity are untouched;
- **the intraday decision-word mapping is deliberately NOT changed.** The
  intraday producer promotes only on a priced R47 outcome (`PROPOSAL_READY` ->
  CHANGE, `HOLD_CURRENT_BOOK` -> HOLD). It does **not** map the reassessment
  owner's `CURRENT_NO_CHANGE` word to a governed decision: an intraday "nothing
  to do" is the absence of a new authoritative answer, not a new one.
  Concluding `CURRENT_NO_CHANGE` *for a session* is the session-terminal daily
  producer's prerogative.

The only intraday change is internal de-duplication: `build_intraday_candidate`
now calls the shared `_governed_identity` and `_merge_reassessment_provenance`
helpers instead of inlining them. The computed identity is byte-identical —
proven by `test_11`, which asserts the two producers' identity dicts and hashes
are equal for the same evidence.

---

## 6. Decision identity contract

`_governed_identity` is the single spelling both producers use:

```
active_book_id, eligible_market_session,
portfolio_state_hash, economic_state_hash, corporate_actions_hash,
universe_scoring_hash, universe_input_contract_hash, ranking_basis_date,
hoc_assessment_hash, hoc_artifact_id, hoc_assessment_evidence_hash,
reassessment_id, reassessment_hash,
proposal_id, proposal_hash, target_outcome
```

Deliberately **excluded**: the event-cycle run id, the DRC run id, wall clocks,
and the materiality trigger fingerprint. Two producers that reach the same
conclusion from the same evidence made the *same* decision; re-deciding it would
be churn dressed as governance. Those provenance facts are still **bound into
the record** (`evidence_provenance`) — they are simply not part of identity.

`decided_at` is the **evidence's own stamp** (the reassessment artifact's
`generated_at`, falling back to the manifest's `completed_at`), never the
writing process's wall clock. No arbitrary clock race may decide capital
authority.

---

## 7. Authority-ordering contract

```
governed_decision_ordering_key(record) =
    (eligible_market_session, decided_at, provenance_rank, candidate_identity_hash)
```

`DECISION_AUTHORITY_ORDER` states the full contract. The R54.4 additions:

4. **Producer is provenance, never authority.** DAILY_DRC and INTRADAY_EVENT are
   ordered by the same key. Neither lane wins by being a lane.
5. **Exact-tie precedence, preserved and justified.** On an exact tie of session
   *and* decision timestamp, `GOVERNED_DAILY_CYCLE` (rank 2) outranks
   `GOVERNED_INTRADAY` (rank 1), because the session-terminal cycle's evidence
   base strictly *contains* the intraday cycle's: full scoring refresh,
   opportunity cost, reassessment, proposal and forward evidence, versus a
   bounded event-driven reassessment. This is a **tie-break only** — it never
   reorders decisions that differ in time, so a later intraday decision still
   outranks an earlier daily one (`test_14`).
6. **Identical evidence identity in either lane is the same decision** — reused,
   never appended twice.

Practically: the daily cycle runs after the close (≈23:51Z) and therefore
normally carries a later `decided_at` than any same-session intraday promotion,
so it wins on timestamp rather than on rank. The rank only ever settles an exact
instant collision.

---

## 8. Idempotency contract

`record_governed_decision` compares the incoming
`candidate_identity_hash` with the standing record for the book:

| Situation | Result |
|---|---|
| Identical evidence identity already stands | `REUSED_EXISTING`, `idempotent: True`, `WR_DUPLICATE` — **no second row** |
| Candidate does not strictly outrank the standing decision | `SUPERSEDED_BY_NEWER_DECISION` — **refused** |
| Genuinely newer evidence | `CREATED` — appended, naming the prior in `supersedes_decision_id` |

This holds **across lanes**: a daily run that reproduces an intraday decision's
exact evidence identity is recognised as the same decision (`test_11`). Daily
and intraday dual production therefore cannot create two authorities for one
body of evidence.

---

## 9. Supersession contract

Supersession is an **append that names the prior record**. The prior record is
never mutated, moved or deleted:

```python
records.append(record)          # append-only; nothing above is rewritten
```

`supersedes_decision_id` / `supersedes_decided_at` carry the lineage.
`test_18` and `test_39` assert byte-equality of the pre-existing rows after a
supersession append.

---

## 10. Proposal relationship (R54.2.3.2 preserved)

| Authoritative decision | Proposal consequence |
|---|---|
| `CURRENT_NO_CHANGE` | No proposal is current. The session requested none; older proposals are **history only**; no approval action exists. |
| `HOLD_CURRENT_BOOK` | A feasible alternative was priced and rejected on its economics. No reviewable proposal; the priced target is precisely the one the system declined. |
| `CHANGE_RECOMMENDED` | The **exact** current proposal is available for MANUAL REVIEW. `manual_review_required: True`. Nothing is automatically approved. |

A governed CHANGE is a **recommendation**, never execution authority. It still
requires the operator approval token (`CONFIRM_TOKEN`) and the Stage-19
order-plan confirmation — which the governed lane cannot perform at all.

The daily gate enforces the binding both ways: a CHANGE must name a proposal
hash that matches the manifest's; a HOLD or CURRENT_NO_CHANGE must bind **none**
(binding a stale artifact to a decision that never asked for one is exactly how
the R54.2.3.2 defect was launched).

---

## 11. Legacy compatibility

**No history was rewritten and no historical row was fabricated.** The migration
is forward-going.

`project_governed_daily_cycle_decision` survives as an explicit **read-only
legacy compatibility shim** for sessions that completed before R54.4 and
therefore have no ledger row. Its output is marked:

```python
"persisted": False,
"projected": True,
"legacy_compatibility_projection": True,
"projection_note": "LEGACY (pre-R54.4): ... Not a ledger row; suppressed once a
                    persisted daily record exists."
```

`load_governed_portfolio_decision` **retires** it the moment a real daily row
exists for the same book and session:

```python
row = load_persisted_daily_decision(active_book_id=..., eligible_market_session=...)
if row:
    projection_suppressed = True
    projected = None
```

and reports `legacy_daily_projection_suppressed`. Two descriptions of one
decision must never both be candidates for authority.

Legacy intraday rows written by the pre-R54.4 writer remain readable and
orderable (`test_38`), and are correctly *not* treated as daily rows.

---

## 12. Operational-book separation

A governed decision, from either producer, **must not by itself** mutate
holdings, advance operational NAV, advance Daily Close, create orders, create
fills, or contact a broker. Every recorded row carries:

```python
"changed_holdings": False, "changed_cash": False, "changed_nav": False,
"created_orders": False, "created_order_plan": False, "created_fills": False,
"broker_enabled": False, "approved_anything": False,
"promoted_model": False, "activated_sleeve": False,
"ran_daily_close": False, "advances_operational_mark": False,
"operational_mark_advanced_only_by": "api.daily_close",
"rewrote_history": False,
```

These are **structural** properties, not runtime preferences: the audit asserts
the daily lane contains no order / fill / broker / promotion / close / scheduler
token at all (`daily_lane_execution_reach == []`), and `test_29_to_31` asserts
the only files the writer touches are its own two.

**Daily Close is unchanged and remains a separate owner and workflow.** It was
not merged with the Daily Research Cycle. `api.daily_close` defines no governed
decision surface (`test_35`).

---

## 13. UI / read-projection contract

Lane A and Lane B stay separate, and neither is decided in JavaScript.

| Lane | Content | Source |
|---|---|---|
| **A** | Standing governed portfolio decision | `pdec.load_governed_portfolio_decision` — one canonical history, whatever the producer |
| **B** | Latest live / intraday reassessment | `api.active_manager_state.live_reassessment_lane`, `advances_governed_decision: False` |

`api.active_manager_state` **reads** Lane A; it never resolves, orders or
supersedes a decision. `api.workflow_state` composes
`resolve_decision_authority` and derives no competing decision.

The producer is exposed **truthfully** and separately from authority:

```python
"current_authoritative_decision_producer": gov.get("provenance"),
"producer_label": "Daily DRC" | "Governed intraday event",
"producer_vocabulary": [...],
```

The audit asserts the UI performs no provenance comparison of its own
(`ui_derives_producer_authority == []`).

**Not done here, deliberately:** the UI does not yet *display* "Produced by:
Daily DRC / Governed intraday event". The backend now publishes it truthfully on
the canonical read, and the UI derives no authority — but rendering a new element
requires the wireframe-first UI workflow and 1920x1080 browser acceptance that
`CLAUDE.md` mandates, which is a UI slice rather than part of this backend
consolidation. Adding an unwireframed element here would violate that workflow.

---

## 14. Live verification (read-only; backend NOT restarted)

The live backend is intentionally still on the committed R54.3 runtime. All
verification below is read-only against production stores; every write went to a
fresh temp directory.

**Pure replay of the real manifest** `drc_2026-09-02_15abfb01856f`:

```
candidate word : CURRENT_NO_CHANGE          <- matches the manifest's own verdict
verdict        : DAILY_DECISION_WITHHELD (17/19)
withheld codes : HOC_ARTIFACT_NOT_PERSISTED, HOC_ARTIFACT_IDENTITY_MISMATCH
```

That is the **correct** answer, and it is R54.3's already-documented finding
rather than an R54.4 defect: the manifest's opportunity-cost artifact
`hoc_..._a162fca969c9` was never persisted by the pre-R54.3 runtime, so it is
genuinely unretrievable. Governance may not stand on a dependency it cannot
produce.

**Write path, with the artifact that DOES exist** (`hoc_..._702c599ee5b3`,
retrievability proven by its own owner):

```
verdict        : GOVERNED_DAILY_DECISION_ELIGIBLE (19/19)
recorded       : True | CREATED
record_id      : gdec_2026-09-02_alpha_paper_book_1_cc9cf245a794
decision       : CURRENT_NO_CHANGE
provenance     : GOVERNED_DAILY_CYCLE
decided_at     : 2026-09-02T23:51:50.475243Z   <- the reassessment artifact's own stamp
proposal_id/hash/target_outcome : None         <- CURRENT_NO_CHANGE binds no target
manual_review_required : False
identical replay : REUSED_EXISTING, ledger rows = 1
Lane A read    : persisted=True, producer_label="Daily DRC"
production decision root : ['decisions.json', 'index.json']  (unchanged)
```

### Five real defects the live replay found (and this release fixes)

1. **A stale live proposal leaked into a CURRENT_NO_CHANGE identity.** The first
   replay withheld on `TARGET_IDENTITY_MISMATCH` because the candidate sourced
   `proposal_hash` / `target_outcome` from the LIVE reallocation key — where
   production was holding a proposal from an earlier event cycle, while the
   manifest recorded `NOT_REQUIRED`. That is precisely the R54.2.3.2 laundering
   defect, reappearing in a new producer. **Fix:** the daily producer takes its
   proposal binding from the MANIFEST (the run's own record of what it built),
   and an unrequested proposal's hash and outcome never enter the identity.
   Regression: `test_49`.
2. **The daily lane recorded no `economic_state_hash`.** Because the daily
   producer performs no live portfolio read, that axis was empty — so identical
   evidence observed by both lanes would NOT have collapsed to one decision.
   **Fix:** `_governed_identity` falls back to the reassessment's own bound
   economic identity, which is where the evidence's economic axis actually
   lives. Regression: `test_11` (both HOLD and CHANGE).

Three further findings, also from running it for real:

3. The gate initially compared the manifest's session to the candidate's own
   session, which became tautological once the candidate was anchored to the
   manifest. It now performs the cross-owner comparison that actually has to
   hold — manifest session vs the **reassessment's** session
   (`MANIFEST_SESSION_MATCHES_EVIDENCE`).
4. **The delegation was reaching production stores from hermetic runs.** The
   daily path defaulted to `_default_portfolio_state_loader()`, rebuilt universe
   scoring via `build_universe_scoring()`, and let the reassessment / target
   reads each independently re-load the live portfolio-state document (a full
   desk-ledger replay). In a pinned-root DRC test that is a fall-through to
   exactly the store the caller pinned away from — the seam-leak class this
   codebase has been bitten by before — and it cost ~11s per cycle
   (the DRC suite went to 5 minutes). **Fix:** the daily producer performs no
   live portfolio read at all; it takes its book/session anchor from the
   manifest, receives the run's OWN scoring identity from the producer rather
   than rebuilding it, and skips the target read entirely when the manifest
   names no proposal (a run that built no target has no transition to price).
   The delegation is now ~2.6s and touches no production store. The gate still
   validates everything the reads return against the manifest, so the anchor
   grants nothing.
5. The daily gate was stamping rows with the **intraday** verdict literal
   (`GOVERNED_INTRADAY_DECISION_ELIGIBLE`), which is simply untrue of a daily
   decision and would have made a governed record misreport which gate admitted
   it. The daily producer now has its own verdict words
   (`GOVERNED_DAILY_DECISION_ELIGIBLE` / `DAILY_DECISION_WITHHELD`) in the same
   `GATE_VERDICT_VOCAB`, the intraday words are unchanged, and the writer echoes
   the refusing gate's own word. The eligibility BOOLEAN remains the single
   contract the writer enforces. Regression: `test_51`.

## 15. What remains after R54.4

The **operational-book cutover** is the next gap. A governed CHANGE is now a
first-class, immutable, ordered, provable recommendation with full lineage — but
the path from an approved governed CHANGE to a controlled paper execution
(Stage-19 order plan → fills → NAV advance) is still a separate manual workflow
that does not consume the governed decision record by id. Closing that means the
executed plan can name the exact governed decision it implements, which is what
would make the decision→execution lineage end-to-end provable.

Secondary: the legacy projection remains live for pre-R54.4 sessions. It retires
itself session by session as the daily producer writes, but it is code that
exists only for history and should be removable once no readable session
predates R54.4.

Third: the operator UI does not yet display the producer label (see §13). The
data is published; the rendering is a UI slice.

**First production side-effect to expect.** The next Daily Research Cycle that
runs on this code will create `governed_decisions.json` + `governed_index.json`
in the production decision root for the first time. That is the intended
behaviour of this release: it is a new append-only file beside the existing
manual `decisions.json`, which is not read, written or altered by the governed
lane.
