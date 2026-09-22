# R63 — GOVERNED TARGET SELECTION & MANDATORY REPAIR ALIGNMENT

**Landed:** 2026-09-21 · single agent, Windows PowerShell only, live checkout on
`stage19-controlled-rebalance` over `c55b944`.

**One sentence.** The opportunity-cost owner's ruling that a holding *must* be
repaired now binds the reallocation kernel's own mandatory tier, so a turnover
budget can never defer it; the operator can choose which reviewed target —
current, minimum repair or full target — goes to the Approve gate; and no
decision bound to a superseded market session may be selected, approved or
confirmed.

---

## What was wrong

R62 surfaced an ownership disagreement and, being read-only, reported it rather
than resolving it:

> The reallocation kernel's mandatory tier is reserved for names the universe or
> a cap cannot hold at all, so it calls a retention exit discretionary when
> ordering against the turnover budget (`mandatory_turnover = 0.0`). The review
> calls it governance-forced, because the opportunity-cost owner already ruled
> the holding outside the retention rules.

The mechanism was one line, `engine/constrained_reallocation.py:791`:

```python
mandatory_exits = sorted(tk for tk, v in current.items()
                         if v > _TOL and (tk not in caps or caps[tk] <= 0.0))
```

Mandatory was derived from **capacity**. A holding the governed retention rules
no longer admitted, but which still *had* capacity, was never in that set. It
became a discretionary leg, was ordered by score-improvement-per-unit-turnover
against a 35% budget, and — because selling a high-scoring name scores badly on
that ordering — was deferred. That is exactly how the 2026-09-18 full target
shipped holding `VLO` at 5.0% and `LH` only halved, both past the exit buffer,
while publishing `mandatory_turnover = 0.0`.

Two further gaps followed from it:

* The review could recommend the minimum repair, but the approval path only knew
  the standing proposal's full target. An operator who agreed with the review had
  no way to act on it.
* A persisted proposal stayed actionable indefinitely. The 2026-09-18 proposal
  survived the 2026-09-21 close and was still, structurally, approvable.

---

## What changed

### 1. ONE authoritative mandatory-repair contract — `engine.holding_opportunity_cost`

The verdict was always this owner's (`deterioration_state == BROKEN`). What was
missing was a **structured handoff** of it. The contract
(`mandatory_repair_obligations.v1`) lives on the deepest leaf module, so the
proposal builder, the constraint kernel and the review can all read one
interpretation:

| Field | Meaning |
|---|---|
| `instrument_id` / `ticker` | the instrument, spelled for any asset class |
| `asset_class`, `sleeve_id` | carried, never assumed |
| `tier` / `obligation_type` | `HARD_CONSTRAINT_VIOLATION` or `GOVERNANCE_RETENTION_FAILURE` |
| `reason_code` | `UNIVERSE_INELIGIBILITY` / `RETENTION_RULE_FAILURE` / `LIQUIDITY_REPAIR` … |
| `source_owner` | the module that **decided** it |
| `required_action` | `EXIT_TO_ZERO` / `REDUCE_TO_LIMIT` / `NOT_SIZEABLE_FROM_PERSISTED_EVIDENCE` |
| `current_weight`, `max_valid_weight`, `required_exit` | the largest weight that still satisfies it |
| `evidence_id`, `evidence_hash` | the assessment it came from |

It invents no new mandatory condition: every row is produced from a verdict an
owner already published. `NOT_SIZEABLE` is an honest third state — the owner
names the obligation but publishes no compliant weight, so a consumer may never
promote it to an exit.

R62's own classifier now **delegates** to this contract (`_retention_obligation`
and `_liquidity_obligation` are adapters), and its tier / reason / repair-action
vocabularies are re-exported rather than forked — the house idiom already used by
`engine.portfolio_reassessment`.

### 2. The obligations enter the EXISTING mandatory tier — no second budget

`solve_feasible_target(..., mandatory_obligations=...)`. The constraint kernel
stays a pure leaf: it **receives** the owner's decision as data and never fetches
or re-derives it. An obligation binds through the ceiling mechanism that already
existed for risk-contribution repair rounds, so the whole solve — placement,
redistribution, budget — respects it without a new code path.

The consequence is the release:

```
1. identify the authoritative obligations
2. apply the minimum valid repair FIRST          (mandatory legs, unconditionally)
3. measure mandatory turnover
4. allocate what REMAINS of the budget to discretionary opportunity trades
```

When the repairs alone exceed the budget the kernel keeps them and defers every
discretionary trade, publishing `turnover_budget_subordinated_to_mandatory_repair`,
`mandatory_turnover`, `normal_turnover_budget` and
`excess_required_by_mandatory_repair`. That subordination behaviour *already
existed* for capacity-mandated exits; R63 widened what qualifies, it did not add
a mechanism.

Attribution is kept separable — `capacity_mandatory_exits` vs
`governance_mandatory_exits`, and `mandatory_basis` on every leg — from a
snapshot of the caps taken **before** any obligation ceiling is applied. Without
that snapshot a forced exit zeroes the cap and is then reported as a capacity
exit, and the kernel would take credit for a decision an upstream owner made.

### 3. A target is not reviewable while it leaves an obligation open

Two fail-closed points:

* `engine.reallocation_proposal` — an open obligation is now itself a reason to
  run the repair (previously the repair ran only on a measured limit breach, so a
  feasible ideal target holding a BROKEN name was published unrepaired), and
  `approvable` gains a third condition. The proposal publishes
  `mandatory_repair` and `full_target_reviewable`.
* `engine.proposal_decision_review` — rung 3 of the verdict ladder is gated.
  `obligations_left_open_by_full_target` was already computed and was explicitly
  documented as *an observation that never changes the verdict*. It now binds,
  and the fall-through is the repair, never the unrepaired target.

Satisfaction is judged on the **resulting book**, never by matching trades — R62's
finding that the `DDOG` breach closed by composition, without that name ever
being traded, is preserved.

### 4. Governed target selection — between Review and Approve

`api.portfolio_decision` (the existing governance owner) gains a
`proposal_review_selection` artifact in the same append-only ledger root. It is a
GOVERNANCE artifact, not a second optimisation: the three targets come from the
review, which derived them from the immutable proposal.

```
Review → SELECT TARGET → Approve selected target → Confirm order plan → next close
```

Each gate stays independent: selecting does not approve, approving does not
confirm, confirming does not execute. A selection carries its own token
(`CONFIRM_PORTFOLIO_TARGET_SELECTION`), distinct from the approval token, so
neither can be replayed as the other. It binds `proposal_hash`, `review_hash`,
`hoc_assessment_hash`, the session, the book, the target's own hash and the
economics the operator was shown. It is idempotent; a conflicting reselection is
an audited revision that preserves both immutable records.

The R62 review had **no identity** — it is a pure, unpersisted projection — so
`api.proposal_decision_review` now publishes `review_hash` over the payload it
just returned. That is sound precisely because the projection is byte-stable for
the same inputs.

Approval consumes exactly the selection: it may not silently fall back to the
standing full target, and a selection of `CURRENT` has no target to approve.

Selectability is decided by the **backend** and published with its reason. The
browser renders the verdict; it never computes one. (The UI's hard-coded
verdict→recommended-target map, a second copy of a rule the kernel owns, was
replaced by the backend value.)

### 5. Decision freshness — a persisted proposal is not actionable forever

No second calendar, session authority or clock. The latest actionable session is
asked of the one owner that already computes it:

```
engine.exchange_calendar → engine.market_session → api.data_freshness → api.workflow_state
                                                    action_session_market_date
```

`action_session_market_date` is the right value rather than raw
`eligible_market_date`: during a catch-up it is the **oldest unclosed completed
session**, which is the one the operator must actually run.

`decision_freshness()` is a pure date comparison and fails closed in both
directions — an unknown session on either side, or a bound session *ahead* of the
world, is `UNVERIFIABLE` and not actionable, because "we could not tell" must
never read as "yes". The gate binds at target selection, at approval (ahead of
the economic guards, because reporting "no material change" about a superseded
session answers a question nobody asked) and at order-plan confirmation. It is
deliberately **not** enforced when hydrating a mark, which commits no capital.

The stale proposal is not rewritten, regenerated, rejected or superseded to
satisfy the gate. Only the ability to ACT on it expires.

---

## What was reused rather than rebuilt

| Reused | For |
|---|---|
| `engine.holding_opportunity_cost` | the BROKEN / eligibility / liquidity verdicts — R63 projects them, it does not re-decide them |
| `engine.constrained_reallocation` | the mandatory tier, `weight_ceilings`, budget subordination, `switching_economics` |
| `engine.reallocation_proposal` | the repair rounds, the cost model, `proposal_hash` |
| `engine.proposal_decision_review` | CURRENT / MINIMUM_REPAIR / FULL_TARGET, the verdict ladder |
| `api.portfolio_decision` | the append-only idempotent ledger, binding, the approval gate |
| `api.workflow_state` → `engine.market_session` | the latest actionable session |

No second optimizer, repair engine, review engine, risk engine, proposal store,
evidence store, calendar or clock. A governance exit is automatically exempt from
the economic hurdle too, because `switching_economics` derives `mandatory_only`
from the set the caller passes — consistent with the reassessment owner's
existing `MANDATORY_EXIT_OVERRIDES`.

One audit invariant was **strengthened**, not relaxed:
`check_decision_supersession` asserted the lane state was in the vocabulary by
matching the literal `"PDS_SUPERSEDED, PDS_UNAVAILABLE)"`. It now parses
`DECISION_STATE_VOCAB` and checks membership, so a later release may add a state
without silently disarming the invariant.

---

## The Sep-18 acceptance case

Read from the live persisted proposal
`reap_2026-09-18_alpha_paper_book_1_9bd6e73a2ef6`, unchanged and unapproved. The
Portfolio Cycle was **not** rerun and the proposal was **not** regenerated.

| | CURRENT | MINIMUM REPAIR | FULL TARGET |
|---|---|---|---|
| Positions | 25 | 14 | 24 |
| One-way turnover | 0.0% | 21.6% | 35.0% |
| Estimated cost | $0.00 | $52.99 | $86.06 |
| Cash | 4.6% | 47.7% | 5.2% |
| Obligations left open | 12 | 0 | **2 (LH, VLO)** |
| Selectable on merits | yes | yes | **NO** |
| Selectable in fact | **NO** | **NO** | **NO** |

* Verdict unchanged: `MINIMAL_REPAIR_PREFERRED`, now carrying
  `FULL_TARGET_NOT_REVIEWABLE_OBLIGATIONS_OPEN` and
  `UNRESOLVED_RETENTION_EXIT_BUFFER`.
* The full target is refused on its merits with the exact blocker:
  *2 mandatory repair obligation(s) remain unresolved: LH, VLO.*
* Every target is then additionally refused by the session gate: the proposal is
  bound to `2026-09-18` and the latest eligible session is `2026-09-21`.
  `NEXT_REQUIRED_ACTION = RUN_PORTFOLIO_CYCLE`.

Both refusals are shown, because they are different facts: one is what the old
behaviour produced, the other is that the world has moved on. Nothing was
selected and nothing was approved; zero orders and zero fills exist.

---

## Safety

`runtime_llm_dependency = NONE`. Identifying a mandatory change, deciding
selectability, selecting a target, approving it and building the order plan are
all deterministic backend state; no language model, prompt, agent call or network
request is in any of those paths.

Selection creates no order plan, no order and no fill. Approval still creates
nothing until the separate order-plan confirmation. Execution remains the
existing next-close paper process. No Create Orders, no automation. The R59
persistent research runtime remains intentionally STOPPED and was not touched.

**Tests:** `tests/test_r63_governed_target_selection.py` (65).

---

## Browser acceptance (1920x1080)

PASS. The selection block renders between *What the proposal changes* and
*Historical evidence*; all three buttons carry non-blank labels
(`Keep current / Defer`, `Select minimum repair`, `Select full target`), all three
are `disabled` from the backend flag, each shows its blocker in the backend's own
words, `MINIMUM_REPAIR` carries `RECOMMENDED` and nothing is pre-selected. Daily
Plan measured at 1.37 screens (no page-level scroll). No `alert()`/`confirm()`
(verified by wrapping `window.alert`/`confirm`/`prompt` before navigation — the
recorder stayed empty). No enabled Create Orders control and no automation toggle;
automation appears only as a read-only `AUTOMATION OFF` badge. Diagnostics remain
behind Audit / Advanced. 103 network requests inspected, every one a GET — nothing
was written and the prediction service was never invoked locally. Only console
error was a 404 on `favicon.ico`.

Three findings from that pass were fixed here rather than deferred:

* An operator-precedence bug in the disabled tooltip: `+` binds tighter than
  `||`, so `'Not selectable. ' + detail || fallback` made the left operand the
  truthy `"Not selectable. undefined"` and the fallback could never fire.
* The handler is now attached ONLY when the backend says the target is
  selectable, and `_pdrSelect` re-reads selectability before it sends. A
  non-selectable option carries no path to the write at all, instead of relying
  on one `disabled` attribute.
* Actionability moved to the TOP of the card. The block was the last thing on a
  2.8-screen sub-tab, so an operator who came to choose a target had to scroll
  past the whole comparison to learn that nothing was selectable.

### One pre-existing incoherence, NOT fixed here

On the same Reallocation tab, below the R63 block, the `CHANGES` card reads *"NO
CURRENT REALLOCATION TARGET — No feasible target exists for the eligible
session"* and `TARGET` reads `POSITIONS 0`, while immediately beneath it
`ECONOMICS` is fully populated (`TURNOVER 35.0% · ESTIMATED COST $86.06 · 25 → 24
positions`). The same screen says both *no target exists* and *here is the
target's economics*. Those cards read different owners
(`/v1/operations/constrained-reallocation` vs `/v1/operations/zero-base-target`)
and R63 touched neither. It is recorded here as a follow-up rather than repaired,
because fixing it means changing read owners this release has no mandate over.

---

## Two things worth recording about the build

* **A forced ceiling destroys its own attribution.** Setting an obligation's
  `max_valid_weight = 0` zeroes the cap, after which the capacity rule claims the
  exit and `governance_mandatory_exits` comes back empty — the kernel would take
  credit for a decision an upstream owner made. The caps snapshot has to be taken
  *before* any obligation ceiling is applied.
* **The session gate sits AFTER the structural and economic guards, deliberately.**
  Placing it first made `record_decision` answer `PROPOSAL_SESSION_STALE` where
  thirteen established tests expected the more specific `CHANGE_CANDIDATE_WITHHELD`,
  `HOLD_CURRENT_BOOK` or `NO_MATERIAL_CHANGE`. Those refusals are properties of the
  proposal itself and are true in every session, so they are the better answer.
  Safety is identical either way — every one of those paths refuses and writes
  nothing — and nothing can reach a write without passing the gate.
