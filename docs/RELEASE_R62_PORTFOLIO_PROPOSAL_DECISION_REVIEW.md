# R62 — PORTFOLIO PROPOSAL DECISION REVIEW

**Landed:** 2026-09-21 · single agent, Windows PowerShell only, live checkout on
`stage19-controlled-rebalance` over `5e2aba3`.

**One sentence.** Paper Trader now adjudicates its own reallocation proposal —
comparing *doing nothing*, *the minimum constraint repair* and *the full
zero-base target*, splitting every proposed change into forced and
discretionary, pricing what the full target adds over the repair, and
recommending a review path in plain English — deterministically, before the
Approve gate, with no language model anywhere in the runtime path.

---

## What was missing

Every fact an operator needs already existed, in eight different read models.
The proposal said *35.0% turnover, $86.06, +0.056 against a 0.050 hurdle*. The
opportunity-cost assessment said *DDOG breaches the risk-contribution cap and
eleven holdings have fallen past the exit buffer*. The outcome store held 1,025
matured observations. Nothing combined them, so the last mile was done by hand —
or by asking a language model to read the JSON.

That is the defect this release closes. It answers:

> Given the current portfolio, the proposal, its costs, its constraints, the
> evidence accumulated from previous decisions and the available alternatives,
> **what should the operator review?**

---

## What was added

### `engine/proposal_decision_review.py` — the pure adjudication kernel

Not an optimiser and not a second proposal engine. It performs no allocation
search, proposes no holding, deploys no capital and introduces no threshold.

**Three comparable states.**

| State | How it is produced |
|---|---|
| `CURRENT` (do nothing) | read **verbatim** from the immutable proposal artifact |
| `MINIMUM_REPAIR` | derived here, from canonical primitives only |
| `FULL_TARGET` (the proposal) | read **verbatim** from the immutable proposal artifact |

**The repair scope** (`proposal_repair_scope.v1`) has two tiers, and *each tier's
verdict is owned elsewhere*:

* `HARD_CONSTRAINT_VIOLATION` — a mandatory limit the current weights breach.
  Weight arithmetic is judged by `engine.constrained_reallocation.verify_feasibility`
  (the constraint kernel's own independent verifier); the per-name
  risk-contribution limit is judged by the `engine.holding_opportunity_cost`
  contract, because that verifier explicitly declares it cannot check that one.
* `GOVERNANCE_RETENTION_FAILURE` — a holding the opportunity-cost decision policy
  has already classified `BROKEN`: past the exit buffer (`RETENTION_RULE_FAILURE`)
  or outside the eligible universe (`UNIVERSE_INELIGIBILITY`).

**The repair rule.** Exit what the rules no longer admit; reduce what breaches a
limit to that limit — for the risk contribution using the *identical* first-order
rule the reallocation kernel applies (`w × limit / share`), re-measured by the
canonical risk owner after each round exactly as that kernel does. **Everything
released goes to cash.** A repair restores validity; the global allocator owns
capital deployment (Release-32 design rule), so a repair may never quietly become
an alpha optimisation. It never adds a position and never increases one.

**Change classification.** Exactly one primary reason per change, resolved in a
declared order: an obligation on that name → a kernel constraint adjustment that
*reduced* it → an opportunity an owner already named → a constraint
re-optimisation of a retained holding → other. A constraint that merely *limited
the size* of an increase is recorded as `constraint_limited_size` and never
promoted to the reason **for** the change.

**Marginal economics.** The increment is the step *from the repaired book to the
full target* — the trading actually added — priced with the proposal owner's own
cost model, and judged against the **existing frozen switching hurdle**. The
review introduces no threshold of its own (`new_threshold_introduced: false`).

**The verdict ladder**, declared before any proposal was scored through it and
total over its inputs:

1. not reviewable / true blocker / repair unverifiable → `BLOCKED_CONSTRAINT_OR_DATA`
2. nothing obliges a change and nothing material is proposed → `NO_CHANGE_REQUIRED`
3. the increment clears the hurdle → `FULL_TARGET_REVIEWABLE`
4. an obligation is open and the increment does not clear → `MINIMAL_REPAIR_PREFERRED`
5. nothing obliges a change and it does not clear → `DEFER_WEAK_INCREMENTAL_EDGE`

When no repair is required the repaired book **is** the current book, so the
increment and the whole switch are the same object and the review agrees with the
proposal owner's own hurdle verdict by construction.

### `api/proposal_decision_review.py` — the read owner

`GET /v1/operations/proposal-decision-review`. Read-only, degrade-safe,
idempotent. It sources the standing proposal's **read state** from the proposal
read contract and its **facts** from the immutable artifact (the flattened read
view does not republish every block — reading it would have shown an empty
after-target breach list where the artifact holds a measured one), the
opportunity-cost assessment **bound by hash** rather than "the latest", the
matured outcome evidence, and the owned return panel. It writes nothing.

### UI — before the Approve gate

The Reallocation / Manual Review screen now renders, between Decision and
Changes: the verdict, the three states side by side at 1920×1080, the plain
paragraphs, the increment, the forced/discretionary split, and the relevant
matured evidence. No new action, no dialog, no approval affordance.

---

## What was reused rather than rebuilt

| Reused | For |
|---|---|
| `engine.constrained_reallocation` | constraint inventory, `name_caps`, `candidate_meta`, `verify_feasibility`, `one_way_turnover`, `weighted_score` |
| `engine.holding_opportunity_cost` | the risk-contribution contract (field, 3/N limit, breaches), the covariance kernel, retention and liquidity verdicts |
| `engine.reallocation_proposal` | annualised portfolio volatility, the coverage gate, concentration, the turnover / transaction-cost model |
| `engine.reassessment_outcomes` | the matured decision evidence |

Proof that these are the same code and not a fork: the canonical primitives
reproduce the persisted artifact's own published numbers to the last digit —
score `0.8532402813` vs published `0.85324`, turnover `0.35`, HHI
`0.03728952715` vs `0.03729`, volatility `0.11536031662` vs `0.11536`, DDOG risk
contribution `0.1287262521` vs `0.128726`.

Two owners gained **public aliases** for primitives they already owned
(`reallocation_proposal.portfolio_volatility` / `effective_volatility` /
`herfindahl` / `largest_weight` / `turnover_and_cost`,
`constrained_reallocation.weighted_score`). Aliases only — no behaviour added, no
existing caller changed.

No second proposal engine, opportunity-cost engine, risk engine or evidence
store. The Sep-18 proposal was not regenerated.

---

## Three things the review says that nobody had noticed

1. **The full target does not close every obligation it was built for.** `VLO` is
   retained at 5.0% and `LH` only halved, both past the exit buffer, because the
   turnover budget deferred thirteen trades. The proposal never said so.
2. **The risk-contribution breach is closed by composition, not by a trade.**
   `DDOG` is never traded, yet its share falls from 0.1287 (limit 0.12 over 25
   names) to 0.1129 (limit 0.125 over 24). Judging satisfaction by matching
   trades would have called this unrepaired — so satisfaction is judged on the
   **resulting book**.
3. **The two owners disagree about what is mandatory, and the review says so.**
   The reallocation kernel's mandatory tier is reserved for names the universe or
   a cap cannot hold at all, so it calls a retention exit discretionary when
   ordering against the turnover budget (`mandatory_turnover = 0.0`). The review
   calls it governance-forced, because the opportunity-cost owner already ruled
   the holding outside the retention rules. Both owners are named on every row.

---

## Honest limitations, declared in the payload

* **The score cannot settle the comparison on its own.** The canonical portfolio
  score is normalised over *invested* weight, and the minimum repair holds 47.7%
  cash against the full target's 5.2%. Cash has no percentile in the eligible
  universe and this review will not invent one for it, so any comparison across a
  materially different invested weight carries
  `SCORE_BASIS_EXCLUDES_UNINVESTED_CAPITAL` and says plainly that expected return
  is `NOT_CALIBRATED`. The decision basis stays the canonical one; inventing a
  percentile for cash would have been a new convention no owner declares.
* **Volatility is published on both bases.** The canonical invested-sleeve figure
  (comparable to the proposal's own before/after) and a capital basis — the
  invested sleeve scaled by the invested share, under the stated and true
  assumption that cash carries no variance and no covariance — so a state holding
  48% cash is not reported as if it were fully invested.
* **The review fails closed.** A repair whose resulting risk was never measured,
  or that cannot close every obligation (an `ILLIQUID` holding whose compliant
  weight the persisted evidence cannot size), returns
  `BLOCKED_CONSTRAINT_OR_DATA` rather than a recommendation nobody can specify.
* **Evidence informs and never overrides.** Adverse replacement evidence and
  control regret are surfaced as cautions; the verdict is byte-identical with
  favourable and adverse evidence, and the canonical policy-intelligence owner
  itself publishes `recommends_manual_review_only`.

---

## The Sep-18 acceptance case

Read from the live persisted proposal `reap_2026-09-18_alpha_paper_book_1_9bd6e73a2ef6`
(`READY` / `PROPOSAL_REVIEW_REQUIRED`), unchanged and unapproved.

| | CURRENT | MINIMUM REPAIR | FULL TARGET |
|---|---|---|---|
| Positions | 25 | 14 | 24 |
| Changes | 0 | 11 | 24 |
| One-way turnover | 0.0% | 21.6% | 35.0% |
| Estimated cost | $0.00 | $52.99 | $86.06 |
| Score | 0.8532 | 0.9344 | 0.9265 |
| Improvement net of cost | 0.0000 | 0.0704 | 0.0558 |
| Volatility (capital basis) | 11.0% | 8.1% | 12.4% |
| Volatility (invested sleeve) | 11.5% | 15.5% | 13.0% |
| Concentration | 0.0373 | 0.0202 | 0.0387 |
| Cash | 4.6% | 47.7% | 5.2% |
| Open obligations | 12 | 0 | 2 (LH, VLO) |

* **12 obligations**: 1 hard (`RISK_CONTRIBUTION_CAP`, DDOG 0.1287 > 0.12) and 11
  governance retention failures (AIZ CAT CVS DVA EOG ITW KEYS LH MNST SPG VLO).
* **10 mandatory / 14 discretionary** changes (10 × `RETENTION_RULE_FAILURE`,
  10 × `OPPORTUNITY_IMPROVEMENT`, 4 × `PORTFOLIO_REOPTIMIZATION`).
* **Increment of full target over repair**: +21.2% one-way turnover, +$52.22
  cost, **−0.008** score, **−0.019** net of cost, against a 0.050 hurdle —
  missed by 0.069. Risk delta +4.23% on the capital basis.
* **Evidence**: `HORIZON_ALIGNED_EVIDENCE`, 75 matured at H20. Replacements
  0 win / 6 loss (mean spread −0.1806). Holds 29/30. Exits 1 avoided / 1 missed.
  `CHURN_COOLDOWN_ACTIVE` withheld 31 actions, 18 of which would have helped.
* **Verdict**: `MINIMAL_REPAIR_PREFERRED`.

---

## Safety

`runtime_llm_dependency = NONE`. No LLM call, no prompt, no network in the
runtime path. The verdict is a recommendation for **manual review**: it approves
nothing, rejects nothing, supersedes nothing, creates no order plan, confirms
nothing, executes nothing, creates no order and no fill, promotes no model,
deploys no capital and mutates no artifact. The gate sequence is unchanged:

    Review → Approve → Confirm order plan → Await next close → Paper execution

This release adds intelligence **before** Approve and weakens none of it. The
Sep-18 proposal is still `READY` / `PROPOSAL_REVIEW_REQUIRED`, no decision
recorded; zero orders and zero fills were created.

**Tests:** `tests/test_r62_proposal_decision_review.py` (49).
