# Research Campaign Contract

> Canonical, durable statement of how a Paper Trader model-research campaign is
> bounded. Read with [PROJECT_CHARTER.md](PROJECT_CHARTER.md) ·
> [MATHEMATICAL_ALPHA_FRONTIER.md](MATHEMATICAL_ALPHA_FRONTIER.md) ·
> [ASSET_PRICING_MATHEMATICS_LIBRARY.md](ASSET_PRICING_MATHEMATICS_LIBRARY.md).
>
> Owner: `alpha_agent/r31/contract.py`. Enforcement:
> `alpha_agent/r31/registry.py`, `alpha_agent/r31/lockbox.py`,
> `scripts/audit_architecture.py::check_release31_mathematical_alpha_frontier`.

## Why a contract exists at all

A campaign that may widen its budget, move its lockbox boundary or relax its
superiority bar **after seeing a disappointing result** is not running an
experiment. It is searching until it finds something, and whatever it finds is a
property of the search rather than of the market.

So every term below is frozen **before the first candidate result exists**, and
every budget is a **number in a module**, checked by code that raises. A budget
in a document is a suggestion.

If a material term changes, that is a **new campaign with a new id** — never an
edit. `registry.assert_contract_stable()` recomputes the contract hash before
every stage and raises `ContractDrift` if it moved while results existed.

## The thirteen frozen sections

| Section | What it fixes |
|---|---|
| **identity** | campaign id, creation timestamp, git HEAD, research root |
| **data sources** | every input file with its **content** SHA-256, not its mtime |
| **sample geometry** | decision-date definition, step, minimum history, minimum cross-section, label horizons, target |
| **universe policy** | the declared TRAINING universes, the single EVALUATION universe, and the investment-universe hash |
| **benchmark policy** | both benchmarks, the benchmark-set hash, and "substitution is not permitted" |
| **architecture policy** | the two tracks, the calibration owner and the allocation owner |
| **evidence partition** | layer fractions, purge/embargo rule, minimum dates per layer, "no random split", "calibration may read DISCOVERY only" |
| **economics** | the canonical policy / allocator / covariance OWNERS and the keys consumed; cash policy; cost base; turnover alignment; the pre-registered γ risk frontier; the covariance-cache key |
| **budgets** | families, configurations, novel campaigns, refinement depth, lockbox accesses, literature limits, and the executed grid |
| **lockbox policy** | one execution per candidate, finalists frozen first, no redesign-and-retry, invisible to calibration |
| **multiple testing** | policy, FDR *q*, bootstrap resamples and block length, "the denominator is every executed candidate", "superseded campaigns are not in it" |
| **superiority** | the bar a candidate must clear to be eligible for manual paper review |
| **inadmissible information** | the families the campaign refuses as predictors, and why |

Three of these are new in Campaign v3, and each exists because v2 left the
corresponding term in prose. A term a contract merely *describes* can drift away
from the code silently; a term it **hashes** cannot. The universe, benchmark,
judge, calibration and covariance-cache identities are therefore passed into
`contract.build()` as arguments rather than imported, so the contract binds the
exact evidence semantics a candidate will be measured under.

## The budgets

| Budget | Value | Enforced by |
|---|---|---|
| known-method families | 12 | `Registry.check_budget` |
| known-method configurations | 240 | `Registry.check_budget` |
| configurations per family | 40 | `Registry.check_budget` |
| novel families | 6 | `Registry.check_budget` |
| novel campaigns | 2 | `Registry.check_budget` |
| novel candidates per campaign | 150 | `Registry.check_budget` |
| novel candidates total | 300 | `Registry.check_budget` |
| novel refinement depth | 3 | `Registry.check_budget` |
| lockbox finalists | 12 | `lockbox.freeze_finalists` |
| lockbox finalists per family | 2 | `lockbox.freeze_finalists` / `authorise` |
| papers screened | 60 | literature registry |
| methods deeply extracted | 24 | literature registry |

**Benchmarks are exempt from the CANDIDATE budgets.** The incumbent and the
transparent references are not part of the search, and charging the search budget
for measuring them would create an incentive to measure fewer baselines.

### Budgets are ceilings, not targets

The contract's own rule is that configurations are executed for what they test,
never to consume a budget. Campaign v3's *executed* grid is far below every
ceiling above — 31 known configurations across all 12 families, one
training-universe variant per family, 30 novel candidates per campaign — because
the v3 judge allocates capital through the canonical optimiser at every decision
date and therefore costs minutes per candidate rather than milliseconds.

Points were dropped only where a grid was dense in a direction the family is
insensitive to (three neighbouring ridge penalties test one hypothesis, not
three) and never where two configurations express materially different
structure; penalties stay log-spaced so the retained points span the same range.
A smaller *pre-registered* search is also statistically stronger, because it
carries a smaller multiple-testing denominator. The executed grid is recorded
inside the frozen contract, so "we ran fewer" is itself part of the hashed
record rather than a later claim.

## Idempotency

The **specification hash** is the key. It binds the family, the parameters, the
sample, the horizon, the feature list, the seed, the refit cadence, the training
universe, the investment-universe hash, the benchmark hash, the judge's behaviour
hash (which itself binds the calibration contract), **and** the snapshot and
partition hashes — so the same model against different evidence, a different
universe, a different benchmark set or a different judge is a different candidate
rather than a silent overwrite.

This is also what makes Campaign v1 and v2 structurally inert: their hashes were
computed under a different judge and without a universe or benchmark hash at all,
so no earlier row can collide with a v3 key.

A resumed campaign re-derives the hash and returns the stored result. Re-running
a candidate would corrupt the multiple-testing denominator as well as waste time,
which is why `Registry.record` raises `DuplicateCandidate` rather than appending.

## The denominator that cannot shrink

Every candidate that **executes** is recorded, including the ones that failed,
errored, or were rejected. `executed_count` is derived from the append-only log,
not from a curated list.

This is not bookkeeping fussiness. Quietly dropping disappointing candidates from
the count is the single most effective way to manufacture an honest-looking
*t*-statistic, and it leaves no trace in the surviving evidence.

## The evidence partition

```
DISCOVERY  →  candidate and model development
  (embargo)
VALIDATION →  family and hyperparameter SELECTION
  (embargo)
LOCKBOX    →  finalists only, touched once
TRUE_FORWARD → post-deployment; owned elsewhere, never synthesised here
```

* **LOCKBOX is the LATEST contiguous block.** A lockbox drawn from the middle of
  history lets a model be selected on data that comes *after* it — the most
  common way a "held-out" result turns out to be in-sample.
* **Every adjacent boundary carries an embargo** of `ceil(horizon / step)`
  decision dates, belonging to no layer. Without it a 60-session label struck at
  date *k* still resolves during *k+1…k+3*, and those rows would appear in two
  layers at once.
* **Training is capped at the last validation date.** No model in the campaign
  ever trains on a lockbox row — not during selection, and not during the lockbox
  evaluation itself, where the model scored is the single final model fitted on
  discovery plus validation.
* If a sample cannot support the declared minimums, the state is
  `POINT_IN_TIME_EVIDENCE_BLOCKED`. A lockbox is never manufactured by shrinking
  the bar.

## The lockbox

Frozen finalist set, hashed, **before** the first execution. Each finalist runs
exactly once, logged with its timestamp and spec hash.

A candidate that fails may not be revised and resubmitted. `lockbox.authorise`
refuses a spec hash it has already served **and** refuses a *new* spec hash from
a family whose attempts are spent — which is the loophole a "small fix and retry"
would otherwise take. A campaign that could retry the lockbox would be using it
as a validation set with extra steps.

## Terminal states

Exactly one primary verdict:

```
R31_KNOWN_METHOD_SUPERIOR_MODEL_FOUND      + MODEL_READY_FOR_MANUAL_PAPER_REVIEW
R31_NOVEL_ALPHA_SUPERIOR_MODEL_FOUND       + MODEL_READY_FOR_MANUAL_PAPER_REVIEW
R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED
R31_NEW_ORTHOGONAL_DATA_REQUIRED
R31_POINT_IN_TIME_EVIDENCE_BLOCKED
R31_RESOURCE_BUDGET_EXHAUSTED
```

Two consecutive null novel campaigns trigger exhaustion. **Exhaustion is a
successful research conclusion**, not a failure, and it explicitly forbids
extending the budget in response to a poor result.

## Safety, permanently

The research lane creates no signal authority, no target, no proposal, no
decision and no order; it writes no operational store; and
`AUTOMATIC_PROMOTION_ALLOWED` is `False`. A winning candidate is packaged for
**manual paper review** and is never activated. All fifteen of these properties
are blocking invariants in the architecture audit.

## Release 32 amendment — the contract generalised beyond one asset class

Release 32 applies this same contract to six strategy sleeves rather than one
model family. Four terms are added, each because a Release-32 campaign was
superseded for getting it wrong:

- **The control is declared with the judge.** A campaign must state what a
  candidate is compared against BEFORE it runs. Excess over cash may be
  reported but may never rank, select or qualify: over a long window every
  strategy holding equities beats bills, so that statistic measures exposure
  rather than skill. Campaign v1 ranked on it and reported a qualified sleeve
  whose every lockbox result lost to buy-and-hold.
- **A control configuration may never become a finalist.** It is executed, it
  counts in the denominator, and it is reported — but it exists to be compared
  against, not to win.
- **A cross-candidate comparison must name its shared decision calendar.**
  Candidates measured on disjoint calendars are being compared across eras. The
  shared-calendar view is REPORTING ONLY: it cannot qualify anything and does
  not enter the denominator, or it becomes a second window to choose from.
- **Sleeves never own capital.** A campaign may measure a research book to
  obtain economics; that book is a measurement device and carries
  `research_book_is_not_a_portfolio_target` in every artifact.

The supersession rule is unchanged and was exercised three times in Release 32:
a material change is a NEW campaign id, never an edit to a frozen artifact, and
the superseded campaigns stay on disk with their defects recorded. Where only
the REPORTING changed and the judge behaviour hash did not, the successor must
reproduce the predecessor's per-candidate numbers exactly — which is the
evidence that a supersession was a reporting fix rather than a quiet re-measure.
