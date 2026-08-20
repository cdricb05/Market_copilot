# Release 31 — Campaign V3 Correction

**Status:** authoritative for `r31_mathematical_alpha_frontier_v3`
**Supersedes:** `r31_mathematical_alpha_frontier_v1`, `r31_mathematical_alpha_frontier_v2`
**Production impact:** none. Release 31 is read-only throughout.

---

## Why a third campaign exists

A research campaign is only worth what its judge is worth. Campaign v1 and
Campaign v2 were both stopped before producing a verdict, because in each case
the judge was measuring something other than the business question.

Neither campaign's evidence has been deleted. Both remain on disk under their own
campaign ids, marked `SUPERSEDED_EXPERIMENTAL_DESIGN`, and neither can influence
v3 — not by convention, but structurally: every v3 candidate's specification hash
binds the investment-universe hash, the benchmark hash and the judge's behaviour
hash, none of which existed in the v2 keyspace. A v2 row cannot collide with a v3
key, so it cannot enter a v3 leaderboard, a v3 lockbox, or the v3
multiple-testing denominator.

### Campaign v1 — transaction-cost accounting defect

The judge charged the canonical per-side cost on **one-way turnover** while
reporting the drag on both sides. Every net return it measured understated
transaction cost by roughly half. Superseded before any verdict.

### Campaign v2 — four defects

1. **The evaluation universe was the training panel.** Portfolio decisions were
   scored over whatever the Norgate Russell 1000 panel contained. That is an
   excellent survivorship-safe *training* sample and a poor statement of the
   business objective, which is S&P 500 capital allocation. A model that wins on
   Russell 1000 has not been shown to win on the book we actually manage.

2. **The primary judge could not express a cash decision.** Books were the top
   *N* names at roughly equal weight, *N* ∈ {15, 25, 40}, cash pinned to zero. A
   model that found nothing worth owning was still made to own 25 names, so the
   judge could not tell "no opportunity" from "twenty-five good names". Book size
   also stood in for risk appetite, which it is not: a 15-name book and a 40-name
   book are both fully invested by construction.

3. **Track B compared portfolios by array position.** The direct-portfolio
   learner's turnover term compared consecutive weight vectors positionally
   whenever their lengths matched. Row order is an artifact of cross-section
   assembly; index membership changes month to month and names delist. Position
   *i* is simply not the same company on two dates, so a book that sold
   everything and bought something else entirely was scored as having done
   nothing — and a learner rewarded for low turnover learns to exploit that
   fiction rather than to trade less.

4. **The investable benchmark was silently replaced.** "Did this beat an
   equal-weight basket of the same names we screened?" and "did this beat buying
   the index?" are different questions, and only the second is the one an
   operator faces. v2 answered the first and reported it in the second's place.

---

## What v3 changes

### Correction 1 — training universe ≠ investment universe

Owner: [`alpha_agent/r31/universe.py`](../alpha_agent/r31/universe.py)

| Concept | Meaning |
|---|---|
| `TRAIN_S_AND_P_500_ONLY` | fit only on index members on the fitting date |
| `TRAIN_RUSSELL1000_PIT` | fit on the broad point-in-time panel |
| `EVALUATE_S_AND_P_500_PIT_MEMBERS_ONLY` | what the judge may let a candidate **own** |

Membership comes from `norgatedata.index_constituent_timeseries` over the
**"S&P 500 Current & Past"** watchlist — 1,897 securities against 503 current —
so a name removed in 2007 keeps its history and a name added last year is
correctly absent from 2005. The mask is aligned by **date string**, never by row
position, because the vendor calendar and the panel calendar differ.

Which training universe a candidate used is part of its specification hash, so
*"train broad, invest narrow"* is a hypothesis the campaign **tests** and pays
for in the multiple-testing denominator, rather than an accident of which panel
was loaded. A broader training choice never widens evaluation.

**Measured, not assumed.** The frozen panel is a Russell 1000 history, so it
cannot represent every name that was ever an S&P 500 member:

| Quantity | Value |
|---|---|
| Member-days represented | 3,338,509 |
| Member-days missing | 11,839 |
| **Missing fraction** | **0.353 %** |
| Verdict | `INVESTMENT_UNIVERSE_MATERIALLY_COMPLETE` |
| Members per session | median 499, min 489, max 505 |

The dominant cause is structural and knowable: S&P removed its non-US
constituents in July 2002, and a Russell 1000 history excludes foreign domiciles
by construction. Six of the fourteen genuine holes share exactly that window.

### Correction 2 — the primary judge is real zero-base economics

Owners: [`alpha_agent/r31/allocation.py`](../alpha_agent/r31/allocation.py),
[`alpha_agent/r31/calibration.py`](../alpha_agent/r31/calibration.py)

```
TRACK A   information → expected return → engine.zero_base_allocator → stocks + cash
TRACK B   information → proposed weights → the SAME feasibility seam → stocks + cash
```

Cash is whatever the allocation does not invest, and is free to be 100 %. The
campaign owns **no** optimiser, **no** covariance and **no** cost model: it reads
`engine.zero_base_allocator.optimise`,
`engine.holding_opportunity_cost.build_covariance` and the canonical policy.

The top-*N* book survives as a **secondary diagnostic** and is barred from
carrying the verdict, in the contract (`TOP_N_MAY_CARRY_PRIMARY_VERDICT = False`)
and in the judge's frozen artifact.

**The risk frontier is now γ, not book size** — pre-registered at
0.5× / 1.0× / 2.0× the canonical `risk_aversion_gamma`, frozen before any result.
Selection always uses the canonical 1.0× point; the other two characterise a
finalist's sensitivity to risk appetite and never choose between candidates, so
no candidate can win by being best at one convenient γ. Only `risk_aversion_gamma`
moves along the frontier — cost, caps, liquidity and lookback are identical at
every point, so a frontier point is a different *appetite*, not a different set
of *rules*.

**The Track-A units contract.** The allocator consumes economic return
quantities. A candidate reaches it by exactly two routes: it already forecasts a
forward excess return, or its score passes a pre-registered **monotonic** affine
calibration fitted only on DISCOVERY. A calibration that reorders the candidate's
own ranking is not a calibration — it is a different model wearing the original's
name — so a negative slope fails closed with
`FORECAST_RANK_IDENTITY_VIOLATION`. This is the Release 30.1 defect, made
impossible rather than documented.

**Historical sector is `UNMEASURABLE_PIT`.** The canonical PIT sector owner
declares the owned entity-SIC snapshot inadmissible for historical construction,
so the sector cap is disabled historically and the limitation is reported on
every result. It is *not* encoded by giving every name one sentinel sector — that
would cap the entire portfolio at the 25 % sector limit and fabricate a
75 %-cash result out of a placeholder string. Each name gets its own singleton
sector, which makes the constraint genuinely non-binding while the name and
liquidity caps continue to bind.

### Correction 3 — Track B turnover by security identity

Owner: [`alpha_agent/r31/learners.py`](../alpha_agent/r31/learners.py)

Training blocks are `(X_t, r_t, symbols_t)`; a two-element block raises. Turnover
is `Σ_i |w_{i,t} − w_{i,t−1}|` over the **union of symbols**. Both Track-B
families — the linear `direct_portfolio` and the novel nonlinear
`novel_direct_decision` — allocate over the names **and a cash unit**, so neither
is forced to be fully invested, and both price the turnover they imply. (The
nonlinear family previously discarded its cost rate outright, which would have
let it win by trading for free.)

Five mandated negative probes, all live in
`tests/test_release31_campaign_v3_corrections.py`: row permutation → zero
turnover; one security leaves and one enters → correct exit + entry; same-length
different-securities → must not align positionally; a delisted security → exit
cost retained; a cash change → correctly represented.

### Correction 4 — two benchmarks, neither substitutable

Owner: [`alpha_agent/r31/benchmarks.py`](../alpha_agent/r31/benchmarks.py)

| Benchmark | Isolates |
|---|---|
| `SP500_PIT_EQUAL_WEIGHT` | **selection skill** — universe neutralised |
| `SPY_TOTAL_RETURN` | **the decision to run the strategy at all** |

The investable series is Norgate **`$SPXTR`** — the S&P 500 *total return* index,
at 100 % coverage of the research window. A price-only index is inadmissible:
comparing a total-return strategy against a price index manufactures roughly two
points a year of fake outperformance.

A candidate that beats the equal-weight basket but loses to the ETF anyone could
have bought has demonstrated stock selection inside a universe that was itself a
bad place to be. Both are reported on every result, and the superiority contract
requires the winner not to lose to the investable one.

---

## Defects found while building v3

These were found by measurement, not by review, and each is now a blocking
invariant with its own negative probe.

**A look-ahead leak in every walk-forward predictor.** The training window
carried a fallback: when the legitimate expanding window held fewer than twelve
dates it trained on `warmup[:60]` — the first sixty dates of the layer,
*regardless of where the scored date sat*. On the validation layer that branch is
unreachable, so Campaign v2 never executed it. Campaign v3 fits its Track-A
calibration by running predictors across DISCOVERY, where the earliest dates do
reach it, and there the fallback trains on dates **after** the one being scored.
Every Track-A calibration would have inherited the leak and then priced capital
with it. There is no legitimate fallback for "not enough history yet": the
predictor now returns NaN and the calibration skips the date
(`MIN_TRAIN_SECTIONS = 24`).

**A sector sentinel that fabricated a cash preference.** See Correction 2.

**A calibration floor set against the wrong error.** The per-date slope floor was
first set at *t* ≥ 3.0, chosen only against the false-positive rate. Measured
against real effect sizes it rejects genuine alpha: a good equity factor
(monthly rank IC ≈ 0.03, sd ≈ 0.10) produces an expected *t* of 0.3·√N — 2.3 over
60 dates, 3.9 over 167. A campaign whose every candidate is refused has measured
its own gate rather than the evidence. The floor is now the conventional 2.0, the
calibration is fitted on **all** entitled discovery dates for power, and
data-mining risk is controlled where it belongs — BH/FDR over the whole executed
denominator, Hansen SPA, the paired block bootstrap, and the one-shot lockbox.

**A sign-stability floor that was a second significance test.** At 167 fitting
dates a 0.55 floor is equivalent to *t* ≈ 1.9, so it silently set the real bar
while the t-gate looked decorative. Sign stability now sits at 0.50 and does the
one job the t-statistic cannot: it uses only the *sign* of each date's slope, so
no handful of violent dates can carry it.

**An inert negative probe.** `test_audit_catches_an_unauthenticated_read_route`
searched for a route pattern that no longer matched the source, so its `str.replace`
was a silent no-op and the probe had never tested anything. Mutations are now
self-verifying (`_must_replace`).

**An audit rule that forbade what the spec mandates.** `engine.holding_opportunity_cost`
was on the research lane's forbidden list — correct while the judge needed no
covariance, wrong once v3 had to choose between reading the canonical covariance
owner and writing a second one. It is now allowed **and** proven pure: the audit
re-parses every admitted engine owner and fails on any import outside the standard
library, so admission by name cannot smuggle in a database dependency.

**Two superiority checks that could not fail.** Found by inspecting the terminal
artifacts rather than the source. The incumbent momentum leg could not be
economically calibrated, so `superiority_verdict` fell back to the equal-weight
benchmark. Substituting `0.0` for the incumbent's *excess* is correct — the excess
statistic is already measured against that benchmark. But the same fallback filled
the absent incumbent's **drawdown** and **turnover** with the candidate's own
values, so `drawdown_not_materially_worse` computed `dd − dd = 0.0` and
`turnover_ratio` computed `turn / turn = 1.0`. Both passed on every input that
could ever be supplied: the candidate was being compared against itself.

They now report `UNAVAILABLE_NO_INCUMBENT` with `pass: None`, the aggregate is
`all(c["pass"] is True ...)` so an unproven check cannot be counted as satisfied,
and `checks_unavailable` names them in the verdict. Audit group
`falsifiable_superiority` (5 invariants) blocks the pattern and is negative-probed
by reintroducing it.

The terminal verdict is **unaffected**: it already failed five checks on merit
(`net_gain_vs_incumbent`, `does_not_lose_to_investable_benchmark`,
`subperiod_stability`, `survives_spa`, `beats_incumbent_paired_bootstrap`).
Recomputing the sealed lockbox evidence under the corrected checks returns the same
best candidate, the same `all_passed = False` and the same `winner = None`. Because
`final_verdict.json` is immutable by contract, the sealed artifact still records the
two vacuous passes; the repair governs the next campaign, and the reader is told
here that those two lines in the stored file proved nothing.

---

## Compute reality

The v3 judge allocates capital through the canonical optimiser at every decision
date. Measured on this machine (i3-10105F, 4 cores / 8 threads):

| Operation | Cost at S&P-500 scale (~500 names) |
|---|---|
| `build_covariance`, 60-session lookback | ~1.2 s per date |
| `optimise`, strong forecast (μ sd ≈ 0.02) | ~0.2 s |
| `optimise`, realistic weak forecast (μ sd ≈ 0.002) | **4–12 s** |

The weak-forecast case dominates: a weak forecast leaves the optimum interior and
Frank-Wolfe runs to its iteration cap. That is ~6 minutes of judging per candidate
per evidence layer, against roughly a second under v2's top-N book.

Two consequences, both pre-registered before any candidate ran:

1. **The covariance cache** ([`covcache.py`](../alpha_agent/r31/covcache.py)).
   Covariance depends on (date, eligible universe, lookback, policy) and **not**
   on the candidate. Building it once costs ~6 minutes for 304 decision dates;
   rebuilding per candidate would cost ~10 hours across a 100-candidate campaign
   recomputing an identical matrix. Without it the primary judge is not
   executable at all.

2. **A smaller executed grid.** The contract's ceilings are unchanged (12
   families, 240 known configurations, 40 per family, 6 novel families, 300 novel
   candidates). The *executed* grid is far below them: 31 known configurations
   across all 12 families, plus one training-universe variant per family, plus 30
   novel candidates per campaign. Points were dropped only where a grid was dense
   in a direction the family is insensitive to — three neighbouring ridge
   penalties test one hypothesis, not three — and never where two configurations
   express materially different structure. Penalties remain log-spaced so the
   retained points span the same range.

   This is not only pragmatic. A smaller pre-registered search carries a
   **smaller multiple-testing denominator**, so each survivor carries more
   evidence; and the contract's own rule already forbids executing configurations
   to consume a budget.

---

## Reading a Track-B cash figure

A Track-B learner proposes weights via a softmax over ~500 names plus a cash
unit. An unconcentrated softmax puts about 0.2 % in each name, which is below the
canonical 0.5 % minimum position size, so the feasibility seam converts almost
all of it to cash. That is the correct operational answer — the book cannot hold
a 0.2 % position — but it is a **different fact** from "the model preferred cash".

The two are therefore reported separately:
`cash_weight_mean` and `proposal_dust_converted_to_cash_mean`. A large second
number means much of the first is a proposal the book could not hold.

---

## Safety

Release 31 writes only under the research root. It creates no signal, no target,
no proposal, no decision and no order; it activates no model and promotes no
champion. A winning candidate ends at `MODEL_READY_FOR_MANUAL_PAPER_REVIEW` with
an evidence package, and a human decides what happens next.
