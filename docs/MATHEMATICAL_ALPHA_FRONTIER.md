# Release 31 — Mathematical Alpha Frontier

> Canonical release document. Read with
> [PROJECT_CHARTER.md](PROJECT_CHARTER.md) ·
> [RESEARCH_CAMPAIGN_CONTRACT.md](RESEARCH_CAMPAIGN_CONTRACT.md) ·
> [ASSET_PRICING_MATHEMATICS_LIBRARY.md](ASSET_PRICING_MATHEMATICS_LIBRARY.md) ·
> [RELEASE31_CAMPAIGN_V3_CORRECTION.md](RELEASE31_CAMPAIGN_V3_CORRECTION.md) ·
> [RELEASE30_1_ZERO_BASE_OPERATIONAL_CUTOVER.md](RELEASE30_1_ZERO_BASE_OPERATIONAL_CUTOVER.md) ·
> [CURRENT_ARCHITECTURE.md](CURRENT_ARCHITECTURE.md)
>
> **Active campaign: `r31_mathematical_alpha_frontier_v3`.** Campaigns v1 and v2
> are `SUPERSEDED_EXPERIMENTAL_DESIGN`, preserved on disk, and structurally
> unable to influence v3. The four corrections and the defects found while
> building them are in
> [RELEASE31_CAMPAIGN_V3_CORRECTION.md](RELEASE31_CAMPAIGN_V3_CORRECTION.md);
> that document is authoritative wherever this one is more general.

## The question

Release 30.1 stopped the zero-base cutover because the approved model's score
could not be turned into an expected return without fabricating one. That left an
open question rather than a defect:

> Is there **any** defensible mapping from the information we already own to a
> capital allocation that beats what we run today?

Release 31 is the bounded campaign that answers it — and, more durably, the
machinery that makes the answer trustworthy whichever way it comes out.

## What is permanent regardless of the result

The campaign is a **closed object**. Its terms are frozen and hashed before the
first candidate result exists; its budgets are integers checked by code that
raises; its lockbox is unreachable from training and selection; its judge reads
the canonical cost and constraint owner rather than restating it; and its
multiple-testing denominator is derived from an append-only log so a
disappointing candidate cannot improve the statistics by being forgotten.

```
OWNED PIT DATA ──► frozen, hashed snapshot          TRAINING universe
                        │                           (broad PIT, or index-only)
        DISCOVERY ─► VALIDATION ─► LOCKBOX      (embargoed, lockbox last)
                        │
       ┌────────────────┴────────────────┐
   TRACK A                           TRACK B
   score → calibration → μ           proposed weights
       └────────────────┬────────────────┘
                        │
              ONE portfolio-construction seam
              ◄── engine.zero_base_allocator.optimise
              ◄── engine.holding_opportunity_cost.build_covariance
                        │
              stocks + CASH, over the INVESTMENT universe
              (PIT S&P 500 members only)
                        │
        judged vs BOTH  SP500_PIT_EQUAL_WEIGHT  and  SPY_TOTAL_RETURN
                        │
            candidate registry (append-only, budgeted)
                        │
              campaign-wide multiple testing
                        │
                  terminal verdict
                        │
        MODEL_READY_FOR_MANUAL_PAPER_REVIEW ──► a human
                        │
                        ✗ no automatic path to the operational model, a target,
                          a proposal, a decision or an order
```

## The three facts the data forced into the design

All are **measured**, not assumed, and each changes what the evidence is allowed
to claim.

### 0. The investment universe is not the training panel

The business objective is S&P 500 capital allocation; the survivorship-safe panel
we own is a Russell 1000 history. Campaign v2 conflated the two and therefore
answered a question nobody asked. Campaign v3 separates them: a candidate may
LEARN from the broad point-in-time cross-section, and every decision the primary
judge scores is restricted to names that were **S&P 500 members on that decision
date**, from `norgatedata.index_constituent_timeseries` over the 1,897-security
"Current & Past" watchlist.

The gap this leaves is measured rather than assumed: **11,839 of 3,350,348
member-days (0.353 %)** cannot be represented, verdict
`INVESTMENT_UNIVERSE_MATERIALLY_COMPLETE`, dominated by S&P's July-2002 removal of
non-US constituents, which a Russell 1000 history excludes by construction.

### 1. The owned fundamental history is survivorship-limited by 3.42×

The owned point-in-time fundamental store covers **846 CIKs**: **46.2 %** of
names still trading at the panel's end, against **13.5 %** of names that stopped.

A factor measured on that sub-sample is measured where the losers are missing —
exactly the bias that inflates fundamental factor returns. Earlier stages
recorded this qualitatively ("survivor-biased"). Release 31 makes it a number,
and the number decides what the sample may conclude:

| Sample | Cross-sections | Span | Features | Survivorship | Carries the verdict |
|---|---|---|---|---|---|
| `PRICE_FULL_SURVIVORSHIP_FREE` | 304 | 2001-01-02 → 2026-04-23 | 14 price | **FREE** | **yes** |
| `FUNDAMENTAL_MATCHED_SURVIVORSHIP_LIMITED` | 194 | 2010-03-12 → 2026-04-23 | 21 | LIMITED | no |

The fundamental sample is fully measured and reported. It is stamped
`may_carry_verdict: false`.

### 2. Historical sector is not measurable point-in-time

The canonical PIT sector owner already classifies the owned entity-level SIC
snapshot as inadmissible for historical signal construction. Using it to compute
a historical sector exposure would be the same violation wearing a different hat,
and a published number would be believed. The judge therefore reports
`sector_exposure: {state: UNMEASURABLE_PIT}`, and no novel peer group may be a
sector. The live sector cap continues to apply where sector is genuinely known —
at the current decision timestamp, inside the canonical allocator.

## The judge

One judge scores every candidate — the incumbent, each reproduced published
method, each novel discovery — on the same dates, with the same cost owner, the
same risk prices, the same cash policy and the same constraints. Two judges
drift, and the more generous one wins wherever it happens to be called.

It owns **no** cost model, risk price, constraint, covariance or optimiser. It
reads `engine.zero_base_allocator.default_policy()` (12.5 bps per side, 10 % name
cap, $10 M ADV floor, 60-session covariance lookback, zero cash return),
delegates the solve to `engine.zero_base_allocator.optimise` and the risk matrix
to `engine.holding_opportunity_cost.build_covariance`. The audit forbids a literal
cost or cap number anywhere inside it, and re-parses the admitted engine owners to
prove they import nothing outside the standard library.

**Every candidate is turned into an actual portfolio.** Track A maps its score
into economic return units through a pre-registered monotonic calibration and
hands them to the canonical zero-base allocator; Track B proposes weights
directly and passes through the same feasibility seam. Cash is whatever the
allocation does not invest, and is free to be 100 %.

**Selection principle: implementable NET portfolio economics at comparable
risk**, at the canonical risk appetite, against the point-in-time S&P 500
equal-weight benchmark, with the investable `$SPXTR` total-return comparison
reported beside it. Never MSE, never IC alone, never gross return, never a
single-period Sharpe, and never a top-*N* book.

The risk frontier is **γ ∈ {0.5×, 1.0×, 2.0×}** the canonical risk aversion,
frozen before any candidate return existed and measured only for finalists and
the lockbox — so no candidate can win by being best at one convenient point.
Campaign v2's book-size frontier is gone: it varied *concentration*, which is not
risk appetite and cannot express a cash decision at all.

Why the distinction is load-bearing: v2's leaderboard showed the regularised
linear and dimension-reduction families reaching a **higher rank IC** than the
incumbent (≈0.016 against ≈0.007) while delivering a **negative** net excess
return, because their books turned over at roughly 9.8 annualised against the
incumbent's 6.15 and transaction costs consumed the difference. A campaign
selecting on IC would have promoted one of them.

## The cost-model defect that superseded campaign v1

Campaign `…_v1` was superseded **before it produced any verdict**. Its judge
charged the canonical per-side cost on *one-way turnover* while reporting the
cost drag on *both* sides, so every net return it measured understated cost by
roughly half and a candidate's headline cost disagreed with the cost its own
return had paid.

Two changes make that class of error self-correcting:

* the cost base is **traded notional** — sells plus buys — times the per-side
  rate, and the reported drag is computed from the same quantity subtracted from
  each period's net return. The first period is the initial purchase: a buy side
  only. `test_the_reported_cost_drag_equals_the_cost_actually_charged` pins it.
* the judge's **behaviour hash** is bound into every candidate's specification
  hash, so a change to the cost arithmetic, the rebalance cadence, the risk
  frontier or the canonical policy invalidates cached candidates instead of
  letting two judges' results share one leaderboard.

The contract's own rule is that a material change is a **new campaign id**, never
an edit to a frozen artifact — so v1's partial artifacts were left in place as
history rather than deleted, and `r31_mathematical_alpha_frontier_v2` was
started clean.

## The four defects that superseded campaign v2

v2 was likewise stopped before any verdict, for four reasons: it evaluated on the
training universe rather than the S&P 500; its judge built top-*N* books with cash
pinned to zero; its Track-B learner compared portfolios by array position rather
than by security identity; and it substituted the equal-weight benchmark for the
investable one. Each correction, the negative probe that now guards it, and the
further defects found while building v3 — a look-ahead training fallback, a
sector sentinel that fabricated a cash preference, a calibration floor calibrated
against the wrong error — are documented in
[RELEASE31_CAMPAIGN_V3_CORRECTION.md](RELEASE31_CAMPAIGN_V3_CORRECTION.md).

## Known-method reproduction vs bounded novel discovery

These are different activities with different failure modes, and a campaign that
blurs them reports the second as if it had the credibility of the first. They are
kept separate, budgeted separately, and both face the same judge. The registry of
what was reproduced, what was excluded, and why, is in
[ASSET_PRICING_MATHEMATICS_LIBRARY.md](ASSET_PRICING_MATHEMATICS_LIBRARY.md).

Literature screening stopped under the contract's rule — **two consecutive
expansions yielding no new materially distinct admissible method family**. Five
expansions ran; three added new families; the last two returned only methods
already registered.

## The terminal result

`R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED`. Verdict hash `17e5e2e84e3a`,
contract `56ac6e9f55ad`, snapshot `6f54863915dc`, universe `eb0c9947aa0d`,
benchmarks `db5d11e8be6a`. **No candidate earned a paper review, and none was
activated, promoted or proposed.**

**Most candidates never became portfolios at all.** 67 of 77 executed
specifications were `CALIBRATION_REFUSED`: 11 produced a *negative* fitted slope,
which would invert the model's own ordering before it reached the allocator, and
16 held their fitted direction on under half of the 142 fitting dates. Ridge,
elastic net, dimension reduction, robust regression, gradient boosting, random
forest, extra trees, shallow networks and quantile regression all failed here. So
did the incumbent momentum leg (slope −0.00477).

**Of the ten that did allocate capital, every one lost.** All ten carry a negative
*t*, from −2.26 to −3.95. The Benjamini-Hochberg block reports 10 rejections of 10
at q = 0.10, and it is important to read the sign: these are ten candidates
significantly *underperforming* the benchmark, not ten discoveries. The stored
artifact records no direction, so the number is only safe read alongside the
per-candidate *t*.

**The best lockbox result, over 2021-06-16 → 2026-04-23 (59 decisions):**

| | `km:fama_macbeth:01:px:h20` |
|---|---|
| net return | **+3.88 %/yr** (gross +6.53 %, cost drag −2.53 %) |
| PIT S&P 500 equal weight | +8.67 %/yr → excess **−4.80 %/yr**, *t*(NW) −1.08 |
| `$SPXTR` total return | +13.00 %/yr → excess **−9.32 %/yr**, *t*(NW) −1.54 |
| Sharpe / Sortino | 0.375 / 0.570 |
| max drawdown | −16.8 % |
| names / cash | 25.7 / 10.0 % |
| turnover | 10.1× annualised (0.84 one-way per decision) |
| hit rate vs equal weight | 39 % |
| information ratio | −0.47 |

It makes money and still loses, and it loses **before** transaction costs (gross
+6.53 % against +8.67 %). Costs deepen the gap; they do not create it. This is a
selection result, not an execution result — which matters, because a cost result
would point at the trading rule and this points at the information.

**The risk frontier does not rescue it.** At γ×2.0 Sharpe improves to 0.427 and
net falls to +3.50 %; at γ×0.5, net +4.13 % at Sharpe 0.342. Every point stays
below both benchmarks. Note the frontier is **inert for Track B by construction**:
a direct-portfolio learner emits weights rather than a forecast, so risk aversion
has nothing to act on, and its three γ rows are identical. They are three
renderings of one portfolio, not three points.

**Novel discovery added nothing.** N1 ran its full 30 specifications; 3 reached the
judge and all lost. N2 was a **13-specification** campaign, not 30: both campaigns
draw from the same seeded generator, so 17 of N2's draws were specifications N1 had
already executed, and the registry refused to refit them — which is why the
multiple-testing denominator is 76 rather than 93. The search therefore covered 43
distinct novel specifications, not 60, and the shortfall is a property of the
grammar rather than a budget cut.

**Training broadly beat training narrowly, twice.** With the evaluation universe
held identical, `direct_portfolio` scored −0.1272 broad against −0.1295 S&P-500-only
and `fama_macbeth` −0.1530 against −0.1632. The hypothesis that a manager should
learn only from the names it may own is rejected in both families it was tested on.

**The dominant constraint is recorded as `INFORMATION_NOT_METHOD`.** Eleven method
families, four architectures' worth of learners and 43 novel specifications reach the
same wall, and two thirds of them cannot be mapped into return units at all. The next
action is genuinely orthogonal information — the pending Intrinio historical
analyst-revision sample, or a survivorship-complete point-in-time fundamental
history — evaluated in *this* framework. Intrinio readiness is
`READY_FOR_EXTENSION_NOT_ACQUIRED`.

## Safety

Production was READ ONLY throughout. No daily close, no research cycle, no
proposal, no decision, no order, no holding or cash change, no model promotion,
no backend restart. `AUTOMATIC_PROMOTION_ALLOWED = False`; a winning candidate
produces `MODEL_READY_FOR_MANUAL_PAPER_REVIEW` and an immutable package, and is
never activated.

The research package may not import `paper_trader.api`, may read only
`engine.zero_base_allocator` and `engine.holding_opportunity_cost` from the
engine — both proven import-pure by the audit rather than trusted by name — and
may contain no order, promotion, activation or decision call. The read surface
(`GET /v1/research/mathematical-alpha-frontier`) writes nothing and exposes no
control. The UI region carries `RESEARCH ONLY`, `READ ONLY`, `NO LIVE ORDERS`,
`AUTOMATION OFF` and `MANUAL REVIEW`, and contains no button and no `onclick`.

Twenty-seven blocking invariant groups in
`scripts/audit_architecture.py::check_release31_mathematical_alpha_frontier`
enforce all of it — the original nineteen plus eight added for Campaign v3
(`universe_separation`, `zero_base_primary`, `calibration_guard`,
`track_b_symbol_alignment`, `benchmark_duality`, `covariance_cache`,
`supersession`, `point_in_time_training`). Every one of them is negative-probed:
a test breaks exactly one thing and asserts the audit blocks the tree, and each
mutation asserts it actually landed, so a probe cannot rot into a no-op.

## Reproducing the campaign

```powershell
$py = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe'
$env:PYTHONPATH = 'C:\Users\binis;C:\Users\binis\paper_trader'
& $py scripts\run_release31_campaign.py --stage all --workers 4
& $py -m pytest tests\test_release31_mathematical_alpha_frontier.py `
                tests\test_release31_campaign_v3_corrections.py -q
& $py scripts\audit_architecture.py --strict
```

Artifacts land under
`D:\Stock_Prediction_app_data\mathematical_alpha_frontier\r31_mathematical_alpha_frontier_v3\`.
No operational store is written. Every stage is resumable; a candidate already in
the registry is not refitted. `--workers` fans candidates across processes while
the parent stays the only writer to the append-only log, so parallelism cannot
corrupt the multiple-testing denominator.

<!-- RESULTS-SECTION -->
