# Information Purchase Gate

> The ten conditions a dataset must satisfy before it becomes a purchase
> candidate, and the states it can legitimately hold.
>
> Related canonical documents:
> [PNL_OPPORTUNITY_FRONTIER.md](PNL_OPPORTUNITY_FRONTIER.md) ·
> [RELEASE32_ZERO_COST_INFORMATION_EXPANSION.md](RELEASE32_ZERO_COST_INFORMATION_EXPANSION.md) ·
> [PROJECT_CHARTER.md](PROJECT_CHARTER.md)

## Purpose

Release 31 concluded that the binding constraint is `INFORMATION_NOT_METHOD`.
The obvious response — buy more data — is also the expensive one, and the
history recorded in this repository is not encouraging: Intrinio's live trial
produced `NO_DEFENSIBLE_ALPHA` and `DO_NOT_BUY`; FMP grades failed
survivorship; free SimFin covered only the 2020s; SEC EDGAR cross-coverage sat
at 0.12–0.16.

So the purpose of this gate is to determine **which economic state variables
actually matter, before paying for them.**

**Release 32 spends nothing.** It does not purchase, does not require Steele or
Intrinio, does not wait for a sample, and does not fabricate analyst-revision
history.

## The ten conditions

No dataset becomes a purchase candidate without all ten.

### 1. Economic mechanism
What latent state is missing, and why would it move prices? A mechanism stated
after seeing the backtest is not a mechanism.

### 2. Cheap proxy
Can existing or free data partially test the mechanism? A proxy that shows
nothing is strong evidence against paying for the precise version.

### 3. Marginal evidence
Is there evidence the mechanism affects **portfolio economics** — not
correlation, not IC, but after-cost outcomes?

### 4. Exact data gap
Which precise field, history length, timestamp granularity and coverage is
missing? "Better fundamentals" is not a gap; a named field with a required
start date is.

### 5. Point-in-time requirement
Does the provider support the historical state **as known then**? Latest
revised history is not what the market knew. If vintages are unavailable, the
state is `PIT_BLOCKED`.

### 6. Survivorship / inactive coverage
Does the dataset cover delisted and inactive names, at what ratio? The owned
fundamental store shows a **3.42×** skew toward still-trading names — enough to
disqualify it from carrying a verdict. Any purchase candidate is measured the
same way.

### 7. Sample evaluation
A real sample, evaluated on its own terms, before money changes hands.

### 8. Incremental backtest
Existing information **versus** existing + new. The comparison is incremental,
not absolute: a new dataset that reproduces what is already known is worth
nothing regardless of how well it performs.

### 9. Economic value
Is plausible incremental PnL greater than subscription cost + implementation
cost + complexity + data risk? A dataset that pays for itself only under
optimistic assumptions does not pass.

### 10. Licensing and retention
Terms that permit the intended research and retention use.

## States

    DO_NOT_BUY               measured, and the answer is no
    SAMPLE_REQUIRED          mechanism is credible; evidence needs a real sample
    PURCHASE_CANDIDATE       all ten conditions satisfied
    PIT_BLOCKED              no legitimate historical-state support
    COVERAGE_BLOCKED         survivorship or universe coverage inadequate
    LICENSING_BLOCKED        terms do not permit the use
    NOT_INCREMENTAL          adds nothing beyond information already held
    WAITING_FOR_SAMPLE       requested, not yet received; work continues without it

Nothing is labelled `PURCHASE_CANDIDATE` unless the gate actually supports it.
`SAMPLE_REQUIRED` is not a soft `PURCHASE_CANDIDATE`, and
`WAITING_FOR_SAMPLE` is not a reason to pause.

## Output

The gate produces `information_purchase_frontier.json`: candidate information
gaps, ranked, each carrying its state, the mechanism it would test, the exact
missing field/history/coverage, and the evidence gathered so far.

The frontier answers **which gap to close first**, not whether to spend money
today.

## Intrinio / Steele

Historical analyst revisions remain `WAITING_FOR_SAMPLE`.

**Release 32 does not wait.** When genuine sample history later arrives, it
plugs into the expectations/event information family and must be tested for
incremental value across equity selection, sector rotation, event-driven
opportunities, volatility forecasting, and market-expectation state.

Do not assume the best use is single-stock prediction. Stage 13B found
prospective PEAD-on-sales at t = 2.27 and Stage 13C found the out-of-sample
result did not replicate at t = −0.29. The lesson is that the first plausible
use is not necessarily the right one — so a future sample is evaluated across
all five families, not just the one that motivated buying it.

## Prohibited substitutions

The gate exists partly to refuse specific, tempting shortcuts:

- current analyst snapshots may **never** become historical revisions
- revised macro history may **never** enter point-in-time history silently
- current sector membership may **never** be backfilled
- ETF history may **never** exist before inception
- external reference links may **never** become features
- GDELT article text may **never** become alpha silently

Each of these is enforced by an architecture audit invariant. They are listed
here because every one of them is a plausible-looking mistake that produces an
impressive backtest and no real money.
