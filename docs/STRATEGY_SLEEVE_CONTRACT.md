# Strategy Sleeve Contract

> Canonical definition of a strategy sleeve, the one contract every sleeve
> implements, and the boundary that stops a sleeve becoming an allocator.
>
> Related canonical documents:
> [PNL_OPPORTUNITY_FRONTIER.md](PNL_OPPORTUNITY_FRONTIER.md) ·
> [DAILY_MULTI_ASSET_GOVERNANCE.md](DAILY_MULTI_ASSET_GOVERNANCE.md) ·
> [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) ·
> [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md)

## The one rule

**A sleeve generates OPPORTUNITIES. A sleeve does not own capital.**

The global portfolio allocator owns capital. A sleeve that allocates capital,
sizes a book, writes a proposal, or creates an order has stopped being a sleeve
and has become a second portfolio optimiser — which the architecture forbids
(Principle 1: one canonical calculation per business concept).

A sleeve's `recommended_exposure` is an *opinion expressed in the sleeve's own
terms*. It is an input to allocation, never an allocation.

## Why this boundary exists

Without it, six sleeves become six competing portfolio managers, each sizing
positions against its own risk budget, each unaware of the others' exposures.
The book would then hold "many tickets" that are one risk repeated, and no
single owner could answer what the portfolio's equity beta actually is.

One allocator, many opinions. That is the whole design.

## The contract

Every sleeve emits a `StrategyOpportunity` carrying at least:

### Identity and timing

    sleeve_id                  stable identifier
    sleeve_family              EQUITY_SELECTION, EQUITY_BETA_TIMING, ...
    as_of                      the state this opinion describes
    decision_timestamp         when the opinion could first be acted on
    forecast_horizon           the horizon the forecast is about
    signal_refresh_frequency   how often this opinion legitimately changes

### Instruments and expression

    investable_instruments     what would actually be traded
    expression_type            LONG_FLAT, LONG_SHORT, RELATIVE, OVERLAY, ...
    market_calendar_state      which calendars the instruments trade on

### Expected economics

    expected_gross_return      before costs
    expected_net_return        after costs
    expected_volatility
    downside_q05               5th-percentile outcome
    expected_cvar              conditional tail expectation
    probability_of_loss

### Implementation reality

    expected_turnover
    expected_transaction_cost
    liquidity
    estimated_capacity

### Portfolio interaction

    correlation_state          against other sleeves and the current book
    concentration_state
    latent_risk_cluster        which common-risk cluster this belongs to

### The opinion

    recommended_exposure       an opinion, NOT an allocation
    residual_cash              what the sleeve would leave uninvested

### Evidence

    evidence_state
    pit_state
    freshness_state
    data_quality_state

### Provenance

    model_spec_hash            binds model BEHAVIOUR, not a schema name
    data_snapshot_hash
    evidence_hash

## `model_spec_hash` binds behaviour

Release 31 learned this the expensive way. Binding a judge *schema name* into a
spec hash allowed a corrected cost model to silently reuse candidates that had
been measured under the old one. The hash must cover the behaviour that
determines the result — economics, cadence, frontier, benchmark — so that a
material change produces a different hash and therefore a new campaign.

A material change is a **new campaign id**, never an edit to a frozen artifact.

## Sleeve states

    OPPORTUNITY            evidence supports acting
    WEAK_OPPORTUNITY       evidence is real but marginal
    NO_OPPORTUNITY         measured, and the answer is no
    DATA_BLOCKED           required data is missing or inadequate
    PIT_BLOCKED            the data exists but cannot be proven point-in-time
    RESEARCH_ONLY          admissible as knowledge, not as an exposure
    READY_FOR_FRONTIER     complete enough to enter the comparison

`NO_OPPORTUNITY` is a successful measurement. So is `PIT_BLOCKED`. Neither is
an error state, and neither may be quietly retried until it changes.

## A sleeve may recommend zero

**0% exposure / 100% cash is a legitimate sleeve output.** A sleeve that
declines to express a view today is behaving correctly, not failing. Any design
that requires a daily position — that cannot say "nothing today" — is
manufacturing turnover, and turnover is a cost that must be earned.

## Operational admissibility

A sleeve's research expression and its operationally admissible expression are
not the same thing, and the difference is recorded explicitly.

The primary operationally compatible expression is:

    LONG / FLAT / CASH,  gross <= 100%,  no leverage

A LONG/SHORT or levered variant may be studied only if the historical
instrument semantics are legitimate and the short/futures cost assumptions are
defensible. It is then labelled:

    RESEARCH_ONLY_NOT_OPERATIONALLY_ADMISSIBLE

Such a variant **cannot** qualify for Release-33 operational integration
without a later, explicit governance approval for shorting or leverage. The
label is not a formality; it is the thing that stops a research curiosity
becoming a position.

## What a sleeve may never do

A sleeve may never:

- write a capital allocation
- create or modify a reallocation proposal
- create or modify a portfolio decision
- create an order of any kind
- mutate holdings or cash
- promote or activate a model
- alter the operational book

These are enforced by architecture audit invariants, not by convention.

## The six initial sleeves

| Sleeve | Question |
|---|---|
| `EQUITY_SELECTION` | Which individual equities? *(Release 31 control — loaded, not rerun)* |
| `EQUITY_BETA_TIMING` | Equity beta, or cash? |
| `SECTOR_ROTATION` | Given equity exposure, which sectors? |
| `CROSS_ASSET_TREND` | Which broad liquid exposures trend persistently? |
| `VOLATILITY_RISK_REGIME` | Can we forecast volatility and tail transitions? |
| `EVENT_DRIVEN` | Does new information sharpen the conditional PnL distribution? |

`EQUITY_SELECTION` enters at
`CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED`, carrying Release 31's frozen
terminal evidence. It remains available for future orthogonal-data extensions;
it is not re-searched over the same information.

`VOLATILITY_RISK_REGIME` begins as **risk and timing information**, not as an
options-execution sleeve. A volatility-trading hypothesis may be recorded for
later research, but cannot qualify for operational integration without real
tradable instrument history, real costs, and explicit tail-risk rules.

## One common judge

Sleeves do not define their own success metric. Every sleeve faces the same
economic judge, described in
[PNL_OPPORTUNITY_FRONTIER.md](PNL_OPPORTUNITY_FRONTIER.md). IC, accuracy and
MSE are diagnostics; they may inform, and they may never carry the verdict.
