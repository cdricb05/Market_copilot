# PnL Opportunity Frontier

> Canonical statement of the question Release 32 exists to answer, and of the
> comparison that is allowed to answer it.
>
> Related canonical documents:
> [PROJECT_CHARTER.md](PROJECT_CHARTER.md) ·
> [STRATEGY_SLEEVE_CONTRACT.md](STRATEGY_SLEEVE_CONTRACT.md) ·
> [DAILY_MULTI_ASSET_GOVERNANCE.md](DAILY_MULTI_ASSET_GOVERNANCE.md) ·
> [INFORMATION_PURCHASE_GATE.md](INFORMATION_PURCHASE_GATE.md) ·
> [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md)

## The question

> **If every investable dollar were cash right now, given everything
> legitimately observable right now, where should capital be deployed to
> maximise expected after-cost, risk-adjusted paper portfolio PnL?**

Equities were the proving ground, not the objective. The objective is
asset-agnostic. Legitimate answers eventually include cash, individual
equities, equity indices, sectors and factors, rates and bonds, commodities,
FX, volatility exposures, event-driven opportunities, and any other validated
strategy sleeve.

Three consequences follow, and they are the whole point of this document.

1. **The system is not required to allocate to every asset class.** Capital
   belongs only where evidence supports it. Breadth of coverage is not a goal.
2. **A NULL result is a valid answer.** "No sleeve qualified" is knowledge, not
   failure.
3. **Cash is a real asset choice**, not the residual left over when nothing
   else was chosen.

## What the frontier is, and what it is not

The PnL Opportunity Frontier is a **research-only comparative read model**. It
ranks where the next research dollar should go.

It is **not** the production capital allocator. Release 32 does not build the
multi-asset allocator and does not move capital. Release 33 consumes this
frontier; Release 32 produces it.

The distinction matters because a ranked list of sleeves looks exactly like an
allocation instruction, and it is not one. A sleeve that tops the frontier has
earned further research and a place in the Release-33 integration queue. It has
not earned capital.

## Why raw return cannot rank the frontier

Ranking by return would reward the sleeve that took the most risk, traded the
most, or happened to exist only during a favourable stretch. The frontier
therefore ranks on economics that survive all three of those failure modes:

- expected / realised **net** return, after transaction costs
- volatility, and downside volatility
- tail risk (maximum drawdown, CVaR)
- turnover, and the cost that turnover implies
- liquidity and plausible capacity
- correlation with the rest of the portfolio
- **marginal portfolio utility** — the value the sleeve adds to what already
  exists, not its value in isolation

A moderate standalone sleeve can be genuinely valuable because it diversifies
the rest of the book. That benefit is real, and it must be **measured, never
asserted**.

## The control a sleeve must beat

Choosing the wrong alternative is the single easiest way to manufacture a false
opportunity, and Release 32 proved it on its own first campaign.

**Cash is the wrong control on its own.** Over any long window, anything holding
equities beats Treasury bills. Campaign v1 ranked and FDR-corrected on excess
over cash, and a sleeve "qualified" with a t-statistic of 4.03 — while *every
one* of its ten lockbox results had negative excess against buy-and-hold. That
statistic measures exposure, not skill.

**The benchmark alone is also wrong.** A sleeve whose entire purpose is to hold
less risk in bad regimes will show negative excess return against a fully
invested benchmark even when it is doing exactly what it should.

The frontier therefore judges each sleeve against a **volatility-matched mix of
the benchmark and cash**:

    w = sleeve_volatility / benchmark_volatility        (capped at 1.0)
    control_t = w · benchmark_t + (1 − w) · cash_t

This control carries the sleeve's own risk with none of its timing. Beating it
means earning more than static de-risking would have earned at the same risk,
which is the only thing that deserves capital. The cap at 1.0 exists because
this project has no leverage: a levered control would be a comparison against
something nobody could hold.

`w` is computed from realised volatilities on the scored window. That is
in-sample and deliberately so — it builds the control rather than forecasting
anything, and a control fitted to match the sleeve is harder to beat, not
easier.

Excess over cash is still **reported**, because it answers a real question
("was being invested at all worthwhile?"). It simply may not rank, select, or
qualify.

## Standalone economics vs marginal portfolio value

Every sleeve is reported twice.

**Standalone economics** answer: how did this sleeve do on its own terms?

**Marginal portfolio value** answers: what changes when this sleeve is added to
the portfolio that already exists? This requires sufficient common history with
the other sleeves. Where that history does not exist, the marginal figure is
reported as unavailable — not estimated, not extrapolated.

## The common-overlap rule

Different information families begin on different dates. Cboe volatility
history, Treasury curves, CFTC positioning and equity price panels do not share
a start date. A sleeve must never win because it only existed during an
unusually favourable period.

Every result therefore reports **three** views:

1. **Maximum legitimate history** — everything the sleeve's own data supports.
2. **Matched-period comparison** — the sleeve measured over the period used for
   whatever it is being compared against.
3. **Common-overlap comparison** — the exact intersecting decision calendar,
   used whenever sleeves are compared directly with each other.

Any cross-sleeve comparison must state the exact overlapping decision calendar
it used. A comparison that cannot name its calendar is not a comparison.

## Asset labels are not risk factors

Holding twenty tickers is not twenty independent risks. Holding an equity
index, a sector ETF and a basket of large-cap names is close to holding one
risk three times.

The frontier is therefore accompanied by a **risk exposure map** that tracks
equity beta, duration/rates exposure, FX, commodity, volatility/tail exposure,
gross and net exposure, cash, instrument and sleeve concentration, liquidity,
drawdown, CVaR, correlation clusters and latent common-risk clusters.

The first implementation uses empirical correlation and factor clustering. That
is an honest starting point, and it will be labelled as exactly that. The
system will not claim a sophisticated latent-risk model it does not have. The
purpose is narrow and important: prevent *many tickers* from being reported as
*many independent risks*.

## What the frontier reports per sleeve

    sleeve_id                     evidence_state
    history / matched-history     expected & realised net return
    relevant benchmark            excess return vs that benchmark
    volatility                    Sharpe, Sortino
    maximum drawdown              CVaR
    turnover                      transaction costs
    liquidity                     estimated capacity
    correlation with sleeves      marginal diversification value
    current information quality   PIT state
    strongest candidate           weakness / blocker
    terminal sleeve verdict

## Qualification

A sleeve does **not** qualify because accuracy is good, IC is good, Sharpe is
good on one subperiod, gross return is high, one parameter works, one asset
dominates, a revised macro series looks predictive, or a short recent history
looks good.

A sleeve qualifies only if **all** of the following hold:

- point-in-time evidence is legitimate
- after-cost economics are positive against the correct benchmark
- matched-period economics remain credible
- the lockbox is untouched by selection
- multiple-testing controls are satisfied
- drawdown and tail risk are acceptable
- performance is not dependent on a single regime
- liquidity and capacity are plausible
- the result survives reasonable sensitivity analysis
- no hindsight reconstruction is required

Diversification value can carry a sleeve whose standalone return is moderate —
but only when that marginal value has been measured under the common-overlap
rule.

## Direction is explicit

A statistically significant **negative** result is not alpha. A sleeve that
reliably loses money is a finding, and it is recorded as a finding. It is never
relabelled as a short opportunity unless the short expression itself was
specified in advance, is operationally admissible, and carries defensible cost
assumptions.

## Terminal verdicts

Release 32 finishes with exactly one primary verdict:

    R32_MULTIPLE_SLEEVES_QUALIFIED
    R32_SINGLE_SLEEVE_QUALIFIED
    R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED
    R32_POINT_IN_TIME_EVIDENCE_BLOCKED
    R32_RESOURCE_BUDGET_EXHAUSTED
    R32_DATA_SOURCE_BLOCKED

If one or more sleeves qualify, a secondary verdict is also reported:

    READY_FOR_R33_MULTI_ASSET_INTEGRATION

If no sleeve qualifies but a paid-data hypothesis passes the purchase gate far
enough to justify a provider sample:

    INFORMATION_SAMPLE_PRIORITY_IDENTIFIED

Nothing is labelled `PURCHASE_CANDIDATE` unless the
[Information Purchase Gate](INFORMATION_PURCHASE_GATE.md) actually supports it.

## Inheritance from Release 31

Release 31 reached `R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED` under the
current equity-information estate, with the dominant constraint recorded as
`INFORMATION_NOT_METHOD`.

That result is **loaded as the equity-selection sleeve's baseline, not rerun**.
More mathematical mining of the same information would raise data-mining risk
without producing knowledge. Release 32 expands the *information state*, the
*prediction target* and the *asset opportunity set* instead.
