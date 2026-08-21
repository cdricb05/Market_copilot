# Daily Multi-Asset Governance Contract

> The authoritative interface for the future daily portfolio manager. Release 32
> defines and tests this contract. Release 33 operationalises it.
>
> Related canonical documents:
> [PROJECT_CHARTER.md](PROJECT_CHARTER.md) ·
> [PNL_OPPORTUNITY_FRONTIER.md](PNL_OPPORTUNITY_FRONTIER.md) ·
> [STRATEGY_SLEEVE_CONTRACT.md](STRATEGY_SLEEVE_CONTRACT.md) ·
> [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md)

## Status

**Release 32 is production read-only.** This document defines a contract; it
does not move capital, create proposals, create orders, or change the
operational allocator. Where a value would have to be invented to make the
contract concrete, ownership of that value is named and the value itself is
left to Release 33.

## The standing rule

    REFRESH FREQUENTLY.
    REASSESS GLOBALLY.
    TRADE SELECTIVELY.
    RECALIBRATE CAUTIOUSLY.

## The daily loop

    DATA / INFORMATION REFRESH
              ↓
    AUTHORITATIVE FRESHNESS
              ↓
    AUTHORITATIVE MARKS / NAV
              ↓
    REFRESH EVERY STRATEGY SLEEVE
              ↓
    EXPECTED PNL / RISK DISTRIBUTIONS
              ↓
    CROSS-SLEEVE OPPORTUNITY FRONTIER
              ↓
    ZERO-BASE MULTI-ASSET TARGET
              ↓
    CURRENT → TARGET TRANSITION ECONOMICS
              ↓
    HOLDING OPPORTUNITY COST
              ↓
    MATERIALITY / RISK / TURNOVER GATES
              ↓
    HOLD / REDUCE / EXIT / REPLACE / ADD
              ↓
    ONE COMPLETE PAPER TARGET PORTFOLIO
              ↓
    MANUAL REVIEW
              ↓
    PAPER ORDERS
              ↓
    RECONCILIATION
              ↓
    IMMUTABLE FORWARD EVIDENCE

## A. Scheduled daily review

At minimum one canonical daily checkpoint. It requires, in order: the current
eligible trading-session state; all available marks; NAV; freshness; a sleeve
refresh; the global opportunity frontier; global risk state; the zero-base
target (Release 33); transition economics; reassessment; a governed outcome;
and forward evidence.

Each of those already has an owner, and this contract **consumes** those owners
rather than reimplementing them. Reuse is not a preference here — a second
implementation of eligible session date or NAV would violate Principle 1.

## B. Event-driven reassessment

The same engine runs when material new information arrives. It reuses
`engine.event_fabric` and the existing material-information authority. **No
second event system is created.**

Triggers include: a material price move; a volatility regime change; a macro
release; an event or revision; liquidity deterioration; a risk breach; new
positioning information; a material sleeve-signal change.

## C. No-churn / hysteresis

**Daily reassessment does not mean daily trading.**

The portfolio may change only when:

    ExpectedUtility(Target)
      - ExpectedUtility(Current)
      - TransitionCosts
      > GovernanceHurdle

and every gate passes: evidence, freshness, risk, liquidity, concentration,
market availability, turnover, operational.

A difference between the current portfolio and the target is **not** by itself
a reason to trade. It is a reason to evaluate whether trading is worth its
cost. Most days, the honest answer is no.

Additional conditions that must hold before a change is proposed:

- positive expected after-cost utility improvement
- materiality above threshold
- evidence above threshold
- acceptable freshness
- sufficient liquidity
- accepted risk
- available turnover allowance
- a minimum economic position size (no economically meaningless trades)

Threshold **values** are deliberately not invented here. Configuration
ownership is defined; the numbers belong to Release 33, set against measured
behaviour rather than guessed in advance.

## D. Global turnover budget

The contract supports daily, weekly and monthly turnover budgets, plus an
explicit risk/emergency override policy. Release 32 defines the shape; Release
33 sets and enforces the values.

A turnover budget is a real constraint, not a report. When it is exhausted, the
correct outcome is that a proposed change does not happen — not that the budget
is quietly raised.

**The values are `NOT_CALIBRATED`, and that is the recorded state.** All three
periods are declared as concepts with a value of `null`; the value owner is
`RELEASE_33_MULTI_ASSET_TARGET_GOVERNANCE_CALIBRATION_OWNER`. Release 32
measured no multi-asset trading behaviour, so any number it wrote would be a
guess that Release 33 inherits as a calibrated limit — indistinguishable, by
then, from one somebody chose.

An uncalibrated budget is **undecidable**: it is not a budget of zero, which
would forbid every trade, and not an unlimited budget, which would permit every
one. `governance.check_turnover_budget()` therefore returns
`TURNOVER_BUDGET_NOT_CALIBRATED` rather than a comparison, because the failure
this prevents is the innocuous-looking `turnover > (budget or zero)` that turns
"nobody has set this" into a hard stop.

## E. Risk-driven change

A portfolio reduction can be legitimate **without** a superior alternative.
When drawdown risk, volatility, correlation, liquidity, leverage, tail risk or
data quality make the current portfolio unacceptable, reducing exposure is
correct even if nothing better exists.

**Cash is a valid destination.**

## Mixed market hours and calendars

A multi-asset portfolio can hold instruments on different calendars. The
contract therefore distinguishes:

    IDEAL TARGET              what the evidence says the portfolio should be
    CURRENTLY EXECUTABLE TARGET   what can actually be traded right now

Every instrument or leg eventually carries:

    market_state
    market_open
    pricing_freshness
    execution_eligible_now
    earliest_execution_time
    target_weight
    pending_target_delta

The engine may reassess globally while some markets are closed. Changes to
closed-market legs remain **pending**; they are not silently dropped and they
are not silently executed at a stale price.

A closed-market leg is **never** approximated through a different instrument
unless an explicitly validated hedge policy exists. Substituting a proxy
because the real instrument is closed is how a portfolio acquires a risk nobody
authorised.

## One authoritative multi-asset NAV

There is exactly one NAV owner. No sleeve computes its own NAV, and no UI panel
interprets NAV locally (Principle 6).

The future authoritative NAV must support: cash; positions; instrument
currency; FX conversion; contract multiplier; accrued economics where relevant;
pending orders; committed cash; settlement.

Release 32 documents and tests the **ownership contract**. Release 33 and later
may generalise the calculation itself.

## Holding opportunity cost — cross-asset extension

The existing HOC engine is **not replaced**. This section defines how Release 33
extends it.

Today HOC asks whether capital in a holding would be better used in another
equity. Extended, it asks whether capital in — say — AMD would be better used
in another stock, SPX, a sector, Treasury duration, gold, a cross-asset trend
sleeve, an event-driven sleeve, or cash.

Each candidate destination must expose: current expected PnL; risk
contribution; tail contribution; liquidity; correlation contribution; the
strongest replacement; switching cost; expected utility improvement.

The recommendation vocabulary is unchanged:

    HOLD    REDUCE    EXIT    REPLACE    ADD

## What this contract must never do

- create a second portfolio-decision owner
- create a second event system
- create a second NAV or covariance owner
- write to the operational portfolio
- promote or activate a model
- create orders
- enable automation

## Safety

Paper-only, preview-first, manual-review and no-automation boundaries remain in
force without exception. Manual review is mandatory for every portfolio-change
proposal. Nothing in this contract weakens
[PROJECT_CHARTER.md](PROJECT_CHARTER.md) safety boundaries.
