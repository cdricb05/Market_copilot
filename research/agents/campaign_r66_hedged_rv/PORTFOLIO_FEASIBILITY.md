# R66 — can the actual paper portfolio hold an institutional hedged trade?

**Read-only measurement. No return is computed here, no signal is scored, and
nothing below depends on whether any of these trades is profitable.** This
answers a prior question the estate has never asked: is a multi-leg futures
structure *expressible* at this book's size at all?

Calculation owner: `research/agents/campaign_r66_hedged_rv/feasibility.py`
Artifact: `D:\Stock_Prediction_app_data\r66_hedged_rv\STRUCTURE_FEASIBILITY.json`

## The book, as at the 2026-09-21 eligible session

| | |
|---|---|
| NAV | **$99,127.48** |
| Invested (25 US equity holdings) | $94,644.77 |
| Free cash | **$4,482.71 — 4.5 % of NAV** |
| Initial capital | $100,000.00 |
| Cumulative return | −0.87 % (SPY +3.49 %) |
| Max drawdown | −4.87 % |

Source: `api.portfolio_state.load_portfolio_state`, state
`PORTFOLIO_STATE_READY`, consistency `CONSISTENT`.

## The answer

**Zero of eleven hedged structures is holdable today. Exactly one is holdable at
any cash policy.**

Contract specifications are Norgate's, from the R38 `futures_market_registry`
(105 markets); prices are the last settle on the frozen R38 layer (2026-08-21).
The hedge ratio equalises the two legs' daily dollar volatility — for two points
on one issuer's curve that is very close to the DV01 ratio and needs no duration
data the estate does not own. The integer expression is the **smallest** one
within a 10 % hedge-error tolerance, declared before any structure was
evaluated, because a small book must prefer the smallest acceptable hedge rather
than the most accurate one.

Machine-readable artifact with the full per-structure record, the SPAN
sensitivity and the forward-evidence portfolio:
`research/agents/campaign_r66_hedged_rv/PORTFOLIO_FEASIBILITY.json`.

**Verdicts: 10 `NOT_IMPLEMENTABLE_AT_THIS_NAV`, 1 `SINGLE_POSITION_ONLY`,
0 `IMPLEMENTABLE`.**

| Structure | ratio | n(long:short) | hedge err | gross notional | ÷NAV | margin | ÷cash |
|---|---|---|---|---|---|---|---|
| FEED ZC/ZW (corn–wheat) | 0.553 | 2:1 | 9.6 % | $82,450 | **0.83** | $4,565 | 1.02 |
| FEED ZC/ZM | 0.722 | 3:2 | 7.7 % | $136,103 | 1.37 | $7,040 | 1.57 |
| CRUSH ZM/ZS | 0.622 | 3:2 | 7.2 % | $217,810 | 2.20 | $9,955 | 2.22 |
| US 10s30s ZN/ZB | 0.531 | 2:1 | 5.9 % | $325,406 | 3.28 | $8,196 | 1.83 |
| LIVE LE/HE | 2.274 | 2:5 | 9.9 % | $336,090 | 3.39 | $14,850 | 3.31 |
| CRACK HO/CL | 2.134 | 1:2 | 6.3 % | $362,902 | 3.66 | $20,900 | 4.66 |
| US 5s10s ZF/ZN | 0.663 | 3:2 | 0.5 % | $535,117 | 5.40 | $8,251 | 1.84 |
| CAD/CRUDE 6C/CL | 0.099 | 10:1 | 0.6 % | $814,460 | 8.22 | $16,720 | 3.73 |
| AUD/GOLD 6A/GC | 0.060 | 12:1 | **39.3 %** | $1,323,890 | 13.36 | $48,934 | 10.92 |
| US 2s10s ZT/ZN | 0.567 | 5:3 | 5.8 % | $1,354,328 | 13.66 | $12,789 | 2.85 |
| **ZN/SR3 swap-spread proxy (r66_01)** | 16.99 | **1:17** | 0.05 % | **$4,203,247** | **42.40** | $17,499 | 3.90 |

The mandate's headline family is the *worst* of the eleven. The swap-spread
proxy needs eighteen contracts and **42× the entire portfolio in gross
notional**, because one SR3 carries so little duration that seventeen of them
are needed to balance one ZN. That is an independent confirmation of what Gate A
found statistically (`GATE_A_HEDGE_MATERIALITY.json`): SR3 is the wrong hedge
instrument for a 10-year Treasury, and the two facts have the same cause.

## Two different constraints, and only one of them is fixable

The estate has always collapsed capital into a single fraction. That hides the
fact that these structures fail for two unrelated reasons.

**Margin is a POLICY constraint.** It depends on how much cash the book chooses
to hold, and the book could raise cash by selling equities.

| cash policy | pass margin | pass margin + variation reserve | pass everything |
|---|---|---|---|
| 5 % of NAV | 1/11 | 0/11 | 0/11 |
| 10 % | 4/11 | 1/11 | **1/11** |
| 15 % | 7/11 | 2/11 | **1/11** |
| 20 % | 9/11 | 4/11 | **1/11** |
| 25 % | 10/11 | 5/11 | **1/11** |
| 30 % | 10/11 | 7/11 | **1/11** |

**Gross notional is a STRUCTURAL constraint.** It is set by the exchange's
contract size against this book's NAV, and *no cash policy changes it*. That is
why the last column stops at 1/11 and stays there: even at 30 % cash — a third
of the portfolio sitting idle — ten of eleven structures can post margin and ten
still carry more market exposure than the entire portfolio.

One ZT contract alone is **$205,906 — 2.08× the whole NAV**. One ZN is 1.09×.
The entire US Treasury relative-value programme the mandate asked for first is
dimensionally incompatible with a $100k account, and it is not close.

**The single survivor is FEED ZC/ZW** — corn against wheat, 2:1, gross notional
0.83× NAV. It fails today by **$82 of margin** ($4,565 against $4,482 of cash)
and becomes holdable at a 10 % cash policy. Corn ($24,188) and wheat ($34,075)
are simply the two smallest-notional contracts in the set.

## The leverage trap this measurement exposes

The mandate warns: *do not confuse return on gross notional with return on
committed capital, and never create attractive percentages through undocumented
leverage.* This book makes that trap concrete.

Holding FEED ZC/ZW means carrying **$82,450 of gross grain-futures notional
against $4,565 of posted margin — about 18:1 on committed capital.** If the
spread earned 1 % of gross notional a year, that is $824, which is 0.83 % of NAV
and simultaneously **18 % "return on committed capital."** The second number is
not skill. It is the first number multiplied by the margin ratio, and quoting it
as a portfolio return would be exactly the misreporting the mandate forbids.

For this reason the portfolio-relevant denominator here is **NAV**, and return on
committed capital is reported only alongside the leverage multiple that produced
it.

## Why granularity, not margin, is the deep problem

The minimum faithful expression is rarely 1:1. US 2s10s needs 5 ZT : 3 ZN —
eight contracts, $1.35M of notional — because ZT's risk per contract is small
and its notional is enormous. AUD/GOLD cannot be hedged within tolerance at
*any* size up to twelve contracts (39.3 % residual at 12:1): one gold contract
($462,830) is too large to be neutralised by any whole number of AUD contracts.

## What would fix this, and whether we own it

The instrument class designed for exactly this problem is **micro futures**.
Micro Treasury and micro FX contracts would cut notional by a factor of 10 and
make the US curve programme dimensionally feasible.

The estate owns **no micro rates and no micro FX data**. The only micro markets
in the 105-market registry are equity index (`MES`, `MNQ`, `M2K`, `MYM`) and
crypto — and none of those four reached the certified 68-market dated-contract
layer either.

So the honest blocker is named precisely: **not economics, not evidence, and not
approval — instrument granularity, plus the absence of owned micro-contract
data.**

## The forward-evidence book, read as a portfolio

The director also asked for a diversification and capacity read on the
prospective candidates. Source: `api.canonical_forward_accrual`.

| | |
|---|---|
| Registered challengers | 8 |
| Predictions emitted | 5 |
| **Matured observations** | **0** |
| **Effective independent observations** | **0** |
| Forfeitures | 0 |

**Diversification: poor.** Six of eight registrations are US equity risk — four
`US_EQUITY` R58 cells plus two `US_ETF` challengers on the *same underlying*
(SPY). The only non-equity members are one FX carry and one multi-asset futures
trend.

**Capacity: not binding, and cannot be yet.** Nothing has matured. Any
correlation, capacity or diversification statistic computed on this book today
would be measuring five pending predictions, not evidence.

**Forfeitures = 0 is correct, not a cleaned-up counter.** The accrual contract
reserves forfeiture for a frozen decision whose emission window shut. A cadence
boundary at which the owner never froze anything is
`AWAITING_NEW_GOVERNED_FREEZE` — a fact about governance, not a loss. R66 did
not alter that definition, and the `forfeitures/` directory has still never been
created.

## What this does NOT say

- It does not say these trades are unprofitable. Profitability is a separate
  question and is ruled on elsewhere in this campaign.
- It does not say the book *should* hold cash. It says what would follow if it
  did.
- Margins are **CURRENT, not point-in-time**, and carry no exchange SPAN spread
  credit. A real spread receives a margin offset, so every committed-capital
  figure above is a deliberate **upper bound**.
- Nothing here is an allocation, a proposal or an approval.
