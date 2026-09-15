# RESULT - SCHEDULED ANNOUNCEMENT EQUITY PREMIUM (FOMC, CPI, EMPLOYMENT SITUATION ON SPY)

**Mechanism:** `SCHEDULED_EVENT_EQUITY_PREMIUM_V1` (global candidate `GC_EQIDX_SCHEDULED_EVENT_PREMIUM`)
**Run:** ALPHA_STRIKE_SEP15_V2, verdict 1. Executed 2026-09-15 10:03 UTC through the canonical handler body
`alpha_agent.r59.mechanisms.execute_job` (global opportunity-cost gate, pin check, executor lease,
result validation, `record_execution`).
**Preregistration:** `29d9fcd` (sha256 `762c2589...`). **Implementation:** `1dbcaac` (`6a36873f...`).
**Pin:** `97fd9ba`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\scheduled_event_equity_premium.json`
(artifact_hash `b977396b...0d177d`).
**Input identity:** `NORGATE:SPY(TOTALRETURN):1993-02-01..2026-09-14; FRED:DTB3:6028a3cbd7fa;` R46 captures
`CPI_INITIAL_RELEASES=874d98892099, CPI_RELEASE_DATES=119cb6d7f518, EMPLOYMENT_INITIAL_RELEASES=520b03a72050,
EMPLOYMENT_RELEASE_DATES=0697e0e34585, FOMC_CALENDAR=5bfa5c19e52e`; FOMC history manifest `5bc5e671bc13`.
**ResearchMemory:** `HM_SCHEDULED_EVENT_EQUITY_PREMIUM_V1`, outcome `NO_ALPHA_EVIDENCE`, settled 2026-09-15T10:03:48Z.

## VERDICT: `NO_EDGE`. Both cells killed at gate 4 (`INCREMENT`). CAPITAL ELIGIBLE = NO.

Gate 0 passed: every capture hash matched, 27 of 27 history pages intact, 8 scheduled FOMC decision days
in every year except 2020 (7), SPY and bill coverage 100 %, independent windows 176 / 84 (PRE_FOMC) and
665 / 324 (ANNOUNCEMENT_DAY).

### Qualification 1994-01..2015-12 (5,540 sessions)

| cell | cost | annualised net excess | NW t | Sharpe | max DD |
|---|---|---|---|---|---|
| PRE_FOMC | 0 bp | +2.62 % | 3.58 | 0.76 | -7.3 % |
| PRE_FOMC | **1 bp** | **+2.46 %** | **3.37** | **0.71** | **-7.5 %** |
| PRE_FOMC | 2 bp | +2.30 % | 3.16 | 0.67 | -7.7 % |
| PRE_FOMC | 5 bp | +1.82 % | 2.52 | 0.54 | -8.2 % |
| ANNOUNCEMENT_DAY | 0 bp | +4.41 % | 2.99 | 0.64 | -25.7 % |
| ANNOUNCEMENT_DAY | **1 bp** | **+3.81 %** | **2.58** | **0.55** | **-26.7 %** |
| ANNOUNCEMENT_DAY | 2 bp | +3.20 % | 2.17 | 0.46 | -27.7 % |
| ANNOUNCEMENT_DAY | 5 bp | +1.39 % | 0.94 | 0.20 | -37.0 % |

| cell | volatility-matched passive long (k) | control ann / Sharpe | **increment ann, NW t** | beta-matched diagnostic |
|---|---|---|---|---|
| PRE_FOMC | k 0.179 | +1.40 %, 0.41 | **+1.06 %/yr, t 1.16** (p 0.12) | +2.21 %, t 3.05 (beta 0.03) |
| ANNOUNCEMENT_DAY | k 0.360 | +2.82 %, 0.41 | **+0.99 %/yr, t 0.61** (p 0.27) | +2.80 %, t 2.01 (beta 0.13) |

Gates 1-3 passed for both cells (right sign, t >= 2, net >= 1.5 %/yr), and both would have passed the
halves (PRE_FOMC +1.53 % / +3.38 %; ANNOUNCEMENT_DAY +3.76 % / +3.86 %) and BH q = 0.10 at m = 6
(p 0.0004 and 0.0049). They fail on the frozen question that decides capital: **does timing beat
holding SPY at the same risk?** It does not, significantly. The per-session premium on announcement
days is real (PRE_FOMC Sharpe 0.71 against the passive long's 0.41), but a volatility-matched passive
long earns most of it without the timing, and what is left (+1.0 %/yr) is not distinguishable from
zero. The beta-matched rows are reported only to show why the preregistration chose the volatility
match: a 3 % beta control makes any long-only book look incremental.

Capital usage and turnover: PRE_FOMC 3.2 % of sessions, 16 x NAV per year; ANNOUNCEMENT_DAY 12.5 %, 60 x.
Window level (gross): PRE_FOMC mean +0.33 % per event, hit rate 59.7 %; ANNOUNCEMENT_DAY +0.15 % per
window, 55.9 %. Per-family mean gross excess per session inside ANNOUNCEMENT_DAY (diagnostic only,
never a cell): FOMC +0.27 %, Employment +0.14 %, CPI +0.01 %, CPI+FOMC +1.22 % (13 sessions).

The untouched 2016-01-01..2026-08-25 confirmation window was **not read**, as preregistered.

## What this means

This is the fourth estate sighting of a real premium with a worthless timing signal (VIX term
structure, FX carry, rates carry, and now the scheduled announcement premium): holding equity through
scheduled FOMC, CPI and Employment Situation resolutions paid, but per unit of risk not
significantly more than holding equity all the time. As a sleeve it adds under one per cent a year
over a passive long at matched risk, with no statistical support. The CPI calendar carried almost
none of the premium (+0.01 % per session).

**Not done, and never to be done:** a FOMC-only or Employment-only cell, pre-2 pm windows, t-1 holds,
crisis-day deletion, a beta-matched control, reading the confirmation window, or rescuing the forward
challengers with this sample. The R46 forward clocks (`r46_4_spx_pre_fomc_drift`,
`r46_4_spx_announcement_day_premium`) are untouched and keep accruing on their own owner; this result is
their historical evidence, not a verdict on their forward rows.

**For the frontier:** RISK_PREMIUM_TIMING gains an executed closed member. The executed mechanism is
claimed by its own CLOSED global opportunity (an OPEN candidate may not hold a settled-closed mechanism
while its forward clocks run), and `GC_EQIDX_SCHEDULED_EVENT_PREMIUM` now cites this evidence and
returns to passive accrual.
