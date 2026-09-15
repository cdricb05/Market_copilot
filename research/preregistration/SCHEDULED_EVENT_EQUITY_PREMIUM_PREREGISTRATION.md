# PREREGISTRATION - SCHEDULED ANNOUNCEMENT EQUITY PREMIUM (FOMC, CPI, EMPLOYMENT SITUATION ON SPY)

**Mechanism id:** `SCHEDULED_EVENT_EQUITY_PREMIUM_V1`
**Global candidate:** `GC_EQIDX_SCHEDULED_EVENT_PREMIUM` (global #7, the highest-value agent-executable
action in `global_multi_asset_frontier.json` built 2026-09-15; next action `PREREGISTERED_HISTORICAL_TEST`)
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 1)
**Run:** ALPHA_STRIKE_SEP15_V2, branch `alpha-strike-sep15-v2`.
**Status:** FROZEN. Written and committed BEFORE any SPY return, event-window return, book, P&L or
t-statistic for this mechanism was computed on real data. Only calendar DATES and SPY session DATES
were read (section 1.3). Not edited after results exist.

---

## 0. THE P&L MECHANISM

Scheduled FOMC decisions and the monthly CPI and Employment Situation releases resolve systematic
macroeconomic uncertainty at known times. Holding equity through those resolutions carries
concentrated, non-diversifiable announcement risk, so investors demand a higher return per session
on announcement sessions than on ordinary sessions: Savor and Wilson (2013) find most of the equity
premium on inflation, employment and FOMC announcement days, and Lucca and Moench (2015) find a
large drift into scheduled FOMC decisions. It is a risk premium, not an information edge.

**The P&L:** a long SPY position held ONLY over sessions that carry a scheduled announcement,
financed at the Treasury bill rate, flat (in bills) on every other session. It matters for capital
only if it beats holding SPY all the time at the same risk, so the decisive control is a
volatility-matched passive SPY long.

**Frozen direction:** LONG equity on announcement sessions. A contradicted sign closes the mechanism;
it is never reversed.

### 0.1 What this is and is not

| record | object | why it is different |
|---|---|---|
| `r46_4_spx_pre_fomc_drift`, `r46_4_spx_announcement_day_premium` | the same two calendars as FORWARD clocks (frozen from the literature, no historical screen) | this test supplies the 1994-2026 history those clocks cannot reach; it registers, re-identifies and backfills nothing |
| `MACRO_EVENT_ANNOUNCEMENT_REACTION_R44_R45` | minutes-scale DIRECTION of the reaction to the released number | this holds equity through the release regardless of its content |
| `R32_EVENT_DRIVEN_CALENDAR`, `r46_3_spx_turn_of_month`, `SAME_MONTH_SEASONALITY` | turn-of-month, quarter-end, witching dummies; per-name month-of-year | none is an announcement calendar |
| `MACRO_TIMING_LANES_R63`, `MACRO_BETA_XS_STAGE15` | macro STATE levels and betas | conditioning on a state, not on when uncertainty is scheduled to resolve |

The post-publication decay of both effects is the named risk. The untouched confirmation window
(2016-01-01 .. 2026-08-25) begins after both papers were published and is decisive.

---

## 1. DATA - OWNED OR FREE, $0

| input | source | identity (sha256) |
|---|---|---|
| SPY daily total return | Norgate `SPY`, `StockPriceAdjustmentType.TOTALRETURN`, unpadded (1993-01-29..) | recorded in the artifact |
| Treasury bill rate | FRED `DTB3`, owned capture `global_multi_asset_frontier_r36\acquired\fred_st_louis_fed\DTB3.json` (acquired 2026-08-21) | `6028a3cbd7faa5e73f92e96e5523119beff50a858bc7030bfd555b5bd24c4faf` |
| CPI headline release days | ALFRED initial releases of `CPIAUCSL`, R46 capture `CPI_initial_releases_20260914T215141Z.json` | `874d988920998d0d23a481ef719ca60b7a7f7b8e5a49d1d75e24683d16437374` |
| CPI release calendar | FRED release 10, R46 capture `CPI_release_dates_20260914T215141Z.json` | `119cb6d7f518dac9444a997fabee0671d34a555cd617c88b8d88b9be57baff23` |
| Employment Situation headline days | ALFRED initial releases of `PAYEMS`, R46 capture `EMPLOYMENT_initial_releases_20260914T215141Z.json` | `520b03a7205018f70ffd4ecef8808f091d9f8752f1a805ea6e0ea13ae5b859bf` |
| Employment release calendar | FRED release 50, R46 capture `EMPLOYMENT_release_dates_20260914T215141Z.json` | `0697e0e34585f9e5517e66d27fc34a5262af24cff757d26aea1b55f3d14261e8` |
| FOMC 2021+ | R46 capture of `fomccalendars.htm`, `fomc_calendar_20260914T215143Z.html` | `5bfa5c19e52ed6eafe960c1ebf85c5702ed4c2cfe1749312181cb91b2f512ea3` |
| FOMC 1994-2020 | 27 public pages `federalreserve.gov/monetarypolicy/fomchistoricalYYYY.htm`, captured 2026-09-15 by `alpha_agent.alpha_recovery.scheduled_event_data`, manifest `alpha_recovery_offensive\data\scheduled_event_premium\fomc_history\fomc_history_manifest.json` | per-page sha256 in the manifest |

R46 captures are read BY PATH and never re-acquired; a changed hash is `DATA_HOLD`.

### 1.1 Event families (frozen)

1. **FOMC** - the decision day (last day) of every REGULARLY SCHEDULED FOMC meeting. Excluded and
   counted: conference calls (46 rows 1994-2011), unscheduled meetings (2013-10-16, 2014-03-04,
   2019-10-04, 2020-03-02, 2020-03-15), the cancelled 2020-03-17/18 meeting and notation votes. Two
   scheduled headings fewer than 7 calendar days apart are one meeting whose decision day is the
   later heading (the only case: 2003-09-15 and 2003-09-16 -> 2003-09-16).
2. **CPI** - every HEADLINE CPI release day: the first ALFRED `realtime_start` of each `CPIAUCSL`
   reference month, which must also appear on the FRED release-10 calendar (all do).
3. **EMPLOYMENT** - every HEADLINE Employment Situation release day, defined the same way on
   `PAYEMS` and release 50.

The FRED release calendars also list revision-only dates that publish no new reference month (CPI:
20 dates 1994-2026, mostly February seasonal-factor revisions; Employment: 8 benchmark dates).
They are EXCLUDED: they are not the monthly announcement.

### 1.2 Differences from the R46 forward rule, declared before returns

* R46's forward emitter uses every FRED release-calendar date; this test uses headline days only
  (1.1). About one date a year differs.
* A release on a market holiday (Good Friday Employment Situation, 10 cases; CPI on Good Friday 2017
  and 2020) is held over the NEXT session here; the R46 forward emitter holds the next weekday.

### 1.3 Census before returns (dates only)

Scheduled FOMC decision days per year after exclusions: 8 in every year 1994-2026 except 2020 (7).

| cell | window | independent windows | per year | held sessions | share of sessions | 2-session runs | holiday-mapped |
|---|---|---|---|---|---|---|---|
| PRE_FOMC | QUALIFICATION 1994-2015 | 176 | 8.0 | 176 of 5,540 | 3.18 % | 0 | 0 |
| PRE_FOMC | CONFIRMATION 2016-01..2026-08-25 | 84 | 7.9 | 84 of 2,676 | 3.14 % | 0 | 0 |
| ANNOUNCEMENT_DAY | QUALIFICATION | 665 | 30.2 | 690 of 5,540 | 12.45 % | 23 (+1 of 3) | 12 over the whole span |
| ANNOUNCEMENT_DAY | CONFIRMATION | 324 | 30.4 | 331 of 2,676 | 12.37 % | 7 | |

Qualification session overlap (ANNOUNCEMENT_DAY): Employment only 263, CPI only 251, FOMC only 162,
CPI+FOMC 13, Employment+FOMC 1.

---

## 2. THE TWO FROZEN CELLS (m_declared = 2)

* **PRE_FOMC** - held sessions `H = {first SPY session on or after each FOMC decision day}`
  (the frozen `r46_4_spx_pre_fomc_drift` calendar).
* **ANNOUNCEMENT_DAY** - held sessions `H = ` the union of the FOMC, CPI and EMPLOYMENT sessions
  (the frozen `r46_4_spx_announcement_day_premium` calendar).

No other cell exists or will be created: no CPI-only, Employment-only, FOMC-ex-crisis, intraday,
pre-announcement-only, two-session, leveraged or futures cell, before or after results.

### 2.1 The book (identical for both cells)

For every SPY session `s` in the evaluated window, with `s-1` the previous SPY session:

```
p(s)     = 1 if s in H else 0                         (100 % of NAV long SPY, else Treasury bills)
r(s)     = SPY_TR_close(s) / SPY_TR_close(s-1) - 1
c(s)     = DTB3 (percent) of the last observation dated strictly before s / 100 / 252
cost(s)  = cost_bps * 1e-4 * ( [p(s)=1 and p(s-1)=0] + [p(s)=1 and p(s+1)=0] )
x(s)     = p(s) * (r(s) - c(s)) - cost(s)             (daily net excess return over cash)
```

Consecutive held sessions are ONE window with one round trip; sessions carrying two families are
held once (no stacking, no leverage). The daily series `x` includes every flat session as 0.

### 2.2 Point in time

The FOMC schedule is published by the Board the year before; BLS publishes release dates months
ahead, and a delayed release (1996, 2013 and 2025 shutdowns) was rescheduled publicly before it
occurred. The position is entered at the close of `s-1`, BEFORE the announcement, and carries
nothing about the announcement's content. The bill rate uses only observations dated before `s`.
The cancelled 2020-03-17/18 meeting is excluded because its cancellation was public on 2020-03-15.

## 3. WINDOWS

* **QUALIFICATION:** sessions 1994-01-01 .. 2015-12-31. Halves: H1 1994-01-01..2004-12-31,
  H2 2005-01-01..2015-12-31.
* **CONFIRMATION (untouched):** sessions 2016-01-01 .. 2026-08-25 - ending the day before the R46.4
  event lane's first capture (2026-08-26), so history never overlaps the forward clocks. READ ONLY
  for a cell that passes every qualification gate.

## 4. COSTS

1 bp per side on full NAV (`alpha_agent.r63.SPY_PROXY_COST_BPS`), primary. Ladder 0 / 1 / 2 / 5 bp.
Survivability rung: 2 bp.

## 5. MEASUREMENT - existing owners only

* `ann_net = mean(x) * 252`; `sharpe = mean(x) / sd(x) * sqrt(252)`; t and one-sided p via
  `alpha_agent.r63.sensitivity.nw_tstat(x, 5)`; maximum drawdown via `S._max_dd(x)`.
* **Control:** the volatility-matched passive long `k * m(s)`, `m(s) = r(s) - c(s)` on EVERY session,
  `k = sd(x) / sd(m)` inside the evaluated window. Increment `d(s) = x(s) - k * m(s)`;
  `ann_inc = mean(d) * 252`, t via `nw_tstat(d, 5)`.
* Diagnostics reported, never gated and never a rescue: gross (0 bp) book; window-level mean and hit
  rate; beta-matched increment; per-family decomposition of ANNOUNCEMENT_DAY sessions; turnover
  (`2 * windows per year` of NAV) and capital usage (share of sessions invested).
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10, one-sided qualification p of BOTH
  cells (always computed) plus four inherited nulls at p = 1: R32_EVENT_DRIVEN_CALENDAR
  turn-of-month, quarter-end and triple-witching (3) and MACRO_EVENT_ANNOUNCEMENT_REACTION_R44_R45
  (1). m = 6.

## 6. GATES - in this order

**Gate 0 - DATA (whole mechanism) -> `DATA_HOLD`:** any frozen capture hash differs; a FOMC history
page is missing; any year 1994-2026 has fewer than 6 or more than 9 scheduled decision days; SPY
total return is finite on fewer than 99 % of sessions in either window; a bill rate within 10
calendar days before the session is missing on more than 1 % of sessions; independent windows below
the floors PRE_FOMC 150 (qualification) / 70 (confirmation), ANNOUNCEMENT_DAY 600 / 280.

Per cell, on QUALIFICATION, first failure fires:

1. **Frozen sign.** mean `x` at 1 bp <= 0 -> `WRONG_SIGN`.
2. **Standalone.** NW t < 2.0 -> `STANDALONE_T`.
3. **Materiality.** `ann_net` at 1 bp < 1.5 %/yr -> `MATERIALITY`.
4. **Increment over the volatility-matched passive long.** `ann_inc` <= 0 or its NW t < 2.0 ->
   `INCREMENT`.
5. **Stability.** `ann_net` <= 0 in either half -> `STABILITY`.
6. **Drawdown.** maximum drawdown of `x` worse than 1.5 x the control's -> `DRAWDOWN`.
7. **Multiplicity.** not a BH survivor at q = 0.10, m = 6 -> `MULTIPLICITY`.
8. **Cost survivability.** `ann_net` at 2 bp < 1.5 %/yr -> `COST`.
9. **Untouched confirmation.** mean `x` <= 0, NW t < 2.0, `ann_net` < 1.5 %/yr, or `ann_inc` <= 0
   -> `CONFIRMATION`.
10. Otherwise the cell is QUALIFIED.

**Mechanism verdict:** `DATA_HOLD` if gate 0 fires; `QUALIFIED` if at least one cell qualifies;
otherwise `NO_EDGE`, with every cell's fired gate recorded. A QUALIFIED verdict raises a HUMAN gate
(prospective registration, contract rule 16). **CAPITAL ELIGIBLE = NO** in every case.

## 7. FALSIFICATION - and what will NOT be done

A null means: holding SPY only over scheduled FOMC, CPI and Employment Situation sessions earned no
after-cost premium beyond what the same risk earned holding SPY all the time, on history or on the
untouched post-publication decade.

**Forbidden after results:** reversing the sign; deleting events (2008, 2020, crisis FOMC days,
shutdown releases); adding unscheduled meetings or other releases (PPI, GDP, retail sales, ECB);
switching to the all-FRED-dates rule; family-subset cells; intraday or pre-2pm windows; t-1 or
two-session holds; volatility targeting or leverage; ES futures or other legs; moving the halves or the
confirmation boundary; relaxing any threshold; reading the confirmation after a qualification failure.

## 8. SAFETY

RESEARCH ONLY. No purchase, subscription, credential, registration, promotion, capital allocation,
portfolio mutation, order, fill, backfill or live write. The live checkout
`C:\Users\binis\paper_trader` is not touched.
