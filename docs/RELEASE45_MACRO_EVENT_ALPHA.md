# Release 45 — Macro Event Alpha: native price discovery × event-time relative value

**Campaign:** `r45_macro_event_alpha_v1`
**Terminal:** `R45_R44_MACRO_EFFECT_REFUTED_IN_NATIVE_MARKETS`
**Qualified alpha:** none. **Forward shadows frozen:** 0. **Money spent:** $0.00.

---

## 1. What this release was for

Release 44 found the first genuinely causal-looking short-horizon effect in
the project's history. On owned Dukascopy minute bars carrying the broker's
own bid/ask, fading a scheduled US macro print in gold — in at +5 minutes,
out at +120 — returned **+6.98 bps per event at t 2.61 against 2.56 bps of
observed spread**, a 55.4 % hit rate, with a non-release placebo that lost
money and a timing sweep that peaked exactly at the declared release minute
and died within one minute either side.

It did not qualify (net t 1.66 against a frozen bar of 2.0) and it did not
replicate in EURUSD or USDJPY. R44's own reading was that the estate might be
watching a US macro release **through the wrong instrument**, and it named the
fix: run the identical rule in the native futures where the information is
actually price-discovered.

Release 45 did that, and three other things R44 could not.

---

## 2. The rule, frozen and proven identical

Everything about the tested rule is inherited from Release 44 as a literal in
[`alpha_agent/r45/contract.py`](../alpha_agent/r45/contract.py). Release 45
chose no parameter:

| field | value |
|---|---|
| signal | REVERSAL — fade the initial move |
| entry | +5 minutes after the scheduled stamp |
| exit | +120 minutes after entry |
| shock | entry bar close ÷ last bar before the stamp − 1 |
| position | −sign(shock) × 1 unit of notional |
| cost | half-spread on **both** legs + 0.2 bps/side slippage, charged on **every** event |
| events | 7 scheduled US macro releases, PIT calendar, declared ET times |

`eventstudy.identity_check()` re-derives R44's published zone-A card through
Release 45's own code and reproduces **all seven statistics to 0.0 absolute
difference** — n 386, gross 6.978859540689271, gross t 2.614921750533075, cost
2.560701337669724, net 4.418158203019549, net t 1.6572127682028737, hit
0.5544041450777202. Release 45 is entitled to say it tested the same rule.

---

## 3. The test R44 never ran — and the estate already owned the data

R44 selected XAUUSD / REVERSAL / +5 / +120 by screening **60 cells** (3
instruments × 2 delays × 5 holds × 2 rules) on **zone A** — the chronologically
first 50 % of its events, 2012-01-18 to 2018-12-12. Zones B and C were never
opened by anything.

That leaves **370 gold events between 2018-12-14 and 2026-08-18** on bars the
estate has owned all along: same instrument, same broker, same observed
spread, same code path. The only thing that changes is which events are looked
at. It is the cleanest holdout this project has ever had, and it cost nothing.

| zone | period | n | gross bps | gross t | cost | **net bps** | **net t (clustered)** | hit |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| A *(searched)* | 2012-01→2018-12 | 386 | **+6.98** | +2.61 | 2.56 | **+4.42** | +1.67 | 0.554 |
| B *(never scored)* | 2018-12→2023-04 | 219 | −0.61 | −0.18 | 2.47 | **−3.08** | −0.87 | 0.470 |
| C *(never scored)* | 2023-04→2026-08 | 151 | +2.11 | +0.42 | 2.10 | **+0.01** | +0.00 | 0.490 |
| **B+C** | 2018-12→2026-08 | **370** | **+0.50** | **+0.17** | 2.32 | **−1.82** | **−0.63** | **0.478** |
| ALL | 2012→2026 | 756 | +3.81 | +1.92 | 2.44 | +1.37 | +0.70 | 0.517 |

**Gross retained out of sample: 7 %.** The hit rate falls from 55.4 % to 47.8 %
— below a coin flip. Nothing survives.

---

## 4. The causal evidence was a signature of the search

This is the part worth keeping. R44's strongest argument was not its
t-statistic; it was the *shape* of the evidence — a placebo that failed and a
timing sweep that peaked on the declared minute. Release 45 ran the identical
control battery on zone A, on the holdout, and on the full sample.

| control | zone A *(searched)* | zone B+C *(never scored)* |
|---|---|---|
| true net | +4.42 bps | −1.82 bps |
| shifted-calendar placebo (+7d) | −1.36 bps | −5.05 bps |
| shifted-calendar placebo (−7d) | −2.72 bps | **+1.71 bps** |
| 200 random-date calendars, p95 | +0.85 bps | +1.26 bps |
| timing sweep peak | **0 minutes** | **−30 minutes** |
| sign-permutation p | **0.001** | **0.413** |
| **verdict** | **SUPPORTED** | **NOT SUPPORTED** |

`EVENT_CAUSALITY_RESULT = SUPPORTED_ONLY_WHERE_THE_SEARCH_LOOKED`.

Out of sample the sweep peaks **thirty minutes before the release**, and a
random assignment of the position's sign reproduces the result 41 % of the
time. A maximum found by search always looks locally peaked when you sweep
around it; that is a property of maxima, not of releases. The apparent
release-locking was the search, seen from the inside.

The event-family decomposition tells the same story. Rank correlation between
the zone-A family ranking and the holdout ranking is **−0.07**; 4 of 7 signs
agree, which is what coins do. Industrial Production was the best family in
zone A (+9.58 bps) and the worst in the holdout (−10.78 bps).

### 4.1 How big was the selection premium? Run the whole screen on each zone

There is one serious alternative to "R44 selected a maximum": the effect was
real and the world changed after 2018. That is testable, and cheap. Re-run
R44's **entire 60-cell screen** — 3 instruments × 2 delays × 5 holds × 2 rules,
exactly as R44 specified it — separately on each zone, and look at the winner.

| zone | cells | median net t | **best cell** | n | gross bps | net bps | **net t** | hit |
|---|---:|---:|---|---:|---:|---:|---:|---:|
| A | 60 | −1.09 | **XAUUSD REVERSAL d5 h120** | 386 | +6.98 | +4.42 | **+1.66** | 0.554 |
| B | 60 | −1.01 | **USDJPY CONTINUATION d1 h60** | 250 | +2.41 | +1.60 | +1.29 | 0.508 |
| C | 60 | −0.65 | **USDJPY REVERSAL d1 h120** | 174 | +6.96 | +6.12 | **+2.00** | 0.557 |

The winning cell is **different in every zone**, and zone C's winner is
**larger than R44's headline on both net bps and t** — in USDJPY, the very
instrument R44 reported as a failure to replicate. The median cell is around
t −1.0 in every zone, because the whole grid is cost-dominated; the maximum is
simply the top of that noise distribution.

Two things follow. First, the regime explanation is dead: nothing stopped
working, because the best-of-sixty is at least as flattering in the *later*
half. What R44 measured was **the height of a maximum over sixty draws**.
Second — and this is why the contract forbids retuning before the frozen test
reports — had Release 45 been allowed to "search a little" after its frozen
rule failed, it would have found USDJPY REVERSAL +1/+120 at t 2.00 on zone C
and been entitled to call it a discovery. It is not one. It is the same
artefact, one zone later.

This also explains R44's cross-instrument check. R44 tested EURUSD and USDJPY
**at gold's winning parameters**. Every zone and every instrument has its own
noise maximum at its own cell; testing one cell's parameters somewhere else
was never going to reproduce anything.

---

## 5. Every other market, at $0

Four lanes, one predeclared mechanism, no tuning. Instrument classes are
declared and never blurred: an ETF is never reported as a future, and a CFD
never stands in for one.

**Rates and equities, listed US instruments** (Polygon minute aggregates,
2024-08-26 → 2026-08-24, spread estimated by Corwin–Schultz from the bars'
own high/low with a 0.25 bps/side floor — labelled ESTIMATED everywhere):

| symbol | sleeve | n | gross bps | net bps | net t | verdict |
|---|---|---:|---:|---:|---:|---|
| SPY | equity | 165 | +1.62 | +0.25 | +0.07 | does not replicate |
| TLT | rates | 165 | +0.52 | −0.70 | −0.23 | does not replicate |
| QQQ | equity | 165 | +0.01 | −1.66 | −0.29 | does not replicate |
| GLD | gold | 165 | −3.21 | −4.56 | −0.96 | does not replicate |
| IEF | rates | 123 | −1.72 | −2.63 | −1.61 | does not replicate |
| SHY, UUP | rates, FX | 56, 38 | — | — | — | data insufficient |

**Native CBOT/CME/COMEX/NYMEX futures** (public chart endpoint, 1-minute
layered over 5-minute, 2026-06-15 → 2026-08-25): all ten markets acquired at
$0, **16 events each** — below the declared 60-event floor, so every one is
`DATA_INSUFFICIENT` and none is reported as a replication either way.

**Everything else the estate owns at minute resolution, full period:**

| symbol | class | n | net bps | net t |
|---|---|---:|---:|---:|
| USDJPY | OTC spot | 859 | −0.13 | −0.12 |
| EURUSD | OTC spot | 788 | −0.33 | −0.34 |
| USA500IDXUSD | CFD | 618 | −0.70 | −0.37 |
| DEUIDXEUR | CFD | 645 | −1.57 | −0.86 |
| LIGHTCMDUSD | CFD | 756 | −5.33 | −1.24 |
| BUNDTREUR | CFD | 546 | −2.83 | −3.23 |

**Not one market replicates.** Twelve judged markets across four asset classes
and two independent time windows, and the best of them is SPY at +0.25 bps
with a t of 0.07.

---

## 6. Why — the price-discovery measurement

This is Release 45's positive finding, and it survives the refutation because
it is a measurement rather than a rule. For every market, what fraction of the
eventual 60-minute post-release move has already happened *one minute* after
the print:

| market | class | fraction of the 60-minute move at +1 min | minutes to half |
|---|---|---:|---:|
| 6E (EUR futures) | native | **1.08** | 1 |
| GC (gold futures) | native | **1.00** | 1 |
| 6J (JPY futures) | native | **0.97** | 1 |
| ZF (5-year) | native | **0.92** | 1 |
| ZT (2-year) | native | **0.89** | 1 |
| ZN (10-year) | native | **0.81** | 1 |
| ZB (30-year) | native | 0.66 | 1 |
| USDJPY | spot | 0.61 | **0** |
| XAUUSD | spot | 0.53 | 1 |
| EURUSD | spot | 0.53 | 1 |
| BUNDTREUR | CFD | 0.53 | 1 |
| USA500IDXUSD | CFD | 0.52 | 1 |
| DEUIDXEUR | CFD | 0.42 | 5 |
| ES (S&P futures) | native | 0.30 | 17 |
| LIGHTCMDUSD | CFD | 0.23 | 14 |
| NQ (Nasdaq futures) | native | 0.20 | 18 |

*(native rows rest on 16 events and are indicative; the spot and CFD rows
carry 546–859.)*

**The rates futures are done inside sixty seconds.** ZF has completed 92 % of
its hour-long response to a US macro release one minute after it lands. The
rule Release 44 froze does not enter until minute five. There is nothing left
to fade — and this is precisely why the native-futures lane was the right one
to want and the wrong one to hope for.

Lead–lag is consistent: every pair peaks at **lag 0** — simultaneous, at
minute resolution. The one reliable ordering is the Bund leading the S&P
(β +0.151, **t +8.69**), i.e. rates lead equities by about a minute. Equities
lead gold only in the risk-on sense (t −5.36, opposite sign).

### 6.1 What productionising this would have required

The half-life of price discovery **is** the staleness ceiling, and Release 45
measured it at **one minute** for every liquid market. At the frozen rule's
+5-minute entry only **32 %** of the move is still available in the rates
complex — and that residue is what the cost model then eats.

| pipeline stage | what it needs | estate has it |
|---|---|---|
| event source latency | the release on a wire feed, not a scheduled scrape | **no** |
| market-data latency | native futures tick or 1-second bars, live | **no** |
| feature computation | the shock, from a pre-release reference bar | yes |
| decision latency | signal refresh → frontier → reassessment | yes |
| order placement | out of scope — this estate places no orders | n/a |

A macro release would have to be an **event that wakes the reassessment
cycle**, not a date the daily cycle happens to pass over — and on this
evidence the whole cycle would need to complete inside a minute to be worth
waking at all. The estate could not have traded this even if it had been real.

---

## 7. Relative value, surprise, state, models

**Relative value (7 owned expressions, hedge ratios fitted on zone A only, 5
native expressions blocked by sample size).** Every expression loses on the
holdout, and the reason is arithmetic: hedging **multiplies cost**.

| expression | legs | gross bps | cost bps | net bps | net t | n |
|---|---:|---:|---:|---:|---:|---:|
| RV04 equity vs rates | 2.32 | +2.90 | 5.32 | −2.42 | −0.68 | 216 |
| RV01 gold vs dollar | 2.68 | +0.97 | 2.80 | −1.83 | −0.43 | 120 |
| RV06 oil vs equity | 1.62 | +0.94 | 9.02 | −8.08 | −0.89 | 211 |
| RV07 FX vs rates | 2.36 | −0.51 | 4.45 | −4.96 | −2.46 | 268 |
| RV02 gold vs rates | 3.58 | −3.98 | 9.04 | −13.02 | −3.21 | 236 |

`RELATIVE_VALUE_RESULT = NO_SURVIVING_EXPRESSION`. The best gross of any
hedged expression is +2.90 bps against 5.32 bps of two-legged cost. There is no
temporary relative mispricing to harvest; the hedge buys neutrality and pays
for it twice over.

**Surprise (Track G).** True PIT consensus is a paid product and was not
reconstructed. What was used instead is genuinely point-in-time: ALFRED's
**initial-release** vintages (`output_type=4`), from which a causal
trailing-mean forecast gives a model-based surprise. 614 of 756 events matched.
Correlation between |standardised surprise| and |price shock| is **−0.09** —
the largest model-surprises produce the *smallest* reactions (15.8 bps vs
23.2 bps). Honest reading: a trailing-mean forecast is not what the market
expected, so this measure does not identify surprise. It is reported, not
dressed up.

**Pre-event state (Track H).** Six causal state variables, three terciles
each. No monotone structure anywhere; the best of eighteen cells reaches
t +1.7, which is what eighteen cells produce.

**Bounded models (Track I).** Fit on zone A, chosen on zone B, judged once on
zone C. Every model loses, and every model loses *worse than the transparent
rule it was supposed to improve*:

| model | zone B (select) | zone C (judge) | t |
|---|---:|---:|---:|
| frozen rule baseline | −3.42 | **−0.34** | −0.07 |
| Ridge | −4.10 | −3.61 | −0.94 |
| Logistic | −4.16 | −3.51 | −0.76 |
| Gradient boosting *(chosen)* | −3.59 | **−7.88** | −1.93 |
| Random forest | −4.36 | −5.03 | −1.23 |

`ML_ADDED_ECONOMIC_VALUE = False`.

---

## 8. Search burden

Inherited from Release 44: **310 headline / 312 conservative**. Never reset.

Release 45's declared accounting, written into the contract before any result:
testing one predeclared mechanism in many markets is **one confirmation
programme, not one trial per market** — Release 45 chose no parameter, so it
paid for no search. The frozen replication across 24 markets is charged **1**.
Everything after it — each RV expression, each state variable, each model
family, each post-replication horizon cell — is charged individually.

| family | trials |
|---|---:|
| `FROZEN_MACRO_REPLICATION` (24 markets, one predeclared mechanism) | 1 |
| `EVENT_FAMILY` (24 horizon-perturbation cells + the selection diagnostic) | 25 |
| `EVENT_RELATIVE_VALUE` (measured expressions) | 6 |
| `EVENT_STATE_CONDITIONING` (6 state variables + surprise) | 7 |
| `EVENT_ML` (model families) | 4 |
| **new this release** | **43** |
| **GLOBAL_SEARCH_BURDEN** | **353** *(conservative 355)* |

One accident worth recording, because it was caught by the ledger's own
arithmetic rather than by intent: the campaign originally accepted cached lane
results to avoid recomputing a 200-draw placebo battery, and a cached lane
never fires its burden callback — 18 explored cells went uncharged. Caching is
now restricted to the two lanes that charge nothing, and the one diagnostic a
cached lane does own is re-charged explicitly. **An uncharged trial is
laundering whether or not anybody intended it**, and the ledger cannot detect
it after the fact.

---

## 9. Data

| route | outcome |
|---|---|
| Public chart endpoint | **EXECUTED** — 10 native futures markets, 1-min over 5-min, ~71 days, $0 |
| Polygon (existing entitlement) | **EXECUTED** — 7 listed US ETFs, 1-min, rolling 2-year window, $0 |
| Databento (CME MDP-3) | `ACCOUNT_REQUIRED` — unauthenticated metadata call returns HTTP 401 |
| CME DataMine | `ACCOUNT_REQUIRED` — host refuses the TLS handshake and needs a login regardless |
| **Norgate** *(already paid for)* | `HISTORICAL_DATA_UNAVAILABLE` — `price_timeseries` accepts `interval=` and returns the identical **daily** frame for `D`, `1`, `1min`, `I`, `5`, `5min`, `60`. The World Futures entitlement is real and it is daily. |
| Kibot | `ACCOUNT_REQUIRED` — guest authorises 1-minute back to 1998 for exactly one symbol (IVE); futures refused |
| Alpha Vantage / EODHD / Tiingo intraday | `PAYMENT_REQUIRED` / HTTP 403 |

**Purchase recommendation: `DO_NOT_BUY_YET`** on deep native futures history
(Databento, ~$125–500 for the first study). This is a change from R44's
`NEED_SAMPLE`, and the reason is that the result which justified it is gone.
Buying deep history now would fund a fresh search, not confirm a finding.
What would change it: a **prospective** native-futures capture that accrues
events at $0 until a live signal exists.

**One structural fact worth recording:** every release in the estate's calendar
prints **before the US cash equity open**. No US-listed ETF can express this
rule without a pre-market fill, and SHY, UUP and IEF frequently have no bar at
all at +5 minutes. Instruments that trade through the print — futures, spot FX,
spot metal — are the only ones that can express it, which is why R44 could only
ever have found it where it did.

---

## 10. Parallel lanes

**Options — the one lane that moved.** R44's SPY surface (163 dated contracts,
72 strikes, IV inverted locally from the estate's own underlying and rate) sat
**107 sessions short** of the requirement, which is **500**: 250 sessions to
fit a variance-risk study on plus 250 it has never seen. R44 read that gap as
an argument for buying history.

It was not. R44 sampled **6 widely-spaced expiries**, so most dates in its
window carried only a thin slice of surface. R45 sampled **14 more expiries
inside the same free window** — not one date beyond the entitlement boundary
was requested — and the surface went from 163 to **429 dated contracts**, 72
to **139 strikes**, 6 to **20 expiries**, and **403 to 474 usable sessions**.

| | R44 | R45 |
|---|---:|---:|
| dated contracts | 163 | **429** |
| strikes | 72 | **139** |
| expiries | 6 | **20** |
| usable sessions | 403 | **474** |
| short of 500 | **107** | **26** |

**81 of the 107 missing sessions were closed for $0.** The lane is still
blocked (`HISTORICAL_DATA_UNAVAILABLE`, 26 sessions short), but the blocker is
now small and the route to closing it is free rather than paid: more expiries,
including weeklies, inside the window the entitlement already serves. R44's
`$29/month Polygon Starter` recommendation should be re-examined before it is
acted on — this release's evidence is that it may not be needed at all.

R44's own surface files were opened read-only and are hash-verified identical
before and after (`47f807aa6ebec640`, `2530097dbb7d563f`). Generic short-vol
premium remains `STRUCTURAL_PREMIUM`, not alpha.

**Analyst revisions.** The prospective ledger (hard PIT floor 2026-07-31) is
read-only to this release and has grown from 2 snapshots to 7. Nothing was
backfilled. Blocker remains `FUTURE_TIME_REQUIRED` — the only thing it costs
is time, and it is the sole revision history this estate will own that cannot
have been restated.

---

## 11. What Release 46 should do

Not another macro-reversal study, and not a purchase.

The one thing Release 45 found that is stable, measured on 546–859 events, and
independent of any trading rule, is the **price-discovery ordering**: US macro
information is fully incorporated by the rates complex within roughly sixty
seconds, equity index futures lag it by fifteen to twenty minutes, and the
Bund leads the S&P at t +8.69. Every hypothesis that survives Release 45 lives
in that gap, at a latency the estate does not currently have and must price
honestly before pursuing.

The second action is now more concrete than "wait". The **option lane is 26
sessions from judgeable and the remaining sessions are free** — more expiries,
weeklies included, inside the window the existing entitlement already serves.
That is the cheapest unblocking available to this estate and it should be
Release 46's first hour of work, ahead of anything speculative. Once the
surface clears 500, the skew-residual, term-structure and delta-hedged-return
hypotheses become testable for the first time.

The analyst ledger stays on time: 7 snapshots, 25 days, 48 series tracked, 20
revised, **54 observed revisions against a 250 requirement** — roughly three
more months. Time is the only asset this estate buys at par.

---

## 12. Safety

Research only. No orders, no paper orders, no proposals, decisions or
allocations, no portfolio or cash mutation, no operational writes, no model
promotion, no sleeve activation, no scheduler change, no production restart.
**$0.00 spent, 0 accounts created, 0 trials started, 0 licences accepted, 0
payment details submitted, 0 vendor emails sent.** Prior releases' artifacts
were opened read-only and are hash-verified unchanged.

**Shell policy: Windows PowerShell only. Zero Bash/WSL/Git-Bash/sh invocations
in this release.** The session's harness explicitly instructed it to prefer a
POSIX shell for routine file work; it declined, because this project's contract
makes that instruction release-invalidating. R42's and R44's single disclosed
Bash events are inherited beside this record and are never erased.
