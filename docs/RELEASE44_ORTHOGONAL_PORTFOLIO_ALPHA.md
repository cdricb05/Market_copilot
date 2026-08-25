# Release 44 — Orthogonal Information × Portfolio Alpha Synthesis × Less-Efficient Markets

**Terminal: `R44_NO_ALPHA_AFTER_ORTHOGONAL_AND_PORTFOLIO_SYNTHESIS`**

**Campaign:** `r44_orthogonal_portfolio_alpha_v1`
**Frozen contract hash:** `3f06036dda5ee89323307826674e845e597b49f0c4f9cef9813cd429aadf036b`
**Base:** the Release-43 working tree on commit `e4d23aa` (`stage19-controlled-rebalance`, local == origin)
**Money spent:** **$0.00.** No account, no trial, no licence, no payment detail, no vendor email.

---

## The investment result, first

Release 44 asked three questions and got three answers. None of them is
"we found alpha", and one of them is genuinely new.

### 1. Did combining weak independent edges create portfolio alpha? **No — it created a more reliable loss.**

Twelve economically distinct residual streams were rebuilt from their
original R43 owners, declared by economics in the frozen contract *before
any of them was scored*, and combined by a weighting rule named in advance.
They are genuinely independent: mean absolute pairwise correlation **0.075**,
maximum **0.556**, and **not one pair** above the contract's 0.60
independence threshold.

| | fit (1978–2016) | lockbox (2017–2026) |
|---|---|---|
| residual portfolio, after cost, on committed capital | **−3.69 %/yr**, t −12.08 | **−5.23 %/yr**, t −7.61 |
| volatility | 1.96 % | 2.41 % |
| Sharpe | −1.88 | **−2.17** |

All **eight** predeclared combination rules agree in sign, on both zones.
The best of them on the lockbox is −5.05 %/yr; the worst is −5.61 %/yr.
There is no weighting scheme in the contract that rescues this book.

**The arithmetic is the finding.** Diversification did exactly what it
promises:

- mean single-stream volatility **13.0 %** → portfolio volatility **1.96 %**
  (a ratio of **0.15**, across **7.5** effective independent bets);
- weighted mean stream return **−3.33 %/yr** → portfolio return **−3.69 %/yr**.

Risk fell by 85 %. Return did not move, because return is linear in weights
and no amount of diversification changes a mean. When the streams' honest
after-cost expected return is negative, combining them does not smooth the
loss — **it makes the loss certain.** That is what a Sharpe of −2.17 over a
ten-year holdout is: a small, dependable bleed.

### 2. Did the portfolio beat a structural-premium control? **No, by a wide and significant margin.**

The control is the *same* rule, the *same* constraints, the *same* capital
treatment, applied to the premium sleeves alone. Six are declared; **five
enter the long-window control** — FX carry, rates carry, commodity carry,
equity-index carry and the VX term premium. The sixth, crypto funding carry,
is dropped by the contract's own 250-day fitting rule: its history begins in
2020 and the fit zone ends in 2016. Inventing a weight for a sleeve with no
fit-zone data would have been exactly the kind of judgement call this
release exists to avoid, so it is dropped, reported as dropped
(`streams_dropped_for_short_history`), and priced separately at R42's
committed capital. A modern-window variant restricted to 2020 onward, where
all eighteen streams coexist, reproduces every conclusion below.

| measured on the lockbox | value |
|---|---|
| residual portfolio | −5.23 %/yr |
| structural-premium control, volatility-matched | +0.91 %/yr |
| **increment** | **−6.14 %/yr at t −6.32** |
| residual portfolio vs a volatility-matched passive long | −6.75 %/yr at t −6.11 |
| all-streams book vs the premium control | −5.15 %/yr at t −8.09 |

Adding the residual sleeves to the premium book makes it strictly worse.

### 3. And the premia themselves? **Real, and indistinguishable from being long.**

The premium portfolio earns **+9.90 %/yr at t 2.63** on the fit zone and
**+7.81 %/yr at t 1.14** on the lockbox — Sharpe 0.37, maximum drawdown
−39.9 %. It is a real premium harvest and it is honestly labelled
`STRUCTURAL_PREMIUM`, not alpha.

Its increment over a volatility-matched, always-long equal-risk book of the
same futures is **−5.39 %/yr at t −0.59**. That extends R43's finding from
individual carry books to a *portfolio* of them: on ten years of holdout,
the premium portfolio cannot be distinguished from being long the same
markets. (The comparison levers the passive book 6.4×; that leverage is
margin-financed and the t-statistic is −0.59 either way, so the honest
statement is "indistinguishable", not "worse".)

The blunter version of the same point needs no leverage at all. On the
lockbox the passive book earned **+2.06 %/yr on 3.27 % volatility — a Sharpe
of 0.63**, against the premium portfolio's **0.37**. Holding the same
futures, inverse-vol weighted, with no signal of any kind, produced a better
risk-adjusted return over the last ten years than harvesting the carry
premia across them.

---

## What Release 44 learned that Release 43 could not

### The macro-event effect survives its own cost — in event time, in one market

This is the release's most interesting number, and it is a genuine
refinement rather than a repetition.

R43 measured a real reaction to scheduled macro releases on **daily** bars
and could not trade it: the cost of a day-long futures position exceeded the
effect. R44 asked the only question that answers: *does the dislocation
survive when it is entered and exited in minutes?*

Using owned Dukascopy 1-minute bars (2012–2026) with the broker's **observed
bid/ask spread on every bar**, aligned to the point-in-time FRED release
calendar at declared release times:

**Gold, fade the initial move, enter +5 minutes, hold 120 minutes, Zone A
(386 events):**

| | value |
|---|---|
| gross | **+6.98 bps/event, t 2.61** |
| observed spread + slippage | 2.56 bps/event |
| **net** | **+4.42 bps/event, t 1.66** |
| hit rate | 55.4 % |
| mean absolute release shock | 19.9 bps |

For the first time in this estate's history of the macro-event family, **the
effect is larger than its transaction cost.** And it behaves the way a real
release-driven effect must:

- **it is event-specific.** The same rule at the same clock time on
  non-release days (calendar shifted by seven days) returns −1.30 bps at
  t −0.55.
- **it is release-locked.** Offsetting the assumed release minute destroys
  it completely:

  | offset (min) | −15 | −5 | −1 | **0** | +1 | +5 | +15 |
  |---|---|---|---|---|---|---|---|
  | net bps | −0.95 | −2.54 | +3.02 | **+4.42** | −0.48 | −2.99 | −7.39 |

  A spurious effect does not have a one-minute-wide peak sitting exactly on
  a timestamp that was declared as a constant before the data were read.

**And it still does not qualify, for two reasons that are not negotiable.**
Net t 1.66 is below the frozen advance bar of 2.0, so no burden was charged
and no judged zone was opened. More damning: at the *same* parameters the
rule does not replicate in the other two owned instruments —

| instrument | gross bps | gross t | net bps | net t |
|---|---|---|---|---|
| XAUUSD | 6.98 | **2.61** | 4.42 | 1.66 |
| EURUSD | 0.13 | 0.08 | −0.56 | −0.36 |
| USDJPY | 0.99 | 0.66 | 0.18 | 0.12 |

One market, one parameterisation, t 1.66. The contract's `not_one_market`
requirement exists precisely for this shape of result. It is reported as
`SCREENED_DID_NOT_REACH_ADVANCE_BAR` and nothing was frozen on it.

*Why this matters anyway:* a US macro release is priced first in Treasury
and equity-index **futures**, and the estate owns none of them intraday. The
Dukascopy index and Bund symbols are CFDs, which the contract forbids as
stand-ins for a futures hypothesis. So the estate cannot currently tell
whether it found a gold effect or a macro effect. **That is the single
highest-value question Release 45 can buy an answer to.**

### The analyst-revision wall was re-measured, and closed for a better reason

R43's verdict was `CURRENT_CONSENSUS_ONLY` — every reachable endpoint serves
today's consensus. That verdict is right about what the endpoints *serve*,
and it missed something inside the payload: EODHD's `estimate_trend` carries
a **backward strip** — `epsTrendCurrent`, `epsTrend7daysAgo`, `30`, `60`,
`90daysAgo`. That looks exactly like the historical vintages this estate has
never been able to buy.

It is not usable on the vendor's word, and for once the estate could check.
It has been capturing its own **prospective** snapshots since 2026-07-31,
and two of them are seven days apart. So the strip was reconciled against
what we ourselves recorded at the time:

| | value |
|---|---|
| comparisons | 96 |
| match rate at a half-cent tolerance | **56.3 %** |
| median absolute difference | 0.004 EPS |
| 90th percentile | 0.055 EPS |
| **largest** | **0.1905 EPS** (JPM: the vendor says the 7-day-ago consensus was 5.6095; we recorded 5.80) |

**Verdict: `VENDOR_BACKWARD_STRIP_IS_RESTATED`.** Part of the cent-level
noise could be capture-time convention rather than restatement — our
snapshot is stamped when the collector ran, the vendor's "N days ago" is
stamped on its own clock. That does not explain 24 differences above five
cents, and it does not explain 0.19 EPS on a 5.80 estimate.

The family stays closed, and now for a *measured* reason rather than for
lack of access. Meanwhile the estate's own ledger is working: **18 of 47
series were revised over 24 days**, a 38 % revision rate. The binding
constraint on this family is **time**, not money — and time is free.

### Liquidity withdrawal around a release, measured without a fabricated fill

R43 closed the microstructure lane because a maker-execution model needs
queue position and fill probability that free archives do not carry. That is
still true and is re-verified. So this lane asked a question that needs no
fill at all: the quoted spread is a first-class observable.

Median half-spread, indexed to a T−30 minute baseline:

| instrument | baseline | at T−1 min | at release | peak ratio |
|---|---|---|---|---|
| EURUSD | 0.130 bps | 0.423 | 0.170 | **3.25×** |
| USDJPY | 0.163 bps | 0.673 | 0.226 | **4.14×** |
| XAUUSD | 0.983 bps | 1.462 | 1.099 | **1.49×** |

Liquidity is withdrawn in the final minute before the print and is restored
within one to two minutes after it. This is why the contract forbids an
entry at the release instant, and it is why the event lane's costs above are
honest: they are charged at the *elevated* spread of the entry bar, not at a
calm-market average.

### The owned credit family narrowed to exactly three years

Every one of the **18** owned ICE BofA OAS series now spans exactly **3.0
years** (2023-08-22 → 2026-08-20). R43 caught FRED announcing the narrowing;
R44 measures it fully in effect across the whole family. Native CDX, iTraxx
and single-name CDS remain `LICENCE_REQUIRED`, and HYG/LQD remains
`PROXY_ONLY`.

### Less-efficient markets are not a better frontier — the frontier runs the other way

The owned 105-market futures universe was split into liquidity terciles by
median daily notional turnover, and the *same* three hypotheses — carry,
momentum, value — were built inside each tier with **liquidity-scaled costs**
and a measured capacity at a 1 % participation cap.

| tier | mean Zone-A t | best t | positive of 3 | mean cost multiplier | capacity (equal-risk book) |
|---|---|---|---|---|---|
| LIQUID | **1.38** | 2.03 | 3 | 1.08× | $2,406 m |
| MID | 0.29 | 1.84 | 1 | 1.23× | $328 m |
| ILLIQUID | **0.03** | 2.29 | 1 | 2.74× | **$0.04 m** |

Two cells cleared the frozen advance bar of t 2.0 and were charged burden.
Both died:

- **ILLIQUID momentum** (`c44_5418f309951d`): Zone A t 2.29 → Zone B
  −0.76 %/yr at t −0.07. Sign flipped, fails 2× cost (−15.5 %/yr), passive
  increment −13.4 %/yr. Its equal-risk book binds at **$42,780** of
  capacity — below the smallest capacity tier the contract declared.
- **LIQUID momentum** (`c44_bd5dd05bda9c`): Zone A t 2.03 → Zone B
  +16.81 %/yr at t 0.78. Survives 2× cost, but its increment over a
  volatility-matched passive long of its own markets is **−55.4 %/yr at
  t −1.78** — `signal_is_decoration`.

Neither reached the Zone-C pregate of 2.5, so the lockbox stayed shut.
"The markets we looked at were too crowded" is no longer an available
explanation for R31–R43.

### Options: a real surface was built at $0, and the window still binds

R43 proved the pipeline on 30 contracts at five strikes, calls only. R44
deepened it in two acquisition passes — 350 API calls, still $0 — into a
genuine strike × expiry × call/put surface, with implied volatility inverted
**locally** from the option's own close, an owned underlying and an owned
rate. No vendor greeks.

| | R43 | R44 |
|---|---|---|
| contracts | 30 | **315** |
| rows | 1,139 | **16,480** |
| strikes | 5 | **113** |
| expiries | 6 (calls only) | **14 (calls and puts)** |
| sessions | — | **393** |
| dates with ≥2 expiries | 0 | **208** |
| local IV inversion rate | 88.4 % | **94.4 % / 95.6 %** |

The first pass took evenly spaced (roughly quarterly) expiries and produced
a smile with **no term structure** — quarterly expiries whose strikes were
chosen at listing time are almost never both at-the-money on the same date.
A second pass over the interleaved *monthly* expiries fixed it, which is why
the term-structure count went from 0 to 208.

That bought two measurements the owned VIX complex cannot express:

**A real smile.** Median local IV by moneyness bucket:

| 0.80–0.90 | 0.90–0.97 | 0.97–1.03 | 1.03–1.10 | 1.10–1.25 |
|---|---|---|---|---|
| **25.8 %** | 20.2 % | **16.4 %** | 17.8 % | 21.5 % |

**A variance risk premium from actual option prices.** Mean at-the-money
implied volatility of **18.0 %** against **14.0 %** subsequently realised —
a premium of **+4.06 volatility points at t 5.94** over 119 observations.

Both are textbook, and neither is Alpha. The contract excludes generic
short-volatility premium by name, and this lane's own frozen rule
`A_SHORT_WINDOW_MAY_DIAGNOSE_AND_MAY_NOT_QUALIFY` forbids a qualification on
this window regardless: 393 sessions is **107 short** of the required 250
fitting plus 250 judged — about **5.1 more months** of history.

The risk-reversal signal (fade a rich downside) is reported as a direction,
not a claim: t −1.29 at 5 days and t −1.59 at 21 days on 78 observations.

---

## Three bugs Release 44 found in its own work

Naming them is what makes the surviving numbers credible.

**1. A sign flip that paid us our own transaction costs.** The
sign-selected diagnostic first multiplied each stream's *excess* series by
±1. That turns `(gross − cost)` into `(−gross + cost)` — it converts a cost
drag into a cost *credit*. It printed a lockbox return of +3.55 %/yr at
t 4.94 with a Sharpe of 1.40, built almost entirely out of spreads that
would have been paid either way. A short position pays the spread exactly
like a long one. Corrected (`streams.excess_frame_signed` flips gross and
re-charges cost in full), the same diagnostic returns **−5.61 %/yr at
t −5.52** — so the streams are *empty*, not merely mis-signed. This is
pinned by `test_sign_flip_charges_cost_it_does_not_credit_it` and by a
blocking audit invariant.

**2. A rolling window that demanded consecutive observations on a union
calendar.** `rolling(1260).sum()` over a panel of markets from different
exchanges needs 1260 *consecutive* non-null observations; every other
market's holiday puts a hole in every column. The VALUE signal came back
entirely empty for two of the three liquidity tiers — silently, as a
tier with "insufficient markets". Fixed with the codebase's standard
`min_periods`, applied uniformly across all tiers before any comparison was
drawn.

**3. A burden ledger that double-counted identical books.** The
portfolio-synthesis candidate id hashed the combination-rule *name*, so
inverse-vol, ERC and capped-ERC — which produced byte-identical weights
because the cap never bound — were charged as three trials instead of one.
The ledger was rebuilt; the inherited 302 was never touched and is
re-derived from R43's own bytes on every run. **Both counts are reported**
(headline 310, conservative 312), so nobody has to take the dedup argument
on faith.

---

## How the portfolio claim was defended before it was made

A portfolio result is the easiest kind of result to fake. Each defence below
was written into the frozen contract before any number existed, and each is
enforced by a blocking invariant in `scripts/audit_architecture.py`.

| the cheat | the defence |
|---|---|
| score many streams, keep the winners | the inventory is declared by ECONOMICS in the contract, and `LOSERS_ARE_INCLUDED` is a tested invariant — a run with no losing stream fails the regression |
| tune a threshold until it works | `NO_THRESHOLD_IS_CHOSEN`: every stream uses its family's CONTINUOUS expression. Not one band, threshold or lookback is chosen anywhere in the release |
| try eight weighting rules, report the best | the primary rule (`FAMILY_BALANCED_ERC`) is named in the contract; every distinct book is charged to the burden ledger; PBO is measured over the rules |
| flip signs until the book works | `SHORTING_A_STREAM_IS_FORBIDDEN`. Where a sign-selected DIAGNOSTIC is run it is declared non-qualifying, un-freezable, and must charge cost on the flipped book |
| optimise on the holdout | weights are fitted on Zone A+B only, and a test poisons the holdout to prove the fitted weights do not move |
| pick a beatable benchmark | the control is the same rule on the premium sleeves, plus a volatility-matched passive long, and increments are volatility-matched |
| call diversified premia "alpha" | `STANDALONE_ALPHA`, `PORTFOLIO_ALPHA` and `STRUCTURAL_PREMIUM` are separate words and the verdict reports them on separate axes |
| let a significant LOSS count as a survivor | `is_a_positive_survivor` — BH rejection plus a positive t |

---

## Search burden

| | |
|---|---|
| inherited from R43, re-derived from its ledger's bytes | **302** |
| new R44 distinct Zone-B candidates | **8** (6 portfolio books + 2 liquidity-tier candidates) |
| **global cumulative** | **310** |
| conservative count (one trial per rule name) | **312** |
| R44 Zone-B evaluations | 12 |
| lanes that spent zero burden | E1A options, E1B intraday, E1C analyst, E1D credit, E1E microstructure |

Five of seven lanes spent nothing, because nothing in them reached the
frozen advance bar. `PORTFOLIO_SYNTHESIS` is a burden family like any other:
combination is a searched hypothesis and is charged.

Benjamini-Hochberg at q = 0.10 across the release's judged candidates:
**2 rejections, 0 positive survivors** — both rejections are significant
*losses*.

---

## Forward evidence

**Zero shadows frozen.** Nothing reached a positive lockbox result that the
kill battery left standing, and the contract's
`DO_NOT_FREEZE_MEDIOCRE_CANDIDATES_TO_CREATE_ACTIVITY` is not a suggestion.
Release 43 refused to freeze a candidate its lockbox had refuted; Release 44
refuses to freeze one that never opened a lockbox at all.

The R40, R41, R42 and R43 shadow registries, burden ledgers, frozen
contracts and verdicts are hash-verified byte-identical before and after this
release — 10 witnesses, 0 changed.

---

## The data purchase gate

**No purchase was made and none is recommended without a sample.** Ranked by
information gain per $1,000 of first-year cost, where every input is a
measurement this release actually made:

| rank | dataset | first-year cost | recommendation |
|---|---|---|---|
| 1 | **Polygon.io Options Starter** — historical option aggregates beyond the free ~2-year window | $348 ($29 for one verifying month) | **NEED_SAMPLE** |
| 2 | **Databento / CME MDP-3** native intraday futures | ~$500 (signup credits may cover it) | **NEED_SAMPLE** |
| 3 | Steele / Intrinio / Zacks historical analyst vintages | ~$10,000 | NEED_SAMPLE |
| 4 | Native credit (CDX / iTraxx / CDS) | ~$25,000 | **RECOMMEND_SKIP** |

The ranking says Polygon is the best value per dollar. **The evidence says
buy the intraday futures sample first**, and the two are not in conflict:
Polygon converts a family that is currently *untestable* into a testable one
for the price of one month, while native intraday futures is the only route
that resolves a live measurement this release could not settle. If the
operator authorises one thing, authorise the one with a t-statistic pointing
at it.

A vendor sample request for the analyst family is **prepared and not sent**
at `_data_analyst/OPERATOR_SAMPLE_REQUEST.txt`. Sending it is the operator's
action; `MAY_SEND_VENDOR_EMAIL = False`.

---

## Machine learning: deliberately none

The contract permits LightGBM, transformers, state-space models and the rest
*where new information justifies them*. No lane produced information whose
structure a transparent rule could not already express. Fitting a
gradient-booster on top of a book whose binding constraint is transaction
cost adds model capacity to a problem that is not short of it — and R41
already measured that scaling degrades (Zone-B t −0.03 versus 2.07). The
honest `MODEL_RESULT` for this release is that no complex model was run and
none was warranted.

---

## What Release 45 should do

**Settle the gold question.** Acquire one native intraday futures product —
ZN or ES, 1-minute, 2012–2019 — through a sample or signup credit, and run
the *identical* release-time rule in the instrument the release is actually
about. The pipeline is built, the calendar is owned, the cost model is
written and the placebo and timing-sweep machinery already exist.

- If it replicates in rates, the estate has its first real edge in fourteen
  releases.
- If it does not, gold was an artefact and the macro-event family is closed
  for good.

Either answer is worth more than another search over owned daily data. The
second-priority action costs nothing at all: let the prospective analyst
vintage ledger keep running. It is 24 days old, it is capturing real
revisions, and it is the only PIT-defensible revision history this estate
will ever own without paying five figures for one.

**What Release 45 should not do:** run another portfolio-combination study
over these streams. Release 44 has measured that frontier, and the answer
does not depend on the weighting scheme.

---

## Safety

| | |
|---|---|
| money spent | **$0.00** |
| accounts created / trials started / licences accepted | 0 / 0 / 0 |
| payment details submitted / vendor emails sent | 0 / 0 |
| orders, paper orders, allocations, proposals, decisions | 0 |
| portfolio mutations, operational writes | 0 |
| model promotions, sleeve activations, scheduler changes | 0 |
| production restarts | 0 |
| prior-release artifacts mutated | 0 (10 witnesses hash-verified) |
| shell policy | **ONE disclosed Release-44 violation — see below.** R42's single read-only Bash event is inherited as a historical disclosure, not erased |

### Shell policy: one disclosed violation

The contract declares `WINDOWS_POWERSHELL_ONLY = True`. It was broken once.

While waiting on the background Polygon option-surface acquisition, a single
`sleep 1; echo waiting` was issued through the Bash tool as a no-op
placeholder. It read no file, wrote no file, touched no repository path,
opened no network connection and invoked nothing belonging to the estate.
Every Release-44 measurement outside the option lane had already been
computed when it happened, and no result depends on it. It should have been
a PowerShell call, or — since the background job reports its own completion
— no call at all.

It is recorded in full in `alpha_agent/r44/shell_policy.py` and in
`R44_SHELL_POLICY_EVENTS.json`, with a waiver token over its contents.
This is exactly how Release 42 handled its own disclosed Bash event, and the
reason that precedent exists is that a policy reported only when it was kept
is not a policy.

### How it was resolved: the Release-44.1 clean recovery

The release was **not** committed by waiving anything. A separate
Release-44.1 recovery session — Windows PowerShell only, **zero**
Bash/WSL/Git-Bash/sh invocations of its own — re-verified the frozen
research independently and prepared the commit. It reran no research,
retuned no result and regenerated no artifact; it hashed all 18 files under
`D:\Stock_Prediction_app_data\orthogonal_portfolio_alpha_r44\` and matched
them **three ways** against the original handoff manifest, the preservation
snapshot and a freshly generated recovery manifest.

Its handoff is `D:\Temp\paper_trader_release44_1_clean_recovery_handoff`,
whose `validate.ps1` prints `R44_RECOVERY_VALIDATE_OK` or
`DO_NOT_COMMIT - <blocker>`. It offers **no shell-policy waiver switch** at
all. Instead it separates the two facts and asserts both:

| fact | value | how it is enforced |
|---|---|---|
| `ORIGINAL_R44_RESEARCH_SESSION_SHELL_POLICY_VIOLATION` | **TRUE** | the validator BLOCKS unless the disclosure is still present and still says `true` in the artifact, the source and this document |
| `R44_RECOVERY_SESSION_SHELL_POLICY_VIOLATION` | **FALSE** | the recovery session's own shell usage |

So the historical violation is never erased or reinterpreted — the validator
now fails if anyone *removes* it. What it stops blocking on is a past event
that no result depends on; what it will not tolerate is that event being
quietly dropped from the record.

---

## Artifacts

All under
`D:\Stock_Prediction_app_data\orthogonal_portfolio_alpha_r44\r44_orthogonal_portfolio_alpha_v1\`:

| artifact | contents |
|---|---|
| `r44_frozen_contract.json` | the contract as frozen before the first number |
| `r44_contract_amendment.json` | the one disclosed post-freeze amendment, hashed separately |
| `R43_CLOSEOUT_IMPORT.json` | inherited facts re-derived from R43's bytes, plus the start-condition deviation |
| `R44_STREAM_INVENTORY.json` | all 18 declared streams, built and blocked, with the gross/cost/net decomposition |
| `R44_PORTFOLIO_ALPHA_FRONTIER.json` | every portfolio, the kill battery, PBO, increments, the sign diagnostic |
| `R44_STRUCTURAL_PREMIUM_CONTROL.json` | the control portfolio and its own controls |
| `R44_STANDALONE_ALPHA_FRONTIER.json` | every standalone candidate judged |
| `R44_ORTHOGONAL_DATA_FRONTIER.json` | analyst, credit and microstructure lanes |
| `R44_PURCHASE_GATE.json` | four candidates, fully specified, ranked per dollar |
| `R44_LANE_RESULTS.json` | every lane, screened and advanced |
| `R44_FINAL_VERDICT.json` | result axes, the fifteen answers, burden, freeze, readiness |
| `r44_search_burden_ledger.json` | the never-reset ledger |

---

## A note on the start condition

The handoff prompt expects the latest **commit** to contain the finalised
Release 43. It does not: R43 is complete on disk with its own handoff
prepared, and the operator has not yet committed it. Local HEAD equals
origin, so there is no `SHA_MISMATCH`.

Release 44 therefore declares the **R43 working tree** as its base, states
the deviation in `R43_CLOSEOUT_IMPORT.json → git.start_condition_deviation`,
stages only R44-owned paths, and preserves every pre-existing untracked
file. `PROJECT_STATE.md` and `scripts/audit_architecture.py` now carry
**both** R43's and R44's uncommitted edits — the operator should commit
Release 43 from its own handoff first, then Release 44 from this one.
