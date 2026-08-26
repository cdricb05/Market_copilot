# Release 46 — The Prospective Alpha Tournament

**Campaign:** `r46_prospective_alpha_tournament_v1`
**Terminal state:** `R46_PROSPECTIVE_ALPHA_TOURNAMENT_LIVE`
**Started from:** `eaf5cc43fd06c8af96ef17a66b7bdb08d5d187e1` (R45, verified local == origin)
**Research root:** `D:\Stock_Prediction_app_data\prospective_alpha_tournament_r46`

Research only. Paper only. No orders, no promotion, no automation, no portfolio
mutation, no purchase. $0.00 spent.

---

## 1. What changed about how this project looks for alpha

Fourteen releases ran the same loop:

```
HISTORICAL SEARCH → STRONG-LOOKING CANDIDATE → ECONOMIC STORY
     → FIRST GENUINELY UNTOUCHED EVIDENCE → COLLAPSE
```

Release 45 did not merely observe that loop, it **measured the mechanism**. It
re-ran Release 44's entire sixty-cell screen separately on each of three event
zones and got a *different winner every time* — the last one (USDJPY REVERSAL
d1 h120, net t +2.00) **larger than R44's published headline**. The median net
t across all sixty cells was about −1.0 in every zone. The maximum of a noisy
grid always looks locally peaked from the inside; that local peak *was* the
finding.

From Release 46 onward:

> **History may NOMINATE a challenger. Only the future may crown one.**

The question is no longer *what would have worked in history?* It is:

> **What did the model predict before the market moved, and did that prediction
> actually make money after costs against the correct control?**

---

## 2. What R46 found on arrival — and what it is really fixing

Five releases each froze a prospective shadow registry:

| Release | Registry | Shadows declared | **Forward observations** |
|---|---|---|---|
| R39 | `research_shadow_registry.json` | 3 | **0** |
| R40 | `shadow_registry_v2.json` | 5 (3 re-listed from R39) | **0** |
| R41 | `r41_shadow_registry.json` | 1 | **0** |
| R42 | `r42_shadow_registry.json` | 1 | **0** |
| R43 | `r43_shadow_registry.json` | 0 (correctly) | — |
| R45 | `R45_SHADOW_REGISTRY.json` | 0 (correctly) | — |
| | **7 distinct shadows** | | **ZERO** |

The forward clock was started **five times and never ticked once**. Not one
row exists in any of the four ledger conventions. The reasons:

* the **R39/R40** shadows decide at per-market month-end or on VX Fridays,
  through their own capture owner — and no run has called that owner since the
  freeze. `forward_capture_ledger_status.json` records `n_rows: 0` on all four
  ledgers, chains intact, which is exactly what an empty ledger looks like;
* the **R41/R42** BTC shadows read the Binance public archive, which publishes
  funding **monthly with a ~24-day lag**, from a location where the venue's
  REST API answers **HTTP 451**. Release 42 probed this, wrote it down in
  `FORWARD_EVIDENCE.json` under `r41_stream_feasibility`, stated
  `can_accrue_today: false` — and the shadow stayed nominally live through
  three more releases producing nothing.

So the estate did not have a prospective-evidence problem. It had **five
prospective-evidence implementations, none of them running, and no single
place that would have shown an operator the count was zero.** The number was
never hidden. It was simply never anywhere.

**R46 adds no sixth registry.** It adopts all seven orphans **by reference** —
prior registries opened read-only, hashed before and after (verified
byte-identical), ledgers and owners left exactly where they are — and puts them
on one board beside its own cohort, so the zero is visible in one place.

---

## 3. Architecture — one owner per concept

| Concern | Owner |
|---|---|
| release contract | `alpha_agent/r46/contract.py` |
| shell policy + disclosure | `alpha_agent/r46/shell_policy.py` |
| emission clock / calendars | `alpha_agent/r46/clock.py` |
| owned live market-data seam | `alpha_agent/r46/marketdata.py` |
| CAN-THIS-STREAM-ACCRUE gate | `alpha_agent/r46/feasibility.py` |
| frozen challenger specifications | `alpha_agent/r46/challengers.py` |
| challenger registry + versioning | `alpha_agent/r46/registry.py` |
| **THE** prediction / outcome ledger | `alpha_agent/r46/ledger.py` |
| idempotent forward emission | `alpha_agent/r46/emit.py` |
| **THE** outcome judge | `alpha_agent/r46/judge.py` |
| evidence maturity + gates | `alpha_agent/r46/evidence.py` |
| **THE** leaderboard | `alpha_agent/r46/leaderboard.py` |
| historical vs prospective burden | `alpha_agent/r46/burden.py` |
| options lane | `alpha_agent/r46/options.py` |
| analyst lane | `alpha_agent/r46/analyst.py` |
| orchestration | `alpha_agent/r46/campaign.py` |
| operator read model | `api/prospective_tournament.py` |
| endpoint | `GET /v1/research/prospective-tournament` |

Ledger mechanics **reuse** the canonical chain-hash primitives from
`api.paper_trading_desk` (`_append_ledger` / `verify_ledger`) — the same
append-only, rewrite-detectable convention every desk ledger has used since
Phase 27, pointed at the R46 research root. No second forward-ledger
implementation exists.

---

## 4. The entry rule — deliberately conservative

`R46_NEXT_TRADING_DAY_CLOSE`:

> A prediction emitted at instant *T* enters at the **close of the first
> trading day whose calendar date, in America/New_York, is strictly greater
> than the Eastern calendar date of T**. Its horizon is then measured in
> eligible sessions of that instrument's **own realised bar calendar**.

At 16:05 ET the US cash equity session is closed, but FX spot and several
futures still have hours to run. Entering on any of those same-day closes would
require a per-instrument, per-day argument that the mark was undetermined at
emission. **R46 declines the argument and gives those hours up**, so the rule
can be checked with a calendar. A few basis points of forgone edge is a cheap
price for an ordering an auditor cannot dispute.

The outcome window opens at **midnight Eastern** on the entry date, not
midnight UTC. This was a real bug caught in test: midnight UTC is 8pm Eastern
on the *previous* day, so a UTC-anchored window would open **before** any
evening emission and the strict ordering the whole release rests on would fail
between 20:00 and 24:00 ET every day.

The ledger **refuses** — raises, does not warn — any row where
`emitted_at_utc >= outcome_window_start_utc`.

---

## 5. The seed cohort — ten challengers, and no parameter chosen

Every seed parameter is a canonical constant from the published asset-pricing
literature, written into the frozen contract **before
`alpha_agent.r46.marketdata` was first called**: 12-1 momentum, five-day
reversal, sixty-day volatility, 252-day trend, the 200-day filter, a one-sigma
reversion band, decile portfolios. **No sweep ran, no cell was ranked, no
winner was picked.**

| Challenger | Family | Asset class | Horizons | Control |
|---|---|---|---|---|
| `r46_eq_xs_mom_12_1` | cross-sectional momentum | US equity | 5, 20 | cash |
| `r46_eq_xs_rev_5d` | cross-sectional reversal | US equity | 1 | cash |
| `r46_eq_xs_lowvol_60d` | low-risk anomaly | US equity | 20 | cash |
| `r46_eq_xs_resid_mom_12_1` | residual momentum | US equity | 20 | cash |
| `r46_fut_ts_mom_252` | time-series trend | multi-asset futures | 20 | cash |
| `r46_fx_xs_mom_252` | cross-sectional momentum | FX | 20 | cash |
| `r46_vx_term_carry_5d` | volatility term carry | volatility | 5 | cash |
| `r46_rates_curve_rv_5d` | rates relative value | rates | 5 | cash |
| `r46_comdty_xs_mom_252` | cross-sectional momentum | commodity | 20 | cash |
| `r46_spx_trend_200d` | index trend timing | equity index | 20 | **SPY buy-and-hold** |

This is why R46 charges **zero** new historical search trials. A trial is
charged when a release *chooses* something by looking at data. R46 chose
nothing. Building infrastructure is not searching.

`r46_vx_term_carry_5d` carries a declared `economic_overlap_with`
`R39:shadow_vx_carry_ts` so the two are never counted as independent evidence.

**No hero candidate.** Every challenger enters at
`HISTORICAL_QUALIFICATION_STATE = HISTORICAL_ONLY` with the summary *"none —
R46 ran no historical screen to select this challenger"*.

---

## 6. The data path — and the gate that checks it

The tournament runs on the estate's **owned, live, nightly-refreshing**
Norgate Data Updater entitlement: nine databases (US equities with delisted
history and point-in-time index membership, continuous futures, dated futures,
forex spot, US and world indices, cash commodities, economic series), all
verified fresh in-run. The risk-free control comes from **FRED DGS3MO** (free,
owned key). Nothing was acquired, installed or purchased.

**The feasibility gate** is the R42 discovery, finally enforced: a challenger
may not be registered active unless its declared data path was **probed in
this run** and demonstrably carries an observation within three sessions. A
blocked challenger is registered `DATA_BLOCKED` with its reason attached and
**does not block any other challenger**.

Two real defects surfaced through this work and are fixed:

1. **`norgatedata` fails to import under a strict warning filter.** It calls
   the deprecated `logging.warn` at import time; pytest configures
   warnings-as-errors, so the import raised, every loader returned `None`, and
   the feasibility gate reported that a fully entitled, locally served,
   nightly-updated database **did not exist**. Completely silent. The vendor
   import is now wrapped so a caller's warning filter cannot decide whether the
   estate can read its own data.

2. **A percentage return across a non-positive price.** Continuous WTI prints
   **−37.63 on 2020-04-20** — a true historical fact, not a data defect.
   `np.log` produced NaN, which would propagate into a rank and become a
   position. Returns spanning a non-positive price are now **refused** and the
   market is recorded as skipped with reason `NON_POSITIVE_PRICE_IN_WINDOW`.

---

## 7. The judge, the control, and what actually decides

For every matured prediction: realised gross, benchmark, residual, cost, net,
and **net alpha vs the correct control**.

* **Cost** — both sides of the round trip, on **traded notional** (Release 31's
  correction), at declared per-class half-spreads plus slippage. A flat market
  still costs money.
* **Control** — for a collateralised book, the **risk-free rate on the capital
  it ties up**; for a benchmark-relative challenger, the benchmark's own return
  over the identical window. Release 42's lesson is now a contract clause:
  *beating zero is not beating cash.*
* **`net_alpha_vs_control` is the only number that decides anything.** Gross is
  reported because hiding it would be dishonest, not because it means much —
  Release 43 watched a real premium disappear entirely into two-legged cost.

The judge **never revises a forecast**. It appends an outcome row keyed by
`prediction_id`; the original prediction stays byte-identical under its chain
hash, and a rewrite breaks the chain.

**`expected_return` is emitted as `null`** with
`expected_return_state = NOT_CALIBRATED`. These are transparent rules, not
calibrated forecasts. The cost, which *is* known in advance, is a number.
Inventing an expected return to fill a schema field would be the first lie in
an evidence chain built to prevent them.

---

## 8. Evidence accounting — overlap is never free

A twenty-session challenger emitting daily produces twenty overlapping bets on
largely the same twenty days. Every count comes in two flavours and they always
travel together:

* `raw_matured` — rows the judge scored;
* `effective_independent` — `raw_matured / horizon`, capped at the number of
  distinct decision dates.

**The gate reads the effective number.** Fifty overlapping twenty-day bets
score `effective_independent = 2`.

A challenger reaches `FORWARD_CONFIRMED` only on: enough effective independent
observations *and* enough raw matured rows *and* enough calendar span *and* a
positive net edge *and* t ≥ 2.5 vs control *and* a confidence interval
excluding zero *and* positive at 2× costs *and* same-sign halves *and* no
single day > 35% of P&L *and* no single leg > 40% *and* no PIT violation *and*
no retune since freeze — under Benjamini-Hochberg FDR 0.10 across every cell.

**`PROVEN_ALPHA` is not a state.** `FORWARD_CONFIRMED` is the strongest that
exists, and it still confers no capital, no promotion and no order.

---

## 9. Versioning — a losing challenger cannot be improved in place

`v1` makes a hundred predictions. Research finds a better parameter. **`v1` is
not touched.** `v2` is registered, starts its own forward clock at zero, and
`v1`'s record stays permanently on the board.

Any change to features, parameters, universe, model family, entry, exit,
horizon, costs, hedge or threshold is **MATERIAL** and forces a new version.
Re-registering an edited spec is detected as `RETUNE_DETECTED` and reported as
a blocker, never silently accepted.

**Forward p-hacking is ledgered.** Choosing a threshold, promoting a version or
picking a challenger *after* seeing forward results is a selection over the
forward evidence and is recorded in `r46_forward_selection_ledger.json`. A
forward screen has exactly the same inflation property as a historical one and
none of the excuses.

---

## 10. Options lane — 474 → 499 of 500, for $0

R44 read its surface as 107 sessions short of the 500 a variance study needs
and priced a $29/month purchase against the gap. R45 showed it was never a
history problem — R44 had sampled six widely-spaced expiries — and closed 81 of
those sessions for **$0**, leaving **26**.

**R46 closed 25 of the remaining 26, also for $0.** The surface went from
**474 to 499 usable sessions**, 20 → 39 expiries, 429 → 575 dated contracts,
spanning 2024-08-26 to **2026-08-21**. One session remains, and it arrives on
its own: the 2026-08-28 weekly has not expired yet, so no `expired=true` query
can reach the dates after 2026-08-21. Nothing needs to be bought and nothing
needs to be decided — the lane becomes judgeable in days.

**It only closed after fixing three bugs, and they are worth recording because
every one of them was silent:**

1. **Wrong root.** The acquired surfaces live at each release's *research
   root*, not under its campaign directory. Pointed one level too deep, every
   loader returned `None`, the lane reported **zero** prior sessions, and the
   dedup that stops R46 re-buying an expiry a prior release already paid for
   never fired.
2. **A budget that bought nothing.** The first batch iterated candidate
   expiries in *ascending* date order and spent all 120 calls on the oldest
   fourteen (2024-09 → 2025-01). It returned 106 contracts and 2,195 real rows
   — and **zero new session dates**, because the surface already covered every
   date they traded on. A budget can be fully consumed, return real data,
   report success, and buy nothing at all. Targets are now ordered **most
   recent first**, and an expiry whose whole trading window sits inside dates
   already held is not worth a call.
3. **Dedup against an assumption instead of against the data.** "Weeklies"
   excluded *all* third Fridays on the grounds that R44 and R45 had sampled
   them. True of the ones they sampled, false in general: the prior surface is
   missing **2026-06-19 and 2026-08-21**, both third Fridays, and the rule made
   them permanently unreachable. R45's 20-day recency embargo compounded it by
   hiding exactly the two most recent expiries — the only ones carrying dates
   the surface lacked. The embargo is now 3 days of settlement slack, every
   Friday is enumerated, and the `already` set does the deduplication.

The 21 business days with no option row inside the covered span are **US market
holidays** — Labor Day, Thanksgiving, Christmas, MLK, Presidents' Day, Good
Friday, Memorial Day, Juneteenth, July 4th. The span is complete; the gap was
never inside it. That is why more expiries could not close it and more *recent*
expiries could.

No date beyond the entitlement boundary was requested and no existing row was
touched — R44's and R45's surfaces are hash-verified byte-identical.

**The part that matters:** three option hypotheses are **frozen now, hashed,
while the answer is still unobservable** — skew residual, IV term-structure
residual, delta-hedged residual return — each with its parameters, its control,
its cost model, and a declared fit window (first 250 sessions) and judge window
(last 250, never read until the fit is frozen). Once the surface clears 500
sessions, whoever looks first would otherwise be able to try all three, keep
the one that worked, and call it a discovery.

**Generic short volatility is excluded by name.** R45 measured the variance
risk premium at 4.50 vol points, t 9.39, on actual option prices. It is real,
it is insurance revenue available to anyone, and it is not alpha.

---

## 11. Analyst lane — the one history that cannot have been restated

The prospective vintage ledger holds **54 observed revisions** across 7
snapshot dates and 25 days, against a 250 requirement — roughly **3.6 months**
remaining at the observed rate. Hard PIT floor 2026-07-31. R46 **added nothing
and rewrote nothing**.

Why it is worth waiting for: R45 compared these captured snapshots against the
vendor's own `epsTrend7daysAgo` backward strip — the field that claims to tell
you what the estimate *was* — and matched on 128 of 240 comparisons, with **25
differences above five cents** and a worst case of **0.19 EPS on JPM**.
Cent-level noise is capture-time convention. A nineteen-cent gap is a
restatement, and a restated series is not point-in-time evidence however
convenient it is.

The analyst-revision challenger is **predeclared and hashed** and will enter
through the same frozen-specification, forward-only door as everything else —
no backfill, no historical-vintage purchase, no head start.

---

## 12. Search burden

| | |
|---|---|
| Inherited from R45 | **353** headline (355 conservative) |
| New R46 historical trials | **0** |
| **Global** | **353** (355 conservative) |

Burden never resets. Prospective forward evidence is ledgered **separately**
and the two may never be netted: a forward observation cannot be re-drawn,
re-parameterised or re-selected, which is the entire reason this release
exists.

---

## 13. Relation to the portfolio manager

The tournament may expose candidate expected return, expected residual return,
confidence, forward evidence state and risk characteristics to the opportunity
frontier. But:

* `FORWARD_CANDIDATE` **is not an order**;
* `FORWARD_CONFIRMED` **is not an automatic holding**.

The canonical portfolio manager still decides HOLD / REDUCE / EXIT / REPLACE /
ADD, manually, subject to risk, concentration, switching cost, liquidity,
settlement, capital and governance. Manual review remains mandatory, and
`check_release46_prospective_alpha_tournament` enforces it.

---

## 14. Failure escalation

If a sufficiently broad prospective tournament accumulates enough independent
forward evidence across at least six economically distinct families and none
produces implementable residual alpha, the classification is
**`INFORMATION_SET_INSUFFICIENT`** — and the required response is to rank the
highest-value **orthogonal information sources**, priced per effective
independent observation unlocked. Running a bigger generic model search over
the same information is a **forbidden** response.

---

## 15. Safety

Money spent **$0.00**. New accounts 0. Trials 0. Licences accepted 0.
Operational writes **0** (`r33_operational_write_attribution --release R46` →
`ATTRIBUTED`, `unattributed: []`). Portfolio mutations 0. Orders 0. Model
promotions 0. Scheduler changes 0. No prior release's artifact was modified —
verified by before/after SHA-256 on every adopted registry and option surface.

**Shell policy: `SHELL_POLICY_VIOLATION = YES` (disclosed, no waiver).** Four
Bash invocations were issued at session start before this release's shell
policy was applied: three read-only git queries and one write of a provenance
file into the `D:\Temp` handoff directory. **No repository source file was
written by a prohibited shell and no repository state was mutated by one.**
Every subsequent command ran in Windows PowerShell. The contract offers no
waiver mechanism and this release does not invent one: `validate.ps1` surfaces
it as a blocker requiring an explicit operator decision. R42's, R44's and R45's
disclosures are carried beside it and never rewritten.

---

## 16. Verification

* 87 R46 tests, all passing.
* 41 new blocking audit invariants under
  `check_release46_prospective_alpha_tournament`; `audit_architecture --strict`
  exits 0.
* 324 tournament / forward-evidence owner tests passing (R46 + Phase 18
  tournament + Phase 28A/28B/28B.2/28C).
* 282 prior-release compatibility tests passing (R43, R44, R45, canonical
  restart, R29 restart contract).
* Idempotency **proved in production**, not only in test: the campaign emits,
  then emits again, every run.
* Every emitted prediction verified **bit-reproducible** after the numerical
  fixes, with the chain intact.
