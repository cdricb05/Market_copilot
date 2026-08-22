# Release 36 — Global Multi-Asset Alpha Frontier & Coverage Closure

| | |
|---|---|
| verdict | **`R36_FRONTIER_PARTIALLY_CLOSED`** |
| SYSTEM_RESULT | **PASS** |
| RESEARCH_CANDIDATE_RESULT | **FAIL** |
| ALPHA_RESULT | **FAIL** (structurally unreachable this release) |
| campaign | `r36_global_multi_asset_frontier_v3` |
| base commit | `46cf96a642c962975ee230fb163aea1319778b56` (Release-35 closeout) |
| executed configurations | **34** of a ceiling of 80 |
| money spent | **$0.00** — 0 trials, 0 accounts, 0 tier changes |
| research root | `D:\Stock_Prediction_app_data\global_multi_asset_frontier_r36\` |

---

## 1. Why this release exists

Five releases had looked for investable alpha and none had found it. The honest
reading of the five together was not "there is no alpha" but **"we have not
looked in most places"**, and nobody had written down where those places were.

Release 34 and Release 35 are both statements about **one decision problem**: a
47-fund ETF cross-section rebalanced monthly. They are not statements about the
FX market, the commodity curve, the Treasury curve, the credit market or the
volatility surface. `FXE` is not the FX market. `USO` is not the WTI futures
curve. `HYG` is not credit. `TLT` is not the Treasury curve. VIX as a feature is
not a volatility sleeve.

Release 36 therefore had two inseparable jobs: **classify every asset-class ×
strategy cell terminally**, and **execute every native lane that owned or free
point-in-time data actually supports**. An inventory is not a release.

Three levels are kept apart everywhere, because collapsing them is how a proxy
result silently closes a native frontier:

| level | meaning | example |
|---|---|---|
| 1 SIGNAL | predictive, not tradable | a yield series, a spot FX rate |
| 2 PROXY | tradable, structure lost | `USO`, `FXE`, `HYG`, `TLT` |
| 3 NATIVE | the market itself | an FX forward, a dated NYMEX contract |

---

## 2. What the estate actually holds — measured, not assumed

Every claim below came from a real endpoint or a real vendor enumeration.

**Norgate, per database.** The decisive measurement of the release:

| database | symbols | what it supports |
|---|---|---|
| **Continuous Futures** | **1** (`&ES`) | nothing broader; this is the wall |
| Forex Spot | 57 (28 pairs) | the FX lane's spot leg, 1990 → 2026 |
| Economic | 144 | the US constant-maturity curve, ICE Treasury total-return buckets, Moody's and CCC spreads, MOVE |
| Cash Commodities | 15 | index-level only — BCOM sub-indices, CRB, GSCI |
| US Equities / Delisted | 14,639 / 27,194 | listed products, survivorship-aware |
| US Indices / World Indices | 1,609 / 31 | index level |

Release 33 measured the same one-market futures entitlement. Release 36
re-measured it independently and got the same answer, so **every "no futures
data" statement in this document is a fact about an entitlement, not a
guess**.

**Free public sources.** `EIA` natural-gas bulk 200; `Cboe` index histories 200;
`FRED` entitled with the estate's existing key. **Cboe VX futures settlement
history: five published routes probed, every one 403 or 404.**

**What is measured blocked, and stays blocked.** Five free analyst-estimate
entitlements (FMP, Finnhub, Nasdaq Data Link, EODHD, Alpha Vantage) carry
forward Release 35's read-only probes; three answered HTTP 403 and two answer
with today's estimate plus deltas, which is `CURRENT_SNAPSHOT_ONLY`.

**Acquisition: 93 payloads, 110.4 MB, $0.** Only 7 MB was downloaded — the EIA
natural-gas archive and 48 FRED series. The CFTC archives (44 MB), the EIA
petroleum archive (55 MB) and the Cboe histories were **located, not
re-downloaded**: a raw vendor archive is an input, and re-fetching it would
change bytes an earlier immutable artifact was hashed against.

---

## 3. The native markets that were built

| lane | level | instruments | decisions | window |
|---|---|---|---|---|
| **FX_NATIVE** | **3 NATIVE** | 20 currencies | 453 | 1990-01 → 2026-07 |
| **COMMODITY_CURVE_NATIVE** | **3 NATIVE** | 5 energy curves | 470 | 1985-01 → 2024-02 |
| RATES_CURVE_NATIVE | 2 PROXY | 5 duration buckets | 258 | 2004-12 → 2026-07 |
| CREDIT_INDEX | 2 PROXY | 1 hedged index | 221 | 2007-12 → 2026-07 |
| VOLATILITY_TERM_STRUCTURE | 2 PROXY | 2 | 785 | 2011-01 → 2026-08 |
| CROSS_ASSET_RELATIVE_VALUE | 2 PROXY | 6 legs | 267 | 2005-01 → 2026-06 |
| CRYPTO_NATIVE | 2 PROXY | 2 majors | 607 | 2014-12 → 2026-08 |

Three constructions carry the release.

**A currency position is a forward, and its return has two legs.** Release 33
held FX spot and had to declare `FX_SPOT_EXCLUDES_CARRY` because it had no
foreign short rate. With one, the excess return of holding currency *c* is
`spot return + (i_c − i_usd)·days/365` — what a deliverable one-month forward
earns under covered interest parity. The rate leg is the OECD three-month
interbank rate, stamped forward by the publication lag the estate already uses.
**This is the first time this project has held a currency rather than a currency
fund.**

**A commodity futures return needs contract identity, and this one never guesses
it.** EIA publishes settlements for the nearest four *dated* contracts. Over one
month the second-nearest becomes the nearest, so the return of buying contract 2
at month end and holding it is exactly `C1(t+1) / C2(t) − 1`. No roll date is
inferred, no contract is chosen with hindsight, and the basis `ln(C1/C2)`
observed at *t* is a real spread between two different contracts quoted the same
day — not a lagged transformation of one price. Five markets: WTI, heating oil,
RBOB gasoline, natural gas and **propane, which was delisted in 2009 and is
included precisely because it was**.

**A rates curve trade needs a duration, and the duration is measured.** Each
bucket's duration is estimated by regressing its realised return on the change
in its matched constant-maturity yield over an *expanding trailing* window, so a
duration-neutral weight on date *t* uses only what was observable before *t*.

### Currencies deliberately excluded, with the measured reason

`CNY` and `MYR` repeat their previous close on 14.5 % and 17.9 % of sessions —
administered, by Release 33's reused 10 % rule. `HKD` has 0.6 % annualised
volatility — a hard peg, and a carry book that loads on one is being paid for a
tail that has not occurred in sample. `TWD` and `SGD` have no OECD three-month
rate. `BRL` and `INR` publish only an overnight or discount rate; **using it
would have added the two highest-carry currencies in the sample**, and a carry
ranking built from mixed tenors ranks rate definitions rather than carry.

---

## 4. What was found

**Nothing qualified. Zero of 34 configurations beat their control significantly,
and the Benjamini–Hochberg rejections all point the other way.**

### The headline: FX carry predicts powerfully and pays almost nothing extra

| | |
|---|---|
| cross-sectional rank IC | **+0.155**, t = **+7.97**, 441 monthly dates |
| net return | +5.06 %/yr, volatility 3.7 %, Sharpe 0.66, max drawdown −7.3 % |
| turnover | 0.80×/yr, cost 4 bp/yr |
| effective instruments | 18.4 of 20 |
| control | volatility-matched passive foreign-currency basket (+3.62 %/yr) |
| **after-cost excess** | **+1.39 %/yr, t = +1.79** |
| minimum detectable excess | 1.55 %/yr |
| gates failed | significance, and multiple testing |

Carry ranks twenty currencies with enormous reliability — the strongest
predictive result this project has ever measured, five times the t-statistic of
Release 34's forecast. It converts into a genuinely attractive-looking book. And
against the passive alternative of *simply owning foreign currency*, its
advantage is +1.4 %/yr with a t of 1.79 over thirty-five years, missing the
pre-registered gate by 11 % of its own standard error and by less than the
smallest effect the design could have detected.

The top five predictive results in the release are all FX: carry (t 7.97),
carry+trend (6.47), crash-conditioned carry (4.83), trend 12-1 (3.89) and
carry+positioning (3.67). **Four of the five have a negative or negligible
after-cost excess.**

### What is significantly WRONG

Two configurations survive multiple testing in the losing direction, which is
information rather than noise:

| configuration | excess | t | reading |
|---|---|---|---|
| `FX_REVERSAL_1M` | −3.58 %/yr | **−4.67** | one-month currency reversal is reliably backwards; rank IC −0.047, t −2.53. Short-horizon currency moves *continue* |
| `XA_CURVE_SLOPE_EQUITY_BOND` | −8.64 %/yr | **−4.31** | tilting to equity when the curve is steeper than usual reliably lost to 60/40 |

### Everything else

`VOL_TERM_LONG_TIMING` — holding long volatility only while the term structure
is inverted — earned +12.3 %/yr more than holding it always, at t = 1.36 against
a minimum detectable effect of 18 %. `CMDTY_SEASONALITY` +3.7 %/yr at t = 0.71
against an MDE of 10.5 %. **The commodity lane's nulls are weak and the report
says so**: with five instruments and monthly decisions it could only have
detected effects of 9–15 %/yr, so "no commodity curve edge" means "no *large*
commodity curve edge".

### Multiple testing

Benjamini–Hochberg over all 34 executed configurations at q = 0.10: threshold
p = 1.65 × 10⁻⁵, **2 rejections, both losing, 0 beating**. Hansen's Superior
Predictive Ability per lane: no lane below 0.12 (FX 0.147, volatility 0.122,
cross-asset 0.424). A paired block bootstrap on the single best configuration
gives p = 0.046 — uncorrected, one test out of thirty-four, and rejected by both
multiplicity-aware procedures.

---

## 5. The global coverage matrix

608 cells — 38 declared markets × 16 strategy families — of which **200 are
economically applicable**. Every cell carries a terminal state; **there are no
ambiguous cells**.

| state | cells | share |
|---|---|---|
| tested NATIVE (rejected) | 64 | 32 % |
| tested PROXY only | 38 | 19 % |
| **blocked** | **95** | **48 %** |
| still untested but executable | 1 | 0.5 % |

Roughly **half the global frontier is blocked**, and the blockers are named:

| blocker | markets |
|---|---|
| `BLOCKED_ENTITLEMENT` | precious metals, industrial metals, grains, softs, livestock, Treasury futures, international government bonds, developed and emerging equity |
| `BLOCKED_COST` | single-name credit and CDS, FX NDFs, FX options, inflation swaps, equity index option surfaces, analyst expectations |
| `BLOCKED_LICENSING` | VIX futures settlement history |
| `BLOCKED_SURVIVORSHIP` | short-volatility ETPs, broad crypto cross-section |
| `BLOCKED_POINT_IN_TIME` | loans/preferreds/EM credit, crypto basis and funding |

**Two survivorship blocks are worth reading twice**, because both are cases
where data exists and using it would have produced a number:

- **Short volatility.** `SVXY` and `UVXY` are in the owned database. `XIV`,
  `TVIX`, `ZIV` and `VIIX` are not — the products that *terminated* are exactly
  the ones missing, and `SVXY` was structurally re-levered from −1× to −0.5×
  after February 2018. A short-volatility book assembled from what survives
  would be backfilled with the instruments that did not blow up. It was refused.
- **Broad crypto.** A current list of surviving tokens cannot be written back
  into history and no free point-in-time listing record exists. Two majors were
  admitted and no third.

**The one untested cell is `CREDIT_INVESTMENT_GRADE::MACRO_CONDITIONAL`.** The
credit lane's four configurations cover carry, momentum, mean reversion and
relative value; none carries the macro-conditional family. It could have been
closed by adding a thirty-fifth configuration or by relabelling an existing one.
Both were refused: the grid was frozen before any result was seen and may not be
widened afterwards, and a label changed after seeing results is not a
measurement. **That single cell is why the verdict is `PARTIALLY_CLOSED` rather
than `NO_NATIVE_MULTI_ASSET_EDGE`,** and it is the discipline working rather
than failing.

---

## 6. Two defects this release found in itself

Both would have shipped a qualified candidate that was not one. Both are
recorded as superseded campaigns with their artifacts preserved on disk, and
both now have permanent regression tests.

**v1 — the control did not match what was traded.** The volatility lane gave
every configuration the same control: a volatility-matched mix of *passive long
volatility* and cash. `VOL_TERM_EQUITY_TIMING` holds **equity**. Measured
against a benchmark that loses roughly 60 %/yr to contango, it returned
**+10.5 %/yr of "excess" at t = 2.81** and would have been reported as a
qualified native candidate. Correcting the control moved it to **−1.8 %/yr,
t = −0.90** — a swing of 3.7 standard errors produced entirely by the benchmark.
v3 declares a control leg per configuration wherever a lane's configurations
trade different instruments.

**v2 — the control was fabricated before its own legs existed.** The
cross-asset lane's 60/40 benchmark filled its missing bond leg with zero, so
every decision before the Treasury total-return index begins in December 2004
was scored against "60 % equity and 40 % nothing" — a portfolio nobody held.
`XA_FX_CARRY_VS_EQUITY` read **+2.27 %/yr at t = 2.15** against it; against the
benchmark that actually existed it reads **+1.29 %/yr at t = 0.91**. v3 lets a
missing leg propagate as missing and trims **every** lane to the window in which
its own control is observable, uniformly and before any strategy runs.

The general lesson is one this project has now learned three times in three
different disguises: **the control decides everything**, and a control that is
wrong is not conservative in a knowable direction.

---

## 7. Why the numbers can be believed

- **Every statistic is trailing.** No full-sample mean, standard deviation,
  median or rank appears anywhere. Each conditioning statistic is an expanding
  window shifted one period first, and the regression tests poison a future
  observation and assert the past output does not move.
- **A position requires an observable return.** A weight on an instrument with
  no return on that date is removed, not held: a book cannot pay cost to trade
  something that did not exist.
- **Decisions do not overlap.** Struck every *cadence* sessions, so successive
  observations of a cadence-length return are independent.
- **Cost is charged on traded notional** at a per-lane tier, and every
  configuration is reported at 0.5×, 1×, 2× and 4× those assumptions.
- **The control is the passive hold of what is traded**, per lane and, where a
  lane's configurations differ, per configuration. `SPY + cash` is not used
  anywhere as a universal control.
- **Every failure reports its minimum detectable effect**, so "not significant"
  arrives with the size of effect the design could have found.
- **Nothing is fitted.** No parameter is searched, tuned or selected; every one
  is pre-declared at its canonical academic value.
- **No fresh evidence was manufactured.** `FRESH_UNSEEN_EVIDENCE_EXISTS = False`,
  declared before the run. Opening a new *market* does not make an old *outcome*
  unseen — the researcher choosing which market to open has already seen what
  happened. The chronological halves are a stability check and are never called
  a lockbox.
- **One owner per concern.** The economic judge, its controls, its cost model
  and its concentration diagnostics are Release 34's, called with a
  lane-appropriate benchmark and cadence. The statistics are Release 31's. The
  vendor readers are Release 33's and Release 34's. The point-in-time alignment
  rule and the HTTP primitive are Release 35's. The architecture audit asserts
  each of these and forbids a second copy of any of them.

---

## 8. Safety

Research only. This release created **no** signal authority, portfolio target,
capital allocation, proposal, decision, order, model promotion, sleeve
activation, forward-evidence registration or operational write; it mutated no
holdings and no cash, restarted nothing, changed no scheduler and integrated no
broker. It spent **$0.00**, started no trial, created no account and changed no
subscription tier.

Proven two ways: statically by `check_release36_global_multi_asset_frontier` in
`scripts/audit_architecture.py` (58 required assertions, all passing), and at
runtime by `scripts/r33_operational_write_attribution.py`, which gained a tested
**R36 release profile** — extended, not copied; an unknown profile still fails
closed — and returns `ATTRIBUTED` with zero writes attributable to Release 36.

---

## 9. The ten questions

1. **Which asset classes are genuinely closed?** None completely. US equity and
   listed real estate are closed at the native level by Releases 23–31 and
   re-confirmed here. FX is now closed for the eight strategy families a
   deliverable forward supports across 20 currencies. Energy commodity curves
   are closed for seven families across five contracts.
2. **Which were only tested through proxies?** Rates (duration-bucket indices,
   not futures), credit (a broad index, not bonds or CDS), volatility (an ETP,
   not the VX curve), crypto (spot, not basis or funding), cross-asset, and
   equity sectors. 38 cells, 19 %.
3. **Which native markets were newly tested?** Deliverable FX forwards across
   20 currencies with a real interest leg — never previously possible — and
   five dated NYMEX energy curves including a delisted contract.
4. **Which strategies survived?** None.
5. **Which failed?** All 34, on either significance or multiple testing. Two
   failed *significantly in the wrong direction*.
6. **Which are still untested?** One cell, named above, plus 95 blocked ones.
7. **Why?** 48 % of the frontier needs a vendor entitlement, a licence or a
   survivorship-complete history this release was not authorised to buy.
8. **What data would unlock them?** A futures settlement archive covering
   metals, agriculture, rates and index futures is the single highest-value
   purchase — it alone would unblock 36 commodity cells, 11 rates cells and 14
   international equity cells. Then a licensed VIX futures history, then a
   historical analyst-consensus panel.
9. **What should the research program do next?** Stop testing new *strategies*
   on markets this estate can already reach. The frontier is now mapped, and
   the map says the remaining opportunity is behind entitlements.
10. **Are we materially closer to finding investable alpha?** Closer to knowing
    *where it could be*, and no closer to holding it. What changed is that
    "we have not tested FX/commodities/rates/credit/volatility" is no longer
    true, and "there is no alpha in them" is still not claimed.

---

## 10. What Release 37 should not conclude

Not "the markets were the wrong ones" — FX carry produced the strongest
predictive statistic in the project's history and a commodity curve panel with
41 years of dated contracts. Not "the method was wrong" — nothing was fitted, so
there was nothing to overfit.

The finding is narrower and it is now the third release in a row with the same
shape: **prediction is real and the increment over the passive alternative is
not.** Release 34 measured it on ETFs, Release 35 on information, Release 36 on
native markets. In all three the forecast was genuine and the money was already
available by simply holding the asset.

The one direction this release makes newly concrete is that **roughly half the
investable world is behind a paywall this program has never priced**. That is
a purchase decision, not a research decision, and it belongs to the operator
through the released Information Purchase Gate.
