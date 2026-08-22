# Release 37 — Native-Market Data Expansion, Purchase Gate & Advanced Intelligence Readiness

| | |
|---|---|
| verdict | **`R37_DATA_INVESTMENT_RECOMMENDED`** |
| SYSTEM_RESULT | **PASS** |
| PURCHASE_RECOMMENDATION_RESULT | **PASS** |
| ALPHA_RESULT | **NOT_TESTED** (structurally — this release runs no experiment) |
| campaign | `r37_native_market_data_gate_v5` (37.1) |
| acquisition decision | `engine.data_expansion_gate` in `RESEARCH_ACQUISITION` context — **`RESEARCH_ACQUISITION_RECOMMENDED`** for one candidate |
| base commit | `0eb3cf79c1b96b0f0f0077ee3c0fa56fa145c885` (Release-36 closeout) |
| datasets challenged | **20**, in 6 lanes |
| money spent | **$0.00** — 0 trials, 0 accounts, 0 tier changes, 0 licences accepted |
| research root | `D:\Stock_Prediction_app_data\native_market_data_gate_r37\` |

---

## 1. Why this release exists

Release 36 classified 608 asset × strategy cells, found 200 economically
applicable, and left **95 of them blocked** — not by method, not by statistics,
but by an entitlement, a licence, a point-in-time gap or a survivorship gap.
Roughly half the investable world sat behind a paywall this programme had never
priced.

Six releases had answered "can we find alpha in what we hold?" and the answer had
been no six times. Release 37 asks a different question, and it is the first
question in this project's history whose answer is a **purchase order**:

> What is the highest-value additional dataset we can obtain that unlocks the
> largest amount of economically distinct, point-in-time, native-market research?

**It is not a model tournament.** No strategy was executed, no model was fitted,
no book was judged. `ALPHA_RESULT` is `NOT_TESTED` and there is no code path in
the release that could return anything else.

---

## 2. The measurement that decides the release

Release 36 recorded that the owned Norgate **Continuous Futures** database serves
exactly one market, `&ES`. Release 37 re-measured it and got the same answer —
and then asked the question Release 36 did not:

**Can the installed client express a dated futures contract at all?**

| measurement | result |
|---|---|
| `futures_market_symbols()` | **1** market: `ES` |
| `futures_market_session_contracts('&ES')` | **raises** |
| dated-contract API present in `norgatedata` 1.0.74 | **15 of 15 calls** |
| contract metadata answered for the entitled market | **7 of 7** |

`point_value` returns `50.0`, `tick_size` `0.25`, `margin` `16060.0`, `currency`
`USD`, `exchange_name` `CME`, `first_quoted_date` `1997-09-09`. Every function a
dated-contract archive needs — `first_notice_date`, `last_quoted_date`,
`point_value`, `tick_size`, `margin`, `currency`, `exchange_name`,
`futures_market_session_contracts` — **exists in the client already installed on
this machine and already used by Releases 33 and 34**.

**The wall is the entitlement, not the vendor and not the code.** That single
measurement converts the implementation cost of the leading candidate from an
integration project into a checkout page, and it is the reason the recommendation
below is as lopsided as it is.

---

## 3. What was challenged

Twenty datasets across six lanes, each with 49 declared scorecard fields and an
explicit **evidence class** saying how its claims were established — vendor
documentation, a live endpoint probe by this release, the owned client measured
locally, a downloaded sample, or an earlier release's measurement.

| lane | datasets | outcome |
|---|---|---|
| Futures & derivatives | 8 | 1 recommended, 1 free, 3 blocked on contact, 3 refused |
| Analyst expectations | 4 | 1 blocked on contact, 3 refused |
| Options & volatility | 2 | 1 sample required, 1 blocked on contact |
| Credit | 1 | blocked on contact |
| Crypto | 1 | refused on survivorship |
| Other orthogonal | 4 | 2 free, 1 owned, 1 free-key registration |

Terminal states: **1** `BUY_RECOMMENDED_FOR_OPERATOR_REVIEW`, **3**
`FREE_DATA_AVAILABLE_ACQUIRE_NOW`, **1** `ENTITLEMENT_ALREADY_OWNED_USE_IT`,
**2** `SAMPLE_REQUIRED`, **6** `BLOCKED_VENDOR_CONTACT_REQUIRED`, **7**
`DO_NOT_BUY_*`. No cell ends in "interesting".

---

## 4. The recommendation

### #1 — Norgate Data Futures Package, USD 270/year

| | |
|---|---|
| what it is | ~100 futures markets across 11 exchange groups; **individual dated contracts** plus unadjusted and back-adjusted continuous series |
| history | ~1977–1980, or each market's first trading day — **45 years** |
| fields | official daily **settlement** as the close, OHLC, volume, open interest, first notice day, last trading day, point value, tick size, currency, exchange |
| delivery | the local Norgate database read through `norgatedata`, **already installed and already integrated** |
| commercial | an **add-on to an existing paid account** — no new vendor, no new counterparty, no new contract |
| cost | **USD 148.50 for 6 months, USD 270 for 12** |

**What it closes, measured against the frozen Release-36 matrix:**

| asset class | cells |
|---|---|
| COMMODITY (precious, industrial, grains, softs, livestock) | **36** |
| INTERNATIONAL_EQUITY (developed ex-US) | **7** |
| RATES (Treasury futures) | **6** |
| VOLATILITY (Cboe VX term structure) | **4** |
| **total, fully unlocked** | **53 of 95 — 55.8 % of the blocked frontier** |

Three further markets — international government futures, emerging index
futures, and CME crypto basis — are recorded as **PARTIAL** and are deliberately
excluded from the headline, because `PARTIAL_UNLOCK_COUNTS_IN_HEADLINE` is False.
Counting them would take the figure to 68.

**The VIX result is worth reading twice.** Release 36 recorded
`VOL_VIX_FUTURES` as `BLOCKED_LICENSING` after five free Cboe settlement routes
answered 403 or 404, and named "a licensed VIX futures history" as the *second*
purchase the programme would need. This release re-probed the settlement archive
and confirmed the 403 still stands — and then found that the Cboe VX curve from
2004 is **inside the USD 270 package**. The second-priority purchase turns out to
be a subset of the first.

**Value metrics:**

| metric | value |
|---|---|
| cost per Release-36 cell unlocked | **USD 5.09 / year** |
| cost per native market unlocked | USD 33.75 / year |
| cost per year of history | USD 6.00 / year |
| cost per distinct asset class unlocked | USD 67.50 / year |
| integrity multiplier | **1.60** — every factor at 1.0, breadth bonus for 4 asset classes |
| research value per dollar | **0.314** |

The integrity multiplier is 1.0 on all seven quality factors — native dated
contracts, settlements published as observed, discontinued contracts retained,
clear research licence, strong identity metadata, raw rather than transformed,
45 years against a 20-year reference. **No other candidate scores 1.0 on all
seven.**

### #2 and #3 are blocked, not beaten

No second candidate is *investable*, because no second candidate has a price.
Ranked by the cells they would unlock if priced:

- **#2 Portara / CQG DataFactory** — up to 54 cells, daily history from 1899,
  individual contracts with settlement as a distinct field. **Quote required.**
  It is the only candidate that adds history *before* the recommendation starts.
  The right time to ask is after the USD 270 package has shown whether pre-1980
  depth is the binding constraint.
- **#3 CME Group DataMine** — up to 42 cells, the exchange's own authoritative
  settlements. **Quote required**, exchange-direct licensing, and it covers one
  exchange group where the recommendation covers eleven.

### What was refused, and why

| dataset | state | reason |
|---|---|---|
| Databento GLBX.MDP3 | `DO_NOT_BUY_COST_VALUE_FAILURE` | the affordable tier carries **12 months** of history; the tier with 16+ years is **USD 54,000/yr** |
| FirstRate Data futures | `DO_NOT_BUY_LOW_INCREMENTAL_VALUE` | no documented settlement series and **no open interest** — roll, curve carry and positioning all impossible |
| Cboe DataShop VX | `DO_NOT_BUY_LOW_INCREMENTAL_VALUE` | its 4 cells are already inside the recommendation; buying both pays twice for one curve |
| LSEG I/B/E/S | `DO_NOT_BUY_COST_VALUE_FAILURE` | the gold standard, priced for institutions; 3 cells against an enterprise contract |
| Free analyst tiers (5) | `DO_NOT_BUY_PIT_FAILURE` | today's consensus plus deltas; writing it backwards is a prohibited substitution |
| Intrinio | `DO_NOT_BUY_LOW_INCREMENTAL_VALUE` | a live trial already returned `NO_DEFENSIBLE_ALPHA` |
| Binance public archive | `DO_NOT_BUY_SURVIVORSHIP_FAILURE` | free, real, and organised by **currently listed** symbol — it is the objection, not the answer |

---

## 5. Is analyst history a better investment than futures data?

**No, and the margin is not close.**

The futures recommendation unlocks **53 measured cells across four asset
classes** for a **known** USD 270 a year, on an account this estate already
holds, with the reader already written and tested. The analyst candidate unlocks
**3 cells**, has **no published price**, **cannot be sampled** without a sales
conversation, and is the one family this estate has already tested twice —
Stage 13B found t = 2.27 on sales PEAD and Stage 13C found the out-of-sample
result did not replicate at t = −0.29, and an Intrinio trial returned
`NO_DEFENSIBLE_ALPHA`.

That does not make analyst data worthless. It makes it the **second** question.
What would change the ranking: a single-user quote under roughly USD 2,000 a year
**with** a sample proving daily vintages and delisted-issuer coverage would put
it back in contention on cost-per-cell — though never ahead on breadth.

The Nasdaq Data Link Zacks table was re-probed by this release and still answers
**HTTP 403**, unchanged since Phase 12-A and Release 35.

---

## 6. What was acquired, free, and what it turned out to be

**5 payloads, 29.7 MB, $0**, every one public and account-free, all fetched
through the released Release-35 HTTP owner and checksummed.

| source | measured |
|---|---|
| LBMA gold PM | 14,667 rows, **1968-04-01 → 2026-08-21** |
| LBMA silver | 14,830 rows, 1968-01-02 → 2026-08-21 |
| LBMA platinum AM | 9,195 rows, 1990-04-02 → 2026-08-21 |
| NY Fed primary-dealer positions | **751,169 rows**, 1998-01-28 → 2026-08-12 |
| Cboe CFE volume & open interest | 5,638 rows, 3/26/2004 → 8/20/2026 |

**None of them unlocks a single Release-36 cell, and the report says so.** An
LBMA fixing is a `LEVEL_1_SIGNAL`: it cannot be held, it has no roll and no
carry, so it cannot close a metals cell. Recording it as though it could would be
precisely the substitution Release 36 spent a release refusing. They are worth
having as controls and as conditioning variables — and they are worth having
*more* once the recommended purchase makes them usable: NY Fed dealer positioning
is the rates analogue of the Commitments of Traders report Release 35 already
owns for commodities, and it becomes testable the moment Treasury futures exist.

### The sample that disproved its own scorecard row

The Cboe file was first recorded as **per-dated-contract** volume and open
interest, on the strength of the exchange's page description. Downloading it and
parsing the bytes showed it is **wide and product-level**: one row per date, one
Volume and one OI column per product, and no expiry key anywhere. The row was
corrected — `dated_contracts_available` became False, incremental distinctness
fell from MEDIUM to LOW — and the run was superseded.

That is `A_MARKETING_CLAIM_IS_NOT_A_MEASUREMENT`, the release's own rule,
catching the release itself. It is the entire reason Track B downloads samples
instead of summarising pages.

---

## 7. One gate, two questions (Release 37.1)

Release 37 **defines no purchase gate**. The canonical answers to "should we buy
this data" already exist, and a fourth would be a fourth answer to one question.
`alpha_agent/r37/purchase_gate.py` is forbidden by the architecture audit so it
cannot appear by accident.

### The semantic gap Release 37 exposed

Release 37 ran every candidate through `engine.data_expansion_gate` (via
`api.data_expansion`) and got `INSUFFICIENT_EVIDENCE` for 15 and `REJECT` for 5 —
**`PURCHASE_RECOMMENDED` for none, including its own recommendation.** It
recorded that verbatim and published a separate capability judgement beside it.

Both halves were honest, and the pair was incoherent: two apparently competing
acquisition truths, with the release's own answer carrying the headline. The
defect was not in either result. It was in the **semantics of the canonical
gate**, which modelled only the post-acquisition question:

```
need the data to measure lift  ->  gate needs lift to recommend buying the data
                               ->  cannot measure lift, the data is not acquired
```

### The correction: an explicit decision context

The ONE canonical gate now answers **both** business questions, selected by an
explicit `decision_context` rather than assumed:

| | **Stage A** `RESEARCH_ACQUISITION` | **Stage B** `POST_ACQUISITION_VALUE` |
|---|---|---|
| question | is this worth paying to **learn**? | did the measured evidence earn continued purchase? |
| when | **before** acquisition | after acquisition and research |
| measured lift | **not required** — it cannot exist yet | **required**, out-of-sample, cost-adjusted |
| dimensions | 18 | 16 |
| states | `REJECT` · `INSUFFICIENT_EVIDENCE` · `CANDIDATE` · `RESEARCH_ACQUISITION_RECOMMENDED` · `NO_ACQUISITION_REQUIRED_ALREADY_ENTITLED` | `REJECT` · `INSUFFICIENT_EVIDENCE` · `RESEARCH_ONLY` · `CANDIDATE` · `PURCHASE_RECOMMENDED` · `INTEGRATION_RECOMMENDED` |
| default | — | **yes**, so no existing caller changed meaning |

`RESEARCH_ACQUISITION_RECOMMENDED` is deliberately **not**
`PURCHASE_RECOMMENDED`. One says "worth paying to learn"; the other says "the
measured evidence earned continued purchase". Collapsing them would let a
pre-research judgement be read as post-research proof, which is the single most
available misreading of this release.

### Stage A is not a softer gate

It is a gate asked a different question. It drops exactly one requirement — the
one that cannot exist yet — and **adds two the post-acquisition context has no
reason to ask**, both of which the caller must declare and neither of which the
kernel will invent:

- `capability_unlocked` — what does acquiring this actually unlock? Nothing
  unlocked at any level is a `REJECT`; unlocked only at the weaker ceiling level
  is a `CANDIDATE`; undeclared is `INSUFFICIENT_EVIDENCE`, never a pass.
- `expected_incremental_distinctness` — declared, with its basis named, plus
  whether an owned or free substitute was tried first (`USE_OWNED_DATA_FIRST`)
  and whether a **bounded evaluation that can return DO_NOT_BUY** exists.

Everything else binds exactly as hard. Point-in-time failure, survivorship
failure, insufficient history and prohibited licensing all still `REJECT`;
unknown cost, unclear licensing and undeclared distinctness still cap at
`CANDIDATE`.

### What the canonical gate said

Run in Stage A, the canonical gate returns **`RESEARCH_ACQUISITION_RECOMMENDED`
for exactly one candidate — the Norgate Futures Package — with no failed
dimension and no outstanding blocker.** It was not hard-coded to pass; it was run
and it passed. The other nineteen: 8 `CANDIDATE` (blocked on unknown cost or
licence), 6 `REJECT`, 1 `INSUFFICIENT_EVIDENCE`, 1 `NO_ACQUISITION_REQUIRED`.

**The canonical Stage-A result is the authority.** Release 37's own gate states
are a *triage* vocabulary — which lane a candidate is in and what a human must do
next — and may never overrule it. `recommended_by_r37_but_refused_by_canonical_gate`
must stay empty, and the campaign refuses to recommend anything on that list.
Stage B's `INSUFFICIENT_EVIDENCE` verdicts are still recorded verbatim, because
deleting the inconvenient half of a pair of results is how a release stops being
checkable.

**Neither result is purchasing authority, alpha evidence, or integration
approval.** Manual operator approval is required, and every state — including the
recommending one — returns `purchase_authorised: false`.

---

## 8. Why the ranking can be believed

- **A vendor claim alone may never unlock a cell.** A market is credited only
  when the dataset's declared instruments implement that market's native
  instrument, and the claim carries the evidence class that established it.
  `claims_without_a_blocked_market` is empty.
- **The unlock arithmetic is derived, not typed.** It reads the frozen
  Release-36 coverage matrix, falling back to the released market table. Release
  37 never re-states the frontier.
- **A proxy may not unlock a native cell.** A `LEVEL_2_PROXY` or
  `LEVEL_1_SIGNAL` dataset receives zero full unlocks by construction, whatever
  its coverage table says — this is Release 36's rule imported by name.
- **Partial is not full**, and never enters a headline.
- **The score may not override a hard gate.** The cheapest candidate in the long
  list is free, survivor-only crypto, and a naive value-per-dollar metric would
  have ranked it second. The artifact publishes that naive ranking next to the
  real one, so the reader can see exactly what the gate prevented.
- **A cost that is unknown produces no score at all** — six candidates carry
  `null` rather than an optimistic guess.
- **Blocked routes were re-probed**, not carried forward on trust. Two blocks
  were confirmed live (Cboe settlements 403, Nasdaq Zacks 403). One probe —
  CME's public settlement route — **timed out under this release's declared user
  agent and is recorded as `UNMEASURED`, not as open.**
- **A sample proves a schema, never an edge.**
  `A_SAMPLE_MAY_SUPPORT_AN_ALPHA_CLAIM` is False.
- **Every blocked candidate carries a named human action** — a page, a question,
  and the decision it would unblock. "Contact the vendor" is not an action.

---

## 9. Track C — advanced ML/AI readiness (secondary, no campaign)

### The machine, measured read-only

| | |
|---|---|
| CPU | Intel i3-10105F, 4 cores / **8 logical** |
| RAM | **68.6 GB** (`kernel32.GlobalMemoryStatusEx`) |
| GPU | NVIDIA GeForce **GTX 1650, 4.0 GB VRAM**, compute capability **7.5**, driver 566.36 |
| CUDA toolkit | **not installed** — and `MAY_INSTALL_CUDA` is False |
| storage | C: **7.3 GB free** of 511; D: **777.9 GB free** of 1,000 |
| numerical stack | `numpy`, `pandas` — **and nothing else** |
| absent | scipy, scikit-learn, statsmodels, xgboost, lightgbm, catboost, torch, tensorflow, jax, transformers, tabpfn, chronos, gluonts, darts |

Binding constraints: **VRAM**, **CPU core count**, **system-volume free space**.

### The readiness matrix — 13 model families, readiness computed

**Release 37 said "8 of 13 run here today". That was wrong, and Release 37.1
corrects it.** Eight families meet the **hardware** minima. The environment holds
`numpy` and `pandas` and nothing else, so the number that could actually be
executed today is **one**. Hardware capability and runnability are now two
separate measured facts, and readiness is a five-value class computed against the
measured library inventory as well as the measured machine:

| readiness | count | families |
|---|---|---|
| `CURRENTLY_INSTALLED_AND_RUNNABLE` | **1** | regularised linear (closed-form ridge on `numpy`) |
| `HARDWARE_FEASIBLE_AFTER_SOFTWARE_INSTALL` | **5** | gradient-boosted trees, extra trees / random forests, mixture-of-experts and regime gating, quantile / distributional regression, ensemble-disagreement uncertainty |
| `LOCALLY_POSSIBLE_BUT_IMPRACTICAL` | **2** | TabPFN-class, temporal convolution / LSTM — both need 4 GB VRAM on a 4 GB card, leaving zero headroom |
| `EXTERNAL_GPU_RECOMMENDED` | **5** | Chronos-class (8 GB), TimesFM/Moirai (12 GB), temporal fusion transformer (8 GB), state-space / Mamba (16 GB), graph cross-market (16 GB) |
| `NOT_CURRENTLY_FEASIBLE` | **0** | — |

Missing libraries: `scipy`, `scikit-learn`, `xgboost`, `torch`, `transformers`,
`tabpfn`, `chronos-forecasting`. **The installs are free and this release
performs none of them** — `installed_anything` is False and
`MAY_DOWNLOAD_MODEL_WEIGHTS` is False.

The broader conclusion is unchanged and now correctly stated:

- classical and high-end tabular ML is **locally feasible after a free install**;
- small development-scale sequence experiments are **locally possible but
  impractical** on 4 GB with no tensor cores;
- serious foundation-model and deep multivariate research will need **rented GPU
  capacity later** — a rental decision, not a purchase now;
- Release 37 buys **no compute** and installs **no framework**.

Every row names its point-in-time and survivorship failure modes, and three of
those are worth quoting because they are traps this estate could walk into:

- **A foundation model pre-trained on public data may have seen the very series
  being forecast.** A zero-shot result on a public price history is **not**
  out-of-sample and may not be reported as one.
- **A temporal fusion transformer's known-future channel is a leakage trap by
  design.** Only a calendar fact — an expiry date, a first notice day, a
  scheduled release — may enter it. Never a price. The recommended purchase is
  the only candidate that supplies expiry and notice dates as fields, which is
  what makes that channel legitimately usable.
- **A regime label assigned with hindsight is how mixtures of experts
  manufacture results.** Phase 10-O already returned `REJECT_REGIME_OVERFIT` on
  owned data.

**The single most valuable family for this estate is the least fashionable
one.** Release 30.1 recorded `CALIBRATION_BLOCKED` and Release 29H recorded
`expected_return NOT_CALIBRATED`. The allocator's binding gap is a **calibrated
distribution**, not a better mean — and quantile / distributional regression is
CPU-only, needing `scipy` rather than a GPU. **10 of the 13 families are made
more valuable by the recommended purchase.**

No row claims a model is superior because it is newer. `NEWER_IMPLIES_BETTER` is
False.

### The ML data contract

One canonical research input/output shape, composing existing owners rather than
declaring a second market-data authority. The input carries instrument identity,
contract month, expiry, first notice day, decision timestamp, **feature
observation timestamp**, the released `as_of_align` rule, curve structure,
volume, open interest, carry and macro features, an explicit missingness mask,
time-assigned partitions with an **embargo larger than the horizon**, cost on
traded notional, the passive control return, and survivorship and point-in-time
state per field.

The output is deliberately wider than a point return: expected **excess** return,
probability of positive excess, quantiles, tail probability, expected volatility,
cross-sectional rank, uncertainty, **model disagreement**, and an explicit
**abstain** — because a NULL result is a legitimate answer in this system and a
model that cannot express one will always find something.

---

## 10. Track D — market structure and visual intelligence (designed, not run)

Nine structural hypotheses (trend structure, impulse/retracement, breakout and
retest, support/resistance, channel geometry, ATR-normalised geometry,
volatility contraction/expansion, volume confirmation, multi-timeframe
agreement), each with a **declared control**, a **stated way it might fail**, and
its **leakage risk**.

**The anti-hindsight rule is the whole design.** A swing high is only a swing
high in retrospect. A pivot is confirmed only when 5 subsequent sessions have
closed without exceeding it **and** price has displaced by at least 1 × ATR — and
it is stamped with the **confirmation date**, never the date of the extreme. A
confirmed pivot is therefore already several sessions old, and any design whose
edge disappears under that lag never had one.

**Fibonacci is a testable hypothesis, not doctrine.** Canonical levels 0.236,
0.382, 0.500, 0.618, 0.786, 1.272, 1.618 are tested **against seven placebo
levels** (0.300, 0.440, 0.560, 0.700, 0.860, 1.400, 1.800) drawn from between
them, with the placebo levels inside the multiple-testing denominator. If the two
arms are indistinguishable, the correct conclusion is *"retracement entries in
trends work"* — not *"Fibonacci works"* — and those are different claims with
different futures.

The visual lane compares three representations — numeric OHLCV, engineered market
structure, and a rendered chart through a frozen vision encoder — because a
chart-image result without the first two arms cannot be attributed to anything.
Its leakage rules are explicit: the rendered window ends at the decision
timestamp, and the y-axis is scaled from the visible window only, because an axis
scaled to a future extreme encodes that extreme.

**Nothing was executed.** `EXECUTED_IN_THIS_RELEASE`, `READS_A_PRICE`,
`COMPUTES_A_FEATURE` and `VISUAL_EXPERIMENT_IN_SCOPE` are all False. The
precondition for running it is a native universe wide enough that a structural
result is not one market's history — which is what the recommended purchase buys.

---

## 11. Five runs, and what the first four got wrong

| campaign | superseded because |
|---|---|
| `v1` | the Slice-9 result was read from the **wrapper** rather than the evaluation, so every gate verdict serialised as `null`; an unreachable probe was recorded as "no longer blocked"; the Cboe sample failed to parse on a ragged row |
| `v2` | a downloaded sample **disproved a scorecard claim** — the Cboe file is product-level, not per-dated-contract |
| `v3` | the compute inventory measured RAM with `os.sysconf`, which does not exist on Windows, so a 64 GB machine reported `total_ram_gb: null` |
| `v4` | **two competing acquisition truths** — the canonical gate answered only the post-acquisition question, so asking it a pre-acquisition question was circular, and the release published its own capability judgement beside it. Also reported eight of thirteen model families as running "today" when eight met the *hardware* minima and one had its library installed |

All four keep their artifacts on disk. A release that quietly re-ran until it
liked the output would be worthless.

**The investment recommendation is unchanged across all five.** The canonical
acquisition gate independently reaches `RESEARCH_ACQUISITION_RECOMMENDED` for the
same single candidate, with no failed dimension and no outstanding blocker — the
correction changed *where the decision is taken*, not what it is.

---

## 12. Safety

Research and purchase-governance only. This release created **no** signal
authority, portfolio target, capital allocation, proposal, decision, order, model
promotion, sleeve activation, forward-evidence registration or operational write;
it mutated no holdings and no cash, restarted nothing, changed no scheduler and
integrated no broker.

It spent **$0.00**, started **no trial**, created **no account**, accepted **no
licence**, submitted **no payment detail**, changed **no subscription tier**,
installed **no CUDA**, downloaded **no model weights** and bought **no cloud
compute**.

`BUY_RECOMMENDED_FOR_OPERATOR_REVIEW` is a recommendation to a person.
`PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE` is False, and every gate state —
including the recommending one — returns `purchase_authorised: False`.

Proven two ways: statically by `check_release37_native_market_data_gate` and
`check_data_expansion_ownership` in `scripts/audit_architecture.py` (**75
required assertions across the two sections**, all passing), and at runtime by
`scripts/r33_operational_write_attribution.py`, which carries a tested **R37
release profile** — extended, not copied; an unknown profile still fails closed —
now covering the two canonical Slice-9 owners as well, and returning `ATTRIBUTED`
with zero writes attributable to Release 37.

The 37.1 additions are enforced, not asserted in prose: the canonical gate must
declare both decision contexts, the default must remain the legacy
post-acquisition one, the acquisition state must stay distinct from
`PURCHASE_RECOMMENDED`, the post-acquisition evidence standard must remain
intact, the two contexts must persist separately, and
`recommended_by_r37_but_refused_by_canonical_gate` must be empty — checked by
**running the real gate**, not by reading the source.

---

## 13. The twelve questions

1. **What is the best dataset to obtain next?** The Norgate Data **Futures
   Package**, USD 270 a year, as an add-on to the subscription this estate
   already pays for.
2. **What does it unlock from Release 36?** **53 of 95 blocked cells (55.8 %)**
   — 36 commodity, 7 international equity, 6 rates, 4 volatility — plus 15 more
   at ceiling if Bund/JGB/Gilt, emerging index futures and CME crypto basis are
   confirmed. These are **EXPECTED** unlocks, derived from the frozen Release-36
   matrix and the dataset's declared instruments. They become **MEASURED**
   unlocks only after the entitlement is activated and the delivered markets and
   contracts are enumerated — which is step one of Release 38. A shortfall is a
   finding, not a failure of the decision.
3. **What does it cost?** USD 148.50 for six months, USD 270 for twelve.
   **USD 5.09 per unlocked cell per year.**
4. **What evidence is there that the data is usable?** The vendor's published
   content tables, and — decisively — the **installed client measured locally**:
   all 15 dated-contract calls present, 7 of 7 metadata calls answering real
   values for the entitled market, and contract enumeration failing on
   entitlement rather than capability.
5. **What still needs a sample or a sales contact?** Six candidates need a
   vendor conversation (Zacks, CSI, Portara, CME DataMine, OptionMetrics, FINRA
   TRACE), two need a sample (ORATS, USDA free key). Each carries a named exact
   step.
6. **Analyst history, better or worse than futures?** **Worse**, by 53 cells to
   3, by a known price to an unknown one, and against two prior in-house tests
   that did not replicate.
7. **What ranks #2 and #3?** Portara/CQG (up to 54 cells, deeper history than
   the recommendation, quote required) and CME DataMine (up to 42 cells,
   single-exchange, quote required). Neither is investable today because neither
   has a price.
8. **Should we spend money now?** **Yes — for the single USD 270 entitlement
   upgrade, and nothing else.** The **canonical Data Expansion gate**, asked the
   `RESEARCH_ACQUISITION` question, returns `RESEARCH_ACQUISITION_RECOMMENDED`
   for exactly one candidate, with no failed dimension and no outstanding
   blocker. It is the only decision-maker here; this release's own
   capability-per-dollar ranking agrees with it rather than substituting for it.
   Manual operator approval is still required, and nothing was purchased.
9. **What ML becomes feasible?** **One of 13 families is installed and runnable
   today; 8 of 13 meet the hardware minima; 5 need only a free install.** 10 of
   13 become more valuable with the purchase. The highest-value one — a
   calibrated predictive **distribution** rather than a better mean — is CPU-only
   (it needs `scipy`, not a GPU) and addresses the `CALIBRATION_BLOCKED` gap
   recorded in Release 30.1.
10. **Is the workstation sufficient?** For classical, regime and probabilistic
    tabular families, **yes after a free install**. For small sequence and
    tabular-foundation work it is *possible but impractical* — 4 GB of VRAM with
    no tensor cores leaves zero headroom. For time-series foundation and
    long-context sequence models, **no**: 8–16 GB would be needed. That is a
    rental decision for a later release, not a purchase now, and this release
    neither buys compute nor installs a framework.
11. **Where do market structure and Fibonacci fit?** As a pre-registered,
    placebo-controlled backlog whose precondition is exactly the universe the
    recommended purchase provides. Confirmed pivots only, confirmation-dated,
    placebo levels inside the denominator, three representation arms.
12. **What is the highest-value Release 38?** **Execute the native futures lanes
    the purchase opens** — build the ~100-market dated-contract panel with the
    Release-36 machinery (trailing statistics, per-lane controls, cost on traded
    notional, minimum detectable effect), and run the 53 cells that have never
    had an instrument. If the purchase does not happen, Release 38 is instead the
    six vendor conversations named in `blocked_vendor_actions.json`.

---

## 14. What Release 38 should not conclude

Not "we have bought our way out of the problem". Six releases have found genuine
prediction and no convertible edge, and a wider universe is a wider place to
discover the same thing. What changes is that the discovery would finally be
**about the markets themselves** rather than about funds that track them — and
that "we have not tested metals, grains, softs, livestock, the Treasury curve or
the VIX term structure" would stop being true.

The honest expectation is that most of the 53 cells will fail, exactly as the 34
configurations of Release 36 did. The reason to spend USD 270 anyway is that at
USD 5.09 a cell, finding out is cheaper than continuing to guess.
