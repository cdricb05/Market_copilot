# Stage 25 — Autonomous Multi-Source Alpha Discovery & Challenger Evolution

**Status:** research complete, no promotion proposed, zero operational mutations.
**Branch:** `stage19-controlled-rebalance` · **Base HEAD:** `d38e49e` (Stage 24)
**Owner module:** [alpha_agent/stage25_alpha_discovery.py](../alpha_agent/stage25_alpha_discovery.py)
**CLI:** [scripts/run_stage25_alpha_discovery.py](../scripts/run_stage25_alpha_discovery.py)
**Index builder:** [scripts/build_stage25_pit_index.py](../scripts/build_stage25_pit_index.py)
**Tests:** [tests/test_stage25_alpha_discovery.py](../tests/test_stage25_alpha_discovery.py) — 52 hermetic
**Research root:** `D:\Stock_Prediction_app_data\stage25_autonomous_alpha_discovery`
**Run id:** `stage25_f811c142f7dbd7e0` (content-addressed; identical inputs reproduce it)

---

## 1. The question Stage 25 exists to answer

Stage 24 proved the operational model's fundamental leg survives honest
point-in-time, survivorship-safe testing, and surfaced one strong candidate —
**R&D intensity** — that it could not falsify. The reason was specific: R&D
intensity is mechanically concentrated in technology and biotech, those sectors
led 2010–2026, and **no classification that was legitimately knowable at the
formation date existed on disk**. Stage 24 reported the wall rather than
substituting today's sector map.

Stage 25 climbs the wall as far as owned data allows, then uses the resulting
control to attack R&D intensity — and, because a campaign that only tests the
prior stage's favourite is not research, runs 28 new pre-registered hypotheses
across six economic families and subjects the winner to the *same* battery.

---

## 2. Point-in-time sector: what was actually resolved

There are exactly two honest answers in owned data, and Stage 25 keeps them
rigidly apart. Conflating them would destroy the only thing that makes the
falsification legitimate.

### Tier A — `PIT_XBRL_DISCLOSURE_SIGNATURE` (leakage-safe)

An issuer that had **filed** deposit, loan, interest-and-dividend-income,
premium, policyholder-benefit or investment-real-estate facts by date *D* was,
at *D*, observably a bank / insurer / REIT. The classifier reads only facts
filed by *D*, so it cannot see forward. Coverage is **100 %** of panel rows in
every year.

It deliberately **excludes the R&D concept** from every rule, so it cannot be
circular with the hypothesis it is used to test. A regression enforces this.

| Tier-A class | panel rows |
|---|---|
| OperatingNonFinancial | 26,122 |
| Banking | 6,702 |
| Insurance | 1,051 |
| RealEstate | 777 |

It resolves **business model**, not industry. It cannot separate Technology from
Industrials — which is exactly the boundary the R&D question turns on.

### Tier B — `ENTITY_SIC_SNAPSHOT_CONTROL` (look-ahead; control only)

The owned SEC entity-level assigned SIC (Phase-10.1 submissions index, 979,405
issuers) mapped through the released `pit_sector` taxonomy. **1,084 of 1,084**
requested CIKs resolve. This is *today's* classification and carries a
look-ahead.

**Why a look-ahead control is still legitimate — the asymmetry.** The test is
adversarial. A signal that **dies** under a control carrying *more*
classification information than an honest contemporaneous control could carry is
dead beyond rescue: no leakage-safe map could revive it. A signal that
**survives** is only *provisionally* cleared, because that same look-ahead could
have absorbed genuine information. Every Tier-B result in this stage is stamped
with that disclaimer.

Tier B is **inadmissible** for signal construction, candidate registration,
challenger evidence, promotion claims and shadow books. A regression asserts
that no registered candidate spec references either tier, and that no factor
value in the panel depends on a classification.

### Fidelity of the control

On the one dimension Tier A *can* verify point-in-time — is this issuer a
financial? — the two tiers agree on **29,833 of 34,624 comparable rows
(86.2 %)**. Most disagreement is Tier A's coarseness (asset managers, brokers
and exchanges file none of its markers), not Tier B drift. This bounds how wrong
the control can be where we can check it; it says nothing about the
Technology/Industrials boundary.

### What is still blocked, and the exact free artefact that closes it

A **per-filing, effective-dated** classification series. The artefact is named
precisely, because "acquire SIC from SEC" was too vague to act on:

> **SEC Financial Statement Data Sets, `sub.txt`** — one zip per quarter from
> 2009Q2, ~2.7 GB total, free, no vendor, no quota. `sub.txt` carries **per
> submission** both the assigned SIC and the acceptance timestamp, which is
> exactly the `(classification, available_at)` observation that
> `alpha_agent.pit_sector.PitSicSeries` **already consumes**.

It was not fetched here because Workstream B was constrained to owned evidence.
It is the number-one item in the research queue.

---

## 3. The R&D verdict

**Reproduced exactly**, then attacked from seventeen angles.

Baseline on the Stage-25 panel: 65 formations, median 209 names,
rank IC **0.0594**, IC t **2.86**, spread t **3.13**, net-25 **13.5 %**,
turnover 0.06 — identical to Stage 24.

| Control | rank IC | IC t | spread t | verdict |
|---|---|---|---|---|
| **raw** | 0.0594 | 2.86 | 3.13 | — |
| sector-neutral, **Tier A (leakage-safe)** | 0.0562 | **2.72** | 3.22 | survives |
| sector-neutral, **Tier B (look-ahead control)** | 0.0430 | **2.83** | 3.60 | survives |
| remove Technology | 0.0564 | 2.68 | 3.22 | survives |
| remove HealthCare | 0.0643 | 3.00 | 3.17 | survives |
| **remove Technology + ConsumerDiscretionary** | 0.0529 | **2.43** | 3.44 | survives |
| size / liquidity neutral | 0.0578 | 2.87 | — | 97 % retained |
| volatility neutral | 0.0615 | 3.33 | — | 104 % retained |
| **beta neutral** (new capability) | 0.0537 | 2.66 | — | 100 % retained |
| joint size+vol+beta | 0.0538 | 3.07 | — | survives |
| drop top 1 winner / period | 0.0511 | 2.42 | 2.00 | survives |
| drop top 3 winners / period | 0.0448 | 2.16 | 1.46 | survives |
| **drop top 5 winners / period** | 0.0391 | **1.89** | 1.10 | **FAILS** |
| first half / second half | 0.0591 / 0.0597 | 2.57 / 1.72 | — | both positive |
| pre-2016 / post-2016 | 0.0294 / 0.0758 | 1.19 / 2.61 | — | both positive |
| pre-COVID / post-COVID | 0.0624 / 0.0550 | 3.06 / 1.29 | — | both positive |

The `remove Technology + ConsumerDiscretionary` row matters because of a mapping
detail worth stating: the released SIC taxonomy sends 3570–3579 and 3670–3699 to
**Technology** but sends 7000–7999 (Services, *including* 7372 prepackaged
software) to **ConsumerDiscretionary**. Removing "Technology" alone would leave
most software firms in the sample. Removing both leaves 129 names and IC 0.0529
at t 2.43.

### Disclosure selection — measured, not assumed

Stage 24's `SELECTION_ON_DISCLOSURE` caveat is now decomposed into four states
using only what owned data can distinguish:

| state | rows | meaning |
|---|---|---|
| REPORTED | 12,896 | tagged with a non-zero value |
| **ZERO** | **8** | tagged, and the value *is* zero |
| NOT_REPORTED | 14,489 | annual record exists, no R&D tag — **never read as zero** |
| NOT_APPLICABLE | 7,259 | bank / insurer / REIT by the **leakage-safe** Tier A |

Reporting rate averages 36.6 %, and varies exactly as economics predicts:
Technology 87.8 %, HealthCare 72.4 %, ConsumerStaples 73.3 %, Financials 1.6 %,
Utilities 3.5 %.

The decisive test is the **membership spread** — the pre-registered
`s25_rnd_disclosure_indicator`, whose long/short legs are literally "R&D
reporters minus non-reporters": **rank IC 0.0146, t 1.06, spread t 0.99.**
Being a reporter does not pay. **Disclosure selection is not the explanation.**

### Verdict: `CONCENTRATION_FRAGILE`

Applying the thresholds pre-registered in source *before* any Stage-25 number
existed (controlled IC t ≥ 2.0 **and** ≥ 50 % of raw rank IC retained), exactly
one control of twenty-two fails: dropping the five best realised outcomes per
period leaves t = 1.89 against a bar of 2.0, with 65.9 % of the IC retained.

Read that honestly in both directions:

* **The sector explanation is dead.** This was the single most likely competing
  story and it does not survive contact with either classification tier, with
  any single-sector removal, or with the joint tech/software removal. So is the
  disclosure explanation, and the size, volatility and beta explanations.
* **It is nonetheless fragile.** A signal whose significance depends on its five
  luckiest names per period is not one to deploy capital against, and its
  pre-2016 sub-sample (t 1.19) and post-COVID sub-sample (t 1.29) are weak.

The threshold was **not** relaxed to rescue it. R&D intensity stays a research
candidate and is **not** a challenger.

### What R&D intensity probably *is*

Three of the campaign's own results point the same way, and all three came out
with the **wrong pre-registered sign**:

| factor | expected | rank IC | IC t | reading |
|---|---|---|---|---|
| `s25_rnd_efficiency` (GrossProfit / R&D) | +1 | **−0.0536** | **−2.68** | *low* profit per R&D dollar outperformed |
| `s25_sbc_intensity` (SBC / Revenue) | −1 | −0.0319 | −1.79 | *high* stock comp outperformed |
| `s25_receivables_growth` | −1 | −0.0280 | −2.05 | *fast* receivable growth outperformed |

Heavy R&D spend, low return on that spend, heavy stock compensation, aggressive
receivables. That is one coherent profile — the expensive, intangible-heavy
growth firm — in the decade that profile led. **It is the spending, not the
productivity, that paid.** These are reported as clean rejections; no sign was
flipped after the fact.

---

## 4. The campaign: 28 pre-registered hypotheses

Quarterly non-overlapping formation, 63-day forward return, 25 bps cost
reference, Benjamini-Hochberg over the **whole** 28-hypothesis family (the
conservative choice; the per-economic-family pass is reported for interpretation
only). Signs fixed before evaluation and never refit.

**66 formations, 2010-01 → 2026-04, median cross-section 554 names,
survivorship-safe.**

### Baselines on the shared cross-section

| | periods | names | rank IC | IC t | spread t | net25 | turnover | gate |
|---|---|---|---|---|---|---|---|---|
| `composite_sn_pit` | 66 | 488 | 0.0231 | **2.03** | 2.10 | 4.45 % | 0.16 | KEEP_FOR_RESEARCH |
| `mom_6_1` | 66 | 554 | 0.0254 | 1.22 | 1.20 | 4.12 % | 0.61 | REJECTED |
| `ensemble_pit_5050` | 66 | 488 | 0.0322 | 1.83 | 2.09 | 6.70 % | 0.48 | REJECTED |

### Results

| hypothesis | family | names | rank IC | IC t | spread t | net25 | BH q | gate |
|---|---|---|---|---|---|---|---|---|
| **`s25_operating_profitability`** | quality | 307 | **0.0550** | **3.63** | **2.15** | **7.14 %** | **0.008** | **KEEP_FOR_RESEARCH** |
| `s25_rnd_to_sales` | innovation | 187 | 0.0421 | 1.94 | 2.40 | 11.3 % | 0.257 | REJECTED |
| `s25_cash_to_assets` | balance sheet | 505 | 0.0331 | 1.90 | 1.92 | 6.13 % | 0.257 | REJECTED |
| `s25_fcf_margin` | cash flow | 418 | 0.0242 | 1.84 | 0.25 | 0.44 % | 0.257 | REJECTED |
| `s25_shareholder_payout` | investment | 503 | 0.0258 | 1.80 | 0.84 | 2.19 % | 0.257 | REJECTED |
| `s25_return_on_equity` | quality | 501 | 0.0206 | 1.58 | −0.10 | −0.61 % | 0.356 | REJECTED |
| `s25_gross_margin_level` | quality | 293 | 0.0240 | 1.44 | 0.72 | 2.43 % | 0.410 | REJECTED |
| `s25_cash_return_on_assets` | quality | 552 | 0.0204 | 1.40 | 0.38 | 1.16 % | 0.410 | REJECTED |
| `s25_net_operating_assets` | balance sheet | 247 | 0.0190 | 1.34 | 1.91 | 6.12 % | 0.420 | REJECTED |
| `s25_external_financing` | investment | 552 | 0.0141 | 1.30 | 0.72 | 1.10 % | 0.420 | REJECTED |
| `s25_rnd_growth` | innovation | 206 | 0.0225 | 1.20 | 1.50 | 5.65 % | 0.460 | REJECTED |
| `s25_capex_intensity` | investment | 488 | 0.0157 | 1.15 | 0.76 | 2.14 % | 0.467 | REJECTED |
| `s25_rnd_disclosure_indicator` | innovation | 554 | 0.0146 | 1.06 | 0.99 | 1.97 % | 0.467 | REJECTED |
| `s25_operating_margin` | quality | 371 | 0.0118 | 0.71 | −0.81 | −3.31 % | 0.661 | REJECTED |
| `s25_sga_efficiency` | operating | 375 | 0.0070 | 0.48 | 0.45 | 1.45 % | 0.800 | REJECTED |
| `s25_ppe_growth` | investment | 482 | 0.0037 | 0.25 | 0.64 | 1.17 % | 0.898 | REJECTED |
| `s25_capex_growth` | investment | 486 | −0.0007 | −0.06 | 0.67 | 1.23 % | 0.952 | REJECTED |
| `s25_intangible_intensity` | balance sheet | 502 | −0.0017 | −0.14 | 1.39 | 2.81 % | 0.923 | REJECTED |
| `s25_leverage_change` | balance sheet | 399 | −0.0017 | −0.21 | −2.04 | −3.72 % | 0.898 | REJECTED |
| `s25_working_capital_accruals` | balance sheet | 442 | −0.0020 | −0.22 | −1.59 | −4.10 % | 0.898 | REJECTED |
| `s25_asset_turnover_change` | operating | 467 | −0.0036 | −0.34 | −0.80 | −2.14 % | 0.898 | REJECTED |
| `s25_earnings_quality_gap` | cash flow | 464 | −0.0070 | −0.68 | 0.79 | 1.45 % | 0.661 | REJECTED |
| `s25_cash_conversion` | cash flow | 402 | −0.0097 | −1.00 | −0.08 | −0.53 % | 0.467 | REJECTED |
| `s25_inventory_growth` | balance sheet | 327 | −0.0125 | −1.01 | −0.70 | −2.42 % | 0.467 | REJECTED |
| `s25_tax_burden_change` | operating | 442 | −0.0065 | −1.10 | 0.41 | 0.22 % | 0.467 | REJECTED |
| `s25_sbc_intensity` | innovation | 435 | −0.0319 | −1.79 | −2.18 | −9.26 % | 0.257 | REJECTED |
| `s25_receivables_growth` | balance sheet | 333 | −0.0280 | −2.05 | −2.27 | −7.95 % | 0.257 | REJECTED |
| `s25_rnd_efficiency` | innovation | 186 | −0.0536 | −2.68 | −3.11 | −15.3 % | 0.103 | REJECTED |

**1 of 28 cleared the released gate; 1 of 28 survived FDR.** All 28 — including
the 27 nulls — are registered through the released candidate lifecycle
(registry went 70 → 98 candidates), so none can be rediscovered.

> A note on `s25_rnd_efficiency`: it survives FDR *within its own economic
> family* on a two-sided p-value, because a strong wrong-signed result is still
> statistically strong. The gate rejects it on the pre-registered sign. A
> two-sided survivor with the wrong sign is a **rejection**, not a discovery.

---

## 5. The one that survived: operating profitability

`s25_operating_profitability = (GrossProfit − SG&A) / Assets`, expected sign +1.

Novy-Marx's gross profitability ignores what a firm spends to convert gross
profit into a going concern; subtracting SG&A isolates the operating surplus
actually available to shareholders (Ball–Gerakos–Linnainmaa). Stage 24 measured
*asset-scaled gross profitability* and rejected it (IC 0.0197, t 1.29). The
refinement is a different economic claim and it behaves completely differently.

**It faced the identical battery R&D faced. It failed nothing.**
Verdict: **`SURVIVES_SECTOR_AND_STYLE_CONTROLS`**.

| control | rank IC | IC t | spread t |
|---|---|---|---|
| raw | 0.0550 | 3.63 | 2.15 |
| sector-neutral **Tier A** | 0.0564 | **3.77** | 2.41 |
| sector-neutral **Tier B** | 0.0447 | **3.33** | 1.87 |
| remove Technology | 0.0589 | 3.78 | 2.30 |
| remove ConsumerDiscretionary | 0.0442 | 2.93 | 1.76 |
| **remove Tech + ConsumerDiscretionary** | 0.0446 | **2.88** | 1.64 |
| size / liquidity neutral | 0.0538 | 3.82 | 98 % retained |
| volatility neutral | 0.0582 | 3.85 | 106 % retained |
| beta neutral | 0.0527 | 3.73 | 99 % retained |
| joint size+vol+beta | 0.0493 | **3.76** | — |
| **drop top 5 winners / period** | 0.0552 | **3.75** | 3.46 |
| first / second half | 0.0551 / 0.0549 | 2.60 / 2.50 | — |
| pre-2016 / post-2016 | 0.0422 / 0.0620 | 1.64 / 3.29 | — |
| pre-COVID / post-COVID | 0.0610 / 0.0460 | 3.43 / 1.69 | — |

The winner-removal row is the striking one: dropping the five best realised
outcomes per period makes it **stronger** (t 3.63 → 3.75, spread t 2.15 → 3.46).
This is the opposite of R&D intensity and the opposite of a lottery factor.

**Disclosure selection:** SG&A is tagged by **77.6 %** of the panel (versus
36.6 % for R&D) — Technology 99.4 %, Materials 98.6 %, Financials 50.5 %,
Utilities 23.3 %. The reporters-minus-non-reporters spread is **IC −0.0032,
t −0.26**: being an SG&A reporter does not pay either. (This diagnostic was
computed *after* the campaign and is explicitly labelled
`POST_CAMPAIGN_DIAGNOSTIC_NOT_IN_FDR_FAMILY` — it is not smuggled into the
multiple-testing family.)

**Alternative constructions are all weak** — gross margin level t 1.44,
operating margin t 0.71, SG&A efficiency t 0.48. The specific
`(GP − SG&A)/Assets` construction is doing the work, which is a caution as much
as a comfort: it means the result does not generalise across the obvious
neighbouring definitions.

**Orthogonality — `INDEPENDENT_ALPHA`:**

| vs baseline | cross-sec. rank corr | partial rank IC t |
|---|---|---|
| `composite_sn_pit` | 0.504 | **3.26** |
| `mom_6_1` | 0.068 | **3.71** |

**Horizon behaviour — `STRENGTHENS_WITH_HORIZON`:** IC t 3.44 at 1 month, 3.63
at 3 months, 3.10 at 6 months (each horizon formed at a stride that keeps its
forward windows non-overlapping). It is not a fast-decaying signal and does not
need high turnover.

---

## 6. Ensembles — and the comparison that changes the answer

A blend requires every name to carry every component, so **adding a
sparsely-reported signal narrows the cross-section**, and a narrower,
better-covered universe can look stronger for reasons that have nothing to do
with the added signal. Every structure is therefore compared **twice**: against
the operational shape on its own full universe, and against the operational
shape scored on the **challenger's own names and dates**. The matched comparison
is the one that decides.

| structure | names | rank IC | IC t | spread t | net25 | turnover | gate |
|---|---|---|---|---|---|---|---|
| `operational_shape_5050` (= the live model's shape) | 488 | 0.0322 | 1.83 | 2.09 | 6.70 % | 0.48 | REJECTED |
| `fundamental_tilted_2to1` | 488 | 0.0296 | 2.02 | 2.74 | 7.32 % | 0.37 | KEEP |
| `operational_plus_s25_operating_profitability` | 300 | 0.0578 | 3.42 | 2.77 | 8.81 % | 0.39 | KEEP |
| `fundamental_plus_op_prof_no_momentum` | 300 | 0.0468 | 3.57 | 2.80 | 8.09 % | **0.14** | KEEP |
| `momentum_plus_op_prof_no_fundamental` | 307 | 0.0589 | 3.28 | 3.56 | 12.5 % | 0.46 | KEEP |
| *(reference)* `operational + s24_rnd_intensity` | 202 | 0.0571 | 2.72 | 3.18 | 14.1 % | 0.39 | KEEP |
| *(reference)* `operational + op_prof + rnd_intensity` | 174 | 0.0642 | 3.17 | 4.04 | 17.5 % | 0.33 | KEEP |

**On the matched universe**, the operational shape is *already* much better than
its full-universe reading — IC 0.0422 at t 2.37 on the 300 SG&A-reporter names,
versus 0.0322 at t 1.83 on all 488. **Restricting to SG&A reporters is itself
worth roughly +0.010 IC.** That is a coverage effect, not alpha, and the naive
comparison would have credited it to the new signal.

Genuine incremental contribution, matched:

| structure | Δ rank IC | Δ IC t | Δ spread t | Δ net25 | Δ turnover |
|---|---|---|---|---|---|
| `operational + op_prof` | +0.0157 | +1.05 | +0.44 | **+0.57 pp** | −0.09 |
| `fundamental + op_prof` (no momentum) | +0.0046 | +1.19 | +0.48 | −0.16 pp | **−0.34** |
| `momentum + op_prof` (no fundamental) | +0.0167 | +0.90 | +1.23 | +4.25 pp | −0.03 |

The best structure that beats the matched incumbent on both IC t and net return
is **`operational_plus_s25_operating_profitability`**. The no-momentum variant
buys a large turnover reduction (0.48 → 0.14) for a small return give-up, which
is the more interesting trade for a real book but does not clear the
both-must-improve rule.

The reference rows exist so "would the Stage-24 shape have been better?" is
answered rather than assumed. They are `reference_only` and can never be
reported as a challenger — a regression enforces it.

---

## 7. Challenger status

**`s25_operating_profitability` is a RESEARCH CHALLENGER.**

It is the only candidate that clears all four pre-stated conditions:

1. cleared the **released** evidence gate on survivorship-safe point-in-time
   evidence;
2. survived Benjamini-Hochberg over the whole 28-hypothesis family (q = 0.008);
3. classified `INDEPENDENT_ALPHA`, not a baseline restatement or a style/sector
   proxy;
4. constructed with **no** look-ahead input — no classification tier enters any
   registered signal.

It additionally passed the full falsification battery, and a candidate whose own
battery returns a damning verdict is demoted automatically, so the bar cannot
drift between stages.

**This is a research state, not a promotion.** `fundamental_momentum_50_50_v1`
is unchanged. No champion moved, no shadow book was created, no weights were
touched. `api.universe_scoring.AUTOMATIC_PROMOTION_ALLOWED` remains `False` and
Stage 25 never references it.

Everything else is `NOT_A_CHALLENGER` with its blockers recorded per candidate.
`s24_rnd_intensity` is explicitly not a challenger: verdict
`CONCENTRATION_FRAGILE`.

---

## 8. Forward tracking

Eligible for research forward tracking: **`s25_operating_profitability`**.

Stage 25 **starts nothing**. Activating a shadow book changes tournament state
and is a governance decision owned by
`tournament.maybe_activate_shadow_books`; the stage reports eligibility and
leaves the action to the gate owner. `shadow_books` remains at 0 rows and no
TRUE_FORWARD evidence was written or altered.

What forward tracking would measure: realised rank IC on cross-sections formed
*after* the evidence date; realised long/short spread net of modelled cost; and
agreement/divergence with the operational model's ranking of the names actually
held.

---

## 9. Capital-deployment relevance

The canonical seam between a model and real decisions remains
`stage23_unified.build_decision_link`, which Stage 25 **calls** rather than
duplicating. Its status is unchanged: **`INSUFFICIENT_FORWARD_EVIDENCE`**
(minimum 12 matured live observations). That is the honest answer about real
decisions and it is not dressed up.

What *is* computable, labelled `COUNTERFACTUAL_NOT_PROOF`: among the names the
operational-shaped model ranked in its top decile, did the challenger score the
eventual losers lower than the eventual winners?

**`s25_operating_profitability`: 65 formations, mean score gap −0.0046,
t = −0.34, losers ranked lower in 47.7 % of formations.**

That is a **null**. The challenger carries independent cross-sectional
information, but there is no evidence it would have spotted deterioration in the
holdings the incumbent actually chose. It is reported as a null rather than
omitted. No historical decision was rewritten.

---

## 10. Alpha family exhaustion

22 families classified. **Do not re-open** (12 exhausted-negative, 3
rejected-robustness, 2 rejected-PIT, 1 redundant):

`residual_momentum` · `low_volatility` · `vol_scaled_momentum` ·
`monthly_liquidity` · `fundamental_momentum_cfo_change` · `asset_growth` ·
`sales_growth` · `gross_profitability_asset_scaled` ·
`eodhd_current_snapshot_fundamentals` · `price_factor_expansion` ·
`analyst_grades_fmp` · `macro_cross_sectional_beta` · `rnd_intensity` ·
**and five of the six Stage-25 economic families**:
`balance_sheet_quality` (0/7), `cash_flow_quality` (0/3),
`innovation_intangibles` (0/5), `investment_and_payout` (0/5),
`operating_improvement` (0/3).

**Still active:** `quality_profitability` (1 of 5 cleared gate *and* FDR).

**Waiting for new data:** `historical_analyst_revisions`,
`pit_valuation_ratios`, `pit_fine_grained_sector`.

Deduplication stays owned by `tournament.CandidateRegistry` (spec_hash +
`processed_experiments`). No second exhaustion registry was created.

---

## 11. Data capability map

17 families, measured from what is actually on disk.

| state | families |
|---|---|
| **READY_FOR_PIT_RESEARCH** (8) | historical prices · historical membership · SEC company facts PIT · PIT filing availability · **PIT trailing beta (new)** · PIT size/liquidity · PIT volatility · tournament registry |
| **READY_WITH_LIMITATIONS** (3) | PIT sector history (two-tier) · delisted/inactive identity · owned daily OHLC |
| **FORWARD_ONLY** (3) | analyst current snapshots · forward evidence · HOC/reassessment outcomes |
| **WAITING_FOR_DATA** (2) | historical analyst revisions · **PIT market cap** |
| **INVALID_FOR_HISTORICAL_RESEARCH** (1) | pre-2009 fundamentals (XBRL begins 2009-04) |

### New capability: point-in-time trailing beta

Stage 24 could neutralise size and volatility but not market beta. It never
needed new data: the momentum panel's own realised returns suffice. The timing
rule is exact — a panel row at month *m* carries `fwd_1m_return`, the return
over *m → m+1*, so the return **realised over** month *m* is the value recorded
at *m−1*. A 36-month window ending at the formation month therefore uses only
returns the market had already printed. Minimum 24 observations; a name with
fewer gets `None`, **never a default of 1.0**.

### PIT market cap is blocked by TWO independent gaps

Stage 24 named one. There are two, and both are real:

1. the owned companyfacts parser emits **monetary USD facts only**, so share
   counts are dropped; **and**
2. the only owned daily price surface (`phase25_fast_ohlc`, Norgate Russell 1000
   Current & Past, 3,076 securities, 2000-01-03 → 2026-07-17, PIT membership
   mask) is **TOTALRETURN adjusted** — so `price_adjusted × shares_as_reported`
   is wrong by the cumulative split-and-dividend factor.

Fixing only the parser would produce a *plausible but wrong* market cap. Both
must be fixed. Every valuation ratio stays unrunnable.

Stage 25 consequently takes **every** forward return from the one monthly panel,
introducing no cross-source join risk.

---

## 12. Accounting surface expansion

The Phase-9.3 released map, extended by Stage 24, extended again by Stage 25 —
**never shadowing** either (a regression fails the build if a Stage-25 key would
shadow a released or Stage-24 key).

* **21 new concepts, 39 total, 59 us-gaap tags.**
* New index `sec_companyfacts_stage25.sqlite`: **3,008,200 facts, 858 CIKs**,
  built **offline in 91 seconds** from the already-owned `companyfacts.zip`.
  Facts loaded into the panel after the fiscal-year duration filter: 1,808,675
  across 847 CIKs, 2009-04-15 → 2026-07-31.
* The Stage-24 and Phase-9.5 indexes are opened by nothing here and never
  written.

**One genuine improvement to the reader.** Stage 24 partitions concepts into
hard-coded FLOW and STOCK sets at module scope. That is correct for its own
concepts but does not generalise: every new concept must be hand-classified, and
a mistake is silent — a duration fact treated as an instant is compared against
a different accounting period. Stage 25 reads the period kind **from the fact
itself**: a duration carries a `period_start`, an instant does not. That is
intrinsic to XBRL, needs no table, and a regression proves it reproduces the
Stage-24 partition exactly on the Stage-24 concepts.

---

## 13. Research gate integrity

**The Stage-24 drawdown defect reproduces exactly on Stage-25's own evidence.**
Of 31 audited long/short series, **three report an impossible "percentage"**:

| series | periods | V1 as reported | V2 (fraction of capital) |
|---|---|---|---|
| `s25_receivables_growth` | 65 | **−140.2 %** | −78.7 % |
| `s25_rnd_efficiency` | 65 | **−249.3 %** | −94.5 % |
| `s25_sbc_intensity` | 65 | **−164.8 %** | −84.9 % |

No portfolio can lose 249 % of itself. All three are wrong-signed factors, whose
consistently negative spread makes the unnormalised cumulative sum fall
monotonically — which is precisely the unbounded behaviour Stage 24 identified.

The **active contract stays V1**. Which drawdown a gate consumes is a model
governance decision; activating V2 would retro-actively re-judge every candidate
ever evaluated. The threshold was not lowered and no historical evidence was
rewritten.

Four further semantics were audited because a multi-horizon, multi-cadence stage
newly puts them at risk. **No defect found in any of them:**

* **annualisation** — each horizon is scored with *its own* `horizon_days`, so
  the annualised numbers are comparable across the family;
* **overlapping forward windows** — removed *by construction* (quarterly
  formation for 1m and 3m, semi-annual for 6m) rather than corrected afterwards;
* **duplicated formation observations** — 0 detected (pseudo-replication would
  inflate every t-statistic);
* **factor missingness** — absent, never zero-filled, with the four-state
  taxonomy above.

---

## 14. Steele / Intrinio

**No historical analyst data has arrived.** State stays `WAITING_FOR_DATA`. No
paid API was called, no quota spent, no provider schema invented.

What changed because of Stage 25: an analyst signal will now be judged on what
it adds to a baseline that is itself survivorship-safe and point-in-time, across
28 additional pre-registered fundamental hypotheses whose nulls are already
registered — so the families it would be redundant with are **known** rather
than assumed.

The pipeline that runs automatically on arrival is unchanged and lives in
`alpha_agent/analyst_revisions.py`: importer → `pit_scan` → adequacy gate → six
frozen Stage-13A hypotheses under BH-FDR → the incremental test against the
current model on the same cross-sections → the same tournament lifecycle, still
with no automatic promotion. No Intrinio-only framework exists.

---

## 15. External data purchase gate

**Headline: TAKE THE FREE SEC ACQUISITION FIRST; WAIT ON EVERY PAID DATASET.**
Nothing is authorised — this stage recommends only.

| dataset | recommendation | why |
|---|---|---|
| **SEC Financial Statement Data Sets `sub.txt`** | **ACQUIRE_FREE_BEFORE_ANY_PURCHASE** | free, no vendor, no quota; converts the Tier-B look-ahead control into a leakage-safe one, unblocks a FULL `composite_sn` reconstruction, and turns every provisional sector verdict here into a conclusive one |
| Historical analyst revisions (Intrinio) | **WAIT** | owned research is not exhausted (`quality_profitability` is active), the free artefact resolves the largest interpretive blocker at zero cost, and a prior live trial already returned `NO_DEFENSIBLE_ALPHA` / `DO_NOT_BUY` on a survivorship-safe 16-year test |
| Steele / other fundamental vendor history | **REJECT** | it would restate the same accounting information already read point-in-time from SEC; high duplication risk, and Stage 25 rejected most of the accounting families it would extend |

Decision rule, applied rather than asserted: a paid dataset is recommendable only
when (a) the owned surface is exhausted for the hypotheses it would unlock,
(b) no **free** artefact would unlock them first, and (c) no prior evaluation of
that same vendor already returned a negative result.

---

## 16. Autonomous research queue

The existing agent (`alpha_agent.autonomous_research` + `alpha_agent.tournament`)
reads this. **No second agent, no second queue.** The agent may prioritise and
launch bounded campaigns; it may **not** promote models or change holdings.

1. **HIGH** — acquire SEC `sub.txt` (free) and build the leakage-safe per-filing
   SIC series. Consumer already implemented: `pit_sector.PitSicSeries`.
2. **HIGH** — research-forward-track `s25_operating_profitability`.
3. **MEDIUM** — extend the PIT concept allowlist to share counts **and** acquire
   an unadjusted price surface (both needed for market cap).
4. **MEDIUM** — work the identity backlog; every unresolved symbol is a name
   silently absent from every cross-section.
5. **WAITING** — historical analyst revision vintages.
6. **LOW** — do not re-open the 18 closed families listed in §10.

---

## 17. Machine-readable outputs

Under `D:\Stock_Prediction_app_data\stage25_autonomous_alpha_discovery\runs\stage25_f811c142f7dbd7e0\`
with `latest.json` pointing at the newest run. Run directories are
content-addressed: identical inputs reproduce an identical `run_id`, and a
regression asserts byte-identical artefact hashes across two runs.

`research_capability_map` · `pit_sector_history_summary` · `rd_falsification` ·
`pit_fundamental_expansion` · `hypothesis_manifest` · `experiment_results` ·
`alpha_family_exhaustion` · `orthogonality_matrix` ·
`incremental_alpha_matrix` · `ensemble_results` · `challenger_results` ·
`forward_tracking_status` · `hoc_counterfactual_results` ·
`autonomous_research_queue` · `intrinio_status` ·
`external_data_purchase_gate` · `research_gate_integrity` · `stage25_summary`

---

## 18. Safety

Research-only and read-only with respect to every operational store. No orders,
fills, signals, trade decisions, proposals, rebalance plans, Daily Close, model
promotion or champion replacement. No network, no provider call, no PostgreSQL,
no prediction service. The backend was never restarted and the port-8001
listener PID was unchanged throughout.

Writes are confined to the Stage-25 research root, the Stage-25 SEC index, and
the **existing** research candidate registry — which is the canonical research
lifecycle, not a new store.

---

## 19. Exact continuation point

The next stage begins here:

1. **Download SEC Financial Statement Data Sets** (2009Q2 → current), parse
   `sub.txt` into `(cik, sic, accepted)` observations, feed
   `pit_sector.PitSicSeries`, and **re-run this stage's falsification battery**
   with Tier B replaced by a leakage-safe fine-grained tier. That single step
   promotes every "provisionally cleared" verdict in §3 and §5 to conclusive,
   and finally allows a FULL `composite_sn` reconstruction including its
   within-sector step.
2. **Decide the shadow book** for `s25_operating_profitability` (governance, not
   research).
3. **Do not** run another single-factor accounting campaign. Five of six
   economic families are closed with evidence. The remaining owned-data upside
   is valuation ratios, and those need the two market-cap fixes in §11.
