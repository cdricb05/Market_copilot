# Alpha Exhaustion Campaign — the continuation document

**Status:** the free/owned alpha frontier is exhausted. `EXECUTABLE_FREE_OWNED_HIGH_PRIORITY_FAMILIES = 0`.
**Branch:** `stage19-controlled-rebalance` · **Base HEAD:** `c2a63c5` (Stage 26)
**Owner module:** [alpha_agent/stage27_alpha_exhaustion.py](../alpha_agent/stage27_alpha_exhaustion.py)
**New reader:** [alpha_agent/sec_filing_behavior.py](../alpha_agent/sec_filing_behavior.py)
**CLI:** [scripts/run_alpha_exhaustion_campaign.py](../scripts/run_alpha_exhaustion_campaign.py) · [scripts/acquire_sec_quarterly_dataset.py](../scripts/acquire_sec_quarterly_dataset.py)
**Tests:** [tests/test_alpha_exhaustion_campaign.py](../tests/test_alpha_exhaustion_campaign.py) — 57 hermetic
**Research root:** `D:\Stock_Prediction_app_data\alpha_exhaustion_campaign`

This document is the single place a new session should start. It replaces the
need to reconstruct Stages 23–27 from their individual stage documents.

---

## 1. Where the alpha actually stands

| | |
|---|---|
| **Operational champion** | `fundamental_momentum_50_50_v1` — **UNCHANGED**, and nothing here proposes changing it |
| **Frozen forward challenger** | `s25_operating_profitability = (GrossProfit − SG&A) / Assets` |
| **Its spec hash** | `67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e` |
| **Its shadow book** | `sb_c9_qualityprofi_e490533606`, inception 2026-08-16, 100 positions, **0 forward marks** |
| **Retained alpha found since** | **none** |
| **Binding constraint** | **`FORWARD_TIME`** |

Do we have historically validated alpha? **Yes, one signal, and its historical
case is as strong as history can make it.** `s25_operating_profitability` has
rank IC 0.0550 at t 3.63, BH q 0.008 over a 28-hypothesis family, net Top-25
7.14 % at 25 bps on turnover 0.10, and it survived a falsification battery that
killed its nearest rival. It is a **capital-efficiency** signal, not a cheapness
one — Stage 26 proved that by swapping the denominator: the identical operating
profit scaled by market equity instead of assets produces a nearly unrelated
signal (rank correlation 0.224) that does not work (t −0.67).

What it has never had is an out-of-sample day. That is not a weakness of the
historical evidence; it is a different evidence dimension, and only calendar
time produces it.

**Did anything stronger emerge in Release 27? No.** Eight economically distinct
information families, 49 pre-registered hypotheses, zero survivors.

---

## 2. Every economically distinct family tested to date

### 2.1 Closed before Release 27

| family | verdict | evidence |
|---|---|---|
| Stage-25 PIT fundamentals — quality, cash-flow quality, balance sheet, investment/payout, operating improvement, innovation | **TESTED_AND_CHALLENGER** | 28 hypotheses; 1 survivor (`s25_operating_profitability`) |
| R&D intensity | `CONCENTRATION_FRAGILE` | survived 17 attacks, failed 1: dropping the 5 best names per period left t 1.89 vs a bar of 2.0. **Do not rescue it.** |
| PIT valuation ratios | **TESTED_AND_REJECTED** | Stage 26: 13 hypotheses, 0 cleared, 0 survived FDR; best absolute t 1.74 and **wrong-signed** |
| Residual momentum | `EXHAUSTED_NEGATIVE` | prior exhaustion memory |
| Macro cross-sectional beta (ALFRED vintages) | **TESTED_AND_REJECTED** | Stage 15: `NO_DEFENSIBLE_ALPHA`, 0 FDR survivors; Phase 10-O separately rejected regime gating as overfit |
| Analyst estimate revisions | **REQUIRES_PAID_DATA** | live Intrinio trial → `NO_DEFENSIBLE_ALPHA` / `DO_NOT_BUY`; Stage 13C OOS did not replicate (t −0.29) |
| Short interest | **TESTED_AND_REJECTED** | Phase 10-A purchased data FAILED at t 1.56; the free path is closed (FINRA bulk → HTTP 403, re-probed 2026-08-16) |

### 2.2 Executed by Release 27

All eight on the **same** 66-formation, 34,652-row survivorship-safe panel, with
signs fixed in source before any number existed, Benjamini-Hochberg within each
family, and a second, strictly harsher campaign-wide correction over all 49.

| family | hypotheses | verdict |
|---|---|---|
| Filing timing / reporting promptness | 10 | **TESTED_AND_REJECTED** |
| Restatements, amendments, reporting corrections | 11 | **TESTED_AND_REJECTED** |
| Share-count dynamics / capital allocation | 7 | **TESTED_AND_REJECTED** |
| Disclosure structure and complexity | 3 | **TESTED_AND_REJECTED** |
| Dividend policy events | 3 | **TESTED_AND_REJECTED** |
| Corporate-action (split) behaviour | 3 | **TESTED_AND_REJECTED** |
| Insider transactions (Forms 3/4/5) | 6 | **TESTED_AND_REJECTED** |
| Filing-stream events (NT / shelf / 13D / 8-K) | 6 | **TESTED_AND_REJECTED** |

**Panel integrity.** Every Stage-25/26 baseline reproduces to the digit on the
enriched panel — `composite_sn_pit` 0.0231 / t 2.03, `mom_6_1` 0.0254 / t 1.22,
`ensemble_pit_5050` 0.0322 / t 1.83, `s25_operating_profitability` 0.0550 /
t 3.63, and the same 34,652 scored rows. The Release-27 enrichment perturbed
nothing, which is exactly why an incremental claim against those baselines is
admissible.

---

## 3. The two results worth remembering

Nothing survived, but two things are worth not re-deriving.

### 3.1 The IC/spread split in the correction family

Four amendment hypotheses produced a **statistically strong rank IC and no
tradable spread**:

| hypothesis | rank IC | IC t | BH q | spread t | net Top-25 | gate |
|---|---|---|---|---|---|---|
| `r27_repeat_amender` | 0.0183 | **3.18** | **0.006** | 0.30 | 0.4 % | `REJECT_WEAK_SPREAD_T` |
| `r27_amendment_count_3y` | 0.0183 | 3.17 | 0.006 | 0.28 | 0.3 % | `REJECT_WEAK_SPREAD_T` |
| `r27_amendment_intensity` | 0.0184 | 3.17 | 0.006 | 0.68 | 0.7 % | `REJECT_WEAK_SPREAD_T` |
| `r27_annual_amendment_3y` | 0.0154 | 2.75 | 0.016 | −0.78 | −1.3 % | `REJECT_WEAK_SPREAD_T` |

Amendment history genuinely ranks the cross-section, and the ranking carries no
money at the decile. That is a coherent picture for a sparse indicator: the
information sits in a small group of names that the decile construction cannot
isolate. It is a rejection under the released gate, and it is not a reason to
build a different portfolio construction to chase it.

### 3.2 Three pre-registered signs were wrong, and stayed wrong

| hypothesis | rank IC on the pre-registered orientation | IC t | reading |
|---|---|---|---|
| `r27_reverse_split_1y` | −0.0473 | **−6.32** | reverse-splitters **outperformed** |
| `r27_insider_cluster_buy` | −0.0237 | **−3.22** | clustered insider **buying** preceded **under**performance |
| `r27_split_magnitude_1y` | −0.0186 | −2.79 | the continuous version agrees |

No sign was flipped. All three are recorded as rejections.

The insider result is the notable one, and the data is not in doubt. A parse
validation independent of anything being tested is stored with the run: across
473,841 open-market events the **buy share** averages 7.9 %, and its three
highest quarters are **2009Q1 (28.4 %)**, **2011Q3 (21.0 %)** and **2020Q1
(16.2 %)** — the three deepest drawdowns in the sample, exactly where insiders
are known to buy. If the transaction codes, direction flags or filing dates had
been mis-parsed, that ordering could not appear. The parse is right; the signal
is simply contrarian in a large-cap universe, and over 2010–2026 that direction
lost. `r27_reverse_split_1y` is additionally marked `INSUFFICIENT_SAMPLE` —
reverse splits are far too rare here for the decile machinery — so the family's
verdict rests on the adequately-sampled continuous member.

---

## 4. The near-misses — and why they are rejections

Two hypotheses cleared the released evidence gate. **Neither survived
Benjamini-Hochberg within its own pre-registered family**, so neither is
retained. They are recorded here precisely so nobody re-derives them and
mistakes them for a discovery.

| hypothesis | rank IC | IC t | spread t | net Top-25 | turnover | BH q | why rejected |
|---|---|---|---|---|---|---|---|
| `r27_annual_filing_lag` | 0.0152 | 2.10 | 2.40 | 3.74 % | 0.11 | **0.120** | failed family FDR |
| `r27_buyback_persistence` | 0.0278 | 2.27 | 2.11 | 3.79 % | 0.17 | **0.137** | failed family FDR |

Eight hypotheses survived the **campaign-wide** correction and failed the gate.
**No hypothesis did both.** The incrementality workstream therefore had nothing
to judge, and no ensemble was offered a new component — the offer rule is that
only a gate-clearing, FDR-surviving, `INDEPENDENT_ALPHA` candidate is ever
admitted, and ensemble performance never decides who is offered.

---

## 5. Free data acquired (cost: USD 0.00)

| source | quarters | content | on disk | over the wire |
|---|---|---|---|---|
| SEC **Insider Transactions Data Sets** (Forms 3/4/5, DERA) | 69 (2009Q1→2026Q1) | 473,841 open-market P/S events, 847 issuers | 1.53 GB | 337 MB |
| SEC **EDGAR quarterly full index** | 71 (2009Q1→2026Q3) | every filing of every form, by CIK and date | 1.68 GB | 235 MB |

Both are first-party US federal government works, free for research use under
SEC fair-access rules: identifying User-Agent with a contact address,
inter-request delay, every member content-hashed and cached so a re-run
re-downloads nothing. Both were **range-read** — only the members a family
actually reads were transferred.

Cached under `D:\Stock_Prediction_app_data\alpha_agent\identity\sec_bulk\`.

---

## 6. Three point-in-time rules that made these families admissible

These are the traps, and they are enforced in
[`sec_filing_behavior.py`](../alpha_agent/sec_filing_behavior.py) rather than
remembered.

1. **`prevrpt` is a look-ahead flag and is refused as a signal input.** The
   column means "this submission was amended before the end cutoff of the data
   set it appears in", which is written *retroactively* — a filing accepted
   early in a quarter carries up to three months of knowledge nobody had on its
   acceptance date. An amendment enters only as its own submission, at its own
   acceptance timestamp. A regression asserts no registered hypothesis
   references it.

2. **A fact revision needs the reporting DURATION in its context key.** The same
   `period_end` legitimately carries both a three-month and a twelve-month
   figure for a flow concept; grouping without `period_start` reports a
   fourth-quarter revenue against a full-year revenue as a 300 % restatement.
   The duration-blind counterfactual is measured on the same pass and published.
   Restating the same number in a later comparative column is **not** a
   restatement — only a materially changed value is, stamped at the *later*
   accession's filed date.

3. **An insider trade is observable at its Form 4 FILING date, never its
   transaction date.** Keying on `TRANS_DATE` would let the research see a trade
   up to two business days before the market could.

A fourth, in the share family: **a split is not issuance.** Share counts are
normalised by the cumulative capital-event factor, so a 2-for-1 split doubles
both the reported count and the factor and cancels exactly, leaving real
dilution and real buyback. Two endpoints resolving to the *same* filing return
`None`, never 0 % — reporting stability that was never observed would be a
fabricated observation.

---

## 7. What the released winsorizer does to a sparse indicator

`Stage25Panel.factor_cross_sections` winsorizes every factor at 1 %. That is
correct for a continuous variable and destructive for a 0/1 indicator whose
positive rate is below 1 %: the winsorizer clips the ones to the modal zero, the
cross-section becomes constant, Spearman is undefined and the period is silently
dropped. Three hypotheses hit this, and `r27_forward_split_1y` scored **zero
periods** because of it:

| hypothesis | months the event appeared in | raw positive rate |
|---|---|---|
| `r27_forward_split_1y` | 66 / 66 | 0.18 % |
| `r27_late_filing_notification` | all | below 1 % |
| `r27_low_detail_tagging` | all | below 1 % |

The released winsorizer was **not** changed — re-tuning a released statistic to
make a hypothesis measurable is exactly the move this programme forbids. What
was added is a pre-winsorization breadth diagnostic, so a zero-period result now
reads as "the event is present in 66 months but is rarer than the released
transform can carry" instead of "no data". All three are classified
`INSUFFICIENT_SAMPLE`, not rejected — claiming a rejection would claim evidence
that was never produced.

Every sparse indicator in this campaign was registered alongside a continuous
companion for exactly this reason, and in every case the companion **was**
measured: `r27_split_magnitude_1y` for the split family (t −2.79, wrong-signed),
`r27_annual_filing_lag` and its relatives for lateness, `r27_coregistrant_complexity`
for disclosure structure. No family's verdict rests on a hypothesis the
winsorizer erased.

---

## 8. The frontier audit — the hard contract

21 economically distinct families have now been considered. Each is assessed
against seven questions: is the information available, is it point-in-time safe,
is survivorship acceptable, is the sample adequate, is it economically distinct,
has it already been answered, and — if not — can it be run now.

**`EXECUTABLE_FREE_OWNED_HIGH_PRIORITY_FAMILIES = 0`.**

Everything that remains is blocked by something no amount of work can shortcut:

| family | state | blocker |
|---|---|---|
| Forward challenger out-of-sample | `REQUIRES_FORWARD_TIME` | a mark may only exist for a date strictly after inception |
| News / RSS / GDELT sentiment | `REQUIRES_FORWARD_TIME` | the collectors are forward-only; there is no historical cross-section and no delisted coverage |
| Auditor identity and changes | `INSUFFICIENT_SAMPLE` | `dei:AuditorName` mandated only from FY2021 — 4 annual cross-sections against a minimum of 12 |
| Analyst estimate revisions | `REQUIRES_PAID_DATA` | needs as-was vintages; prior live trial returned `DO_NOT_BUY` |
| Earnings-announcement surprise | `REQUIRES_PAID_DATA` | a surprise needs an expectation; the timing half is free and was tested here |
| Operational promotion decision | `MANUAL_GOVERNANCE_REQUIRED` | `AUTOMATIC_PROMOTION_ALLOWED` is `False`; a champion change is a human decision |
| Short interest | `TESTED_AND_REJECTED` | prior test failed; free path returns HTTP 403 |
| Filer-status transitions | `REDUNDANT_WITH_EXISTING_INFORMATION` | mechanically a function of public float, i.e. size — already rejected; 93.7 % of submissions carry one status |
| Debt issuance / repayment | `REDUNDANT_WITH_EXISTING_INFORMATION` | Stage 25 rejected both external financing and leverage change |
| Macro regime conditioning | `TESTED_AND_REJECTED` | Stage 15 / Phase 10-O |

Every terminal state carries a `reopen_if` condition in
`research_exhaustion_state.json`. An exhaustion record without one is just a
wall; with one, a later session knows exactly what new information would justify
re-opening it. The condition is never "the test would be easy now".

---

## 9. Paid data — **REJECT**, and for the first time the gate is not the reason

The released rule: a paid dataset is recommendable only when (a) the owned
surface is exhausted for the hypotheses it would unlock, (b) no free artefact
would unlock them first, and (c) no prior evaluation of that vendor already
returned a negative result.

**Condition (a) is now satisfied for the first time in this programme.** And the
gate still authorises nothing:

| dataset | recommendation | why |
|---|---|---|
| Historical analyst consensus revision vintages | **REJECT** | fails (c) outright: a live trial already returned `NO_DEFENSIBLE_ALPHA` / `DO_NOT_BUY` on a survivorship-safe 16-year test, and Stage 13C's OOS confirmation did not replicate. It has been the standing number-one candidate for four stages on the strength of its *mechanism*; the evidence says the mechanism did not pay here. |
| Vendor-normalised insider transaction panel | **REJECT** | superseded — this campaign acquired the same information free and first-party from the SEC, 17 years deep, delisted issuers retained |
| Restatement / audit-analytics event database | **WAIT** | it would add the *cause* of a correction (fraud vs standard adoption), a genuine gap — but the free restatement channel built here showed no signal, so the refinement has nothing to refine |

**Analyst revisions are no longer the highest-value paid candidate.** Nothing is
authorised. Nothing was purchased. No quota was spent.

---

## 10. Forward evidence and capital deployment

**TRUE_FORWARD today: zero observations, and that is correct.** The shadow book
was opened at inception 2026-08-16 with 100 positions and no marks. A mark may
only exist for a date strictly *after* inception, and marks accumulate on
production ticks this campaign is forbidden from running. Writing one would
backdate the exact evidence the book exists to collect.

Continuity was proved, not assumed: same candidate id, same frozen spec hash,
100 inception positions intact, read-only flag set, no mark fabricated, no
reset, no backfill, no refit.

**HOC / capital deployment** stays `INSUFFICIENT_FORWARD_EVIDENCE` under its
existing owner (`stage23_unified.build_decision_link`, minimum 12 matured live
observations). Stage 25's historical counterfactual was a null; Stage 26 refused
to re-cut it and so does this campaign — re-cutting a fixed sample until it
turns favourable is specification search. With no Release-27 survivor there is
additionally nothing new to counterfactual against. No historical decision was
rewritten.

The question only forward evidence can answer is unchanged: does the challenger
rank a holding lower *before* it deteriorates, on a date it has not seen.

---

## 11. Exact next action

1. **Let the book run.** Forward marks accumulate on production ticks. Do not
   refit the frozen spec while waiting; do not backfill.
2. **Do not open another historical alpha campaign on owned data.** The frontier
   audit is zero. Eight families and 49 hypotheses were added to the 41 already
   closed, and the honest reading is that the free/owned information surface has
   been read.
3. **Do not buy anything.** The purchase gate's leading candidate fails on our
   own prior evidence.
4. When roughly eight more annual cross-sections of `dei:AuditorName` exist, the
   auditor family becomes runnable. That is calendar time, not work.
5. The next genuine research event is the **first matured forward observation**
   on `sb_c9_qualityprofi_e490533606`.

---

## 12. How to re-run

```powershell
# the campaign (research-only; ~3.5 minutes)
.venv-win\Scripts\python.exe scripts\run_alpha_exhaustion_campaign.py

# one family at a time
.venv-win\Scripts\python.exe scripts\run_alpha_exhaustion_campaign.py `
    --families release27_insider_transactions

# re-acquire a free SEC archive (idempotent; a verified cache re-downloads nothing)
.venv-win\Scripts\python.exe scripts\acquire_sec_quarterly_dataset.py `
    --dataset insider_transactions_data_sets --contact <email>
```

Artefacts land under `D:\Stock_Prediction_app_data\alpha_exhaustion_campaign\runs\<run_id>\`,
content-addressed, with `latest.json` pointing at the newest. The CLI exits
non-zero unless the frontier audit reports zero executable free families.
