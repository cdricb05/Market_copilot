# Release 67 — the research-to-capital projection, and four registrations that could never be funded

**RUN_ID** `R67_MULTI_STRATEGY_ALPHA_AND_PORTFOLIO_CAPITAL_ENGINE`
**Date** 2026-09-23
**Book** NAV **$99,127.48**, free cash **$4,482.71**, 25 holdings, eligible session **2026-09-21**

Research only. Paper only. No order, no fill, no promotion, no adoption, no
automation, no backfill. `NEW_PAID_DATA_COST = $0`.

---

## 1. What was actually completed

| | |
|---|---|
| **R66 landed** | committed `8d5302b`, worker restarted, repair **verified live** |
| **A defect nobody could see** | **4 of 8** forward registrations can never reach the capital gate |
| **R66's feasibility corrected** | all **11** hedged structures refused by an *authorised policy* R66 never checked |
| **Strategy inventory** | 8,472 hypotheses reduced to **267 economic mechanisms**, 5 flagged |
| **Research campaign** | 6 cells proposed, **6 refused** on prior art verified at source |
| **A false leakage check** | published by this release, caught by the director, **verified and corrected** |
| **Portfolio assessment** | 19 of 25 holdings flagged; top 5 names hold **53% of risk on 19% of weight** |

---

## 2. R66 is landed, installed and verified

Committed as **`8d5302b`** (17 files) after 170 targeted regressions passed and
`scripts\audit_architecture.py` returned exit 0.

The worker was running `8ce63b95b311` — three commits stale, holding pre-fix
imports exactly as R66 predicted. After the canonical scoped restart it runs
`8d5302b3f4e3`. **Verified on the live deployed worker**, not asserted:

```
surface_ends        2026-09-21
surface_last_usable 2026-09-21      (was 2026-08-28 — the 15-session hole is closed)
```

And the journal, which for eight days recorded three fields and never a reason:

```json
{"stage": "next_open_prospective_decision", "state": "DATA_BLOCKED",
 "blocked_owner": "VENDOR",
 "blocked_on": "the historical vendor has not published session 2026-09-22 yet
                (publication_state=NOT_PUBLISHED); no local action can change this",
 "append_detail": "the venue serves through 2026-09-21, which does not reach 2026-09-22",
 "publication": "NOT_PUBLISHED", "paid_dollars": 0.0, "frozen": false}
```

Today's blocker is **genuinely the vendor's**, and it says so by name. That is
the whole point of R66: the same word now means what it says.

---

## 3. The finding: four registrations that can never be funded

The forward book has been reported as "8 registered · 5 emitted · 0 matured" for
weeks, and read as *early days*. It is early days for three of them.

The capital gate needs **60 matured observations**. An observation needs a
decision. A decision needs a **cadence producer**. So the date is arithmetic:

```
sessions_to_floor = (60 − matured) × cadence + horizon
```

| challenger | cadence | producer | floor |
|---|---|---|---|
| `REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1` | 5 | live | 305 sessions ≈ **1.2 yr** |
| `ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7` | 5 | live | 305 sessions ≈ **1.2 yr** |
| `ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1` | 21 | live | 1,281 sessions ≈ **5.1 yr** |
| `REVERSED_SPY_PUT_CALL_SKEW_H5` | 5 | none — **by design** (superseded) | n/a |
| `R58_DISCLOSURE_INTENSITY_V1` | 21 | **none** | **never** |
| `R58_FCF_PURE_V1` | 21 | **none** | **never** |
| `R58_FUND_MOMENTUM_VETO_V1` | 21 | **none** | **never** |
| `R58_SHORT_VOLUME_PRESSURE_V1` | 21 | **none** | **never** |

`alpha_agent.r58.challengers.freeze(price, session)` exists and declares
`CADENCE = 21`. A repository-wide search for a caller returns **two hits: its
own docstring, and a string inside `FROZEN_DECISION_OWNERS` naming it as an
owner.** Nothing invokes it. The four records on disk were last written
2026-09-04 and each emitted exactly one prediction, on 2026-09-10, by the hand
that registered it.

**This is an absent capability, not a forfeiture.** R66 settled that a cadence
boundary at which the owner never froze anything is
`AWAITING_NEW_GOVERNED_FREEZE` — governance, not loss. That contract is
untouched and `forfeitures/` has still never been created.

**So: the soonest ANY sleeve can become capital-eligible is ≈ 1.2 years**, and
half the forward book is not on a path to eligibility at all.

---

## 4. R66's capital feasibility, corrected

R66's verdicts are not disputed and are not recomputed. What changes is **whose
rule produced them**, because that decides what would have to change to lift
them. Four owners; only two bind:

| owner | binds | examples |
|---|---|---|
| `AUTHORISED_POLICY` | **yes** | long-only book, max name weight |
| `EXCHANGE` | **yes** | contract indivisibility, initial margin |
| `RESEARCH_ASSUMPTION` | no | hedge-error tolerance, SPAN credit, vol-matched beta |
| `R66_CONVENTION` | no | **gross notional ≤ 1.00× NAV**, variation reserve ×1.0 |

### Error 1 — a convention was reported as a law of nature

`MAX_GROSS_NOTIONAL_OVER_NAV = 1.0` was declared inside R66's own module. **No
authorised policy in `api/` contains it.** R66 then called gross notional "a
STRUCTURAL constraint … no cash policy changes it". Contract size *is*
exchange-set and indivisible; the *threshold* is a choice nobody with authority
made. It survives here only as a labelled sensitivity.

### Error 2 — the constraint that actually binds was never checked

Every one of the eleven structures has a **short leg**, and the book is long-only:

```
api.capital_pool               semantics.long_only = True
                               safety.short_exposure_supported = False
api.capital_eligibility_gate   SHORT_LEG_NOT_EXPRESSIBLE_LONG_ONLY_BOOK
```

Re-ruled with policy evaluated first: **11 of 11 `NOT_EXPRESSIBLE_UNDER_AUTHORISED_POLICY`**,
refused before notional or margin is consulted.

> **The consequence that matters.** R66's single survivor — corn/wheat `ZC/ZW`,
> reported as missing by **$82 of margin** and becoming holdable at a 10 % cash
> policy — **is not holdable at any cash policy.** No cash policy makes a short
> leg expressible in a long-only book. R66 named the blocker as instrument
> granularity plus missing micro contracts; that is the constraint on the *long
> leg's size*, one level below the one that actually binds.

Governance contract R02 permits long/short **research** and requires an
inexpressible leg to be *reported, never silently dropped*. These rows are that
report.

---

## 5. The strategy opportunity inventory

8,472 experiment names are not a useful unit — 4,112 came from a symbolic tree
search and 3,475 from an auto-transform grammar. Deduplicated by **economic
mechanism**: **267 mechanisms**, **0 qualified survivors**, ever.

Every row carries its evidence **by layer**, because a headline `t` hides
whether a result exists only in the most recent window. Five mechanisms are
flagged and **may not be cited as prior plausibility**:

| mechanism | best lockbox t | verdict |
|---|---|---|
| `CALENDAR_TERM_STRUCTURE` / CROSS_ASSET | **4.74** | `VALIDATION_HOLE` |
| `CARRY` / COMMODITY_FUTURES | 3.19 | `REGIME_ARTIFACT_SUSPECTED` |
| `LIQUIDITY_PREMIUM` / US_EQUITY | 2.89 | `REGIME_ARTIFACT_SUSPECTED` |
| `MACHINE_REPRESENTATION:SYMBOLIC` / US_EQUITY | 2.76 | `REGIME_ARTIFACT_SUSPECTED` |
| `MACHINE_REPRESENTATION:AUTO` / US_EQUITY | 2.56 | `REGIME_ARTIFACT_SUSPECTED` |

The estate's **single strongest record** is the first one, and it is the shape
most likely to be argued back to life:

```
discovery   2011-07..2017-09   t 3.78   ann net excess  +4.51%
validation  2018-01..2022-09   t 0.09   ann net excess  +0.20%     <-- the hole
lockbox     2023-01..2026-05   t 4.74   ann net excess +10.55%
```

Validation is the *designated* out-of-sample window. Two good layers out of
three is not mitigation — it is the shape of a signal that works in some regimes
and not others. It was settled `NO_ALPHA_EVIDENCE` on `validation_material`, and
that was correct.

---

## 6. The research campaign: six cells proposed, six refused

The campaign ran through the governed V2 agents. The spawn plan authorised four
roles; each got its own brief, never the estate.

**The director refused all five cells, and the two the spec called "uncontested"
died hardest — on prior art found in research memory, not on data.**

| cell | disposition |
|---|---|
| `r67_01` commodity basis momentum | **REFUSED** — already built as `bm_252` on this exact layer and settled `HALTED_AT_V` at **−0.82 %/yr**; its record says verbatim *"BLOCKED, not reopenable: no re-parameterisation on the same panel."* |
| `r67_02` open-interest positioning | **REFUSED** — family burden 4, measured **negative twice at discovery** (−2.42 %/yr, −4.51 %/yr) against a declared sign of +1 |
| `r67_03` extension liquidity premium | **HELD** — the panel is `FROZEN PENDING AUDIT` |
| `r67_04` extension residual momentum | **HELD** — same |
| `r67_05` extension H5 reversal | **REFUSED** — its halt was a real **21.9 %/yr cost**, and the proposed hold band is a parameter chosen after that cost was seen |

**Burden charged: 0. Experiment ids minted: 0. Lockbox reads: 0.**

### The contested ruling: a third category

*Does re-testing a settled mechanism on a disjoint PIT universe satisfy
`NEW_ORTHOGONAL_INFORMATION`, or is it duplicate identity?* The director refused
the binary and ruled **REPLICATION**, proving it rather than arguing it: the
settled record and the proposed cell produce a **byte-identical family key**,
`LIQUIDITY_PREMIUM|PRICE_STATE|US_EQUITY|RANK_TOPN`. The reopen condition is
about **information**, not sample. *More rows are more observations, not more
information.* A replication inherits its **parent's** burden and, for a
replication of a null, must take its expected sign from published literature.

### Two corrections this release owes its own earlier claims

- **The extension universe is not an unlock.** It was presented here as a major
  find — 4,409 survivorship-safe mid/small-cap symbols against the 1,899
  large-cap the estate has ever searched. The estate has **frozen** that panel:
  its identity bridge resolves delisted names **by issuer name** and is
  unaudited, and a ratio feature turns a mis-match into a rank **extreme**, so a
  bottom-decile book selects *toward* mis-bridged names. Until audited,
  "success on it is uninterpretable and failure uninformative."
- **`NEXT_CAMPAIGN_CENSUS.json` has a real defect.** It summarises the
  open-interest family from its most recent row, presenting a **measured, failed**
  family as unmeasured. A director following the context contract and reading
  only the census would have registered `r67_02`. Caught only by querying
  research memory directly.

### Cell `r67_07` is `DATA_HOLD`, not a finding

H5 reversal conditioned on FINRA short-volume share is a genuine new-information
reopening. The owned normalized store
`external_normalized\short_interest\short_interest_normalized.csv` contains
**zero rows**. That is `DATA_HOLD` and may never be reported as `NO_ALPHA_EVIDENCE`.

### Cell `r67_06` reached the pre-registration door and was refused there

`XA_TERMS_OF_TRADE_RELATIVE_VALUE`. The full governed foundation chain was
completed for it:

```
PIPELINE_OK certify_data       r38_native_contract_layer_with_deferred_leg_v1  PIT_SAFE
PIPELINE_OK define_universe    r67_terms_of_trade_pairs   CROSS_ASSET  FUTURES_NOTIONAL
PIPELINE_OK publish_features   fs_r67_terms_of_trade_residual   leakage_check PASS
```

| pair | basket | obs | range |
|---|---|---|---|
| 6A | GC, HG, SI | 9,870 | 1987-04-14 → 2026-08-21 |
| 6C | CL, NG, RB | 5,186 | 2006-01-09 → 2026-08-21 |
| 6N | ZL, ZS, LE, HE | 5,199 | 2005-12-21 → 2026-08-21 |

**Refused, before pre-registration, so it minted no id and charged nothing.**
The director declined to call `preregister` precisely *because* the chain was
now complete: the call would have **succeeded**, minting an id and charging
burden before any refusal could bite, and nothing in this system deletes a
minted id.

Seven independent grounds. Three matter beyond this cell:

- **It is the cell R66 already refused, and it violates R66's recorded reopen
  condition in terms.** Same instruments, same 63-session beta, same 21-session
  horizon; the feature set's name differs from the refused one by two
  characters. R66 wrote the family back only as *"an UNCONDITIONAL, ALWAYS-ON
  hedged **carry** structure with a sign derived from one named economic channel
  and NO deviation-based entry rule."* `r67_06` scores **0 of 5** on that.
- **The "zero conditioning variables" claim is self-refuting.** R66 recorded the
  generalised test: *if position size or on/off state is a function of the
  CURRENT DEVIATION of the spread from its own history, it is premium timing.*
  Mean reversion of a residual **is** conditioning on the current deviation —
  the same operation described twice.
- **Information-family laundering, using this release's own measurement as the
  evidence.** The novelty claim rests on "no `TERMS_OF_TRADE` family exists in
  the estate". True — because the **label** is new, not the information. Every
  input to all three features is a futures **return**: no trade balances, no
  export volumes, no export-share weighting. *There is no terms-of-trade data in
  the terms-of-trade cell.* Its true key is `PRICE_STATE`, where CROSS_ASSET
  carries thousands of tests. Same offence as the model-family laundering
  already refused, on a different component of the same key.

### The leakage check this release published was false

The most consequential finding of the campaign, and it is a defect **this
release created**. Raised by the director as ground G4 and **verified
independently at source before it was accepted**:

```
research/agents/campaign_r56_v2/data_r38.py:116-117
  #: Scores read data through index t-1 (run_book enters at the settlement of t).
  SIGNAL_LAG_SESSIONS = 1

FEATURE_SET.json features[*].formula
  residual_6A_t = ret_6A_t − beta_6A_t * (equal_weighted_sum(ret_i_t ...))
```

The formula reads returns **at t** for both legs; only beta is lagged. Each
feature also labels itself `"lag": "1 session"` while its own formula on the
next line is lag 0 — **the label and the formula contradict each other inside
one JSON object**, and the label is the wrong one. `leakage_check: PASS` was
asserted against that, and the pipeline accepted it.

**Nothing is contaminated.** No experiment was pre-registered against these
features, no return was measured, estate burden is unchanged at 8,415. The
damage is one false `PASS` in an append-only journal, corrected in place beside
it by `FEATURE_SET_LEAKAGE_CORRECTION.json`.

The generalised lesson: **a leakage check the author asserts about their own
work is not a check.** `publish_features` enforces the *shape* of the claim —
the string equals `PASS`, every feature carries name/lag/source — and cannot
evaluate its *truth* against the timing rule of the data source the features are
built from. That is escalation **H5**.

---

## 7. The portfolio, and what stops a governed proposal today

**Before** — NAV $99,127.48 · cash 4.5 % · 25 holdings · **100 % US equity**,
0 non-equity positions · cumulative **−0.87 %** against SPY **+3.49 %**
(−4.36 pp) · max drawdown −4.87 %.

The read-only opportunity-cost assessment (`api.holding_opportunity_cost`,
state `READY`) reviews all 25 and flags **19**:

```
EXIT 11 · REPLACE 6 · REDUCE 2 · HOLD 6 · 10 addition candidates
```

**Risk is far more concentrated than weight suggests:**

| name | weight | risk contribution | flag |
|---|---|---|---|
| AMD | 4.97 % | **14.54 %** | `RISK_CONTRIBUTION_BREACH` |
| DDOG | 3.96 % | **14.19 %** | `RISK_CONTRIBUTION_BREACH` |
| ANET | 3.73 % | 8.75 % | |
| ALAB | 1.72 % | 7.94 % | |
| FTNT | 4.24 % | 7.73 % | |

**Top five: 53.1 % of portfolio risk on 18.6 % of weight.**

**After** — a complete target exists but has **no capital authority**:

- `api.zero_base_target` returns `authority.lane = RESEARCH_PREVIEW`,
  `forecast_state = NOT_ACTIVATED`, `can_become_a_proposal = false`.
- `api.opportunity_frontier`: `expected_return_state = NOT_CALIBRATED`,
  `candidate_rows_for_proposal = 0`, `eligible_non_equity_count = 0`.
- `api.reallocation_proposal`: `NOT_RUN`.

The governed proposal is produced by the operator's own DB-writing action,
`POST /v1/operations/daily-research-cycle/run` — named by the opportunity-cost
owner as its sole execution path. **R67 is read-only and did not trigger it.**
The assessment it consumes is complete and current.

### The non-equity gap, classified

Zero eligible non-equity instruments, and the reason is not research:

- 8 of 10 sleeves: `NO_APPROVED_OPERATIONAL_SIGNAL`
- 2 of 10 (`sleeve_fx_futures`, `sleeve_managed_futures_trend`): `GATE_NOT_PASSED`
  on 6 requirements, all of which reduce to **0 matured observations against 60**

This is a **product/architecture gap, not an evidence gap**, and §3 gives its
earliest possible remedy date.

---

## 8. What R67 built

`alpha_agent/r67/` — read-only projections over existing owners. No second
registry, gate, queue or clock.

| module | what it answers |
|---|---|
| `forward_producer` | can anything registered ever be funded, and when |
| `strategy_inventory` | what mechanisms exist, what each is worth, next action |
| `capital_feasibility` | what the book can hold, and **whose rule** decides |
| `cycle` | one repeatable pass; serves **portfolio reassessment** only |

`cycle.run()` is a pure function callable by the existing worker, the operator
cycle, or a test — it **starts nothing and owns no clock**, and explicitly does
not serve model recalibration, because a research pass having run is never a
reason to recalibrate.

### A defect found while testing the tests

A suite run landed mid-write of `accrual_projection.json` — the **live** worker
rewrites it — and the reconciliation reported **0 registrations while 8 were on
disk**. A file-system race was about to become a false statement about the
forward book. The read now cross-checks the artifact's own roll-up, and treats
an artifact that *exists and reads as nothing* as a **failed read**, while a
*missing* artifact still reads as a legitimately empty book. Same zero, opposite
meanings — both pinned by test.

---

## 9. Validation

- `tests\test_release67_research_to_capital.py` — **34 tests, all passing**
- `scripts\audit_architecture.py` — **exit 0**, inventory drift `OK`
- R66's 170 targeted regressions — passing (pre-commit gate for `8d5302b`)

---

## 10. Escalations for the human owner

1. **Wire a cadence producer for the four R58 challengers, or retire them.**
   They cannot reach the gate as they stand. Attaching a producer to an identity
   registered when none existed changes what that identity means — a governed
   decision, deliberately not taken here.
2. **Authorise the identity-bridge audit.** The single gate on the estate's best
   unexplored equity domain. Its hardest requirement — a permanent identifier to
   measure false-match rate against — needs **no purchase**: SEC Fails-to-Deliver
   files carry CUSIP and SYMBOL in the same row at ~99.8 % coverage, free.
3. **Decide whether gross notional > 1× NAV is policy.** R67 refuses to inherit
   R66's threshold as one. If it should be policy, an owner must declare it.
4. **Fix `NEXT_CAMPAIGN_CENSUS.json`'s open-interest summary** — it presents a
   measured, failed family as unmeasured.
5. **Short exposure.** Every hedged structure the estate can imagine is
   inexpressible until the position contract and NAV replay support a short leg.
6. **H5 — make `leakage_check` verifiable rather than assertable.** Validate a
   published feature set's declared lag against the `SIGNAL_LAG_SESSIONS` (and
   where relevant `OI_LATEST_USABLE_OFFSET`) of the dataset its universe is
   bound to, and refuse a `PASS` that contradicts it. Every future cell on this
   layer inherits the gap until this lands.
7. **H6 — record R66's reopen condition where an agenda-setter will see it.** It
   lives only inside `DIRECTOR_R66_RULING.json`, while the census still carries
   this cell at rank 3 with no mention that it was refused a campaign ago with a
   five-clause condition attached. Uncorrected, it will be proposed a third time.

---

## 11. What prevents further paper PnL today

Not research. **0 matured forward observations against a 60-observation floor**,
with the soonest reachable date ≈ 1.2 years away and half the forward book not
on a path at all. The equity book meanwhile carries 53 % of its risk in five
names and is behind SPY by 4.36 points, and its governed reallocation is one
operator action away — that action, not this release, is the next thing that can
move paper PnL.
