# R69.6 — Alpha-to-PnL: what is actually stopping this system making money

**Scope.** A diagnosis plus one bounded repair. No cap moved, no threshold
relaxed, no exception granted, no target approved or selected, no order created,
no model promoted, no portfolio mutated, no research campaign pre-registered and
no multiple-testing budget spent.

Everything below was measured against the live stores and the running backend on
2026-09-24. Where a number is inferred rather than measured it is marked.

---

## 1. The finding, in one paragraph

The system is not failing to convert research into PnL because its controls are
too strict. It is failing because **it has almost no evidence at all, and the
evidence it does have is one stock.** Every replacement candidate the operational
book has ever measured — all 8,825 stored observation rows, all 1,225 distinct
economic observations — is the same ticker, **SNDK**, always rank 1. The
"0 wins / 6 losses" that the proposal review reports as adverse replacement
evidence, and the "47 of 62 withheld actions would have improved the book" that
points the other way, are **the same number split in two by a bookkeeping
accident**. Neither is evidence about alpha. Separately, the research estate's
8,472 hypotheses were all graded against **one fixed lockbox window**
(2023-01-03 → 2026-09-03) that has now been looked at 8,415 times, so it is no
longer a holdout. The only untainted evidence channel left — forward accrual —
is correctly built, correctly running, and has produced **0 matured observations**
because the earliest one is 1.2 years away.

---

## 2. Why replacements are not working

### 2.1 The two headline statistics are one statistic

| | rows | session clusters | distinct replacement candidates |
|---|---|---|---|
| "0 wins / 6 losses" (matured replacements) | 6 | **1** (2026-08-17) | **1** (SNDK) |
| "47 of 62 withheld by CHURN_COOLDOWN_ACTIVE" | 62 | **4** (08-18, 08-20, 08-24, 08-25) | **1** (SNDK) |

The six incumbents whose REPLACE was *not* withheld on 2026-08-17
(`CAT, DVA, DVN, EOG, MNST, VLO`) are the **same six names** whose REPLACE *was*
withheld on 2026-08-18. Same recommendations, one session apart, opposite
evidence buckets. 2026-08-17 was simply the first session with any
recommendations, so no change history existed and the cooldown could not bind.

The mechanism: a withheld action is persisted with `recommendation = "HOLD"`
(`engine/portfolio_reassessment.py:1199-1200`, `:1297-1299`;
`api/portfolio_reassessment.py:335`), and the replacement scorecard filters on
`recommendation == REC_REPLACE` (`engine/reassessment_outcomes.py:693`).
**`replacement_outcomes` can therefore only ever contain sessions in which the
cooldown did not bind.** The 0/6 is frozen for ever; the 47/62 grows every
session. Observed growth: `18 of 31` (09-21) → `33 of 47` (09-23) → `47 of 62`
(09-24).

### 2.2 Neither figure supports relaxing the cooldown

* **Wrong horizon.** `CONTROL_REGRET` is judged at 20 sessions
  (`engine/reassessment_outcomes.py:142`); the cooldown protects **5**
  (`engine/portfolio_reassessment.py:329`). The store already contains the
  aligned horizon and the verdict **inverts**:

  | horizon | n | wins | hit rate | verdict |
  |---|---|---|---|---|
  | 1 | 363 | 176 | 0.485 | **CONTROL_BENEFIT** |
  | 5 — *the cooldown's own window* | 295 | 199 | 0.675 | CONTROL_REGRET |
  | 20 — *the one published* | 62 | 47 | 0.758 | CONTROL_REGRET |

* **Not identified to the cooldown.** All four contributing sessions were
  `CHANGE_CANDIDATE` or `MANUAL_REVIEW_REQUIRED` with 1–8 other named blockers,
  and none was in `PROPOSAL_ELIGIBLE_STATES`. With the cooldown disabled, not one
  of the 62 could have been proposed. *(Read from the recorded blockers and
  `engine/portfolio_reassessment.py:118`; not re-run, because re-running writes.)*
* **38 of the 62 are mis-specified** — 30 `REDUCE` (policy moves only half the
  weight) and 8 `EXIT` (makes no claim about SNDK) are scored as full swaps.
* **Hindsight over exactly the window the control disclaims knowledge of.**

### 2.3 The research record says the same thing

Layer-aware, from `research_memory.sqlite` (8,472 rows). The three layers are
**fixed calendar windows shared by every hypothesis**: D 2006-01-03→2017-12-14,
V 2018-01-02→2022-12-15, L 2023-01-03→2026-09-03.

| paired on the same row (n=1,776) | D | V | **L (lockbox)** |
|---|---|---|---|
| median `ann_net_excess` | −2.27% | −2.54% | **+0.44%** |
| p90 | +0.55% | +2.20% | **+11.23%** |

113 rows clear `t ≥ 2` **and** every materiality floor
(`ann_net_excess ≥ 1.5%/yr` or `net_sharpe ≥ 0.4`). What kills them:

| failed gate | of the 113 |
|---|---|
| `burden_corrected_significant` | 111 (98%) |
| `validation_material` | 39 (35%) |
| `validation_same_sign` | 38 (34%) |

Their shape is unmistakable — lockbox `ann_net_excess` **+13% to +15%/yr**
against validation **+0.1% to +0.9%/yr**; lockbox Sharpe +1.3 to +1.5 against
validation +0.11 to −0.76. A real effect does not appear only in the last 3.7
years at fifteen times the magnitude. **The gate is correctly refusing
lockbox-window artifacts, and the burden correction is the thing doing it.**

Exactly **one** row in 8,472 cleared `t ≥ 2` with material economics in both
lockbox and validation: `CALENDAR_TERM_STRUCTURE|FUTURES_TERM_STRUCTURE|
CROSS_ASSET` (t 4.739, L +10.55%/yr) — and its validation excess was +0.20%/yr
and it had ~14 effective observations against a floor of 36.

**The deeper problem: the lockbox is spent.** One window, 8,415 charged tests.
Its out-of-sample status is gone, and no new hypothesis on the same panels can
restore it. This is why the frontier reports EXHAUSTED for seven of eight asset
classes and why R68 pre-registered zero cells.

---

## 3. Why no non-equity candidate reaches the frontier

There are **two independent walls**, and the estate published only the first.

### Wall 1 — forward evidence (governance, ~1.2–5.0 years)

8 challengers are registered; every one has a live producer; 0 are orphaned or
failed. Measured today:

| | emitted | matured | effective independent |
|---|---|---|---|
| all 8 registrations | 5 | **0** | **0** |

`MIN_RAW_MATURED_OBSERVATIONS` requires **60**. At the declared cadences:

| cadence | obs/year | years to 36 | years to 60 |
|---|---|---|---|
| 5 sessions (FX carry, both SPY skew) | 50.4 | 0.7 | **1.2** |
| 21 sessions (4 × R58, futures trend) | 12.0 | 3.0 | **5.0** |

The FX carry emitter works — it froze a real decision for entry session
2026-09-22 with six signed weights. The prior release note *"FX cadence
registered but no emitter = no accrual"* is **no longer true at HEAD**.

### Wall 2 — unit granularity (arithmetic, unfixable by evidence)

This one sat **below** the gate and was never computed, because the granularity
check only ran over sleeves that had already passed. It would still have blocked
every candidate on the day the evidence arrived. At NAV **$97,973** and the
declared 10% name cap (cap = $9,797):

| sleeve | cheapest contract | NAV needed for ONE unit | multiple of today's book |
|---|---|---|---|
| crypto futures | `&MBT` | $84,360 | **0.86×** *(fits — but R42 rules the sleeve DO_NOT_ACTIVATE)* |
| volatility futures | `&VX` | $179,100 | 1.83× |
| FX futures | `&6M` | $281,050 | 2.87× |
| managed futures trend | `&NG` | $329,700 | 3.37× |
| international index futures | `&FESX` | $716,422 | 7.31× |

**The paper book is too small to hold a single contract in four of the five
non-equity sleeves.** No amount of research evidence changes that. The owned
micro contracts do not rescue it either: `M2K $14,284`, `MYM $25,859`,
`MES $38,835`, `MNQ $61,534` are all above the $9,797 cap, and `M6E/M6A/M6B/MJY/
MCD/M6S` are `NOT_FOUND` in the owned provider.

Verified not to be blockers: the instrument universe is **not** hard-bound to
equities (19 futures rows entered the frontier); the mark/NAV seam serves futures
(Norgate `latest_session 2026-09-24`, `freshness CURRENT`); the covariance
universe aligns futures beside equities (138 paired observations); and the
position contract represents futures notional, margin and sign correctly.

### The repair landed here

`api/investability_registry.py` — `eligible_non_equity_instruments` now publishes
`minimum_nav_for_one_unit_usd`, `nav_multiple_required`, `name_cap_usd` and
`sleeve_capital_eligible`, and takes `include_ineligible_sleeves` so the question
can be asked of a sleeve the gate has **not** passed.
`api/opportunity_frontier.py` — the admission ledger carries the sleeve minimum
and `cheapest_instrument_id`, and the explanation sentence states the NAV when
granularity binds.

It **admits nothing**: every described row carries `sleeve_capital_eligible:
False`, no default widened, no threshold moved, and `executable_at_nav` is
byte-identical (`tests/test_r69_6_granularity_requirement.py`, 11 tests).

---

## 4. What would change if a candidate qualified

Nothing, today — and that is the point. Even a sleeve that cleared all six gate
codes would be refused at `UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV`. The first
non-equity dollar cannot be allocated until **either** the book reaches ~$281k
(FX, the cheapest non-crypto sleeve) **or** the name cap is deliberately raised
for indivisible instruments — and at $98k no cap short of ~0.29 admits even the
smallest FX contract, which is a real concentration, not a bookkeeping fix.

---

## 5. The single highest-value next step

**Not another backtest.** A ninth campaign on the same panels would be charged
against the same spent lockbox and would fail on `burden_corrected_significant`
exactly as the previous 8,415 did.

The highest-value work is to **protect and shorten the forward channel**, because
it is the only untainted evidence this estate has left:

1. **Reconcile `MIN_RAW_MATURED_OBSERVATIONS` with cadence.** The threshold table
   (60 raw matured) and its companion `min_calendar_days` row (180 days at h5,
   365 at h20) are mutually inconsistent for a cadence emitter: 60 non-overlapping
   h5 decisions need 300 sessions, 2.4× what the calendar-days row allows. This
   looks like a per-session-emitter threshold applied unchanged to a per-cadence
   emitter. *(Inferred from the internal inconsistency, not from a design
   document.)* **This lowers an evidence bar and is a governance decision — it is
   recommended for a ruling, not done here.**
2. **Fix the measurement defects in §2** so the next 62 observations mean
   something: stop the withholding rewriting `recommendation` to `HOLD` before
   persistence; publish `distinct_replacement_candidates` and session clusters on
   every bucket; judge a control at its own horizon; gate the
   `REPLACEMENT_EVIDENCE_ADVERSE` caution on the `min_observations_for_review = 20`
   floor the policy already declares and currently bypasses.
3. **Widen the replacement candidate set.** One candidate (SNDK) across 25
   sessions is the root cause of every weak statistic in §2.

---

## 6. Exact blockers

| # | Blocker | Class | Clearable by |
|---|---|---|---|
| B1 | Every measured replacement is one ticker (SNDK, 1,225/1,225) | measurement | widening candidates; no code change needed |
| B2 | Withheld actions relabelled `HOLD` before persistence | code | `engine/portfolio_reassessment.py:1199` |
| B3 | Control judged at H=20, protects H=5; verdict inverts at H=1 | contract | binding the horizon to the policy |
| B4 | Lockbox window spent (8,415 looks at one 3.7-year window) | structural | **only** new data, new sample, or forward time |
| B5 | 0 matured forward observations; earliest 1.2 years | data (time) | nothing — and nothing should |
| B6 | `MIN_RAW_MATURED = 60` vs a 5/21-session cadence | contract | a governance ruling |
| B7 | `CONDITIONAL_OPERATIONAL_APPROVAL_DECLARED` never declared (store absent) | governance | an operator act; **grants nothing today** |
| B8 | Unit notional exceeds the name cap at $98k NAV | contract | NAV growth, or a deliberate cap ruling |
| B9 | 8 of 10 non-equity sleeves have no declared gate candidate | code | `api/capital_eligibility_gate.py:165-197` is a hard-coded list |

---

## 7. What was NOT done

No campaign pre-registered. No hypothesis measured. No burden charged. No
challenger promoted or registered. No gate threshold changed. No conditional
approval declared. No proposal approved or selected. No order. No portfolio
mutation. The operational book was read, never written.

**Live book for the record (2026-09-23):** NAV $97,973.38, cumulative
**−2.03%** (−$2,026.62) against SPY **+2.73%**, cumulative excess **−4.76%**,
drawdown −4.17%.
