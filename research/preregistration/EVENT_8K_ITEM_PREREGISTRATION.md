# PREREGISTRATION — SEC FORM 8-K ITEM-CODE EVENTS

**Family id:** `DISCLOSURE_8K_ITEM_EVENT_V1`
**Ontology dimension:** `DISCLOSURE_INTENSITY_LANGUAGE` (US_EQUITY), scored under
the separate block key `SEC_8K_ITEM_EVENT`
**Status:** FROZEN. Written and committed BEFORE any forward return, event
return, rank IC, book or t-statistic for this family was computed on real data.

This document is the contract. It is not edited after results exist. If a
statement here turns out to be inconvenient, the inconvenience is the result.

---

## 0. WHY THIS AXIS, AND WHAT IT IS NOT

13F ownership breadth is CLOSED (`8cf9fa3`, no useful alpha). Schedule 13D/G
control blocks are CLOSED (`37c5e8e`, no edge). Neither is revisited here.

The lesson carried forward from 13D/G is frozen as a design constraint:
**orthogonal information is not valuable information.** 13D/G was 99% residual to
the owned factors and still subtracted return, because its information was spent
before the first legitimate entry. This family is therefore judged on the return
AFTER legitimate entry, never on the announcement session and never on
orthogonality alone.

A Form 8-K is a current report of a specific corporate event, stamped at SEC
acceptance and tagged by the filer with structured Item codes. The axis is not
"8-K activity" — the estate already owns that as `DISCLOSURE_INTENSITY_LANGUAGE`
(`k8_rate_z`, NT filings, amendments) and this family must beat it. The axis is
the ECONOMIC TYPE of the event.

---

## 1. DATA — FREE SEC ONLY

Source: the per-issuer EDGAR submissions histories already acquired by the
canonical owner `alpha_agent.r63.acquire` (1,080 issuer histories). No purchase,
no subscription, no licence, no key. `paid_dollars: 0`.

EDGAR's submissions API publishes an `items` column: the Item codes the filer
declared in the EDGAR header at submission. It is point-in-time by construction
and structured, so an Item code is READ, never inferred from text.

### 1.1 Measured stream facts

| measurement | value |
|---|---|
| modern-era 8-K + 8-K/A filings (from 2004-08-23) | **220,226** (215,183 originals, 5,043 amendments) |
| `items` field populated | **1.0000** |
| pre-2004-08-23 filings (legacy integer Items, different meanings) | 24,450 — excluded, never mapped |
| post-regime filings still carrying a legacy token | 2 |
| successor-registration forms `8-K12B`/`8-K12G3`/`8-K15D5` | 97 — excluded (exact form match) |
| accessions filed under more than one stored CIK | 118 — deduplicated on (CIK, accession) |
| filings carrying only Item 9.01 | 3,151 |
| panel calendar | 2004-06-01 → 2026-09-03; discovery grid from 2011-07-01 |

Census artifact: `results/event_8k_census.json`
(`artifact_hash 3013467b8e6881eac161cb5ce5505774e3a0624159ff24ebc7770e11a1c9eb5c`).

### 1.2 Traps measured, each of which silently corrupts the stream

1. **Two Item vocabularies.** Before SEC Release 33-8400 (effective 2004-08-23)
   Items were integers with different meanings — legacy "5" was Other Events,
   modern 5.xx is governance. Only `d.dd` tokens are Items.
2. **Prefix matching admits the wrong forms.** The canonical keep-list's prefix
   `8-K` admits successor registrations. Forms are matched exactly.
3. **`acceptanceDateTime` is UTC** (measured on the 13D/G axis). The one owner
   `control_block_data.acceptance_et` is reused.
4. **`searchsorted` stacks pre-calendar filings on day one.** An instant before
   the first panel session is refused, not mapped to position 0. The 13D/G stream
   began after the panel and never met this; the 8-K stream begins in 2004 and would.
5. **An Item-section heading can be a cross-reference.** ("The information set
   forth under Item 1.01 is incorporated by reference.") The operative narrative
   is the longest segment. Used only for the text audits in §3; no frozen cell
   depends on text.

---

## 2. PHASE-1 CENSUS BY ITEM (no return computed)

All issuers in the stored histories, modern era. *Eligible events* are original
8-Ks mapped to a panel row eligible (PIT S&P 500 membership, price ≥ $5, median
dollar volume ≥ $10M) at the decision session, collapsed to one per (row,
session). *Sessions* counts distinct decision sessions from 2011-07-01.

| Item | filings | issuers | history start | zero years | min–max / yr | eligible match | outside regular session | amendment rate | eligible events | sessions ≥2011-07 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1.01 Material agreement | 27,889 | 916 | 2004-08-23 | 0 | 846–3,074 | 0.531 | 0.69 | 0.013 | 14,671 | 3,443 |
| 1.02 Termination | 3,034 | 726 | 2004-08-27 | 0 | 89–259 | 0.569 | 0.71 | 0.011 | 1,729 | 1,012 |
| 2.01 Acquisition/disposition | 2,924 | 746 | 2004-08-27 | 0 | 50–194 | 0.509 | 0.68 | 0.111 | 1,336 | 786 |
| 2.02 Results | 65,795 | 912 | 2004-08-24 | 0 | 2,800–3,117 | 0.618 | 0.85 | 0.008 | 40,240 | 3,159 |
| 2.05 Exit/disposal costs | 1,631 | 435 | 2004-09-08 | 0 | 25–143 | 0.615 | 0.74 | 0.127 | 854 | 456 |
| 2.06 Material impairments | 805 | 326 | 2004-09-09 | 0 | 12–78 | 0.657 | 0.77 | 0.060 | 494 | 261 |
| 3.01 Delisting / listing | 648 | 384 | 2004-10-01 | 0 | 11–74 | 0.319 | 0.60 | 0.003 | 204 | 112 |
| 4.01 Accountant change | 461 | 249 | 2004-08-25 | 0 | 7–75 | 0.453 | 0.64 | 0.217 | 148 | 75 |
| 4.02 Non-reliance | 290 | 203 | 2004-09-13 | 0 | 1–86 | 0.376 | 0.71 | 0.059 | 103 | 30 |
| 5.02 Officers/directors | 40,511 | 913 | 2004-08-23 | 0 | 1,229–2,339 | 0.629 | 0.73 | 0.043 | 24,284 | 3,738 |
| 7.01 Reg FD | 48,686 | 885 | 2004-08-23 | 0 | 1,867–2,727 | 0.624 | 0.79 | 0.008 | 29,719 | 3,726 |
| 8.01 Other events | 52,907 | 922 | 2004-08-23 | 0 | 1,909–2,753 | 0.640 | 0.72 | 0.008 | 34,205 | 3,775 |

The eligible match rate is well below 1 because most filings are made while the
issuer is outside the PIT S&P 500, not because identity failed.

Duplicate rate (several originals on the same row and session) is ≤ 1% for every
Item except 4.01 (10.8%). 8.01 is not a candidate: it is a residual category with
no preregisterable economic meaning.

---

## 3. CELL SELECTION — WITHOUT RETURNS

Criteria, in the brief's words: ex-ante economic meaning, sufficient sample, PIT
integrity, clean Item-code definition, operational tradability. **Not one return
was computed for any candidate.**

### 3.1 The sufficiency measure, in the scorer's own units

The canonical scorer counts effective periods as `periods × min(1, cadence/h)`
over every tested grid period — including periods whose cross-section contains
no event and therefore says nothing about the event. For a sparse event signal
that overstates the evidence. Sufficiency here counts ONLY the canonical
walk-forward's out-of-sample test periods whose cadence window holds at least one
eligible event, in the same formula, against the existing floor
`MIN_EFFECTIVE_PERIODS = 36` (`event_8k_data.oos_sufficiency`).

| candidate (eligible originals from 2011-07) | events | h=5 | h=21 | h=63 |
|---|---|---|---|---|
| 1.02 (raw) | 1,209 | 367 | 114 | 37 |
| 1.02 without 1.01/2.01 | 242 | 132 | 85 | 28 |
| 1.02 without 1.01/2.01/2.03/5.01 | 227 | 126 | 83 | 27 |
| 2.05 or 2.06 without 2.02 | 481 | 193 | 97 | 31 |
| **2.05 or 2.06 without 2.01/2.02** | **476** | **193** | **97** | **31** |
| 3.01 without 2.01/3.03/5.01 | 91 | 64 | 52 | 17 |
| 4.01 | 76 | 36 | **29** | 9 |
| 4.02 | 30 | 19 | **17** | 5 |
| 2.04 | 75 | 45 | 36 | 11 |
| 3.02 | 502 | 234 | 111 | 36 |

**Structural fact, declared now:** at h=63 the canonical grid tests 113 periods
at cadence 21, so the maximum possible count is 37. No sparse cell can reach 36
at h=63.

### 3.2 A — MATERIAL CONTRACT TERMINATION (Item 1.02): NOT SELECTED

*Clean definition fails.* Co-filing structure alone: **73.2%** of 1.02 filings
also carry 1.01 and 54% carry 2.03 — the old agreement ends because a new one
replaced it (refinancing). After excluding co-filed 1.01/2.01/2.03/5.01, a
deterministic text read of the 225 documents found **134 (59%) still financing
housekeeping** (credit facilities expiring or prepaid, notes redeemed, 364-day
facilities lapsing), 28 transaction-agreement terminations (deal breaks, whose
sign depends on which side files), and 63 residual terminations that are
themselves mixed (EPC contracts, shareholder and at-the-market agreements,
cross-references). The non-financing subset is 91 events, 41 informative periods
at h=21 and 13 at h=63, and still has no single economic direction. A NEGATIVE
sign cannot be frozen on it honestly.

### 3.3 C — LISTING / AUDITOR / EXECUTIVE SHOCK: NOT SELECTED

* **3.01** — mixed sign. Co-filed with 5.01/3.03/2.01 in 25–28% of filings
  (merger closings, after which the stock stops trading; 12.7% of names leave the
  universe within 63 sessions). Of the 91 remaining S&P 500 filings the text
  shows late-filing deficiency notices (negative), voluntary NYSE↔Nasdaq
  transfers (neutral; the 2006 spike is Nasdaq's exchange registration),
  audit-committee technicalities after a director's death (benign), a reprimand
  letter, and notices of REGAINED compliance (positive). The negative subset is a
  handful of events.
* **4.01** — insufficient (29 informative periods at h=21) and not a shock: 56 of
  75 filings are dismissals after competitive RFP processes; 5 are resignations,
  some of them firm reorganisations.
* **5.02, clean subset** — the one deterministic candidate is the appointment of
  an interim or acting CEO. On the 2011–2016 development documents (5,328 of
  6,147 eligible 5.02 filings) the rule accepted 37 filings, **about 24 of them
  genuine new appointments (~65% precision)**; the rest were follow-up
  compensation reports, "will continue to serve as interim" updates, tenure
  endings, an interim made permanent, biographies of past interim service and an
  affiliate's seat. Repairing that means patching on the same documents plus an
  arbitrary episode window, for ~4.4 episodes a year (~40 informative periods at
  h=21, 13 at h=63). Under the brief's own rule, **5.02 is dropped.** The rule and
  its misreadings stay in code and tests so the audit is reproducible.

### 3.4 Replacements considered by the same criteria: NOT SELECTED

* **2.04** accelerated obligations — 42 of 75 S&P 500 filings are voluntary
  optional redemptions of notes (make-whole calls), not distress.
* **4.02** non-reliance — clean and negative, but 17 informative periods at h=21.
* **3.02** unregistered equity sales — not directional in this universe.
* **1.03** bankruptcy (7 events), **1.05** cybersecurity incidents (12 events,
  history from 2023-12) — insufficient.

### 3.5 THE ONE FROZEN CELL

**B — RESTRUCTURING_OR_IMPAIRMENT.** An event is an ORIGINAL Form 8-K whose
filer-declared Items include **2.05 or 2.06**, and include **neither 2.02 nor
2.01**.

* **Why combine 2.05 and 2.06, stated before returns.** Both Items are triggered
  by the same economic fact: the board's conclusion that part of the existing
  asset base will not earn its cost of capital. 2.06 recognises that the carrying
  value is unrecoverable (ASC 350/360); 2.05 commits to exiting or disposing of the
  activity (ASC 420). They are frequently the two accounting faces of ONE
  decision — 35% of 2.06 filings also carry 2.05 — so separate cells would split
  one event across two correlated tests.
* **Why exclude 2.02.** When the charge is disclosed inside a results release,
  the report's information is the earnings release, whose surprise sign is
  unknown and would dominate (24.5% of 2.05/2.06 filings).
* **Why exclude 2.01.** A charge disclosed on completing an acquisition or
  disposition is the accounting consequence of a transaction already announced.
* **Direction: NEGATIVE, frozen.** Both Items are management's admission that past
  investment destroyed value and that the affected assets' future cash flows are
  lower than recorded.
* **Composition** (eligible, from 2011-07-01): 476 events, 213 names, 2011-07-19 →
  2026-07-28; 258 carry 2.05 only, 159 carry 2.06 only, 59 carry both; 389
  selection-period and 87 lockbox events. 94 amendments carrying these Items are
  excluded (an amendment is not a new event). 2 same-row-same-session pairs
  collapse.
* **PIT profile:** 216 accepted after the close, 190 before the open, 67 intraday,
  3 on non-session days — **86% outside the regular session**.
* **Tradability:** 1.4% of event names leave the eligible universe within 63
  sessions; the panel retains delisted rows.

A pattern read of all 475 documents finds restructuring language (restructuring,
workforce reduction, closure, exit) in 416 and impairment language in 269, and
the sampled narratives are what the Items say they are: workforce reductions,
facility closures, exit plans, goodwill and asset impairments. No text rule is
part of the cell.

**No second or third cell will be added, before or after results.**

---

## 4. POINT-IN-TIME CONTRACT

**The boundary is the SEC ACCEPTANCE INSTANT**, converted from UTC to Eastern
with the US daylight-saving rule for the year. EDGAR's filing DATE is never used
as the availability boundary.

**Decision session `t`** = the FIRST session whose 16:00 Eastern close is
STRICTLY after acceptance. A filing at 11:00 on session `s` has `t = s`; a filing
at 17:30 on `s`, or on a weekend, has `t` = the next session.

**Entry at the close of `t+1`** — the estate's canonical NEXT_CLOSE
(`alpha_agent.r63.pit.forward_compound`), window sessions `t+2 … t+1+h`. Every
entry is strictly after publication. This forgoes one legal session and is
identical to every other family in the estate. The entry is not moved after
results.

**Diagnostics — reported, excluded from every gate by test:**

* **announcement session** — close `t−1` → close `t`, the session that CONTAINS
  publication (market-adjusted). Information spent here is not tradable alpha.
* **forgone first post-publication session** — close `t` → close `t+1`. Legal to
  trade, forgone by the canonical lag; reported so "no information after entry"
  can be told apart from "information spent in the one session not claimed". **It
  is not a rescue, and no same-day or first-session variant will be scored.**
* **prior 21-session run-up** (the 13D/G post-hoc diagnostic).

---

## 5. IDENTITY AND SURVIVORSHIP

Identity is **filer CIK → panel row**: an 8-K is filed under the registrant's own
CIK, so the join is one hop, through the SAME authoritative routes as 13D/G (the
owned R58 bridge, then SEC `company_tickers.json` for still-listed rows only). **No
issuer-name matching anywhere.** A CIK on several rows attributes the event to
each and is reported.

**Declared limitation, measured before results:** 1,130 of 1,897 panel rows carry
a CIK (1,086 CIKs; 6 identified rows have no stored history and are treated as
unassessable). On the eligible cross-section the assessable share is mean
**94.52%**, worst **88.82%**, and **49.2% of decision sessions sit below 95%**.
Unidentified names leave the investable universe at 15.3%/yr against 3.1% —
22.3% of all exits. A company that restructures and later disappears is exactly
what this blindness hides.

---

## 6. HORIZONS — FROZEN, BY THE EXISTING BUDGET

The brief: 5/21/63 if the existing multiplicity budget supports all three,
otherwise 21/63. The budget is `FAMILY_PRIMARY_MAX = 6`. One cell × three
horizons = 3 ≤ 6, so **horizons are 5, 21 and 63 sessions**; cadence
`min(h, 21)` on the canonical grid. Nothing is added, removed or searched after
results.

**Declared in advance:** h=63 carries 31 informative periods (< 36), so under §10
it resolves to NEED_MORE_EVIDENCE whatever its t-statistic.

---

## 7. MEASUREMENT — two arms, both frozen

### Arm 1 — EVENT STUDY (the qualification input)

The frozen 13D/G owner `control_block_alpha.event_study`, entry per §4, per event
and horizon:

* **raw** compounded forward return;
* **market-adjusted** — minus SPY total return over the identical window. **This
  is the primary statistic;**
* **sector-adjusted** — DIAGNOSTIC only (the estate owns a current GICS snapshot,
  no PIT sector history — inherited from R58);
* **equal-risk** — market-adjusted divided by 63-session realised volatility.

Clustered by decision session; Newey-West t at lag `h−1`. Also reported: a
SELECTION (< 2023) / LOCKBOX (≥ 2023) split, and the market-adjusted return in the
FROZEN direction net of the cost ladder (`signed_net_by_cost`) with Sharpe and
maximum drawdown.

### Arm 2 — CROSS-SECTIONAL PANEL (the incremental test)

The signed event count on the canonical R63 equity grid:
`sig[i,t] = sign × (events for name i in (t−cadence, t])`, 0 — never NaN — for an
eligible name without an event. Scored by the ONE scorer
`alpha_agent.r63.sensitivity.run_cell`. No second scorer.

### The incremental-information requirement

The effect must survive, in the frozen direction:

* the owned US_EQUITY baseline (`ontology.baseline_for`, 10 dimensions), **plus**
* the owned 8-K intensity block `DISCLOSURE_INTENSITY_LANGUAGE` — an Item-coded
  event is mechanically correlated with how often a company files, so information
  that block carries cannot be credited to the Item. This is the scorer's baseline
  `B` for the conditional test;
* the incumbent `fundamental_momentum_50_50_v1` — a second scorer run with the
  incumbent as baseline, and a contemporaneous cross-sectional residualisation on
  controls + incumbent for the orthogonalised rank IC and book.

The incumbent top-50 book blended 50/50 (z units) with the orthogonalised signal is
reported as DESCRIPTIVE only; its blend weight is not a preregistered parameter.

---

## 8. COSTS

1 / 2 / 5 bp per side, plus the desk single-name rate `EQ_COST_RATE_PER_SIDE` =
**12.5 bp per side** reported alongside. The event study pays entry and exit.

---

## 9. MULTIPLE TESTING

Owner `alpha_agent.r31.multiple_testing` (Benjamini-Hochberg, q = 0.10), the same
owner R39 and 13D/G use. ONE declared family: **m = 3**, over the event-study
market-adjusted two-sided p-values. The denominator is not reset; a rejected cell
stays in it.

---

## 10. QUALIFICATION — existing gates only, in this order

1. **Coverage.** More than `MAX_UNCOVERED_SHARE` = 0.20 of decision sessions under
   `MIN_DATE_COVERAGE` = 0.95 → **DATA_HOLD**. *Measured: 49.2%. EXPECTED TO FIRE.*
2. **Missingness bias.** Unassessable-but-eligible names' mean forward return
   differing by more than `MATERIALITY_ANN_NET` = 1.5%/yr → **DATA_HOLD**. *The
   same identity layer fired this gate on 13D/G.*
3. **Effective periods.** Scorer DATA_HOLD, or `min(scorer effective periods,
   informative periods of §3.1)` < 36 → **NEED_MORE_EVIDENCE**. *Expected at h=63.*
4. **Frozen sign.** Post-entry market-adjusted mean with the sign opposite to
   NEGATIVE → **NO_EDGE**. The sign is never reversed.
5. **Post-publication effect.** Event-study market-adjusted |t| <
   `STANDALONE_T_FLOOR` = 2.0 → **NO_EDGE**.
6. **Conditional value.** Scorer conditional t < `CONDITIONAL_T_FLOOR` = 2.0 (the
   increment must HELP; a negative t does not pass) → **NO_EDGE**.
7. **Multiplicity.** Failing BH q = 0.10 over m = 3 → **NO_EDGE**.
8. **Incremental.** Redundancy `REDUNDANT`, or orthogonalised rank-IC t < 2.0, or
   incremental-vs-incumbent scorer t < 2.0 (all in the frozen direction) →
   **NO_INCREMENTAL_INFORMATION_EDGE**. An effect that disappears after
   orthogonalisation is not rescued.
9. **Materiality.** Net annual increment < 1.5%/yr → **NO_EDGE**.
10. Otherwise → **QUALIFIED**.

**Declared merits reading.** Because gates 1–2 are statements about data, the SAME
frozen function is also evaluated with both data gates satisfied and reported
beside the formal verdict — exactly the separation the 13F and 13D/G results
needed. It cannot turn a formal DATA_HOLD into a qualification.

**CAPITAL ELIGIBLE remains NO** unless every qualification gate AND every existing
forward-evidence and governance gate is satisfied. Nothing here can promote a
model, allocate capital, or create a proposal, order or fill.

---

## 11. WHAT WOULD FALSIFY THE HYPOTHESIS

A null result means: an original 8-K reporting a restructuring commitment or a
material impairment — outside an earnings release and not a completed
transaction — observed at its true acceptance instant and entered at the canonical
next close, carries no information about subsequent market-adjusted return that
the estate's owned families and its own 8-K intensity block do not already carry,
subject to the blindness declared in §5.

If the announcement session moves and the post-entry return does not, the
conclusion is **already priced**, the cell closes, and no earlier entry is tried.

## 12. SAFETY

RESEARCH ONLY. No purchase, no subscription, no licence, no model promotion, no
capital allocation, no portfolio mutation, no proposal, no order, no fill, no
backfill, no live deployment, no live write. The live checkout and the live
next-open skew challenger are not touched.
