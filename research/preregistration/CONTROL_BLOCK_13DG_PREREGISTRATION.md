# PREREGISTRATION — SEC SCHEDULE 13D/13G CONTROL-BLOCK EVENTS

**Family id:** `OWNERSHIP_CONTROL_BLOCK_13DG_EVENT_V1`
**Dimension:** `OWNERSHIP_INSTITUTIONAL_FLOW` (US_EQUITY)
**Status:** FROZEN. Written and committed BEFORE any forward return, event
return, rank IC, book or t-statistic for this family was computed.

This document is the contract. It is not edited after results exist. If a
statement here turns out to be inconvenient, the inconvenience is the result.

---

## 0. WHY THIS AXIS, AND WHAT IT IS NOT

The quarterly 13F ownership-breadth axis is CLOSED (commit `8cf9fa3`, verdict
DATA_HOLD, economic conclusion NO USEFUL ALPHA: raw rank IC 0.004 / 0.001,
orthogonalisation REVERSED the sign, no incremental utility over the incumbent).
It is not revisited, rescued, re-signed or re-scored here.

This axis is a different object. 13F breadth is a **quarterly census of passive
holders**, which is largely a restatement of size and liquidity — that is why
it died. A Schedule 13D/G is an **event**: a single identified person crossing
5% beneficial ownership of a specific issuer, disclosed on a statutory clock,
with a named economic purpose. The information is discrete, attributable and
tied to an instant rather than to a quarter-end.

**Economic hypothesis.** A meaningful NEW or INCREASED control block reported on
Schedule 13D/G may carry information about subsequent equity returns, because a
large beneficial owner has committed capital to a concentrated position and may
exercise monitoring, engagement, activism or informed positioning that the
market has not yet priced.

**Direction: TWO-SIDED.** No sign is pre-specified. A sign read off the sample
is a DISCOVERY, is labelled `DISCOVERY_ONLY`, and cannot retroactively qualify
this experiment. It may motivate a NEW prospective hypothesis later.

---

## 1. DATA — FREE SEC ONLY

Source: EDGAR. No purchase, no subscription, no licence, no key, no account.
`paid_dollars: 0` is an asserted and recorded property of the acquisition.

Forms in scope, and their two EDGAR spellings, which are the SAME schedule:

| canonical | EDGAR before 2024-12-18 | EDGAR from 2024-12-18 |
|---|---|---|
| `13D`   | `SC 13D`   | `SCHEDULE 13D`   |
| `13D/A` | `SC 13D/A` | `SCHEDULE 13D/A` |
| `13G`   | `SC 13G`   | `SCHEDULE 13G`   |
| `13G/A` | `SC 13G/A` | `SCHEDULE 13G/A` |

`SC 13E3` (going-private) is NOT a 5% beneficial-ownership schedule and is
excluded by definition, not by filtering after the fact.

### 1.1 Measured feasibility (Phase 1, before any return)

| measurement | value |
|---|---|
| schedules, 2009-01-02 → 2026-09-09 | **97,613** |
| schedules in the full acquired history (from 1994) | 136,395 |
| distinct subject issuers | 1,070 |
| acceptance timestamp populated | **1.0000** |
| accession populated | 1.0000 |
| primary document named (2001+) | 1.0000 |
| form mix (2009+) | 13D 1,363 · 13D/A 6,649 · 13G 24,830 · 13G/A 64,771 |
| initial schedules (not `/A`) | 29.5% of all schedules |
| percent-of-class extracted, text era | **0.983** (n=120 sample) |
| percent-of-class extracted, structured era | 1.000 for 13G; 13D required a second element vocabulary (below) |
| CUSIP extracted, text era | 0.950 |
| eligible-universe identity coverage | mean **0.9528**, worst **0.8963** |
| panel rows carrying a CIK | 1,130 of 1,897 |

**Feasibility verdict: PASS.** The event stream is complete, timestamped and
large. The identity limitation in §3.2 is declared, measured and carries its own
stopping rule.

### 1.2 Three measured facts that would silently corrupt the stream

1. **EDGAR renamed the form type** at the structured mandate. The canonical
   keep-list `alpha_agent.r63.acquire.keep_form` matches the prefix `SC 13`, so
   `SCHEDULE 13D` fails it and every schedule filed since 2024-12-18 is absent
   from the shared filings index — while present in the raw histories. Taking
   the index at face value ends the event stream on 2024-12-13 and makes 21
   months of the panel look eventless. Both spellings normalise together.

2. **`acceptanceDateTime` is UTC**, despite EDGAR's business-day rule being
   Eastern. Settled without assuming any cutoff: EDGAR accepts submissions only
   06:00–22:00 Eastern, and the hour histogram of 97,613 filings has its dead
   zone at raw 03:00–07:00 — raw 03:00 contains exactly ONE filing, which is
   22:00 Eastern, EDGAR's closing time. Read as Eastern, 12.95% of filings would
   fall in hours when EDGAR is shut; read as UTC, 1.98%. Reading the stamp as
   published would move most after-hours filings a full session early — a
   look-ahead.

3. **The submissions index names the XSL-RENDERED document.**
   `xslSCHEDULE_13G_X02/primary_doc.xml` is HTML wearing an `.xml` extension;
   the machine-readable original is the same leaf at the accession ROOT. The
   identical trap the N-PORT measurement hit.

Additionally: **the two structured schedules do not share an element
vocabulary.** A 13G says `<classPercent>` and `<issuerCusipNumber>`; a 13D says
`<percentOfClass>` and `<issuerCUSIP>`. Reading only the 13G spelling silently
drops every structured 13D — the activist half of the axis.

---

## 2. POINT-IN-TIME CONTRACT

**The PIT boundary is the SEC ACCEPTANCE INSTANT**, converted from UTC to
Eastern with the US daylight-saving rule in force for the year. EDGAR's filing
DATE is a label assigned by a cutoff rule and is NOT used as the availability
boundary anywhere.

**Decision session.** For an event accepted at Eastern instant `a`, the decision
session `t` is the FIRST session whose 16:00 Eastern close is STRICTLY after
`a`. A filing accepted at 14:00 on session `s` has `t = s`; a filing accepted at
19:00 on session `s` has `t = s+1`. There is no same-day look-ahead: the
information is public before the close that defines `t`.

**Entry.** At the close of session `t+1`, the estate's canonical NEXT_CLOSE
convention (`alpha_agent.r63.pit.forward_compound`), which is one further
session of broadcast lag. This is deliberately conservative and identical to
every other family in the estate.

**The announcement window is deliberately forgone.** The return from close `t-1`
to close `t` is reported as a clearly-labelled NON-TRADEABLE DIAGNOSTIC so that
"no information" can be distinguished from "information we decline to claim".
It can never contribute to qualification.

Nothing future-dated is used: no later amendment restates an earlier event, no
identifier mapping from after the decision date is consulted, and no filing is
counted before it was accepted.

---

## 3. IDENTITY

### 3.1 The contract

Identity is **subject-issuer CIK → panel row**, one hop, and is authoritative
rather than fuzzy: EDGAR indexes a Schedule 13D/G under the SUBJECT issuer's own
CIK (measured: 80,335 `SC 13*` rows across 833 of 842 issuers before the
universe was completed), and the estate owns an effective-dated
security → CIK bridge. **No CUSIP licence is required and no CUSIP is needed.**

Routes, in order, all authoritative, none fuzzy:

1. the owned R58 CIK bridge;
2. SEC `company_tickers.json` — ONE record stating both ticker and CIK, with
   class-share punctuation normalised on both sides (`BF.B` ↔ `BF-B`), used only
   for STILL-LISTED rows, where the panel and the SEC both spell the security by
   its CURRENT ticker.

**Issuer-name matching is not used anywhere, in any tier, for any purpose.**
Where a CIK maps to more than one panel row (dual class, or a relisted ticker —
42 CIKs, 86 rows), the event is attributed to every such row and the ambiguity
is reported; where identity is unresolved the name is DROPPED and counted.

### 3.2 THE DECLARED LIMITATION — measured, not discovered afterwards

1,130 of 1,897 panel rows carry a CIK. On the eligible cross-section the
assessable share is mean **95.28%**, worst **89.63%**, and **46.9%** of decision
sessions sit below 95%.

The gap is NOT random, and its direction is the dangerous one for THIS axis:

| | share of cross-section | leave the investable universe within 252 sessions |
|---|---|---|
| identified | 95.3% | **3.08%** |
| unidentified | 4.7% | **17.82%** |

The unidentified are 4.7% of names but **22.2% of all exits**. A Schedule 13D is
often the public first step of a control contest that ENDS IN ACQUISITION, and
an acquired company leaves the panel — so the experiment is least able to see
exactly the outcome the hypothesis predicts. This is a property of the estate's
owned identity layer (767 securities sit in its `unresolved_backlog`, 95 with
conflicting candidate CIKs), it cannot be closed with free authoritative data,
and it will NOT be closed with the fuzzy name matching this axis forbids.

**It is declared here, before results, because a null result on this axis is
partly a null result about what can be seen.**

---

## 4. THE TWO EVENT DEFINITIONS (the ONLY two)

### CELL A — NEW CONTROL BLOCK

An event for issuer *i* is the acceptance of an **INITIAL** Schedule — canonical
form `13D` or `13G`, i.e. NOT an amendment.

This is the legal definition of a new block, not a proxy for one: Rule 13d-1
requires an initial Schedule when a person FIRST crosses 5% beneficial
ownership; an existing 5% holder files amendments. The event therefore needs no
document, no percent and no filer identity, and is observable at 100% of the
stream.

### CELL B — MATERIAL BLOCK INCREASE

An event is the acceptance of a Schedule whose reported block percent EXCEEDS
the same reporting person's most recent previously-accepted percent for the same
issuer by **≥ 1.00 percentage point**.

**The threshold comes from law, not from returns.** Rule 13d-2(a) deems an
acquisition or disposition of **1% or more** of the class to be MATERIAL,
obliging a prompt amendment. 1.00 pp is therefore the regulator's own
materiality standard for exactly this quantity. No other threshold is tried. No
grid of cutoffs is searched. No cutoff is chosen after seeing a return.

**A filing's block percent** is the MAXIMUM cover-page percent across the
filing's reporting-person pages. A joint filing repeats the cover page per
person; the group's aggregate is at least the maximum, and taking the maximum
avoids double-counting overlapping holdings. Declared before results.

**Filer identity** for the "same reporting person" comparison is the EDGAR
full-index pairing of subject CIK and filer CIK on one accession (measured:
19,053 of 19,202 accessions in 2019Q1 carry exactly two CIKs), corroborated in
the structured era by `<reportingPersonCIK>`. A filing whose filer cannot be
identified is DROPPED from Cell B and counted.

**No third cell will be added, before or after results.**

---

## 5. EVENT CLEANING — fixed in advance

| case | treatment |
|---|---|
| initial vs amendment | canonical form carries `/A`; Cell A admits only non-`/A` |
| 13D vs 13G | pooled within each cell; the split is reported as a DIAGNOSTIC only and is not a cell |
| passive vs activist | not separately observable before the structured era; NOT used as a cell |
| amended percentages | an amendment supersedes that filer's prior percent from ITS OWN acceptance instant forward, never backwards |
| reductions / exits | a decrease, or a percent of 0, is NOT a Cell B event; it updates the filer's state |
| duplicate filings | deduplicated on (issuer CIK, accession); an issuer's `recent` block and older shards overlap and would otherwise invent events |
| multiple filers, one block | events for the same issuer resolving to the SAME decision session collapse to ONE event |
| issuer ticker changes | identity is the CIK, which does not change with the ticker; the panel row is the target |
| mergers / delistings | the panel retains delisted rows; a position is held to horizon or to the name's last eligible session, whichever is first |
| weekends / holidays | the decision session is the next SESSION on the owned trading calendar |
| after-market filings | handled by the 16:00 Eastern close rule in §2 |
| the 2024 rule change | shortened 13G deadlines raised filing frequency (7,729 schedules in 2024 vs 4,602 in 2023); the event definitions are unchanged and the change is reported as a regime diagnostic |

---

## 6. HORIZONS — FROZEN

**5, 21 and 63 sessions.**

Economic justification, fixed before results: 5 sessions covers the
post-announcement adjustment, 21 the monthly drift, 63 the quarter over which
engagement or a control contest would plausibly develop.

**Multiplicity consequence, accepted in advance:** three horizons × two cells =
**six tests in ONE declared family**, so the family's own Benjamini-Hochberg
denominator is **m = 6**. This is strictly HARDER than the two- or four-test
alternative and touches no other family's denominator, so it expands this
family's burden and not the campaign's. No horizon is added, removed or
searched after results.

---

## 7. MEASUREMENT — two arms, both frozen

### Arm 1 — EVENT STUDY (the primary economic measure)

Entry exactly per §2. For each event and horizon, four returns:

* **raw** — the name's compounded forward return;
* **market-adjusted** — minus SPY total return over the identical window;
* **sector-adjusted** — minus the equal-weight forward return of the eligible
  same-GICS-sector names over the identical window;
* **equal-risk** — the market-adjusted return divided by the name's 63-session
  realised volatility, so a large move in a quiet name is not counted as a
  larger discovery than the same move in a volatile one.

Aggregation clusters by decision session; the t-statistic is Newey-West at the
canonical lag for (horizon, cadence) from `alpha_agent.r63.pit`. Overlapping
events are discounted to EFFECTIVE observations; overlapping windows are never
counted as independent.

### Arm 2 — CROSS-SECTIONAL PANEL (the incremental test)

The same events as a cross-sectional signal on the CANONICAL R63 equity grid
(`experiments.assemble_equity`, cadence `min(h, 21)`), scored by the ONE canonical
scorer `alpha_agent.r63.sensitivity.run_cell`. No second scorer is written.

The signal is defined for EVERY eligible name (0 where there is no event), never
NaN, because a NaN would silently restrict the scored rows to event names only.

**Declared weakness:** the panel arm places entry on the next grid point, so it
dilutes by up to one cadence, and a sparse binary signal has many
cross-sectional ties. It is the right instrument for the INCREMENTAL question
and the wrong one for the MAGNITUDE question. That is why both arms exist, and
why neither is dropped after seeing which is kinder.

### Controls the effect must survive

Incremental information beyond, all measured, none optional:

* the incumbent `fundamental_momentum_50_50_v1`;
* every owned baseline dimension for US_EQUITY (`ontology.baseline_for`) —
  price-state, fundamental, short-positioning and the rest;
* 13F ownership breadth is reported as a DIAGNOSTIC only, because it is already
  economically dead and cannot be a required control.

Orthogonalisation is a CONTEMPORANEOUS cross-sectional regression, which uses no
future information; time-series residualisation is not used.

---

## 8. COSTS

The canonical equity ladder, **1 / 2 / 5 bp per side**, plus the estate's own
single-name desk rate `EQ_COST_RATE_PER_SIDE` = **12.5 bp per side**, which is
the honest cost of this asset. The frozen gates are judged at the frozen ladder;
the desk rate is reported alongside and can only make qualification harder.

An event study pays entry AND exit.

---

## 9. MULTIPLE TESTING

Owner: `alpha_agent.r31.multiple_testing` (Benjamini-Hochberg, q = 0.10), the
same owner R39 uses. ONE declared family, six cells, **m = 6**.

The denominator is NOT reset. Rejected cells STAY in the denominator. No variant,
transformation, sign or horizon is added after observing a return.

---

## 10. QUALIFICATION — existing gates only

Evaluated in this order. No threshold is invented; every number below already
exists in the estate.

1. **Coverage.** If more than `MAX_UNCOVERED_SHARE` = 0.20 of decision sessions
   resolve under `MIN_DATE_COVERAGE` = 0.95 of the eligible universe →
   **DATA_HOLD**.
   *Measured in advance: 46.9% of sessions are below 0.95, so this gate is
   EXPECTED TO FIRE.* It is frozen unchanged anyway, because weakening a rule
   after measuring that the data fails it is precisely the move this discipline
   exists to prevent. The economics in §7 are computed and reported in FULL
   regardless, so the axis is answered on its merits and not only on a gate —
   exactly the separation the 13F result required.
2. **Missingness bias.** If the unassessable-but-eligible names' mean forward
   return differs from the assessable names' by more than the estate's
   materiality floor `MATERIALITY_ANN_NET` = 1.5%/yr → **DATA_HOLD**, because
   the estimate is then biased by what cannot be seen.
3. **Scorer floors.** `run_cell` returning DATA_HOLD, or fewer than
   `MIN_EFFECTIVE_PERIODS` = 36 effective independent periods →
   **NEED_MORE_EVIDENCE**.
4. **Standalone.** Conditional |t| below `CONDITIONAL_T_FLOOR` = 2.0 →
   **NO_EDGE**.
5. **Multiplicity.** Failing BH at q = 0.10 over m = 6 → **NO_EDGE**.
6. **Incremental.** Redundancy `REDUNDANT`, or orthogonalised |t| < 2.0, or
   incremental-vs-incumbent |t| < 2.0 → **NO_INCREMENTAL_INFORMATION_EDGE**.
   If the effect disappears after orthogonalisation it is NOT rescued.
7. **Materiality.** Net annual increment below 1.5%/yr → **NO_EDGE**.
8. Otherwise → **QUALIFIED**.

**CAPITAL ELIGIBLE remains NO** unless every qualification gate AND every
existing forward-evidence gate is satisfied. Nothing in this document can
promote a model, allocate capital, or create a proposal, order or fill.

---

## 11. WHAT WOULD FALSIFY THE HYPOTHESIS

A null result here means: a new or materially increased 5% control block,
observed at its true acceptance instant and entered at the next close, carries
no information about subsequent return that the estate's owned families do not
already carry — subject to the declared blindness in §3.2.

## 12. SAFETY

RESEARCH ONLY. No purchase, no subscription, no licence, no model promotion, no
capital allocation, no portfolio mutation, no proposal, no order, no fill, no
backfill, no live deployment, no live write. The live checkout is not touched.
