# Preregistration — institutional ownership breadth (13F), US equities

**Status:** FROZEN BEFORE ANY RESULT IS COMPUTED. Nothing in this document has
been fitted to an outcome, because no outcome exists yet. Written 2026-09-13.

**Why this exists now.** `alpha_agent.r63.ontology` declares four
`POSITIONING_AND_FLOW` dimensions. Three have owned families.
`OWNERSHIP_INSTITUTIONAL_FLOW` (US_EQUITY, 13F breadth change, PIT =
`FILING_ACCEPTANCE_INSTANT`) has none, and `r63.sourcing` records it as
`BLOCKED_NO_CUSIP_BRIDGE` with the remedy "a CUSIP Global Services licence
(`LICENCE_REQUIRED_NOT_PURSUED`)".

**That blocker is falsified, and the measurement is what unlocks this.** See the
measurement section below. A free, authoritative CUSIP→ticker bridge exists in
SEC's own published data at 99.8 % coverage of the S&P 500, so the axis is
reachable with **$0 of new subscription**.

---

## 1. The measurement that unblocked it (already performed, 2026-09-13)

SEC Fails-to-Deliver files (`cnsfails<YYYYMM><a|b>.zip`, semi-monthly, free, no
key, 433 files available) publish `SETTLEMENT DATE | CUSIP | SYMBOL | QUANTITY |
DESCRIPTION | PRICE`. **One publisher states CUSIP and SYMBOL in the same
record**, which is what makes the join authoritative rather than a name match.

Measured against the SPDR S&P 500 Trust N-PORT (CIK 0000884394), 475 real CUSIPs:

| FTD files unioned | distinct CUSIPs bridged | coverage of the 475 |
|---|---|---|
| 1 | 13,255 | 94.7 % |
| 2 | 14,947 | 99.6 % |
| 8 | 17,481 | **99.8 %** (474 / 475) |

- The one miss, `436CVR021` ("CONTRA HOLOGIC INCORPO"), is a contra/CVR clearing
  placeholder, not a listed equity.
- Ambiguity is **5 of 474**. Two are REAL ticker changes (`BK`→`BNY`,
  `ECHO`→`SATS`); three are clearing placeholders (`DDZZZZ`, `XOMXXXX`,
  `HONXXXX`, `6205REGWAY`).
- 98.9 % of single-symbol matches are present in the owned Norgate universe.
- The fuzzy alternative actually on record — SEC `company_tickers.json` joined on
  a normalised issuer name — reaches only **83.7 % with 64 ambiguous**.

---

## 2. Frozen identity contract

1. **Bridge source:** SEC Fails-to-Deliver files only. No purchased identifier
   product. No fuzzy name matching anywhere in the identity path — a name join
   may be used to *report* a diagnostic, never to assign identity.
2. **Placeholder rule (declared here, before use):** a SYMBOL is rejected as a
   clearing placeholder if it matches `^.*(ZZZZ|XXXX)$` or `^[0-9]`. This is a
   pattern rule fixed in advance, not a per-case judgement.
3. **Ambiguity rule:** a CUSIP that maps to more than one surviving SYMBOL is
   resolved **effective-dated** through the estate's existing identity layer
   (`alpha_agent.historical_identity`), never collapsed to "the most common one".
   A CUSIP that cannot be resolved effective-dated is DROPPED, and the drop count
   is reported with the result.
4. **PIT rule:** for a decision dated *D*, only FTD files **published before D**
   may contribute to the bridge, and only 13F filings whose
   `FILING_ACCEPTANCE_INSTANT` is strictly before *D*. A bridge built from future
   files is a look-ahead even when the mapping is "obviously" stable.

## 3. Frozen hypothesis

> **H:** The one-quarter CHANGE in the number of distinct 13F filers reporting a
> position in a US equity ("ownership breadth change"), measured strictly from
> filings already accepted by EDGAR, carries cross-sectional information about
> that equity's subsequent excess return that is not already carried by the
> estate's owned price, fundamental and short-positioning families.

The alternative worth stating plainly: 13F is filed **45 days after** quarter end
and is therefore stale by construction. This experiment is as much a test of
whether that staleness destroys the signal as of the signal itself.

## 4. Frozen specification

| item | frozen value |
|---|---|
| universe | the owned Norgate US equity universe, survivorship-safe, including delisted |
| signal | Δ (distinct 13F filers holding name *i*) quarter over quarter, cross-sectionally ranked |
| normalisation | cross-sectional rank to [0,1] within each rebalance date |
| direction | **NOT PRE-SPECIFIED.** Two-sided test. A sign read off the sample is a discovery, not a confirmation, and is labelled as such |
| horizons | 21 and 63 sessions, non-overlapping |
| rebalance | quarterly, on the first session after each 13F deadline |
| costs | the estate's canonical ladder, 1.0 / 2.0 / 5.0 bp per side, applied to both legs |
| benchmark | equal-weight universe return, and the incumbent `fundamental_momentum_50_50_v1` |
| scorer | `alpha_agent.r63.sensitivity.run_cell` — the ONE scorer, not re-implemented |
| PIT engine | `alpha_agent.r63.pit` |

## 5. Gates, fixed in advance

- Minimum **36 effective independent periods** (`MIN_EFFECTIVE_PERIODS`) before
  any verdict other than `NEED_MORE_EVIDENCE` may be issued.
- Multiple-testing burden carried through `alpha_agent.r39.burden`; the family is
  declared as ONE family with the two horizons as its cells, so the denominator
  cannot be quietly reset.
- Incremental utility against the owned families is **required**, not optional: a
  result that does not survive orthogonalisation against price, fundamental and
  short-positioning factors is `NO_INCREMENTAL_INFORMATION_EDGE`.
- **Stopping rule:** if the identity bridge resolves under 95 % of the universe
  on a rebalance date, that date is reported UNCOVERED and excluded, and if more
  than 20 % of dates are UNCOVERED the experiment returns `DATA_HOLD` rather than
  a verdict.

## 6. What would falsify it

A two-sided test whose burden-corrected p exceeds the frozen threshold, or whose
edge does not survive the cost ladder at 1.0 bp, or which fails orthogonalisation
against the owned families. Any of those returns a NEGATIVE result, which is a
legitimate outcome and is recorded as one.

## 7. Standing constraints

- **NO PURCHASE.** The whole point is that the axis is reachable for $0. If it
  turns out to need a paid identifier product, the answer is to stop, not to buy.
- Research only. No capital, no promotion, no order, no fill.
- This document is frozen. If the specification changes, the change is a NEW
  preregistration with its own identity, and this one is retained and marked
  superseded.
