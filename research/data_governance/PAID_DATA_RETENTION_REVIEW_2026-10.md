# Paid data retention review — October 2026

**Decision:**

| Provider | Decision | One-line reason |
|---|---|---|
| **Norgate Data** | **KEEP_CORE** | Every price, curve and survivorship layer in the estate rests on it, and nothing owned or free replaces it. |
| **EODHD** | **KEEP_PROVISIONALLY** | Its value now sits in three forward-only archives that started on 2026-10-02. Cancelling destroys them. But none of its 7 dependent experiments has survived yet. |

Nothing was cancelled, bought or trialled. Unknown prices were not guessed. Machine-readable evidence is in
`PAID_DATA_RETENTION_REVIEW_2026-10.json`.

## Norgate Data — KEEP_CORE

- **Status.** Active. The updater has been running since 2026-09-29, and the `norgate_local` collector last succeeded 2026-10-03.
- **Cost.**
  - World Futures: USD 270/yr (operator-stated).
  - US equities tier: unknown.
  - Renewal: unknown.
- **What depends on it.** Instrument returns for **all 20 R99** and **all 3 R100** measured experiments, plus R96–R98 equity panels:
  - 63 code files import `norgatedata`.
  - Local caches total 74.9 GB.
- **Unique.** No owned or free source replaces any of these:
  - dated futures contracts with open interest and volume across 105 markets, including ZQ from 1988, SR3/SO3/CRA, and international bond futures;
  - PIT index membership;
  - US delisted equities.
- **Underused.**
  1. Per-contract OI/volume already drives the R96 per-instrument cost model, but the book cost layer still charges flat rates. On the owned model, **29 of 68 futures markets cost more than the flat rate** (2018+).
  2. No OI/ADV capacity check exists.
  3. Capital-event, dividend-yield and unadjusted-close series are never read.

  Adopting (1) is a governed cost decision, because it can change verdicts. It is the next Norgate action, not something to switch on silently.

  Open follow-up: **NORGATE_INSTRUMENT_COST_MODEL_REVIEW**, status **REQUIRES_SEPARATE_GOVERNED_DECISION**. Not implemented in R99.1.

## EODHD — KEEP_PROVISIONALLY

- **Status.** All four collectors are healthy. The weekend pause is policy: these lanes run on trading days, with 4-day maximum staleness.
- **Cost.**
  - Price: unknown (`/api/user` exposes only `monthly`).
  - Renewal: monthly.
- **Forward collection, verified on disk 2026-10-04.**

  | Lane | State | Evidence |
  |---|---|---|
  | News (A1) | **ACTIVE** | 1,775 / 1,775 names captured 2026-10-02; 4-day lookback covers weekends |
  | Economic events (A2) | **ACTIVE** | Immutable snapshots 2026-10-02 and 2026-10-03 |
  | Estimate snapshots (A3) | **ACTIVE** | 1,774 / 1,775 vintages (SNBRQ has no fundamentals) |
  | Earnings history (C2) | **ACTIVE** | Actual, estimate and before/after-market flag in 97.5% of sampled vintages |
  | Dividends / corporate events (C3) | **DONE** | R98 dividend v2 panel certified and tested to NO_ALPHA_EVIDENCE |

- **Still wasted.**
  - **SharesStats and Holders** (short % of float, float, top institutional and fund holders). The daily fundamentals call already pays for them, but the collector discarded them. R99.1 fixed this in the collector. It goes live on the next committed, canonical restart.
  - **ETF_Data holdings.** Not collected. SEC N-PORT is a free quarterly substitute, so it is not worth a lane without a mechanism.
- **Dependency.** 7 measured experiments R95–R100, 0 survivors.
- **Re-review when the first of these happens:**
  1. The archives hold a Discovery window the director accepts for a preregistered forward test.
  2. A paid estimate or consensus vendor is bought, making the EODHD snapshots redundant.
  3. Any archive lane is STALE for more than 4 days. The data scout reports this automatically.
