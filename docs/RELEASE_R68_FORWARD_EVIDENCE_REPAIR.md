# R68 — Forward Evidence Repair and Alpha Activation

**Run id** `R68_FORWARD_EVIDENCE_REPAIR_AND_ALPHA_ACTIVATION`
**Date** 2026-09-23
**Predecessors** R67 `f820f11`, R66 `8d5302b`

---

## 1. The business failure, and what actually fixed it

R67 measured that four registered forward strategies had **no operating
prediction producer**, and then declined to wire one on the ground that
"attaching a cadence producer to an identity that was registered when no such
producer existed changes what that identity means."

R68 examined that claim against the records and found it **false in this case**.

| Evidence | Where |
|---|---|
| All four frozen constructions declare `rebalance_cadence_sessions = 21` | the R58 challenger artifacts |
| The registrar copied that construction verbatim into each registration | `api.forward_challenger_registry` |
| The accrual owner's state at every boundary since was `AWAITING_NEW_GOVERNED_FREEZE`, whose own definition is *"the originating owner never froze a new decision"* | `api.canonical_forward_accrual` |

A cadence producer does not change what these identities mean. It is **the
capability they were declared to have and never had**. The determination, its
evidence, and the one-step procedure for reversing it are written to
`R58_PRODUCER_ACTIVATION_DECISION.json` by the producer itself, because it is a
governed judgement and a human owner must be able to read it and disagree.

Nothing was loosened to achieve it:

- The declared emission boundary is `PRIOR_SESSION_ONLY` — the strictest rule in
  the vocabulary, and byte-for-byte what an **absent** declaration already
  resolved to. A test asserts the two produce the identical window instant.
- Each per-session decision carries the **original adoption freeze's**
  `record_hash`, so the registrar's integrity binding is unchanged and a book
  whose hash does not match is still refused.
- `execution_offset_sessions` stays 0, so no R58 decision grid moved.
- No adoption freeze file was opened for writing. The one genuine observation of
  2026-09-10 stands. No session that passed while no producer existed was
  decided — those are reported `MISSED` and are never written.

## 2. What was broken, and what code changed

| Owner | Change |
|---|---|
| `alpha_agent/r68/r58_cadence_runtime.py` | **NEW.** The cadence producer. At each boundary it re-scores the frozen specification through `alpha_agent.r58.challengers.build` — the originating owner's own scorer — and freezes through the estate's one decision owner. Contains no formula, no rank, no universe rule, no weight. |
| `alpha_agent/alpha_recovery/prospective_decision.py` | Store root parameterised (`root=`), and `declare_policy` can now declare the prior-session boundary. One decision-freezing implementation for the whole estate instead of a second 458-line copy. Every pre-R68 call path resolves to the same bytes. |
| `api/canonical_forward_accrual.py` | R58 resolves a **per-session** decision, falling back to the adoption freeze. Release-specific branches generalised to one `_PER_SESSION_DECISION_ROOTS` table. New `armed_cadence_sessions` lets a declaring release see ONE future boundary while its window is still open — appended to the *grid*, never to the session list, because injecting it into the sessions would shift every `[::cadence]` index behind it. |
| `alpha_agent/r52/runtime.py` | New `r58_cadence_prospective_decision` stage, inside the existing lock, after the three per-session owners and **before** the accrual. No second scheduler. |
| `alpha_agent/r52/eligibility.py` | The stage is **ungated**. Gating it would forfeit a boundary on the one evening that mattered to save work that costs nothing on the other 19 sessions. |
| `alpha_agent/r57/panel.py` | `build_panel(end=, force=)`. One builder, two stores: the frozen research panel (2026-09-03) is never opened for writing. |
| `api/forward_producer_health.py` | **NEW.** See §4. |
| `alpha_agent/r67/forward_producer.py` | Reads the producer declaration from the health owner instead of holding a second copy. |

## 3. The chain, proved

`tests/test_release68_forward_evidence_repair.py` — **41 passed**. It proves,
for **each of the four strategies** separately:

```
registered specification -> eligible boundary -> current PIT inputs
  -> originating-owner signal -> immutable frozen decision
  -> canonical emission -> pending observation -> genuine maturation
```

and the refusals: duplicate invocation (idempotent), conflicting second freeze
(refused, held record stands), crash before the accrual (recovers), freeze after
the window shut (`MISSED`, never backfilled), missing provider data, stale panel,
a panel that holds the decision session itself, missing adoption freeze, changed
adoption freeze (`INTEGRITY_HASH_MISMATCH`), empty book, and no decision ever
frozen for a past session.

**What is simulated and what is not, stated plainly.** The hermetic chain tests
inject the weight book — a ~1,900-symbol Norgate panel cannot be opened in a
hermetic test. The scorer is proved **separately and for real**:
`test_the_live_scorer_produces_a_book_for_every_challenger` runs
`alpha_agent.r58.challengers.build` against the live forward panel and is
*skipped* when that panel is absent rather than silently passing.

Measured live, 2026-09-22, 500-name eligible universe:

| Challenger | held | scored |
|---|---|---|
| `R58_SHORT_VOLUME_PRESSURE_V1` | 50 | 498 |
| `R58_DISCLOSURE_INTENSITY_V1` | 50 | 333 |
| `R58_FUND_MOMENTUM_VETO_V1` | 50 | 295 |
| `R58_FCF_PURE_V1` | 50 | 443 |

## 4. Silent orphans are now impossible

`api/forward_producer_health.py` owns the registration lifecycle. Eight states,
and **`ACCRUING` is unreachable without a live producer by construction** —
which is the whole defect, because "registered" and "accruing" had been the same
word:

`REGISTERED_NOT_ARMED` · `ARMED_FOR_NEXT_DECISION` · `ACCRUING` ·
`AWAITING_PUBLISHED_DATA` · `MISSED_DECISION_GAP` · `PRODUCER_FAILED` ·
`SUPERSEDED` · `RETIRED`

- **Heartbeat.** Per producer stage: last run, last state, what it said. Read
  from the runtime's existing run journal — the fact was always being written
  and was never being read.
- **The invariant.** `producer_coverage()` fails on any active registration with
  no *declared* production path. An absence the file has never heard of is a
  failure, not a default — that is exactly the shape the R58 four had.
- **Build-failing.** `check_release68_forward_producer_health` asserts every
  declared stage exists in the runtime and runs before the accrual.
- **R67's tests corrected.** They asserted the four had *no* producer. That
  assertion was true and is now the wrong test: it would go green again the day
  somebody unwired them. They now assert `REACHABLE_ON_CADENCE`.

## 5. Research integrity

**The leakage check is now performed, not claimed.**
`alpha_agent/agents_v2/leakage.py` reads a feature's **formula**, derives the
newest session index it touches, and compares it to the dataset's declared
timing rule. The exact R67 feature is refused on its own text: effective lag
**0** against a required **1**.

It is graded, and the grading is declared rather than accidental:

| Case | Outcome |
|---|---|
| Formula present and leaky, or label contradicts formula | **REFUSED** — cannot be published |
| Formula absent | Publishes, `verified: false`, verdict `NOT_MACHINE_VERIFIABLE`, and the flag is copied into the frozen spec of every experiment built on it |

`formula` has never been a required lineage field, so refusing every feature
without one would reject essentially every feature set the estate has published
— a change far larger than closing the defect. The remaining gap is **named in
code** (`REQUIRE_MACHINE_VERIFIABLE_FORMULA = False`) and is now measurable
instead of invisible.

**The census could present a measured failure as untested.** It keyed a family
by `(asset_class, economic_family)` and folded `information_family` into a set.
A mechanism needs **four** parts — the same four the burden ledger counts.

Systematically re-checked against research memory, all five top-ranked queued
proposals carry settled rows, and the correction **does not overstate itself**:

| Verdict | Proposals |
|---|---|
| Outright false novelty claim | `FUT_BASIS_MOMENTUM_COMMODITY`, `FUT_OPEN_INTEREST_GROWTH` |
| Settled mechanism proposed as a sample replication | `EQ_EXTENSION_LIQUIDITY_PREMIUM`, `EQ_EXTENSION_RESIDUAL_MOMENTUM` |
| Economic family settled, **mechanism not established** (checked on a coarser key) | `XA_TERMS_OF_TRADE_RELATIVE_VALUE` |

Also: `ResearchMemory.is_novel` returned `novel: True` for a mechanism that
already carried a settled row, because it hashes `(family, spec)`. New
`mechanism_state()` answers the family-key question, and `is_novel` now attaches
it so `novel: True` cannot be read without seeing a settled sibling. The
existing three callers are unaffected — a caller that passes nothing gets
exactly what it always got.

Nothing was deleted. Every false claim stays visible beside its correction.

## 6. Research measured

The R68 director **refused all three proposed cells** and charged **zero
burden**. That was the honest outcome and it is reported as one.

- **`r68_03` curve curvature — REJECTED.** Curvature needs three curve points.
  Across all 69 certified market files the column union is
  `Date, close, held, open_interest, ret, ret2, slope_ann, volume` — **two legs**.
  The signal cannot be built. (The campaign spec asserted it needed no new
  collection. That was wrong.)
- **`r68_02` dividend declaration — REFUSED, novelty claim false.**
  `H_63012714_9e5453e70d5c` is the same hypothesis, `DATA_HOLD` on coverage
  0.717 against a frozen 0.80 floor. The spec aimed its novelty comparison at
  `DIVIDEND_MONTH_DEMAND_PREMIUM` and pointed away from the actual collision.
- **`r68_01` SEC comment letters — HELD, novelty upheld.** `SEC_COMMENT_LETTER`
  carries **0 of 8,472** rows and the regulator-initiated vs issuer-initiated
  distinction was verified row by row. Blocked on data: only a 2-month EDGAR
  fragment exists locally.

**One real measurement was produced.** The reopen condition on
`H_63012714_9e5453e70d5c` is `COVERAGE_CLEARS_THE_FROZEN_FLOOR`, and it now has
an auditable answer on the S&P 500 PIT universe — 15,566 records over 309
symbols, full 18-year per-year series in `DATA_CERTIFICATION.json`:

```
2009 0.7795   2010 0.7715   2011 0.7685 (min)   2012 0.8227   2013-2026 0.95-1.00
```

**Verdict: DOES_NOT_CLEAR** (min 0.768508 < 0.80). Earliest year continuously at
or above the floor: **2012**. The parent stays `DATA_HOLD`.

The director then refused to rule on the trimmed-sample question **because the
series existed only in a chat report and in no artifact** — a correct refusal,
and the reason this release convened. It was written down, and the ruling's
pre-committed logic is recorded for when the remaining substrate exists.

Two corrections this release owes its own campaign spec: two of its three
novelty claims were presented as verified against the information-family
histogram, and one was false while another rested on data that does not exist.
The author had read the defective census.

## 7. The capital path

`scripts/r68_capital_path_report.py` → `CAPITAL_PATH.json`. **8 registered, 7
can advance toward capital, 0 orphaned, 0 producer-failed.**

**Multi-leg accounting on the research side is SUPPORTED, and measured rather
than asserted.** A market-neutral long/short book run through
`engine.shadow_portfolio_evidence`: `has_short_leg: True`, gross 2.0 against net
0.0, entry cost **$250 — exactly twice** the long-only control's $125 because
cost is charged on gross, 14 curve points, and the short leg's return subtracted
(12.11% against the control's 14.95%).

So no engineering was missing and none was invented. What is **not** modelled is
stated rather than implied: **margin** (a `capital_pool` question, not a kernel
question), **cash on a market-neutral book** (`max(0, 1-NET)` reads as 100% cash
at 2.0 gross — a convention, and it omits the short rebate), and **borrow cost**.

**Human-gated, unchanged:** `api.capital_pool` declares `long_only` and
`short_exposure_supported = False`; the gate applies
`SHORT_LEG_NOT_EXPRESSIBLE_LONG_ONLY_BOOK`. Every hedged structure the estate
has measured is operationally inexpressible at any size, at any NAV, under any
cash policy. R68 does not change it, route around it, or build the margin model
that would only matter after somebody with authority changed it. R66's
undeclared `1.0x` gross-notional convention is still undeclared.

## 8. What remains blocked

| Blocker | Nature |
|---|---|
| 0 matured observations against a 60-observation floor | **Time.** Soonest floor: 305 sessions (FX carry). The R58 four: 1,281 sessions (~5.1 yr) at their own declared 21-session cadence. |
| Short exposure | **Governance.** Human-gated policy. |
| Gross notional > 1× NAV | **Governance.** Nobody with authority has declared a threshold. |
| SEC comment letters | **Collection.** Free and feasible; ~2 months on disk. |
| Dividend declaration coverage 2009-2011 | **Data vintage.** Measured and recorded. |
| Extension mid/small-cap panel | **Audit.** Frozen pending an identity-bridge audit. |
| `REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1` | **`MISSED_DECISION_GAP`** — it missed the 2026-09-23 open. Pre-existing, now visible because the lifecycle vocabulary has a word for it. |

## 9. Safety

No order, no fill, no proposal, no approval, no model promotion, no capital
allocation, no sleeve activation, no operational-store write, no holding, cash
or NAV change, no purchase, no backfill. The frozen R57 research panel is
byte-unchanged (`date_end 2026-09-03`, `manifest_hash 9c35d293…`). Every
historical record is preserved; corrections are append-only.

**No new forward evidence was generated today, and none should have been.** The
next legitimate R58 decision boundary is **2026-10-09**. The producer reports
`R58_BOUNDARY_BEYOND_THE_FREEZE_LEAD`, 13 eligible sessions away, and declines
to freeze. A decision emitted today would be a fabrication.
