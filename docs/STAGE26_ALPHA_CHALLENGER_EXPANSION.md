# Stage 26 — Alpha Challenger Launch & New-Information Expansion

**Status:** research complete, forward lane live, no promotion proposed, zero operational mutations.
**Branch:** `stage19-controlled-rebalance` · **Base HEAD:** `a43bebc` (Stage 25)
**Owner module:** [alpha_agent/stage26_challenger_expansion.py](../alpha_agent/stage26_challenger_expansion.py)
**New capabilities:** [alpha_agent/sec_financial_statement_sets.py](../alpha_agent/sec_financial_statement_sets.py) · [alpha_agent/pit_market_equity.py](../alpha_agent/pit_market_equity.py)
**CLI:** [scripts/run_stage26_challenger_expansion.py](../scripts/run_stage26_challenger_expansion.py)
**Tests:** [tests/test_stage26_challenger_expansion.py](../tests/test_stage26_challenger_expansion.py) — 45 hermetic
**Research root:** `D:\Stock_Prediction_app_data\stage26_alpha_challenger_expansion`
**Run id:** `stage26_b69b8f1c58b5fae4` (content-addressed)

---

## 1. What Stage 25 proved, and why Stage 26 did not retest it

Stage 25 ran 28 pre-registered point-in-time fundamental hypotheses across six
economic families. One cleared the released gate and survived Benjamini-Hochberg
over the whole family: **`s25_operating_profitability = (GrossProfit − SG&A) /
Assets`**, rank IC 0.0550 at t 3.63, BH q 0.008, classified `INDEPENDENT_ALPHA`.
Five of the six families closed with evidence. R&D intensity survived seventeen
attacks and failed one — dropping the five best realised outcomes per period left
t 1.89 against a pre-registered bar of 2.0 — and was recorded
`CONCENTRATION_FRAGILE`.

That is as much as history can say. Re-running it would produce the same numbers
and no new information, so Stage 26 does not. **The Stage-25 panel is reproduced
exactly as an integrity check and nothing more:**

| baseline | periods | names | rank IC | IC t | spread t | net25 | turnover |
|---|---|---|---|---|---|---|---|
| `composite_sn_pit` | 66 | 488 | 0.0231 | 2.03 | 2.10 | 4.45 % | 0.16 |
| `mom_6_1` | 66 | 554 | 0.0254 | 1.22 | 1.20 | 4.12 % | 0.61 |
| `ensemble_pit_5050` | 66 | 488 | 0.0322 | 1.83 | 2.09 | 6.70 % | 0.48 |
| `s25_operating_profitability` | 65 | 307 | 0.0550 | 3.63 | 2.15 | 7.14 % | 0.10 |

Every figure matches Stage 25 to the digit on a panel that now also carries
market equity and a third sector tier. The additions perturbed nothing.

What Stage 25 left open was two lanes, and Stage 26 advances both.

---

## 2. Lane A — the challenger is now in genuine forward competition

### 2.1 The freeze

`s25_operating_profitability` is frozen into an immutable, content-hashed
research specification **before** any forward outcome exists:

```
spec_hash 67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e
```

The hash covers the specification and only the specification: the signal
formula, source concepts, expected sign, transform, PIT policy, missing-data
policy, universe contract, cadence, horizon, cost policy, selection-control
family, immutable registry identity, data fingerprints and ensemble structure.

**It deliberately excludes** the candidate's lifecycle state, combined score,
evidence date, active shadow-book id, historical evidence and measured ensemble
deltas. This is not cosmetic. The first implementation hashed the registry row
wholesale, so the value changed the moment the candidate moved
`KEEP_FOR_RESEARCH → SHADOW_BOOK_ACTIVE` — a "frozen" hash that drifts on a
state transition is not a freeze, and the value recorded inside the shadow book
would never have reproduced. It was found and corrected while the book still had
**zero** forward marks, so nothing observed was discarded. A regression now
asserts the hash is invariant across lifecycle changes.

Nothing was refit. No weight was re-optimised because forward tracking was about
to start. That is the entire point of doing it in this order.

### 2.2 The governance defect that had to be fixed first

The canonical activator `tournament.maybe_activate_shadow_books` enrolled every
`KEEP_FOR_RESEARCH` candidate above a combined-score floor of 0.55. Two
candidates are retained and **both clear it**:

| candidate | combined score | verdict |
|---|---|---|
| `s25_operating_profitability` | 0.685 | forward-eligible |
| `s24_rnd_intensity` | 0.674 | **`CONCENTRATION_FRAGILE`** |

A combined score is a weighted aggregate. It cannot express "this factor's
significance depends on its five luckiest names per period", so the released path
would have opened an irreversible forward book on the candidate the prior stage
explicitly refused to call a challenger. On the next production tick, silently.

Stage 26 makes activation **fail-closed**. `shadow_books.require_forward_eligibility_allowlist`
is now `true`, and `maybe_activate_shadow_books` accepts `eligible_candidate_ids`.
With no allowlist supplied, **nothing activates** — regardless of score. This
only ever adds a condition; no existing condition was removed or relaxed.

### 2.3 The two hooks that were inert

Both provider hooks were stubs written for a world with no retained candidate,
and that world ended when Stage 25 retained one:

* **no `inception_provider`** → the activator writes a book with an *empty*
  membership. It would have looked active and measured nothing, forever.
* **`runtime._tournament_mark_provider` returned `None` for every (candidate,
  date)** — its own docstring said the real replay is wired "once a candidate is
  actually retained and a shadow book exists".

Stage 26 fills exactly those two hooks and adds no lifecycle of its own. The NAV
kernel is pure and refuses to post a mark below 90 % priced coverage, because a
partially-priced book would silently assume the unpriced names were flat — a
fabricated observation. The price adapter lives in `runtime` (the owned local
price service is a runtime concern) and degrades to an omitted symbol on every
failure, which becomes an honest `SHADOW_MARK_COVERAGE_MISSING` diagnostic.

### 2.4 The book

| property | value |
|---|---|
| shadow book | `sb_c9_qualityprofi_e490533606` |
| candidate | `c9_qualityprofi_e490533606` (`s25_operating_profitability`) |
| lifecycle | `KEEP_FOR_RESEARCH` → **`SHADOW_BOOK_ACTIVE`** |
| inception | 2026-08-16 |
| membership | 100 positions — 50 long / 50 short, dollar-neutral |
| formation | 2026-08, ranked from data available at inception |
| entry prices | 100 / 100 present |
| **forward marks** | **0** |
| `s24_rnd_intensity` | still `KEEP_FOR_RESEARCH`, **no book** |

Zero marks is the correct state, not an omission. A mark may only exist for a
date strictly *after* inception; writing one today would backdate the very
evidence the book exists to collect.

---

## 3. Lane B — the information frontier

### 3.1 Free SEC acquisition — Stage 25's number-one queue item, closed

Stage 25 named the artefact precisely: **SEC Financial Statement Data Sets,
`sub.txt`**, which carries per submission both the assigned SIC and the
acceptance timestamp — exactly the `(classification, available_at)` observation
`pit_sector.PitSicSeries` already consumes.

| | |
|---|---|
| quarters acquired | **68** (2009Q2 → 2026Q1; 2026Q2 not yet published) |
| observations | 46,680 across 854 CIKs, 263 distinct SIC codes |
| span | 2009-04-15 → 2026-03-31 |
| **acceptance-timestamped** | **100.0 %** |
| `sub.txt` bytes on disk | 126.8 MB |
| **bytes over the network** | **39.9 MB** |

That last pair is the engineering point. Mirroring the full quarterly archives
would have moved ~3.4 GB, almost all of it `num.txt`, which this stage does not
need. The fetcher reads each remote zip's central directory over HTTP Range and
pulls **only the `sub.txt` member** — three small requests per quarter, ~1 % of
the naive transfer. SEC fair access is respected with an identifying User-Agent
carrying a contact address and an inter-request delay; every member is
content-hashed and cached, so a re-run re-downloads nothing.

### 3.2 Tier C — the look-ahead is gone

| tier | fine-grained? | leakage-safe? | status |
|---|---|---|---|
| A `PIT_XBRL_DISCLOSURE_SIGNATURE` | no (business model only) | yes | unchanged |
| B `ENTITY_SIC_SNAPSHOT_CONTROL` | yes | **no** | superseded as the fine tier |
| **C `PIT_FILING_SIC_SERIES`** | **yes** | **yes** | **new** |

Coverage on the panel Tier C is used to control:

| | |
|---|---|
| panel rows classified | **34,652 / 34,652 (100 %)** |
| unknown rate | 0.0 % |
| **delisted rows classified** | **100 %** |
| issuers | 729 |
| classification stability | 97.4 % (19 issuers genuinely reclassified, captured point-in-time) |
| agreement with Tier B | **98.5 %** (34,106 / 34,624) |

The taxonomy and the no-look-ahead query rule are unchanged and stay owned by
`alpha_agent.pit_sector`; only the observation source is new. Tier C remains
**inadmissible for signal construction** — a classification is a control variable
in this programme, and keeping it out of every registered spec is what makes the
sector falsification non-circular. A regression asserts no registered valuation
spec mentions any tier.

### 3.3 Sector revalidation — confirmation, not a new search

This re-runs the **exact** frozen tests, on the **exact** factors, with the
**exact** pre-registered thresholds (IC t ≥ 2.0 *and* ≥ 50 % of raw rank IC
retained, imported verbatim from Stage 25 — a regression asserts identity, not
equality). One thing changes: the tier the tests consume.

**`s25_operating_profitability`** — raw IC 0.0550, t 3.63:

| control | rank IC | IC t | spread t | retention | survives |
|---|---|---|---|---|---|
| sector-neutral Tier A | 0.0564 | 3.77 | 2.41 | 1.025 | yes |
| sector-neutral Tier B *(look-ahead)* | 0.0447 | 3.33 | 1.87 | 0.813 | yes |
| **sector-neutral Tier C *(leakage-safe)*** | **0.0454** | **3.46** | 1.87 | 0.825 | **yes** |
| remove Technology (Tier C) | 0.0593 | 3.82 | 2.28 | 1.079 | yes |
| remove ConsumerDiscretionary (Tier C) | 0.0426 | 2.77 | 1.68 | 0.774 | yes |
| remove Tech + ConsDisc (Tier C) | 0.0433 | 2.72 | 1.66 | 0.787 | yes |

**`s24_rnd_intensity`** — raw IC 0.0594, t 2.86:

| control | rank IC | IC t | spread t | retention | survives |
|---|---|---|---|---|---|
| sector-neutral Tier A | 0.0562 | 2.72 | 3.22 | 0.946 | yes |
| sector-neutral Tier B *(look-ahead)* | 0.0430 | 2.83 | 3.60 | 0.725 | yes |
| **sector-neutral Tier C *(leakage-safe)*** | **0.0439** | **2.95** | 3.67 | 0.739 | **yes** |
| remove Technology (Tier C) | 0.0571 | 2.72 | 3.17 | 0.961 | yes |
| remove ConsumerDiscretionary (Tier C) | 0.0621 | 2.97 | 3.52 | 1.045 | yes |
| remove Tech + ConsDisc (Tier C) | 0.0536 | 2.45 | 3.38 | 0.903 | yes |

Both are slightly **stronger** under the leakage-safe fine tier than under the
look-ahead one, which is the direction Stage 25's stated asymmetry predicted:
the look-ahead control could absorb genuine information, and removing it gives
some back. Every Tier-B result Stage 25 stamped *provisional* is now
**`CONCLUSIVE_LEAKAGE_SAFE`**.

**This changes the reason R&D is not a challenger; it does not change the
verdict.** R&D failed on *concentration*, not on sector. `CONCENTRATION_FRAGILE`
stands, and no threshold was moved to revisit it.

### 3.4 PIT market equity — both gaps closed, for free

Stage 25 recorded market cap as blocked by two independent gaps and warned that
fixing only one produces a *plausible but wrong* number.

| gap | close |
|---|---|
| share counts discarded by the monetary-USD unit filter | opt-in `extra_units={'shares'}` / `extra_taxonomies={'dei'}` on the **released** parser; every existing caller keeps byte-identical behaviour, asserted by regression |
| only owned price surface is TOTALRETURN adjusted | the same owned, entitled local Norgate installation serves `NONE` (raw traded price) and `CAPITAL` (capital events only) |

Neither cost anything: the share counts were inside the `companyfacts.zip`
already on disk, and the price adjustments were a parameter on a service already
running. No purchase, no upgrade, no new entitlement.

The construction is:

```
shares(formation) = shares(report) × f(formation) / f(report)
market_equity     = shares(formation) × close_none(formation)
        where f(t) = close_capital(t) / close_none(t)
```

The carry term is what makes a split *between* the report date and the formation
date a handled case rather than a silent error. Verified against real events:
AAPL's factor runs 1/28 → 1/4 → 1 across its 7:1 and 4:1 splits; NVDA's runs
1/40 → 1/10 → 1 across its 4:1 and 10:1. Spot checks land where they should —
AAPL $478.8 bn at 2014-03-31, $1.98 tn at 2020-09-30 (carry exactly 4.0); NVDA
$3.04 tn at 2024-06-28 (carry exactly 10.0); MSFT $357.2 bn at 2015-06-30.

**Coverage: 96.5 %** of panel rows. Share counts are 171,419 facts across 828
CIKs, filed 2009-04-15 → 2026-07-31; prices are 1,981 symbols × 4,431 trading
days, delisted names retained.

Honest remaining shortcomings: multi-share-class issuers (companyfacts reports
the cover-page count without a class dimension); the share count is as of its
filing rather than the formation date, so issuance and buyback between the two
are untracked; `LongTermDebt` excludes current maturities and leases, so
enterprise value is understated where debt is short-dated; and the pre-existing
identity backlog leaves unresolved symbols absent, exactly as they were absent
from every Stage-25 cross-section.

### 3.5 PIT valuation — 13 pre-registered hypotheses, and a clean null

Signs were fixed in source before any number existed. Benjamini-Hochberg over the
whole family.

| hypothesis | names | rank IC | IC t | spread t | net25 | BH q | gate |
|---|---|---|---|---|---|---|---|
| `s26_payout_yield` | 488 | 0.0222 | 1.33 | 0.26 | 0.47 % | 0.737 | REJECTED |
| `s26_market_equity_size` | 535 | 0.0125 | 0.80 | 0.24 | 0.75 % | 0.737 | REJECTED |
| `s26_free_cash_flow_yield` | 470 | 0.0085 | 0.57 | 0.60 | 1.36 % | 0.737 | REJECTED |
| `s26_operating_profit_to_ev` | 214 | 0.0046 | 0.24 | 0.49 | 1.31 % | 0.874 | REJECTED |
| `s26_earnings_yield` | 535 | 0.0016 | 0.10 | −1.37 | −4.54 % | 0.918 | REJECTED |
| `s26_sales_to_ev` | 315 | −0.0052 | −0.25 | 0.44 | 1.28 % | 0.874 | REJECTED |
| `s26_cash_flow_to_ev` | 362 | −0.0121 | −0.61 | −0.80 | −3.74 % | 0.737 | REJECTED |
| `s26_operating_profit_to_market` | 294 | −0.0134 | −0.67 | −0.78 | −3.29 % | 0.737 | REJECTED |
| `s26_sales_to_market` | 453 | −0.0136 | −0.67 | −0.32 | −1.50 % | 0.737 | REJECTED |
| `s26_operating_cash_flow_yield` | 533 | −0.0180 | −0.91 | −1.14 | −4.93 % | 0.737 | REJECTED |
| `s26_tangible_book_to_market` | 438 | −0.0155 | −0.97 | 0.47 | 1.15 % | 0.737 | REJECTED |
| `s26_gross_profit_to_market` | 313 | −0.0353 | −1.71 | −1.26 | −6.78 % | 0.563 | REJECTED |
| `s26_book_to_market` | 504 | −0.0350 | −1.74 | −0.85 | −3.68 % | 0.563 | REJECTED |

**0 of 13 cleared the released gate. 0 survived FDR.** The best absolute
t-statistic in the family is 1.74 — and it belongs to `book_to_market` with the
**wrong sign**.

This is a real result, not a coverage artefact: market equity resolved for 96.5 %
of rows, the median cross-section is 294–535 names, and the same panel reproduces
every Stage-25 baseline exactly. Read plainly, **large-cap value did not pay over
2010-2026**, and the two most canonical measures — book-to-market and
gross-profit-to-market — were wrong-signed at t ≈ −1.7. That is the same decade
Stage 25 described from the other side, when heavy R&D spend, low return on that
spend and heavy stock compensation all outperformed: the expensive,
intangible-heavy growth firm led, and every cheapness measure is the short side
of that trade.

### 3.6 The sharpest finding in the stage

`s26_operating_profit_to_market` exists to answer one question: is the Stage-25
challenger a value factor in disguise? It uses the **identical numerator** and
swaps `Assets` for `MarketEquity` in the denominator.

| | scaled by Assets | scaled by Market Equity |
|---|---|---|
| rank IC | **+0.0550** | −0.0134 |
| IC t | **+3.63** | −0.67 |
| cross-sectional rank correlation between them | — | **0.224** |
| top-25 name overlap | — | **22.7 %** |

The same profit, scaled two ways, produces two nearly unrelated signals — and
only one of them works. **The information is in the asset scaling, not in the
operating profit.** `s25_operating_profitability` is a capital-efficiency signal,
not a cheapness signal, and it is definitively not HML wearing a different hat.

Because nothing cleared the gate, the incrementality workstream would otherwise
have had nothing to judge. The four strongest factors were therefore measured
anyway and are stamped `POST_CAMPAIGN_DIAGNOSTIC_NOT_IN_FDR_FAMILY`, with the
selection disclosed: chosen by absolute IC t after results were read, plus two
comparisons pinned in source. None is a candidate, none is registered, none
enters the multiple-testing family. All four classify as not-independent, and the
artefact records **which limb** of the pre-registered rule fired — restating an
existing signal, or carrying no information at all — because those mean very
different things.

---

## 4. Ensembles

Reuses the released Stage-25 menu builder and evaluator unchanged, so the
mandatory matched-universe correction applies by construction. Only gate-clearing
candidates are ever offered; no valuation factor qualified, so the menu is the
Stage-25 one.

| structure | names | rank IC | IC t | spread t | net25 | turnover |
|---|---|---|---|---|---|---|
| `operational_shape_5050` | 488 | 0.0322 | 1.83 | 2.09 | 6.70 % | 0.48 |
| `operational_plus_s25_operating_profitability` | 300 | 0.0578 | 3.42 | 2.77 | 8.81 % | 0.39 |
| **`fundamental_plus_..._no_momentum`** | 300 | 0.0468 | **3.57** | 2.80 | 8.09 % | **0.14** |
| `momentum_plus_..._no_fundamental` | 307 | 0.0589 | 3.28 | 3.56 | 12.5 % | 0.46 |

Against the incumbent **on matched names and dates**, `operational + op_prof`
adds Δ rank IC +0.0157, Δ IC t +1.05, Δ net25 +0.57 pp, Δ turnover −0.09. The
no-momentum variant scores highest on IC t and cuts turnover from 0.48 to 0.14 —
the more interesting trade for a real book, though it gives up return and does
not clear the both-must-improve rule.

The universe-effect correction remains mandatory and remains decisive:
restricting to SG&A reporters is itself worth roughly +0.010 IC, and only the
matched delta is attributable to the signal.

---

## 5. Research exhaustion

| concept | state |
|---|---|
| `OPERATING_PROFITABILITY` | **RESEARCH_CHALLENGER / FORWARD_TRACK** |
| `R_AND_D` | `CONCENTRATION_FRAGILE` — do not re-open; the sector explanation is now dead on leakage-safe evidence too |
| `RESIDUAL_MOMENTUM` | `EXHAUSTED_NEGATIVE` |
| `STAGE25_REJECTED_FUNDAMENTALS` | `DO_NOT_REOPEN_WITHOUT_NEW_INFORMATION` |
| `PIT_FINE_SECTOR` | **READY** |
| `PIT_MARKET_CAP` | **READY** |
| `PIT_VALUATION` | **COMPLETE** — 13 tested, 0 survived |
| `ANALYST_REVISIONS` | `WAITING_FOR_DATA` |

Stop testing: the 18 closed Stage-25 families, the 13 rejected valuation ratios,
any `s24_rnd_intensity` re-specification, and any further accounting ratio scaled
by assets, revenue or equity.

Updated in the **existing** agent and registry. No second queue.

---

## 6. The free/owned frontier is not empty

Three economically distinct families are runnable **today**, at zero cost, on
data already on disk — and this stage's own acquisition created two of them:

1. **Filing timing and lateness** — `sub.txt` carries `accepted`, `filed`,
   `period` and `form` per submission, so the filing-lag distribution is already
   acquired. Promptness is a governance/stress signal orthogonal to the reported
   numbers.
2. **Restatement and amendment history** — `sub.txt` carries `prevrpt` and the
   `/A` form suffix, and the companyfacts indexes already preserve amendments as
   distinct rows keyed by accession.
3. **Share-count dynamics** — net issuance became point-in-time observable only
   when this stage built the share index. Issuance is management's own valuation
   opinion and is not the payout ratio.

None was run here. Each is a genuinely new family and deserves its own
pre-registration, not a rider on the valuation family.

---

## 7. Purchase gate — **WAIT**

The released rule is applied, not asserted: a paid dataset is recommendable only
when (a) the owned surface is exhausted for the hypotheses it would unlock,
(b) no free artefact would unlock them first, and (c) no prior evaluation of that
vendor already returned a negative result.

| dataset | recommendation | why |
|---|---|---|
| SEC FSDS `sub.txt` | **ACQUIRED** | free; was Stage 25's number-one item; now closed |
| Historical analyst revisions (Intrinio) | **WAIT** | (a) fails — three free families remain runnable; (b) fails; (c) a prior live trial already returned `NO_DEFENSIBLE_ALPHA` / `DO_NOT_BUY` on a survivorship-safe 16-year test |
| Steele / other fundamental vendor history | **REJECT** | it would restate accounting this programme already reads point-in-time — and the one gap that mattered (share counts) turned out to be in the owned archive all along |
| Vendor PIT sector/industry classification | **REJECT** | superseded — `sub.txt` supplies a leakage-safe per-filing classification at zero cost |

Taking Stage 25's free recommendation did **not** exhaust the owned surface; it
revealed three more free families. Condition (a) fails, so nothing clears the
rule. **Nothing is authorised.**

The hard requirement on analyst data is unchanged: we need historical **as-was**
consensus vintages. Fiscal-period final estimates are not sufficient, because a
final estimate embeds everything learned after the formation date — the exact
look-ahead this programme exists to avoid.

---

## 8. Model governance

**No evidence justifies operational promotion, and none is proposed.**
`fundamental_momentum_50_50_v1` is unchanged. `api.universe_scoring.AUTOMATIC_PROMOTION_ALLOWED`
remains `False` and this stage never references it. The challenger's state is
`SHADOW_BOOK_ACTIVE` — a research state. The strongest state reachable in this
system, `READY_FOR_MANUAL_REVIEW`, still never changes the operating model, and
this stage does not reach it.

Classification: **`PROMOTION_ELIGIBLE_AFTER_FORWARD_GATE`** — the historical case
is as strong as history can make it; what is missing is out-of-sample evidence,
which needs time rather than another backtest.

---

## 9. Capital deployment (HOC)

Stage 25's historical counterfactual was a null (mean score gap −0.0046,
t −0.34, losers ranked lower in 47.7 % of formations). Stage 26 **does not
re-run it**: a fixed sample re-cut until it turns favourable is specification
search. `stage23_unified.build_decision_link` stays the owner and its status is
unchanged — `INSUFFICIENT_FORWARD_EVIDENCE`, minimum 12 matured live
observations.

What only forward evidence can answer: whether the challenger ranked a holding
lower *before* it deteriorated on a date it had not seen; whether it found
replacements earlier; whether those replacements beat the holdings released;
what switching cost was actually incurred; whether regret fell. A counterfactual
can only ask how the challenger would have *scored* names the incumbent chose —
never what the portfolio would have become had the challenger been choosing,
because the subsequent holdings, cash and opportunity set would all have
differed.

---

## 10. Safety

Research-only and read-only with respect to every operational store. No orders,
fills, signals, trade decisions, proposals, rebalance plans, Daily Close, model
promotion or champion change. No PostgreSQL, no prediction service, no backend
restart; the port-8001 listener PID was unchanged throughout.

Network access was limited to `www.sec.gov` for the free Financial Statement Data
Sets. No paid API was called and no quota spent.

Writes are confined to the Stage-26 research root, the Stage-26 share index, the
owned SEC bulk cache, and the **existing** tournament registry and shadow-book
root — the canonical research lifecycle, not new stores.

---

## 11. Exact continuation point

1. **Let the book run.** Forward marks accumulate on production ticks from
   2026-08-17. Nothing to do but wait — and specifically, do not refit the frozen
   spec while waiting.
2. **Open one of the three free families** in §6 with its own pre-registration.
   Filing timing is the cheapest: the data is already on disk.
3. **Do not** run another accounting-ratio campaign. Five of six fundamental
   families and the entire valuation family are now closed with evidence.
4. Analyst revisions stay `WAITING_FOR_DATA` and the pipeline stays immediately
   pluggable.

The next binding constraint is **`NEW_FREE_INFORMATION`**, with `FORWARD_TIME`
close behind. Paying for information while free information sits unused is the
wrong trade, and another single-factor test inside a closed family is the wrong
work.
