# Release 63 — Information Sensitivity, Orthogonal Information Discovery and the Alpha Offensive

**Branch:** `r63-information-sensitivity`
**Built over:** `89a066f` (R62.1.2, the live HEAD on 2026-09-09)
**Worktree:** `D:\paper_trader_r63_information_sensitivity`
**Canonical checkout:** `C:\Users\binis\paper_trader` — READ ONLY for the whole release: no restart, no daily close, no portfolio cycle, no adoption, no registration, no purchase, no live-store write, no merge, no deploy.
**Protocol:** `research/r63/R63_RESEARCH_PROTOCOL.json`, registered before any experiment ran; one disclosed amendment (a dimension-count typo) is recorded inside it.
**Research root:** `D:\Stock_Prediction_app_data\r63_information_sensitivity`

---

## 1. The gap, stated plainly

At the start of R63 the persistent research estate held **8,380 settled hypotheses across 311 families with 0 QUALIFIED**. 8,257 of them — 98.5% — carried `information_family = PRICE_STATE` (or its placebo). Every settled historical row carried the same reopen condition, `NEW_ORTHOGONAL_INFORMATION`, and the live frontier table marked every scope except CROSS_ASSET `EXHAUSTED`. The four ACTIVE R58 freezes had emitted four predictions and matured zero observations.

The estate was information-constrained, not compute-constrained, and it had no owner that could say **which economic information matters, for which assets, at which horizons, conditional on what it already knows** — or where it was information-blind. R59's frontier ranks PRICE families; R59's opportunity frontier ranks DATASETS; nothing ranked INFORMATION NEEDS, and nothing measured a dimension's value conditional on the baseline the estate already uses.

## 2. What landed — one research package, no second owner of anything that existed

`alpha_agent/r63/` (fifteen modules), all RESEARCH ONLY. Every write lands under the R63 research root; the live checkout, the live services and every live store are read-only, and the audit proves it by token.

| module | owns | it is NOT |
|---|---|---|
| `ontology.py` | the ONE canonical economic-information ontology: 42 dimensions in 10 classes, deterministic, hashed | a feature list; formula variants of one state are one dimension |
| `inventory.py` | the owned-information CERTIFICATION: every meaningful owned or entitled field classified USED / TESTED / REJECTED / BLOCKED from evidence | a data catalogue; ingestion is not testing and implementation is not evidence |
| `panels.py`, `features.py` | the substrates and the point-in-time observables, from owned data only | a new collector; nothing is fetched here |
| `pit.py` | the ONE as-of join (strict availability, declared publication lags, a broadcast session) and the purged / embargoed walk-forward | a second calendar owner |
| `sensitivity.py` | the engine: standalone association, PAIRED conditional increment on identical rows with a forced model, partial rank IC, OOS R² increment, permutation drop, residual share, stability by block / regime / instrument, cost-aware economics, Benjamini-Hochberg | a tournament; it emits no challenger and freezes nothing |
| `experiments.py` | baseline vs baseline + dimension over scope × mode × horizon × dimension, checkpointed, parallel by slice | an allocator |
| `asset_horizon.py` | the asset × horizon × dimension observation map | the R59 scope frontier (which ranks price families) |
| `gaps.py` | the ranked INFORMATION GAP FRONTIER with remaining research value per need | the R59 data-opportunity frontier (which ranks datasets by acquisition state) |
| `sourcing.py` | the cheapest-first ladder per need and the break-even alpha of any paid rung against the live NAV | a purchase verdict; `engine.data_expansion_gate` keeps that |
| `challengers.py` | immutable research-only challenger candidate records | a registrar, an adopter or a promoter |
| `handoff.py` | the AlphaAgent frontier: "which asset × horizon × dimension has the highest remaining research value" | wired into the live runtime — it is an interface, tested, not deployed |
| `acquire.py` | two bounded free acquisitions (SEC submissions histories, EIA natural-gas archive) under the estate's own collector conventions | a subscription, an account or a purchase |
| `report.py` | the human-readable result rendered from the artifacts | a dashboard |

Companion files: `research/r63/R63_RESEARCH_PROTOCOL.json`, `scripts/run_r63_information_sensitivity.py`, `tests/test_release63_information_sensitivity.py` (28 hermetic tests), `check_release63_information_sensitivity` in `scripts/audit_architecture.py` (18 strict-blocking invariants), five new canonical-concept rows in `docs/architecture/system_inventory.json`, and the numbering correction in `docs/CONSOLIDATION_ROADMAP.md` (the planned operational-book cutover is now R64; R63 is this release).

### 2.1 Stage 1 — certification built, not requested

No canonical R62.2 owned-information certification existed (`git grep` finds no owner). R63 built it as its first internal stage: 47 fields across 15 providers, each carrying provider, source, dataset, field, normalised field, ontology dimension, asset classes, horizons, history, cadence, PIT status, `available_at` and `effective_at` semantics, canonical collector, normaliser, consumer, research families, evidence ids, disposition, reason and blocker. Dispositions are DERIVED at build time from three evidence sources — the live ResearchMemory through its read-only handle, the R58 information inventory, and R63's own cells — never typed in. UNKNOWN is not in the vocabulary, and a test proves the artifact carries none.

### 2.2 The substrates — owned data the estate had never joined

| substrate | what it is | why it matters |
|---|---|---|
| R41 dated-contract curve store | 107 Norgate futures markets, 1977–2026, front/second/third dated-contract returns under the observable roll rule, settlements c1..c8, days-to-expiry, OI, volume, curve slopes | replaces the two-contract R38 layer and the back-adjusted R57 panel: 94 markets admitted by a mechanical rule across five scopes, and a genuine policy path from ZQ/SR3 c1..c8 |
| R57 PIT S&P 500 panel + R58 PANEL-F | 1,897 securities with PIT membership; 885 CIK-bridged names with SEC-filed fundamentals | the equity cross-section, delisted names retained |
| SEC Form 3/4/5 structured data sets (R35) | 766,387 issuer-filing-days, 14,023 CIKs, 2008–2026 | owned since R35, tested only at sector-ETF level; R63 joins it to the STOCK panel through the CIK bridge |
| SEC submissions histories (R63 acquisition) | 842 PANEL-F issuers, 1.22 million filings 2009–2026, acceptance timestamp on every row | 8-K rate, late-filing notices, amendments and the expected periodic-filing window, historically for the first time |
| EIA petroleum archive (R35) + natural-gas archive (R63) | weekly ending stocks of crude ex-SPR, gasoline, distillate; working gas in storage | the INVENTORY dimension for energy futures, never tested before |
| CFTC COT archives (R35, R46 code map) | 39 markets, 1986–2026, six-day publication lag | positioning per market |
| FRED daily market series (R41), Cboe term structure (R41), ALFRED vintages (owned store) | unrevised yields, OAS, breakevens, VIX/OVX/GVZ, VIX term structure; true vintages for UNRATE, CPI, ICSA, NFCI | market-level conditioners with honest PIT treatment |

Excluded, with the measurement: FINRA short-volume history (HTTP 403 for history files, measured 2026-09-09; only the current day is served), 13F ownership (no owned CUSIP bridge), analyst revisions (blocked on PIT and payment), options surface history (payment), BEA/BLS (no release timestamps), intraday (not manufactured).

## 3. The chain, measured

For every cell the engine measures the whole chain on IDENTICAL rows, dates, realised returns, splits and costs, with the ridge penalty chosen by blocked inner cross-validation on the BASELINE arm and forced on the augmented arm:

    standalone association (descriptive)
      -> PAIRED conditional increment of OOS rank IC (XS) or of the unit-risk timing statistic (TS), Newey-West t
      -> partial rank IC, OOS R² increment, OOS permutation drop
      -> residual share against the baseline (REDUNDANT < 0.10, PARTIALLY_REDUNDANT < 0.35)
      -> stability by 3-year block, two PIT regimes, instrument, lockbox-vs-selection sign
      -> a book from each arm's OOS score, charged the scope's own costs on turnover
      -> Benjamini-Hochberg at q = 0.10 across every conditional p-value the campaign produced

A BASELINE dimension is measured by ABLATION (the marginal value of information the estate already uses); a NEW dimension by AUGMENTATION. Expanding yearly walk-forward folds from 1995 (futures) or 2011 (equities), purge and embargo of one horizon, the 2023+ block a LOCKBOX evaluated once per pre-registered cell.

## 4. Results

See section 4 of `PROJECT_STATE.md` and the machine-readable artifacts; the numbers below are copied from `r63_experiment_results.json`, `information_inventory.json`, `asset_horizon_information_matrix.json`, `information_gap_frontier.json`, `sourcing_economics.json` and `r63_challenger_candidates.json`.

### 4.1 The campaign in numbers

| quantity | value |
|---|---|
| cells (scope × mode × horizon × dimension) | 996 |
| cells that produced a conditional statistic | 978 |
| `NO_CONDITIONAL_VALUE` | 951 |
| `CONDITIONAL_VALUE_NOT_ECONOMIC` | 21 |
| `CONDITIONAL_VALUE_NOT_FDR_SIGNIFICANT` | 3 |
| `CONDITIONAL_VALUE_UNSTABLE` | 2 |
| `INCREMENTAL_INFORMATION_CANDIDATE` | 1 |
| `DATA_HOLD` (coverage or sample) | 18 |
| Benjamini-Hochberg, conditional increments | m = 978, raw one-sided p < 0.05: 54, survivors at q = 0.10: **3** |
| Benjamini-Hochberg, economic increments | m = 974, raw p < 0.05: 64, survivors: 13 |
| certification | 51 fields, 17 providers: USED 9, TESTED 10, REJECTED 13, BLOCKED 19, UNKNOWN 0 |
| asset × horizon × dimension map | WELL_OBSERVED 672, PARTIALLY_OBSERVED 80, NOT_OBSERVED 56, BLOCKED 64 |
| challengers | READY_FOR_FORWARD_QUALIFICATION 1, MORE_RESEARCH_REQUIRED 23, REJECTED 954 |
| purchases, subscriptions, trials, accounts | 0, 0, 0, 0 |

### 4.2 The three Benjamini-Hochberg survivors

| cell | kind | increment | t | lockbox t | partial-IC t | permutation drop | residual share | net inc/yr | t (net) | Sharpe inc | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `FX_FUTURES / XS / 1 session / CARRY` | augmentation | +0.0126 rank IC | 4.26 | 2.80 | 4.38 | 0.020 | 0.82 (DISTINCT) | +2.50% | 2.34 | +0.28 | **INCREMENTAL_INFORMATION_CANDIDATE** |
| `CROSS_ASSET / XS / 1 session / TREND` | ablation | +0.0033 rank IC | 3.89 | 3.94 | 5.19 | 0.008 | 0.28 (partially redundant) | +6.5% (arithmetic) | 6.16 | +0.67 | `CONDITIONAL_VALUE_NOT_ECONOMIC` — book degenerate (see 4.5) |
| `RATES_FUTURES / XS / 63 sessions / REALISED_VOLATILITY` | ablation | +0.0225 rank IC | 3.45 | 0.39 | — | — | 0.38 | +0.81% | 3.28 | +0.07 | `CONDITIONAL_VALUE_NOT_ECONOMIC` — below the 1.5%/yr floor |

**FX carry is the one new-information finding.** The dated-contract slope between the front and second CME currency contracts — the interest differential, observed daily — adds ranking information to the nine-currency cross-section conditional on the whole price block: increment t 4.26 (one-sided p 1e-5), lockbox increment t 2.80, partial rank IC t 4.38 after residualising both sides on the baseline, residual share 0.82, nine of ten three-year blocks positive (2007-2009 the exception), positive under both implied-volatility regimes and both trend regimes, and a net increment of +2.5%/yr (t 2.34, Sharpe +0.28) on a tercile long-short book charged the R38 per-market costs at 19.5% one-way turnover. It fails the ECONOMIC Benjamini-Hochberg family (13 survivors of 974; its economic p sits above the threshold) and its book drawdown is −52% against the baseline's −67%, so the record names `EFFECTIVE_SAMPLE` as its strongest failure mode: the economic evidence is real but not yet strong enough to survive a campaign-wide correction on its own. The same information at 5 and 63 sessions carries the same sign (t 2.47 and 3.29, lockbox t 2.02 and 2.55) without clearing the economic floor. R35 measured FX interest carry on ETFs at monthly cadence with no increment; R59 invalidated its carry features when the second contract turned out to be the same front contract; this is the first time the estate has measured carry from genuine dated contracts, daily, conditional on the price block — and it is the first free information dimension in the estate to survive Benjamini-Hochberg conditional on the baseline.

**Cross-asset trend at daily cadence is the estate's own information at a cadence it never prosecuted.** 8,279 of the 8,380 memory hypotheses are 21-session; nine are 1-session. Ablating the TREND block from the 94-market daily cross-sectional ranking removes rank IC worth t 3.89 (lockbox t 3.94, both regimes positive, eight of ten blocks positive). Its economic increment (+6.5%/yr arithmetic, t 6.16) cannot be read: the book is degenerate (4.5). It is `MORE_RESEARCH_REQUIRED` with the named next step — the same signal through a volatility-targeted, leverage-bounded book such as `alpha_agent.r57.futures_tournament`.

### 4.3 What drives the current predictive value, and what is redundant

Ablation of each baseline dimension across 52 futures/credit cells and 4 equity cells (mean marginal t, cells at t ≥ 2, mean net increment): TREND +0.27 / 3 / +0.36%; TAIL_CRASH_STATE +0.18 / 2 / +0.01%; MOMENTUM +0.13 / 1 / +0.22%; REVERSAL +0.05 / 0 / −0.94%; PRICE_RETURN_STATE −0.09 / 1 / +0.38%; LIQUIDITY −0.10 / 1 / +0.31%; VOLUME_PARTICIPATION −0.32 / 0 / −0.56%; REALISED_VOLATILITY −0.45 / 2 / −0.06%; FREE_CASH_FLOW (equities) +1.84 / 1 / +2.26%; FUNDAMENTAL_LEVELS (equities) −0.32 / 0 / +0.35%. Read plainly: **the price block's marginal value sits in trend and tail state; reversal, volume participation and realised volatility subtract from it net of costs; in equities the fundamental leg's value is its free-cash-flow term, and the accrual/operating-income term adds nothing** — which corroborates R58's component attribution on a walk-forward design R58 did not run.

Redundancy against the baseline (residual share of the dimension composite): POSITIONING_COMMITMENTS is the most baseline-like new dimension in commodities (0.72) and FX (0.62); INVENTORY 0.79; VOLATILITY_EXPECTATIONS_IV is half explained by the price block in index futures (0.49) and RISK_APPETITE likewise (0.51); FUNDING_LIQUIDITY_CONDITIONS 0.73-0.88; in equities INSIDER_BEHAVIOUR retains 0.82 and every disclosure dimension ~0.99. The most distinct market-level dimensions everywhere are POLICY_EXPECTATIONS, TERM_STRUCTURE, CURVE_SHAPE and CALENDAR_SEASONALITY (0.95-0.99) — distinct, and (below) without conditional value.

### 4.4 The top gaps actually sourced and tested — all three rejected

| information need | source used (rung) | cells | best conditional t | verdict |
|---|---|---|---|---|
| INSIDER_BEHAVIOUR, US equities, stock level | owned R35 Form 3/4/5 archives joined through the CIK bridge (OWNED_DEEPER); 94% of eligible names covered | 4 | +0.33 (1 session); −2.13 / −2.22 at 21 / 63 sessions | REJECTED: adding open-market insider counts LOWERS OOS rank IC at the month and quarter horizons; net +0.5-1.0%/yr at t ≤ 1.5 |
| DISCLOSURE_INTENSITY_LANGUAGE and EVENT_INFORMATION, US equities | SEC submissions histories acquired for 842 issuers, 1.22 M acceptance-stamped filings (FREE_PUBLIC_PROXY) | 8 | +1.55 (21 sessions); +1.44 (63 sessions, expected-filing window) | REJECTED: no cell reaches t 2; residual share 0.99 (the information is orthogonal and inert) |
| INVENTORY, energy futures | owned EIA petroleum stocks + acquired natural-gas storage (OWNED_DEEPER + FREE) | 16 | −0.02 (all negative at t ≤ −3 in the 1- and 5-session timing lanes) | REJECTED: seasonal inventory surprises do not time crude, products or gas after the price block |
| POLICY_EXPECTATIONS, all futures scopes | ZQ / SR3 dated contracts (OWNED_DEEPER) | 28 | +2.05 (VX, 5 sessions, no economics) | REJECTED |
| POSITIONING_COMMITMENTS, all futures scopes | CFTC archives through the R46 code map (OWNED_DEEPER) | 24 | +2.43 (FX, 21-session timing; lockbox sign flips) | MORE_RESEARCH_REQUIRED for FX timing only; rejected elsewhere |
| CREDIT_CONDITIONS / FUNDING / MACRO_* / INFLATION / RATES / TERM_STRUCTURE / CURVE_SHAPE / RISK_APPETITE / CROSS_ASSET_TRANSMISSION / DISPERSION (timing lanes) | FRED market series, Baa spread, ALFRED vintages, Cboe | 372 | INFLATION_EXPECTATIONS on the HYG/LQD credit proxy +2.86 (lockbox sign flips) | no candidate; four cells at t ≥ 2, none economic |

FUNDAMENTAL_CHANGE (owned XBRL, 84% coverage) is likewise rejected at every horizon (best t −0.00; −2.93 at 1 session), corroborating R58 on a different design.

### 4.5 Turnover, cost and the book limitation

Net increments are charged per-market R38 costs (2-15 bp per side; 15 bp for the markets the R38 panel does not price), 12.5 bp per side in equities and 5 bp on the credit proxy. The FX carry book turns 19.5% one-way per session; its increment RISES at 2× cost (+2.88%) because the augmented book trades less than the baseline. The cross-sectional futures books are UNLEVERED GROSS-2 daily books with inverse-volatility weights capped at max(1/k, 25%): on the 94-market cross-asset scope the arithmetic net increment is positive while the compounded path collapses under volatility drag (maximum drawdown −100%), and on a nine-name scope the cap of 1/3 still leaves the book concentrated. Such a book is DEGENERATE by rule (drawdown below −90% or a single name above 50%) and cannot pass the economic gate. The rank-based conditional statistics do not depend on the book. A volatility-targeted, leverage-bounded book is the named next step for every `BOOK_DEGENERATE` record, and the Sharpe increment is the scale-free economic statistic to read in the meantime.

### 4.6 Where the estate is information-blind

After the map's proxy lanes, 120 of 872 cells are NOT_OBSERVED or BLOCKED. The blind dimensions, by expected value before feasibility: CALENDAR_SEASONALITY and CROSS_ASSET_TRANSMISSION for the equity cross-section (derivable from owned data; not built this release), EVENT_INFORMATION for futures scopes (no owned macro-release calendar history — the FRED release-calendar API with the owned key is the free rung), and for US equities ANALYST_REVISIONS, EARNINGS_EXPECTATIONS and REVENUE_EXPECTATIONS (paid and PIT-unverified), OWNERSHIP_INSTITUTIONAL_FLOW (free 13F data sets, blocked on a CUSIP bridge), ETF_FUND_FLOW (market-level free aggregates only), SHORT_POSITIONING (history not served free; owned since 2026-07-23, forward only). US equities remain the most under-informed scope (7 of 26 dimensions blind at every horizon), followed by the credit proxy (6 of 24).

### 4.7 The paid-data gate

NAV 97,496.72 (desk ledger, 2026-09-08, read-only). Nine paid rungs were taken through Gate 1. **None is a purchase-experiment candidate.** Single-name options implied-volatility history has the only proxy evidence (index-level IV in the rates scope, t 2.72, not economic) and no recorded fee, so it `FEE_UNQUOTED_CANNOT_GATE`; every other paid rung — analyst revisions, earnings and revenue expectations, ETF flows, freight, short-interest history, consensus macro surveys, analyst dispersion — fails Gate 1 because no owned or free proxy shows conditional evidence, which is the gate's first question and the cheapest place to stop. Break-even arithmetic (fee / NAV + 1.2% trading cost + half the expected gross increment + 10% of fee / NAV) is implemented and applied the moment a fee is recorded; at this NAV a $2,000/yr dataset needs 2.05% + half the expected increment before the first dollar of research return, and anything above $1,950/yr is `EXTREME_HURDLE`.

### 4.8 Where the AlphaAgent should research next

The frontier's remaining-value ranking (product of expected value and feasibility, tested-negative cells zeroed) is led by carry at the month horizon in cross-asset and index futures, implied-volatility term structure in rates, carry at the quarter in commodities, and FX carry at the week — i.e. **finishing the economics of information that already shows conditional value on a proper book**, before sourcing anything. The value-only ranking of unseen cells puts equity seasonality and cross-asset transmission (derivable, unbuilt) and a futures macro-event calendar (free) ahead of every paid dimension. Stop-transforming flags are TRUE for EQUITY_INDEX_FUTURES, VOLATILITY and CREDIT_PROXY: no baseline dimension shows marginal net value there, so another price transformation is not the next experiment.

## 5. Safety

`SAFETY` in `alpha_agent/r63/__init__.py` declares every dangerous flag False and the audit asserts each on the source text. The package imports no `api`, `engine` or `db` module and no registrar, adopter or accrual owner; it contains no forbidden call shape; it reads the live ResearchMemory only through `open_memory_readonly()`; it reads the paper NAV only through `read_json` on the desk ledger; its research root is on the data drive and a runtime assertion refuses the live checkout; no live runtime consumer imports it. The runner has no execute flag because there is nothing operational to execute.

Tonight's Daily Cycle does not depend on R63.

## 6. Gates

- `tests/test_release63_information_sensitivity.py`: 28 tests, hermetic (research root redirected through the owner's env-var constant; the live memory read patched out; a test proves the resolved root is nowhere near the live estate).
- `scripts/audit_architecture.py --strict`: exit 0; 18 R63 invariants strict-blocking; inventory drift zero.
- Impacted suites re-run: `test_architecture_contracts.py`, `test_canonical_backend_restart.py`, `test_release29_restart_contract.py`, `test_release60_architecture_consolidation.py`.
- `git diff --check`: clean.
- Full-repository gate: NOT run. R63 changes no shared runtime or business behaviour (one additive audit check, five inventory rows, documentation); the protocol says a full gate is only required when shared behaviour changes.

## 7. What R63 does not do

It does not merge, deploy, restart, register, adopt, promote, approve, order, fill, purchase, subscribe, start a trial, create an account, write a live store or modify a holding, cash or NAV. It does not wire the handoff into the live ResearchRuntime. It does not manufacture intraday history, analyst history, sector history or a consensus. A surviving specification is a research artifact for a later governed forward process, and a human opens that door.
