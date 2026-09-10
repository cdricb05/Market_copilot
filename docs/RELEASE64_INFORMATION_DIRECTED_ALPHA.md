# Release 64 — Portfolio proposal integrity and information-directed alpha

**Branch:** `r64-information-directed-alpha`
**Built over:** `a3a1bf0` (R63, the development HEAD on 2026-09-09); live HEAD `89a066f` (R62.1.2)
**Worktree:** `D:\paper_trader_r64_information_directed_alpha`
**Canonical checkout:** `C:\Users\binis\paper_trader` — READ ONLY for the whole release: no restart, no daily cycle, no daily close, no reassessment, no proposal regeneration, no approval, no snapshot confirmation, no order, no fill, no holdings / cash / NAV mutation, no corporate-action registration, no adoption, no promotion, no merge, no deploy.
**Protocol:** `research/r64/R64_RESEARCH_PROTOCOL.json`, registered 2026-09-10T03:12Z before any R64 cell ran; three disclosed amendments recorded inside it (section 3.2).
**Research root:** `D:\Stock_Prediction_app_data\r64_information_directed_alpha`
**Two tracks, one release.** Track A is the read-only integrity review of tonight's governed portfolio proposal (`docs/PORTFOLIO_PROPOSAL_INTEGRITY_REVIEW_2026-09-09.md`). Track B is the information-directed alpha campaign that continues R63.

---

## 1. Track A — the Sep-9 proposal, reviewed read-only

**Verdict: `PORTFOLIO_PROPOSAL_REVIEW_READY`**, with four advisories.

| what | result |
|---|---|
| proposal | `reap_2026-09-09_alpha_paper_book_1_0a29467c0f55`, hash `0a29467c…24f280`, durably present (87,220 bytes, 2026-09-10T00:34:35Z), bound by the governed record `gdec_2026-09-09_alpha_paper_book_1_f90423061b45` (46 checks passed, 0 failed, 1 not applicable) and by every live read at 03:00 UTC |
| session / NAV | 2026-09-09 / **97,572.00** (cash 4,482.71, invested 93,089.29, 25 holdings) |
| MNST | `MNST_CORPORATE_ACTION_INTEGRITY_OK`: one registered 2:1 split (ex 2026-08-11), applied once at read time; 42 + 2 raw fills → 84 + 2 = **86 shares**, cost basis 4,116.64 invariant; 44 is the raw pre-action count, 172 is a preview defect that applies the split twice (D-R64-1) |
| the three NAVs | 95,772.72 raw (44 shares) · **97,572.00 authoritative** (86) · 101,256.24 double-applied preview (172); differences are exactly 42 × 42.84 and 128 × 42.84 |
| complete target | 24 names, 23 changes (9 exit, 1 reduce, 5 increase, 8 add, 10 retain, 0 replace); weights sum 0.953412 + cash 0.046588; one-way turnover **0.350000** (budget 0.35, binding); cost **85.38** counted once; score 0.852218 → 0.925100, hurdle 0.0175, net **+0.055382 ≥ 0.050** |
| risk | DDOG 12.23 % of risk at 3.69 % weight (3.16×, real breach of the HOC 3× trigger) → **8.44 % (2.18×)** in the target although its trim was deferred by the turnover budget; portfolio volatility 11.75 % → 14.97 %; declared limits all pass; SNDK (20.7 %, 4.94×) and ALAB (17.8 %, 4.23×) would trigger the HOC rule next |
| "unavailable" surface | `PRESENT_AND_READ_TIMEOUT_ONLY`: the strings are the UI's transport-failure labels; live reads answer in 3.6–7.5 s against a 45 s budget; the seven-minute event cycle can push a queued read past it; state consistent across every owner |
| legacy target | separate owner, ledger, ids and tokens; UI-hidden, not backend-blocked (D-R64-4) |

The exact manual operator sequence (review in the cockpit; Stage-18 decision
and Stage-19 order-plan confirmation as explicit API calls, since the cockpit
deliberately carries no approval button) is in section 7 of the review
document. Nothing was performed.

Findings carried as architecture decisions, not fixed here: D-R64-1 (report
double-applies a registered split), D-R64-2 (two score-cost-hurdle
definitions), D-R64-3 (HOC 3× trigger vs 25 % target cap), D-R64-4 (legacy
route UI-hidden only).

## 2. Track B — what landed

`alpha_agent/r64/` (nine modules), RESEARCH ONLY, plus ONE live-runtime
adapter `alpha_agent/r59/information_needs.py` and a nine-line additive hook
on the R63 scorer.

| module | owns | it is NOT |
|---|---|---|
| `handoff_validation.py` | the exact R63 FX-carry artifact and evidence, verified (hashes recompute, matrix quoted, protocol stamped, fresh run reproduces) | a re-derivation of R63 |
| `carry.py` | genuine carry from DISTINCT dated contracts; per-market distinct-contract evidence; three formula variants of the ONE CARRY dimension | a new dimension or family |
| `construction.py` | the ONE bounded, realistic, risk-controlled research book applied to BOTH arms | a second scorer |
| `experiments.py` | the R64 cells: the R63 scorer keeps its OOS scores; R64 prices them; R63 statistics are reproduced to 1e-9 | a second walk-forward, as-of or BH owner |
| `family.py` | family-aware multiple testing: campaign BH plus Holm within the FX CARRY family | a smaller family run to manufacture survival |
| `challenger.py` | immutable candidates with a `freeze_record_hash` over the complete forward specification, hash-named files | a registrar, adopter or promoter |
| `frontier.py` | an OVERLAY of R64 verdicts keyed by the R63 frontier's `cell_key` | a second frontier |
| `report.py` | `R64_REPORT.md` from the artifacts | a dashboard |
| `alpha_agent/r59/information_needs.py` | the governor's second question, answered by READING the frontier by path; DATA_OPPORTUNITY kind, lane, fairness and queue reused; watermark dedupe | a second memory, queue, scheduler or forward owner |
| `alpha_agent/r63/sensitivity.py` | `run_cell(..., keep_predictions=False)`: attaches the OOS scores of both arms under `_predictions`; every default byte unchanged | a changed statistic |

Companion files: `research/r64/R64_RESEARCH_PROTOCOL.json`,
`scripts/run_r64_information_directed_alpha.py`,
`tests/test_release64_information_directed_alpha.py` (31 hermetic tests),
`check_release64_information_directed_alpha` in `scripts/audit_architecture.py`
(22 strict-blocking invariants), four canonical-concept rows in
`docs/architecture/system_inventory.json`, an autouse hermeticity guard in
`tests/conftest.py`, the roadmap renumbering (the operational-book cutover is
now R65), and D-R64-1…5 in `docs/ARCHITECTURE_DECISIONS.md`.

### 2.1 The R63 handoff, validated

`r63_handoff_validation.json`: verdict **`R63_HANDOFF_VALID_WITH_STALE_CHALLENGER_FILE`**, 18 checks, 0 failed, fresh-run reproduction **True**.

The candidates artifact's `artifact_hash` and its one READY record's `record_hash` recompute; the record quotes the `FX_FUTURES|XS|1|CARRY` cell of the sensitivity matrix (whose `artifact_hash` also recomputes) verbatim: increment 0.01256187954250206, t 4.25850004003311, lockbox increment 0.0382074993253441, `INCREMENTAL_INFORMATION_CANDIDATE`, Benjamini-Hochberg pass at m = 978. Both artifacts are stamped with the sha256 of the committed R63 protocol (`f1c9637c…`, LF-normalised; the `core.autocrlf` working copy hashes differently, a checkout artefact, not a content change). A fresh run of the same cell through the same owner reproduces every conditional statistic and every sample count to 1e-9 (6,238 effective periods, 5,395 selection, 843 lockbox).

One inconsistency is named, not hidden: `challengers/R63_FX_FUTURES_CARRY_H1_E82A5C66.json` (record_hash `23b11af2…`, written 21:28 UTC on R63's first pass and never overwritten by R63's write-once rule) carries the FIRST-PASS book economics (net increment 0.02476/yr, max weight 0.71, turnover 0.226), while the regenerated candidates artifact (record_hash `1f8f9099…`, 23:07 UTC) carries the capped-book economics (0.02497, 0.50, 0.195) that match the matrix. Cell, conditional statistics and classification are identical in both. The FINAL evidence is the candidates artifact plus the matrix; R64's own challenger files carry their record hash in the file name so this cannot recur.

### 2.2 Genuine carry needs two contracts

The R41 dated-contract store carries the front and second dated settlements
(`c1`, `c2`) and their annualised slope for every market. Measured per
market over 94 admitted markets: **`ALL_DISTINCT` — 94 markets, 0 refused, maximum share of sessions with `c1 == c2` 0.464 (YYT)**. The nine FX markets
(6A 6B 6C 6E 6J 6M 6N 6S DX) coincide on 0.0–2.9 % of sessions and their
front/second returns correlate at 0.993–1.000 — two contracts, one currency,
distinct settlements. The thin-second-contract markets (YYT 46 %, YXT 42 %,
SJB 34 %, TN 22 %, SSG 20 %, YIB 19 %, EUA 17 %) stay below the registered 50 %
refusal line and are published. The R59 `close_b` pseudo-curve construction
is refused by rule (`PseudoCurveError`) and never used.

### 2.3 The risk-controlled book

Applied to BOTH arms of every cell identically (protocol section
"risk_controlled_construction"): R63's quintile/tercile selection and capped
inverse-volatility weights; equal ex-ante risk per asset class on each side
(cross-asset scope); no instrument above 25 % of book risk (water-filled
redistribution, the cap binds); a 10 % annualised volatility target from the
unlevered book's own trailing 63-period realised volatility, strictly past;
gross leverage ≤ 5.0; a 25 % no-trade band; per-market R38 costs on one-way
turnover. A book below −60 % drawdown or at the leverage cap on more than half
its periods is `DEGENERATE_UNDER_CONTROLS` and cannot pass the economic gate.

## 3. Results

### 3.1 Cells

41 cells; 33 were R63 cells and **all 33 reproduce the persisted R63 conditional statistics exactly** (0 failures). By verdict: ECONOMIC_UNDER_CONTROLS_NOT_FDR 2, NOT_ECONOMIC_UNDER_CONTROLS 16, NO_CONDITIONAL_VALUE 23. Campaign BH over the R64 cells: conditional m = 41, 19 raw p < 0.05, survivors 19; economic m = 41, 10 raw p < 0.05, survivors 7 — reported for transparency; the conditional FDR every verdict uses is the R63 campaign's (m = 978), inherited by each cell R63 ran and by each formula variant from its base cell (protocol section "multiple_testing"; amendment 3).

| cell | tag | R63 verdict | R64 verdict | cond t | FDR | R63 net inc | R64 net inc | at 2x cost | Sharpe inc | R64 t | aug net | aug vol | aug dd | turnover | gross |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `COMMODITY_FUTURES|XS|1|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.43 | False | 2.40 % | 1.16 % | 1.33 % | 0.109 | 2.01 | -6.5 % | 10.7 % | -89 % | 0.31 | 0.97 |
| `COMMODITY_FUTURES|XS|21|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.02 | False | 1.66 % | 0.49 % | 0.49 % | 0.041 | 0.74 | 1.7 % | 12.6 % | -48 % | 0.44 | 0.89 |
| `COMMODITY_FUTURES|XS|5|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | -0.42 | False | -0.61 % | 0.24 % | 0.22 % | 0.022 | 0.41 | 0.3 % | 11.1 % | -55 % | 0.33 | 0.94 |
| `COMMODITY_FUTURES|XS|63|CARRY` | CARRY_FRONTIER | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 2.28 | False | 2.65 % | 1.23 % | 1.22 % | 0.095 | 2.29 | 3.3 % | 12.9 % | -70 % | 0.48 | 1.03 |
| `CROSS_ASSET|XS|1|CARRY` | CARRY_FRONTIER | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 3.03 | False | 1.36 % | 1.49 % | 2.23 % | 0.241 | 1.45 | -20.5 % | 11.5 % | -100 % | 0.60 | 1.44 |
| `CROSS_ASSET|XS|1|CARRY_CLASS_NEUTRAL` | CARRY_VARIANT | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 3.18 | False | - | -1.03 % | -4.18 % | -0.057 | -0.90 | -28.8 % | 11.0 % | -100 % | 0.79 | 1.80 |
| `CROSS_ASSET|XS|1|TREND` | CONSTRUCTION_RESCUE | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 3.89 | True | 6.51 % | 9.95 % | 18.05 % | 0.968 | 7.96 | -35.6 % | 11.2 % | -100 % | 0.85 | 1.63 |
| `CROSS_ASSET|XS|21|CARRY` | CARRY_FRONTIER | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 2.38 | False | 0.84 % | 1.31 % | 1.51 % | 0.101 | 1.44 | 1.8 % | 12.3 % | -55 % | 0.65 | 1.16 |
| `CROSS_ASSET|XS|21|CARRY_CLASS_NEUTRAL` | CARRY_VARIANT | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 3.61 | False | - | -0.01 % | -0.22 % | -0.006 | -0.01 | 1.9 % | 11.0 % | -61 % | 0.96 | 1.60 |
| `CROSS_ASSET|XS|21|TREND` | CONSTRUCTION_RESCUE | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.68 | False | 1.74 % | -0.20 % | 0.20 % | -0.019 | -0.23 | 0.5 % | 11.2 % | -55 % | 0.72 | 1.23 |
| `CROSS_ASSET|XS|5|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.56 | False | 0.20 % | 0.95 % | 1.12 % | 0.088 | 1.12 | -0.3 % | 11.0 % | -47 % | 0.54 | 1.35 |
| `CROSS_ASSET|XS|5|CARRY_CLASS_NEUTRAL` | CARRY_VARIANT | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 3.34 | False | - | 0.25 % | -0.32 % | 0.023 | 0.26 | -1.1 % | 11.1 % | -71 % | 0.69 | 1.66 |
| `CROSS_ASSET|XS|5|TREND` | CONSTRUCTION_RESCUE | CONDITIONAL_VALUE_NOT_FDR_SIGNIFICANT | **NOT_ECONOMIC_UNDER_CONTROLS** | 2.67 | False | 4.88 % | 3.29 % | 5.56 % | 0.300 | 2.52 | -0.7 % | 10.9 % | -61 % | 0.61 | 1.43 |
| `CROSS_ASSET|XS|63|CARRY` | CARRY_FRONTIER | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 3.18 | False | 1.01 % | 0.39 % | 0.41 % | 0.028 | 0.69 | 2.6 % | 11.3 % | -74 % | 0.82 | 1.36 |
| `CROSS_ASSET|XS|63|CARRY_CLASS_NEUTRAL` | CARRY_VARIANT | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 3.24 | False | - | -0.99 % | -1.09 % | -0.089 | -1.04 | 3.5 % | 10.8 % | -64 % | 1.04 | 1.85 |
| `CROSS_ASSET|XS|63|TREND` | CONSTRUCTION_RESCUE | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.63 | False | 0.22 % | 0.82 % | 0.99 % | 0.079 | 1.11 | 2.1 % | 11.0 % | -71 % | 0.87 | 1.43 |
| `EQUITY_INDEX_FUTURES|XS|1|CARRY` | CARRY_FRONTIER | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 2.31 | False | 3.41 % | 2.07 % | 2.74 % | 0.182 | 1.94 | -18.1 % | 10.7 % | -99 % | 0.65 | 1.46 |
| `EQUITY_INDEX_FUTURES|XS|21|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 0.89 | False | 1.45 % | 1.88 % | 1.72 % | 0.184 | 1.18 | -2.3 % | 10.5 % | -58 % | 1.27 | 2.03 |
| `EQUITY_INDEX_FUTURES|XS|5|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | -0.82 | False | -0.58 % | -0.57 % | -0.42 % | -0.053 | -0.60 | -4.9 % | 10.7 % | -81 % | 1.01 | 1.82 |
| `EQUITY_INDEX_FUTURES|XS|63|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 0.49 | False | -0.48 % | -0.94 % | -0.98 % | -0.087 | -1.23 | 0.6 % | 10.9 % | -67 % | 0.84 | 2.40 |
| `FX_FUTURES|TS|1|CARRY` | REPRODUCTION | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.12 | False | 2.04 % | 2.69 % | 4.33 % | 0.249 | 2.41 | -19.6 % | 11.1 % | -100 % | 0.46 | 2.14 |
| `FX_FUTURES|TS|21|CARRY` | REPRODUCTION | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | -0.16 | False | -0.20 % | -0.13 % | -0.31 % | -0.013 | -0.12 | -0.8 % | 10.0 % | -62 % | 0.55 | 2.10 |
| `FX_FUTURES|TS|5|CARRY` | REPRODUCTION | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 0.36 | False | -0.11 % | 0.27 % | -0.09 % | 0.032 | 0.21 | -5.0 % | 10.6 % | -86 % | 0.53 | 2.31 |
| `FX_FUTURES|TS|63|CARRY` | REPRODUCTION | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 0.25 | False | 0.08 % | 0.20 % | 0.18 % | 0.017 | 0.28 | 0.3 % | 11.0 % | -95 % | 0.43 | 1.80 |
| `FX_FUTURES|XS|1|CARRY` | REPRODUCTION | CONDITIONAL_VALUE_NOT_FDR_SIGNIFICANT | **NOT_ECONOMIC_UNDER_CONTROLS** | 4.26 | True | 2.50 % | 3.56 % | 4.01 % | 0.335 | 2.49 | -1.3 % | 10.8 % | -66 % | 0.26 | 2.62 |
| `FX_FUTURES|XS|1|CARRY_TO_RISK` | CARRY_VARIANT | CONDITIONAL_VALUE_NOT_FDR_SIGNIFICANT | **NOT_ECONOMIC_UNDER_CONTROLS** | 4.26 | True | - | 3.40 % | 3.83 % | 0.319 | 2.62 | -1.5 % | 10.7 % | -62 % | 0.26 | 2.61 |
| `FX_FUTURES|XS|21|CARRY` | REPRODUCTION | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.20 | False | 0.07 % | 0.01 % | 0.06 % | 0.001 | 0.01 | 1.0 % | 9.8 % | -48 % | 0.53 | 2.32 |
| `FX_FUTURES|XS|21|CARRY_TO_RISK` | CARRY_VARIANT | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.26 | False | - | -0.46 % | -0.39 % | -0.047 | -0.49 | 0.6 % | 9.9 % | -57 % | 0.52 | 2.25 |
| `FX_FUTURES|XS|5|CARRY` | REPRODUCTION | CONDITIONAL_VALUE_NOT_FDR_SIGNIFICANT | **ECONOMIC_UNDER_CONTROLS_NOT_FDR** | 2.47 | False | 1.67 % | 2.00 % | 2.16 % | 0.191 | 1.36 | 0.5 % | 10.6 % | -55 % | 0.47 | 2.42 |
| `FX_FUTURES|XS|5|CARRY_TO_RISK` | CARRY_VARIANT | CONDITIONAL_VALUE_NOT_FDR_SIGNIFICANT | **ECONOMIC_UNDER_CONTROLS_NOT_FDR** | 2.64 | False | - | 2.27 % | 2.51 % | 0.216 | 1.75 | 0.8 % | 10.6 % | -49 % | 0.47 | 2.47 |
| `FX_FUTURES|XS|63|CARRY` | REPRODUCTION | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 3.29 | False | 0.47 % | 0.93 % | 0.93 % | 0.085 | 1.06 | 2.0 % | 10.7 % | -88 % | 0.45 | 2.28 |
| `FX_FUTURES|XS|63|CARRY_TO_RISK` | CARRY_VARIANT | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 2.60 | False | - | 0.76 % | 0.79 % | 0.069 | 0.95 | 1.8 % | 10.7 % | -89 % | 0.43 | 2.29 |
| `RATES_FUTURES|XS|1|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 0.85 | False | 1.36 % | 0.77 % | 0.40 % | 0.043 | 0.83 | -98.6 % | 10.1 % | -100 % | 1.55 | 4.15 |
| `RATES_FUTURES|XS|21|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 0.53 | False | 0.05 % | 0.27 % | -0.33 % | 0.027 | 0.55 | -1.5 % | 9.3 % | -41 % | 1.57 | 4.07 |
| `RATES_FUTURES|XS|5|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.50 | False | 0.13 % | -0.21 % | -0.98 % | -0.017 | -0.32 | -18.0 % | 9.1 % | -99 % | 1.61 | 4.13 |
| `RATES_FUTURES|XS|63|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | -0.22 | False | 0.02 % | 0.33 % | 0.39 % | 0.038 | 1.12 | 0.6 % | 9.2 % | -78 % | 1.35 | 4.04 |
| `RATES_FUTURES|XS|63|REALISED_VOLATILITY` | CONSTRUCTION_RESCUE | CONDITIONAL_VALUE_NOT_ECONOMIC | **NOT_ECONOMIC_UNDER_CONTROLS** | 3.45 | True | 0.81 % | 0.84 % | 1.03 % | 0.080 | 2.62 | 1.7 % | 9.9 % | -77 % | 1.02 | 3.70 |
| `VOLATILITY|TS|1|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | -1.63 | False | -0.22 % | 0.16 % | 0.16 % | 0.015 | 0.60 | 3.8 % | 12.5 % | -39 % | 0.00 | 0.16 |
| `VOLATILITY|TS|21|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | -0.81 | False | -0.64 % | -0.58 % | -0.59 % | -0.028 | -1.51 | -3.3 % | 18.5 % | -66 % | 0.02 | 0.09 |
| `VOLATILITY|TS|5|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | -0.48 | False | -0.20 % | -0.22 % | -0.23 % | -0.020 | -1.46 | 1.8 % | 11.0 % | -31 % | 0.01 | 0.13 |
| `VOLATILITY|TS|63|CARRY` | CARRY_FRONTIER | NO_CONDITIONAL_VALUE | **NO_CONDITIONAL_VALUE** | 1.49 | False | 0.73 % | 0.95 % | 0.95 % | 0.076 | 1.45 | 0.1 % | 11.9 % | -74 % | 0.02 | 0.10 |

Read across the table: every book is at its 10 % volatility target (augmented volatility 9–13 %) and never at the leverage cap except the rates cells (41–48 % of periods, treasury and short-rate futures at 2 %/yr volatility); the increments are real where the conditional statistic is real; and the AUGMENTED books at 1- and 5-session cadence lose money after R38 costs — cost drag, not concentration, is what a realistic book cannot carry at that cadence.

### 3.2 Protocol amendments (all disclosed, dated, no threshold moved)

1. 2026-09-10T03:25Z, BEFORE any cell ran: the gross leverage cap registered
   at 3.0 was raised to 5.0 — the 2 % volatility floor already bounds the
   vol-target leverage at 5×, and a gross-2 unlevered cross-sectional book at
   5–7 % realised volatility needs a gross of 3–4 to reach 10 %, so a 3.0 cap
   would have measured the cap, not the information.
2. 2026-09-10T03:45Z, AFTER a first partial pass (20 of 41 cells), DISCLOSED:
   the instrument contribution cap had been implemented with a per-side gross
   rescale that could re-breach the cap it enforced (caught by the hermetic
   test, not by a result). Repaired so the cap binds; every cell re-run; the
   first-pass cells discarded; the R63 conditional statistics are unaffected
   by construction.
3. 2026-09-10T04:40Z, AFTER the first complete pass, DISCLOSED and AGAINST the
   pass's own result: the merge step had given the carry variants the R64
   campaign BH (m = 41) instead of their base cell's R63 campaign verdict
   (m = 978), and on that smaller family `FX_FUTURES|XS|5|CARRY_TO_RISK` came
   out READY while `FX_FUTURES|XS|5|CARRY`, the same information, had failed
   the R63 family. Variants now inherit their base cell's R63 FDR; the
   first-pass challenger records are discarded. The correction removes a
   survival and manufactures none.

### 3.3 The FX carry family (one economic family)

Verdict **`FAMILY_FAILS_HOLM`**: family Holm p **8.23e-05** on the conditional increments (the 1-session XS cell survives at 0.00008, the 63-session at 0.0035, the 5-session at 0.0404) and **0.0509** on the risk-controlled economic increments — the 1-session economic p of 0.0064 misses the Holm line at m = 8 by 0.0009. FX carry's conditional value is established at the family level; its economics under controls are not, by a hair.

| cell | cond t | cond p | Holm | econ t | econ p | Holm | net inc | at 2x | Sharpe inc | R64 verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| `FX_FUTURES|TS|1|CARRY` | 1.12 | 0.13239 | 0.57585 | 2.41 | 0.0080 | 0.0563 | 2.69 % | 4.33 % | 0.249 | NO_CONDITIONAL_VALUE |
| `FX_FUTURES|TS|5|CARRY` | 0.36 | 0.36004 | 1.00000 | 0.21 | 0.4167 | 1.0000 | 0.27 % | -0.09 % | 0.032 | NO_CONDITIONAL_VALUE |
| `FX_FUTURES|TS|21|CARRY` | -0.16 | 0.56219 | 1.00000 | -0.12 | 0.5468 | 1.0000 | -0.13 % | -0.31 % | -0.013 | NO_CONDITIONAL_VALUE |
| `FX_FUTURES|TS|63|CARRY` | 0.25 | 0.40256 | 1.00000 | 0.28 | 0.3880 | 1.0000 | 0.20 % | 0.18 % | 0.017 | NO_CONDITIONAL_VALUE |
| `FX_FUTURES|XS|1|CARRY` | 4.26 | 0.00001 | 0.00008 | 2.49 | 0.0064 | 0.0509 | 3.56 % | 4.01 % | 0.335 | NOT_ECONOMIC_UNDER_CONTROLS |
| `FX_FUTURES|XS|5|CARRY` | 2.47 | 0.00674 | 0.04043 | 1.36 | 0.0868 | 0.5209 | 2.00 % | 2.16 % | 0.191 | ECONOMIC_UNDER_CONTROLS_NOT_FDR |
| `FX_FUTURES|XS|21|CARRY` | 1.20 | 0.11517 | 0.57585 | 0.01 | 0.4952 | 1.0000 | 0.01 % | 0.06 % | 0.001 | NO_CONDITIONAL_VALUE |
| `FX_FUTURES|XS|63|CARRY` | 3.29 | 0.00050 | 0.00351 | 1.06 | 0.1440 | 0.7202 | 0.93 % | 0.93 % | 0.085 | NOT_ECONOMIC_UNDER_CONTROLS |

**1 session** (the R63 candidate). Baseline arm: -4.8 %/yr net, Sharpe -0.45, drawdown -79 %. Augmented arm: -1.3 %/yr net, Sharpe -0.12, drawdown -66 %, cost drag 6.0 %/yr on 0.26 one-way turnover per session, gross 2.62. Increment **+3.56 %/yr, +0.335 Sharpe, t 2.49** (+4.01 % at 2x cost: the augmented book trades less), nine of ten 3-year blocks positive, lockbox sign agrees. Verdict `NOT_ECONOMIC_UNDER_CONTROLS`: the augmented book's −66 % drawdown crosses the registered −60 % line, and the drawdown is the price block's negative drift after daily costs, not a construction collapse. The information is real; the book it improves is not deployable at daily cadence.

**5 sessions.** Baseline -1.5 %/yr, drawdown -62 %; augmented **+0.5 %/yr**, drawdown **-55 %**, gross 2.42, turnover 0.47 — the only FX book in the estate that is deployable-shaped. Increment +2.00 %/yr, +0.191 Sharpe, t 1.36. Verdict `ECONOMIC_UNDER_CONTROLS_NOT_FDR`: every economic gate passes; the R63 campaign Benjamini-Hochberg (m = 978, p 0.0067) refused it and R64 quotes that refusal. Carry-to-risk at the same horizon repeats the picture (+2.27 %/yr, +0.216 Sharpe, drawdown −49 %) and inherits the same refusal. **21 sessions:** no conditional value. **63 sessions:** conditional t 3.29, economics +0.93 %/yr on a book with an −88 % drawdown.

### 3.4 Construction rescue

The construction question has an answer, and it is not the one hoped for. Every R63 cell whose economic verdict was decided by a degenerate book was re-priced through the risk-controlled book on identical scores, and the binding failure moved from CONCENTRATION to COST. At the 1-session horizon the 94-market cross-sectional TREND book turns over 85 % one-way per session; R38 costs are 41.6 %/yr of drag on a 10 %-volatility book, and BOTH arms collapse (baseline -45.5 %/yr, with TREND -35.6 %/yr; drawdown −100 % on each). TREND is worth **+9.95 %/yr and +0.97 Sharpe** relative to the book without it (paired t 7.96) — the largest increment the estate has measured — and it rescues nothing, because the book it improves cannot exist at that cadence. At 5 sessions the same information is +3.29 %/yr and +0.30 Sharpe (t 2.52) on a book that loses 0.7 %/yr with a −61 % drawdown, one point past the registered line. Equity-index carry at 1 session (+2.07 %/yr, +0.18 Sharpe) sits on a book losing 18 %/yr; cross-asset carry at 1 session (+1.49 %/yr, +0.24 Sharpe) on one losing 20.5 %/yr; commodity carry at 63 sessions (+1.23 %/yr, +0.10 Sharpe, t 2.29) on a positive book (+3.3 %/yr) whose −70 % drawdown still fails; rates realised volatility at 63 sessions (+0.84 %/yr, t 2.62) on a book at the leverage cap 41 % of the time. The class-neutral cross-asset carry — the genuinely distinct implementation — sharpens the conditional statistic (t 3.18–3.61, residual share 0.97 against 0.91–0.92 for the raw slope) and destroys the economics (−1.0 to +0.25 %/yr): the raw-slope ranking carried a class-level tilt the neutral version removes, and what remains does not pay its 0.69–1.04 one-way turnover. Verdict for the rescue set: `NOT_ECONOMIC_UNDER_CONTROLS` throughout. Previously promising information does NOT survive reasonable risk controls at the cadence it was measured; the cadence, not the cap, is the binding failure.

### 3.5 Challengers

Counts: **READY_FOR_FORWARD_QUALIFICATION 0, MORE_RESEARCH_REQUIRED 16, REJECTED 25.** Nothing promoted, registered or adopted; `api.forward_challenger_registry` was never called. Each MORE record is an immutable, hash-named research artifact carrying the full result standard (hypothesis, mechanism, information class, asset class, horizon, universe, PIT source, baseline, incremental OOS result, conditional information value, multiple testing, net return, turnover, costs, volatility, drawdown, concentration, robustness, binding failure) and a `freeze_record_hash` over its complete forward specification, so a human-gated registration can bind to exactly that specification later.

| candidate | cond t | net inc | at 2x | Sharpe inc | econ t | aug net | aug dd | FDR (R63 family) | binding failure | freeze_record_hash |
|---|---|---|---|---|---|---|---|---|---|---|
| `R64_FX_FUTURES_CARRY_H1_E82A5C66` | 4.26 | 3.56 % | 4.01 % | 0.335 | 2.49 | -1.3 % | -66 % | True | DEGENERATE_UNDER_CONTROLS | `642efddb6e51…` |
| `R64_FX_FUTURES_CARRY_TO_RISK_H1_A0CA9F31` | 4.26 | 3.40 % | 3.83 % | 0.319 | 2.62 | -1.5 % | -62 % | True | DEGENERATE_UNDER_CONTROLS | `5419f5526561…` |
| `R64_CROSS_ASSET_CARRY_CLASS_NEUTRAL_H21_22318EFE` | 3.61 | -0.01 % | -0.22 % | -0.006 | -0.01 | 1.9 % | -61 % | False | DEGENERATE_UNDER_CONTROLS | `f5107b6eaea3…` |
| `R64_RATES_FUTURES_REALISED_VOLATILITY_H63_8F5FB0D5` | 3.45 | 0.84 % | 1.03 % | 0.080 | 2.62 | 1.7 % | -77 % | True | DEGENERATE_UNDER_CONTROLS | `3d6d5832be05…` |
| `R64_CROSS_ASSET_CARRY_CLASS_NEUTRAL_H5_F737850E` | 3.34 | 0.25 % | -0.32 % | 0.023 | 0.26 | -1.1 % | -71 % | False | DEGENERATE_UNDER_CONTROLS | `2d7aa6564830…` |
| `R64_FX_FUTURES_CARRY_H63_598854BE` | 3.29 | 0.93 % | 0.93 % | 0.085 | 1.06 | 2.0 % | -88 % | False | DEGENERATE_UNDER_CONTROLS | `6f00fe19c579…` |
| `R64_CROSS_ASSET_CARRY_CLASS_NEUTRAL_H63_86C9C330` | 3.24 | -0.99 % | -1.09 % | -0.089 | -1.04 | 3.5 % | -64 % | False | DEGENERATE_UNDER_CONTROLS | `95ca54766f48…` |
| `R64_CROSS_ASSET_CARRY_CLASS_NEUTRAL_H1_6A654E8B` | 3.18 | -1.03 % | -4.18 % | -0.057 | -0.90 | -28.8 % | -100 % | False | DEGENERATE_UNDER_CONTROLS | `6d5bfcf35a6c…` |
| `R64_CROSS_ASSET_CARRY_H63_64DB7985` | 3.18 | 0.39 % | 0.41 % | 0.028 | 0.69 | 2.6 % | -74 % | False | DEGENERATE_UNDER_CONTROLS | `1cd08355f3d7…` |
| `R64_CROSS_ASSET_CARRY_H1_DFBC0751` | 3.03 | 1.49 % | 2.23 % | 0.241 | 1.45 | -20.5 % | -100 % | False | DEGENERATE_UNDER_CONTROLS | `1c60701f9f7d…` |
| `R64_FX_FUTURES_CARRY_TO_RISK_H5_384E794A` | 2.64 | 2.27 % | 2.51 % | 0.216 | 1.75 | 0.8 % | -49 % | False | MULTIPLE_TESTING | `8d1fd7cd5fe4…` |
| `R64_FX_FUTURES_CARRY_TO_RISK_H63_488D90F5` | 2.60 | 0.76 % | 0.79 % | 0.069 | 0.95 | 1.8 % | -89 % | False | DEGENERATE_UNDER_CONTROLS | `43a5c8a5fa14…` |
| `R64_FX_FUTURES_CARRY_H5_41E918F2` | 2.47 | 2.00 % | 2.16 % | 0.191 | 1.36 | 0.5 % | -55 % | False | MULTIPLE_TESTING | `d5192245bc05…` |
| `R64_CROSS_ASSET_CARRY_H21_5467D464` | 2.38 | 1.31 % | 1.51 % | 0.101 | 1.44 | 1.8 % | -55 % | False | NOT_ECONOMIC_UNDER_CONTROLS | `2ba806a20f52…` |
| `R64_EQUITY_INDEX_FUTURES_CARRY_H1_E54BF15C` | 2.31 | 2.07 % | 2.74 % | 0.182 | 1.94 | -18.1 % | -99 % | False | DEGENERATE_UNDER_CONTROLS | `3b4db402a143…` |
| `R64_COMMODITY_FUTURES_CARRY_H63_F37C04AF` | 2.28 | 1.23 % | 1.22 % | 0.095 | 2.29 | 3.3 % | -70 % | False | DEGENERATE_UNDER_CONTROLS | `0fad8fa00b5f…` |

The first complete pass had classified `FX_FUTURES|XS|5|CARRY_TO_RISK` READY on the smaller R64 family; amendment 3 corrected the inheritance and the record now reads MORE_RESEARCH_REQUIRED with binding failure MULTIPLE_TESTING, as its base cell does. Every REJECTED row names its gate (drawdown under controls, lockbox sign flip, costs, multiple testing).

### 3.6 The AlphaAgent frontier, consumed

The R63 information gap frontier is now a question the R59 governor asks. In
`generate_mandates`, after the owned-data-opportunity probes,
`information_needs.candidates(mem, limit=6)` yields the highest-remaining-value
actionable needs (sourcing step OWNED or FREE; PAID needs are purchase
questions and never become research mandates) as `DATA_OPPORTUNITY` mandates
with family `DATA:INFORMATION_NEED:<cell_key>`, EIV `0.55 + 3 × remaining
value` capped at 0.95, through the existing fairness cap and the existing
queue. The `DATA_VALIDATION` handler recognises the payload source, records
`INFORMATION_NEED_MANDATED` in the ONE research memory and sets a
`memory_meta` watermark per (need, frontier `artifact_hash`), so an unchanged
frontier is never re-mandated and a new frontier version revives the need.
The R64 overlay (`r64_information_need_updates.json`) multiplies a need's
remaining value by what R64 measured: 29 needs updated over frontier `255ca36e…`: ECONOMIC_UNDER_CONTROLS_NOT_FDR 1, NOT_ECONOMIC_UNDER_CONTROLS 11, NO_CONDITIONAL_VALUE 17; `FX_FUTURES|5|CARRY` keeps 0.75 of its value with next action MORE_RESEARCH_SAMPLE, the answered-under-controls needs keep 0.25, the no-value needs 0.10.

Executing an information-sensitivity cell stays in the offline research
campaign; the live runtime records the need and its watermark. The adapter
imports no research package (the artifact is the interface), and
`tests/conftest.py` redirects both artifact paths so no governor test can read
the live frontier.

## 4. Safety

`SAFETY` in `alpha_agent/r64/__init__.py` declares every dangerous flag False
and the audit asserts each on the source text. The package imports no `api`,
`engine` or `db` module and no registrar, adopter or accrual owner; it
contains no forbidden call shape; it reads the R63 artifacts only from the R63
results root (never written) and writes only under the R64 root on the data
drive; a runtime assertion refuses a research root inside the live checkout.
The governor adapter reads by path and writes only to the research memory
through the existing handler. The runner has no execute flag.

## 5. Gates

- `tests/test_release64_information_directed_alpha.py`: 31 tests, hermetic
  (R64 root, R63 results root, R59 memory root and both frontier paths
  redirected through the owners' env-var constants; a test proves none resolves
  near a live root).
- `scripts/audit_architecture.py --strict`: exit 0; 23 R64 invariants
  strict-blocking; inventory drift zero.
- Impacted suites re-run: `test_architecture_contracts`, `test_canonical_backend_restart`, `test_release29_restart_contract`, R57, R58, R59 (engine, persistent runtime, reallocation coherence), R60, R60.1, R61, R62.1, R62.1.1, R62.2 and R63 — 762 collected: **755 passed, 5 skipped, 2 failed**. The two failures (`test_release62_2_automatic_forward_accrual::test_03b_the_emission_is_strictly_earlier_than_its_session` and `::test_26b_without_a_projection_the_registrar_still_reports_its_own_zeros`) assert that the REAL UTC date is still 2026-09-09; they fail identically on the untouched R63 parent worktree at `a3a1bf0` and are pre-existing, clock-dependent and unrelated to R64 (left to the R62.2 owner to freeze their clock). The R64 suite and the R63 and R59-governor suites re-run green after the last edit (153 tests).
- `git diff --check`: clean.
- Full-repository gate: NOT run (`FULL_GATE_REQUIRED = NO`). The only shared
  runtime changes are additive: an optional keyword on the R63 scorer whose
  default output is byte-identical, and an additional candidate source in the
  R59 governor that yields nothing when no frontier artifact is present — the
  exact pre-R64 behaviour, which the governor's own suite re-ran green.

## 6. What R64 does not do

It does not merge, deploy, restart, register, adopt, promote, approve, order,
fill, purchase, subscribe, write a live store or modify a holding, cash or
NAV. It does not approve, regenerate or repair tonight's proposal; it does not
register the second MNST action the corporate-action report suggests. A
surviving specification is a research artifact for a later governed forward
process, and a human opens that door.

## 7. Next research target

FX carry at the **5-session horizon** is the only cell in the estate whose risk-controlled book is deployable-shaped (+2.0 to +2.3 %/yr increment, +0.19 to +0.22 Sharpe, drawdown −49 to −55 %, gross 2.4–2.5, one-way turnover 0.47) and whose binding failure is MULTIPLE TESTING alone (R63 campaign p 0.0067 against a 978-cell family). More backtest variants cannot buy it out of that failure, and R64 declined to let a smaller family do so. The honest next step is PROSPECTIVE: the immutable candidate `R64_FX_FUTURES_CARRY_H5_41E918F2` (freeze_record_hash `d5192245bc05…`) is the specification a human may take through the governed R61/R62 adoption path so TRUE_FORWARD evidence accrues on its own calendar. For the 1-session information — the strongest increment measured (+3.6 %/yr, +0.34 Sharpe, +4.0 % at 2x cost) on a daily book that is not deployable — the genuinely distinct implementation to pre-register next is SIGNAL HORIZON ≠ TRADING CADENCE: score at 1 session, trade on the 5-session cadence inside the no-trade band, both arms identical, and let the R63 scorer and the R64 book measure it (an R65 protocol, registered before any number is seen). Nothing paid is warranted: every need the frontier ranked above these was OWNED information, and the two it ranked highest — cross-asset and equity-index carry at the month — have now been measured under a proper book and fail.
