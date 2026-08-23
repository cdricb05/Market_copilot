# Release 39 — Autonomous Universal Alpha Discovery Engine

- **Date:** 2026-08-22
- **Branch:** `stage19-controlled-rebalance` (base commit `f484fc9`, the
  Release 38 closeout)
- **Owner package:** `alpha_agent/r39/` (17 modules)
- **Runner:** `scripts/run_release39_universal_alpha_discovery.py`
- **Regression:** `tests/test_release39_universal_alpha_discovery.py`
  (46 tests)
- **Audit guard:** `check_release39_universal_alpha_discovery`
  (25 blocking invariants)
- **Research root:** `D:\Stock_Prediction_app_data\universal_alpha_r39\`
- **Authoritative campaign:** `r39_universal_alpha_discovery_v1`

Release 39 removes the IDEA boundary and strengthens the EVIDENCE boundary.
No artificial limit on asset class, model family, representation family or
trade structure; a hard limit on leakage, hindsight, survivorship,
uncontrolled search, test-set reuse, wrong benchmarks, fake significance,
spend and production mutation. The central question:

> Given everything the estate knows point-in-time, what predictive
> structures and trade expressions can machine intelligence discover that
> produce robust, after-cost excess return versus the correct economic
> control?

## The five results, never collapsed

| axis | result |
|---|---|
| SYSTEM_RESULT | **PASS** — every phase executed end to end |
| DATA_RESULT | **UNIVERSAL_STATE_ASSEMBLED** — 4 lanes, 833 instruments, 311,267 decision rows, 17 families classified |
| DISCOVERY_RESULT | **EXECUTED** — 608 generated / 594 screened / 107 on Zone B / 12 locked confirmations |
| HISTORICAL_ALPHA_RESULT | **FAIL** — 2 BH survivors, 0 survive the deflated-Sharpe search-burden gate |
| FORWARD_CANDIDATE_RESULT | **NONE_FROZEN** |

**Terminal verdict: `R39_NO_ROBUST_ALPHA_DESPITE_UNIVERSAL_SEARCH`.**
`ALPHA_RESULT = FAIL` — it may be PASS only alongside
`R39_AUTONOMOUS_ALPHA_DISCOVERED`, a constant enforced in
`campaign.build_verdict`, and no success state was forced.

## Predecessor integrity — RESEARCHABLE is not TESTED

Release 38 measured 59 R36 cells `NATIVE_DATA_VERIFIED_RESEARCHABLE` and
executed 13 broad configurations. Those are different claims, and
`r38_cell_to_experiment_integrity_map.json` closes the gap cell by cell over
all 95 R36 blocked cells with a declared mapping (no experiment was
manufactured to make labels match):

| research status | cells |
|---|---|
| DIRECTLY_TESTED | 2 |
| VALIDLY_REPRESENTED_BY_GROUP_TEST | 45 |
| **DATA_AVAILABLE_BUT_NOT_TESTED** | **12** |
| MISSING_REQUIRED_INFORMATION_LEG | 6 |
| STILL_BLOCKED | 30 |

The 12 untested-with-data cells (inter-market relative value, international
index mean reversion / relative value / macro-conditional) are recorded as
AVAILABLE, never as rejected hypotheses. No conflating sentence was found in
the R38 canonical artifacts; the map makes the distinction a table rather
than a reading.

## The universal point-in-time state

One alignment rule everywhere: features at decision date *d* read
observations dated ≤ *d* minus the family's declared publication lag;
targets realise strictly after *d*; missingness is a mask, never a fill;
nothing is forward-filled before it was observable.

- **FUT** — 68 native futures markets (commodities, US + international
  rates, FX, international equity index) from the frozen R38 dated-contract
  layer: 29,728 monthly decision rows, 1979–2026, per-market R38 modelled
  costs, per-class equal-weight controls, 21- and 63-session targets.
- **VX** — the Cboe VX curve weekly: 1,076 rows, 5-session target.
- **EQ** — the survivorship-safe US single-name cross-section (Release-30
  dataset, Russell-1000 current+past): 277,466 rows, 304 monthly dates
  2001–2026, forward labels with truncation masks.
- **ETF** — a fixed 11-name total-return sleeve read from the existing
  Norgate entitlement (credit HYG/LQD, duration TLT/IEF/SHY, equity
  SPY/EEM/EFA, GLD, VNQ, BIL): 2,997 rows — the CREDIT and REAL_ESTATE
  cells futures cannot express.
- **Macro overlay** — market-quoted FRED series (credit spread, curve,
  bills, breakevens, real rates, Coinbase BTC/ETH) at one-session lag; TRUE
  ALFRED vintages for CPI (publication-dated); Cboe VIX/VIX3M term.
  Current-vintage snapshots of revised series (foreign CPI, OECD monthly
  averages) are **excluded as PIT_FAILURE** — a 2026 snapshot of a revised
  series is not history.

Estate classification (`universal_data_estate.json`): 17 families, 12
admitted (8 used in state, 4 available-not-used with
`DATA_AVAILABLE_BUT_NOT_TESTED` status: SEC insider archives, direct
companyfacts, NY-Fed dealer positioning, EIA physical balances), 5 excluded
for NAMED reasons only — crypto outright `INSUFFICIENT_HISTORY` (CME
futures fail the frozen 10-year floor; spot indices admitted as
information), news/events `INSUFFICIENT_HISTORY`, analyst archives
`SURVIVORSHIP_FAILURE`, revised-snapshot macro `PIT_FAILURE`,
international single names `UNAVAILABLE`.

## Evidence zones — search without p-hacking

All lanes share ONE calendar, inherited from the R38 partition:

- **ZONE A (discovery)** ≤ 2007-02-23 — searched freely, purged 3-fold CV,
  no Alpha claim can come from it;
- **ZONE B (selection)** 2007-04-24 → 2016-11-01 — every evaluation counted
  in a reuse ledger (107 distinct candidates, 107 evaluations);
- **ZONE C (locked confirmation)** 2016-12-29 → 2026-07-09 — opened only
  for a frozen, hashed finalist set under the Release-31 lockbox budget
  (≤ 12, ≤ 2 per family, ONE execution per spec, no revise-and-retry).

Zone C is labelled **`HISTORICAL_CONFIRMATION_EVIDENCE`** and never
`FRESH_UNSEEN_EVIDENCE`: Releases 31–38 read this chronology at the family
level, and the label refuses to pretend otherwise. TRUE_FORWARD evidence
begins only after a frozen forward handoff.

## The search that ran

- **Targets** (7, no combinatorial explosion): after-control excess at
  21/63/5 sessions, cross-sectional rank, sign, realised vol, and a Q10
  tail expressed as a loss, all realised strictly after the decision.
- **Trade space** (registered, costed, controlled): TS_OUTRIGHT vs the
  vol-matched passive EW basket of its own scope; XS_LONG_SHORT (terciles,
  ≥ 2 per leg) vs risk-matched cash; GROUP_SPREAD inside declared economic
  groups; regime gates and abstention overlays as variants. Cost on TRADED
  NOTIONAL (buys and sells) at per-market R38 modelled rates
  (`MODELLED_NOT_OBSERVED`), 2× stress arm.
- **Representations** (11 families, 206 machine-generated features with
  full lineage): raw, classical, seeded AUTO-transform grammar, seeded
  SYMBOLIC expression trees (depth ≤ 3), spectral (autocorrelation,
  variance-ratio), walk-forward LATENT PCA (loadings + residual momentum),
  an annually re-estimated sparsified lead-lag GRAPH, market structure
  (MA-distance, breakout, contraction, range), and FIBONACCI retracement
  proximity from REAL-TIME-CONFIRMED pivots (a pivot exists only 10
  sessions after its extremum) against PLACEBO levels (33/42/55/58/65/71).
- **Model zoo** (2026 inventory, licences and machine limits recorded):
  admitted CPU families — baselines, ridge/elastic-net/Huber/logistic,
  XGBoost 3.4 / LightGBM 4.7 / sklearn HistGB, random/extra forests, kNN
  analogues, LightGBM quantile, a GaussianMixture regime gate with
  per-regime ridge experts, equal-weight prediction ensembles, transparent
  hand rules (the R31–38 anchors). Deferred with NAMED reasons and compute
  classes: deep sequence (TFT/PatchTST/Mamba-class), foundation time-series
  (TimesFM/Chronos/Moirai-class), tabular foundation (TabPFN-class), GNNs,
  self-supervised and chart-vision encoders — torch does not fit the venv
  drive's ~6 GB free space, the GTX 1650 4 GB is below training footprints,
  and `MAY_DOWNLOAD_MODEL_WEIGHTS = False` is a frozen contract flag. The
  lane exits through `compute_escalation_request.json` (16–24 GB GPU,
  ~40 GPU-hours, ~$40–90, or a $0 CPU-only path if the operator frees
  ~10 GB), not through a quiet install. CatBoost and full GPs are
  `NOT_WORTH_COMPUTE` (redundancy is not diversity).
- **Budget** (`search_budget_ledger.json`): 608 generated → 594 screened
  (Zone A) → 99 promoted → 40 in stage 3 (ensembles, regime/tail variants)
  → 11 finalists + the reserved combination slot frozen → 12 locked
  confirmations. Free open-source installs for this release:
  scikit-learn, scipy, xgboost, lightgbm, statsmodels (licences recorded
  in the contract).

## What the locked confirmations said

Benjamini–Hochberg (q = 0.10, the Release-31 owner) over all 12 locked
p-values rejected **two**:

| candidate | what it is | Zone C |
|---|---|---|
| `c39_c9233eccaa74` | MACHINE-GENERATED: ridge over the WIDE bundle (classical + macro + spectral + latent + graph + market-structure), tercile long/short across all 68 futures markets | **+3.47 %/yr, t = 2.43, Sharpe 0.66**, halves same-sign, 0/68 leave-one-market-out sign flips, survives 2× costs (+2.80 %/yr) |
| `c39_diversified_combination` | inverse-Zone-B-vol mix of all 11 finalist excess streams (mean cross-correlation 0.31) | **+0.96 %/yr at 2.6 % vol, t = 2.47, Sharpe 0.37**, hit rate 55 % |

**And both die on the search-burden gate.** The deflated Sharpe ratio with
the effective trial count from the Zone-B reuse ledger (107 distinct
candidates, cross-trial Sharpe variance 0.0046) prices the expected maximum
per-period Sharpe of a null search at 0.173; the survivors sit at 0.190
(DSR 0.586) and 0.106 (DSR 0.058) against the frozen 0.95 bar — statistics
a lucky best-of-107 produces routinely. Hansen SPA over the finalist family
agrees (p = 0.15). `T_ABOVE_2_IS_NOT_QUALIFICATION` did exactly its job:
the frozen qualification (BH **and** DSR ≥ 0.95 **and** sign stability
**and** 2× cost survival **and** no LOMO flip) passed nothing, so nothing
was frozen for TRUE_FORWARD and the verdict states it plainly.

Honest reading of the strongest finding: the wide-representation machine
book confirmed out-of-selection at t = 2.43 having ranked only 10th of 107
on Zone B (t = 1.62) — that ordering inversion is precisely why the DSR
denominator exists. Its Zone-C excess is after-cost net return versus
**risk-matched cash** — the frozen control for a self-financed XS book
(futures returns are already excess of financing); an earlier narrative
sentence here said "its own passive basket", which was wrong and is
corrected by the continuation's `wide_control_reconciliation.json` — and
it is robust to costs and concentration, and still not distinguishable
from selection luck at this search scale. It is the single best candidate
this estate has produced since Release 31 and it is NOT Alpha.

## What machine search added — and did not

- Zone-B family ranking: machine families led (REGIME_TAIL t 2.41,
  ENSEMBLE 2.19, AUTO 1.90) over every hand rule (best 1.70, the carry
  rule); the best surviving confirmation was machine-generated (WIDE).
  Machine search added ordering, breadth and the only BH survivor — and
  none of it cleared the burden its own breadth creates.
- **Fibonacci lost to its own placebo**: FIB max Zone-B t = −0.90; PLACEBO
  levels +0.97; both indistinguishable from zero. Not even
  `PULLBACK_STRUCTURE_MAY_MATTER` is supported on this estate — the family
  is closed as uninformative, not as "controversial".
- LATENT/GRAPH/SPECTRAL/MSTRUCT/POSITIONING representations: nothing above
  t = 1.0 on Zone B. The single-name EQ lane (survivorship-safe, 753
  names/month): best ensemble t = 1.60 on Zone B decayed to 0.25 on
  Zone C — consistent with the terminal owned-equity exhaustion verdicts
  of Stages 8–26.
- The VX carry rule re-measured at weekly cadence: +64 %/yr gross-of-scale
  excess at t = 1.60 (Sharpe 0.31) — direction confirmed for the third
  time in this estate, significance still absent. Two VX finalists
  collapsed to the same realised book (a rule ignores its feature bundle);
  the redundancy cost one lockbox slot and is recorded, not hidden.

## Safety

MONEY_SPENT **$0.00** · CLOUD_COMPUTE_SPEND $0.00 · NEW_SUBSCRIPTIONS 0 ·
SUBSCRIPTION_CHANGES 0 · TRIALS_STARTED 0 · OPERATIONAL_WRITES 0 (proven by
`scripts/r33_operational_write_attribution.py --release R39` →
`ATTRIBUTED`, 0 findings, 18 sources scanned) · PORTFOLIO_MUTATIONS 0 ·
MODEL_PROMOTIONS 0 · PRODUCTION_RESTARTS 0 · norgatedata stays pinned at
1.0.74. Models were TRAINED (that is the release) and none was promoted;
every purchase/renewal state carries `purchase_authorised: False`,
`renewal_authorised: False`.

## Parallel track — Intrinio / Steele Barcomb

Unchanged and unblocked: the five-ticker sample request (AAPL / MON / META /
HTZ / CALM, `SCHEMA_AND_PIT_VALIDATION_ONLY`) remains operator-ready and
unsent; fingerprints recorded in `intrinio_parallel_status.json`. Nothing
was sent, purchased, trialled or licensed.

## Ownership map

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            `alpha_agent/r39/contract.py`
universal data estate        `alpha_agent/r39/estate.py`
R38 cell/experiment map      `alpha_agent/r39/integrity.py`
universal PIT state          `alpha_agent/r39/universal_state.py`
target factory               `alpha_agent/r39/target_factory.py`
trade-space generator        `alpha_agent/r39/trade_space.py`
representation factory       `alpha_agent/r39/representation_factory.py`
model technology + adapters  `alpha_agent/r39/model_registry.py`
evidence zones + lockbox     `alpha_agent/r39/zones.py`
autonomous director          `alpha_agent/r39/discovery_director.py`
search-budget ledger         `alpha_agent/r39/search_budget.py`
economic judge (delegating)  `alpha_agent/r39/judge.py`
search-burden correction     `alpha_agent/r39/burden.py`
alpha frontier + combination `alpha_agent/r39/frontier.py`
forward handoff + Intrinio   `alpha_agent/r39/handoff.py`
orchestration + verdict      `alpha_agent/r39/campaign.py`
===========================  =================================================

Reused, never rebuilt: r31 hashing/immutability, r31 multiple testing
(BH / SPA / stationary bootstrap), the r31 lockbox BUDGET constants, r34
economics (annualisation, Newey–West excess, volatility-matched control,
drawdown/CVaR via r31), r36 minimum detectable excess. Release 39 defines
NO purchase gate, NO coverage authority, NO second economic judge, NO
second multiple-testing owner and NO second forward-evidence system
(`alpha_agent/r39/economics.py`, `multiple_testing.py`, `purchase_gate.py`,
`coverage.py`, `lockbox.py` et al. are forbidden by the architecture audit).

## What Release 40 inherits

The binding constraint is now measured, in order: (1) **information** — the
12 DATA_AVAILABLE_BUT_NOT_TESTED cells and the four available-not-used
information families (EIA physical balances, NY-Fed positioning, insider
flow, direct PIT fundamentals) are the only owned information this engine
has not consumed; (2) **compute** — the deep/foundation model families are
the only unexecuted model families, priced at ~$40–90 or a $0 disk-cleanup
path in `compute_escalation_request.json`; (3) the wide-representation
cross-asset book is the natural first TRUE_FORWARD shadow candidate IF the
operator chooses to spend a forward-evidence slot on a DSR-unqualified
near-miss — Release 39's own recommendation is to let it season: the same
spec re-confirms cheaply on genuinely fresh data as time passes, which is
the one denominator-free test that exists.

---

# Continuation (campaign `r39_universal_alpha_continuation_v2`)

The operator ordered the campaign continued — candidate prosecution,
information expansion, model-frontier completion, prospective shadow
registration — under a NEW immutable campaign id with the search burden
NEVER reset. The v1 artifacts are unchanged; the v2 Zone-B reuse ledger is
initialised from v1's 107 distinct candidates and every continuation
evaluation adds to it. Final cumulative burden: **107 v1 + 87 continuation
= 194 effective trials** (`cumulative_search_ledger.json`).

## Track A — WIDE prosecuted, reconstruction EXACT

`c39_c9233eccaa74` reconstructs bit-for-bit from immutable artifacts: the
candidate id reproduces from the spec dict, the spec hash reproduces from
the frozen 86-column bundle
(`cfd2ec367018412d5fb83909ba15f62900bd0e4f2ef974f53e04dea1096a361c`), and
the recomputed Zone-B and Zone-C economics match the locked confirmation
within 1e-9 (`wide_reconstruction.json`; the frozen return stream is
`wide_zone_c_streams.csv`). **Control reconciled**: the computation always
used RISK_MATCHED_CASH — the correct frozen control for a self-financed
futures long/short book — and the "passive basket" narrative sentence was
the error (`wide_control_reconciliation.json`,
`RESULT_STANDS_UNDER_ITS_DECLARED_CONTROL`).

## Track B — factor residualisation: the excess is NOT known premia

The frozen Zone-C excess stream regressed on 13 known-premia factor
streams built from the same frozen panel (trend, XS commodity momentum,
carry ×3 classes, betas ×3, short-VX, credit HYG−IEF, seasonality,
positioning; HAC/Newey-West, monthly aggregation, 115 months):
**residual alpha +3.65 %/yr, t = 2.58, R² = 0.415** — the factor set
explains 41 % of the variance and essentially none of the mean
(factor-explained ≈ +0.57 %/yr). Largest exposures: XS commodity momentum
(β 0.35, t 4.2, +1.6 %/yr) offset by a NEGATIVE credit loading (β −0.37,
t −2.5, −1.6 %/yr). Halves: +5.27 %/yr (t 2.53) then +2.82 %/yr (t 1.38),
same sign. Zone-B reference: residual alpha only +1.7 %/yr (t 0.81).
Nested hierarchy (RAW +4.08 % → AFTER COST +3.38 % → AFTER CONTROL
+3.47 % → AFTER KNOWN PREMIA / RESIDUAL +3.65 %) in
`wide_factor_residual_alpha.json`. Diagnostics cannot upgrade
qualification: the DSR gate remains failed at the cumulative trial count.

## Track C — group/cluster kill tests: zero sign flips

Twelve kills (every asset class, every commodity economic group, US vs
international rates), fixed model, cached predictions, canonical r31
stationary block bootstrap: **0/12 sign flips**. Weakest survivor:
exclude ALL 39 commodities → +1.72 %/yr, t 1.03 (bootstrap p 0.138);
excluding energy alone leaves +2.17 %/yr (t 1.67). The edge is broad, not
one block (`wide_group_kill_tests.json`).

## Track D — information attribution: aggregation, not one family

Paired BASE(+family) increments on Zone B (all ledger-counted): no single
family is significant (largest paired increments: positioning t 1.10,
macro t 0.91, graph t 0.82). The edge is the UNION of many weak
orthogonal features under ridge. Fixed-coefficient Zone-C decomposition:
LATENT loadings carry ~53 % of prediction variance, CLASSICAL ~31 %
(`wide_information_attribution.json`) — which exposed a real v1 defect:

**Representation-coverage defect (found and repaired as NEW features).**
The v1 latent/graph features exist only era-patchily — the fragmented
per-market date axis leaves per-window coverage at the knife edge of the
80 % rule — measured EMPTY across the entire selection zone (Zone B),
partially live in Zones A/C. Zone-B family verdicts therefore judged
filler features, and the confirmed WIDE book carried live latent loadings
into Zone C that its selection evidence never saw. The frozen generator
stays byte-identical; the continuation ships month-period-aligned
`latent2_*`/`graph2_*` features (~89 % coverage) as new candidates — and
they are STILL noise (latent2 ridge t 0.45, graph2 rules ≤ 0), and
appending them to the live WIDE columns dilutes it (t 0.61 vs 1.62)
(`representation_repair.json`).

## Tracks E/F — all 12 cells executed; four information families consumed

All twelve DATA_AVAILABLE_BUT_NOT_TESTED cells now carry judged Zone-B
results (`integrity_cell_execution.json`), including
`RATES_INTERNATIONAL::RELATIVE_VALUE` on an 11-market international bond
futures layer built at $0 through the canonical R38 builder (CGB, FBTP,
FGBL, FGBM, FGBS, FGBX, FOAT, LLG, SJB, YXT, YYT; 3 bps/side per the
frozen R38 contract; frozen layers untouched). Best cells: international
rates carry RV t 2.47, DM macro-conditional t 2.14, precious RV t 2.19 —
all below the declared Zone-C pre-gate (t ≥ 3.0 at the cumulative burden),
so no confirmation budget was burned.

The four owned information families (`new_information_increments.json`,
paired increments only — standalone significance does not count): **EIA**
(full-zone paired, 1982+): no robust increment (ridge pair 1.90 → 1.99,
inc t 0.21; the lightgbm inc t 2.61 is recovery from a negative base to
t 0.84, not evidence). **NYFed** (current-format codes begin 2013-04):
fitted point-in-time pairs are structurally impossible in any in-zone fit
window — named blocker, $0 unlock = decode the legacy 1998–2013
mnemonics; the covered-window dealer rule is mildly negative (t −1.99).
**SEC insider** (2008+, sub-split): per-name feature joined through the
phase-24 position-indexed identity bridge; market-breadth SPY timing
NEGATIVE vs holding SPY (t −1.18). **Direct PIT fundamentals** (R30
7-column block, sub-split): increments t 0.62–0.90, nothing robust.
Structural finding: free information families begin 1982/1998/2008/2009 —
families without Zone-A coverage cannot train PIT models in the discovery
zone at all.

Trade-space extras (`trade_space_expansion.json`): abstention overlay on
CLASSICAL t 1.58; sector-neutral equity books EXECUTED (the earlier
"no sector column" blocker was wrong — the phase-24 panel carries a
sectors axis; the corrected claim is recorded); butterflies remain blocked
(NO_THIRD_CONTRACT_IN_FROZEN_LAYER) and free pairwise cointegration stays
excluded by the no-pair-explosion contract.

## Track G — $0 model frontier completed

torch 2.13 CPU (BSD-3) installed to the research drive; deep models
trained FROM RANDOM INITIALISATION — `MAY_DOWNLOAD_MODEL_WEIGHTS` remains
False and no pretrained weights entered the estate. New families: neural
tabular (mlp), calibrated probability (isotonic), distributional
quantile blend, causal TCN and GRU over trailing 12-decision sequences,
masked-autoencoder embeddings. Best new model: **from-scratch TCN
cross-section, Zone-B t 2.07** — above every ridge/boosted baseline
(ridge WIDE 1.62, ridge CLASSICAL 1.14) and still below the pre-gate.
Foundation lanes stay blocked for NAMED reasons (TabPFN-class: gated
weights/licence; Chronos/TimesFM-class: indefensible contamination
semantics on public financial series; pretrained vision: the hypothesis
IS the pretrained prior) in `model_frontier_completion.json`.

## Tracks H/I — prospective evidence started

Three economically distinct research shadows frozen BEFORE any future
outcome (`research_shadow_registry.json`,
`forward_specification_hashes.json`): the WIDE book (its A+B ridge
coefficients serialised and hashed — capture never refits), the
transparent all-futures carry rule, and the VX term-structure carry rule.
All carry RESEARCH_SHADOW_ONLY / HISTORICAL_QUALIFICATION=FAIL /
PROMOTION_ALLOWED=False. Ledgers reuse the canonical Phase-27/28
chain-hash primitives (`api.paper_trading_desk`) pointed at the research
root; capture refuses any decision date at or before the freeze, so
historical observations can never enter. First eligible TRUE_FORWARD
decisions: the first per-market month-end after the freeze (2026-08-31)
for the futures shadows, the first 5th-session VX decision after it for
the VX shadow. Operator capture:
`scripts/run_r39_shadow_capture.py --mode capture` (manual; AUTOMATION
OFF). The pre-registered sequential design
(`prospective_validation_design.json`) uses an anytime-valid capped-bet
e-process (success e ≥ 20, declared futility and horizon boundaries) and
registers the honest arithmetic: at WIDE's own volatility, detecting its
point estimate at 80 % power needs ~173 monthly observations — the
shadows are cumulative evidence, not a fast verdict.

## Continuation verdict

`R39_CONTINUATION_NO_NEW_QUALIFIED_ALPHA`
(`continuation_verdict.json`): no continuation candidate cleared the
declared Zone-C pre-gate (best Zone-B t 2.47 < 3.0), so v2 made ZERO
Zone-C accesses; WIDE's status is UNCHANGED by diagnostics — a fully
robust, factor-orthogonal near-miss whose only remaining test is the
prospective evidence now accumulating. Track-K qualification (all v1
gates at the cumulative 194-trial denominator PLUS positive residual
alpha) admitted no historical alpha candidate. Safety: $0 spent, 0
operational writes, 0 promotions, production untouched.
