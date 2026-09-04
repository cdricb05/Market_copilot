# Release 58 — Orthogonal Alpha Offensive

**Verdict: `PROSPECTIVE_ALPHA_CANDIDATES_FROZEN`.** Thirteen pre-registered
families across three economically distinct groups — fundamental rescue, blend
and momentum gating, information change — prosecuted for the first time on a
**survivorship-safe point-in-time FUNDAMENTAL panel** built by joining the
Norgate S&P 500 Current & Past price panel to the owned SEC EDGAR companyfacts
store: **11/11 judged families `NO_ALPHA_EVIDENCE`, 2 `DATA_HOLD_COVERAGE`, 0
survive Benjamini–Hochberg at q=0.10.** Four immutable forward challengers were
frozen, and four information families were *refused* rather than described.

- **Branch:** `r58-orthogonal-alpha-offensive` (worktree
  `D:\paper_trader_r58_orthogonal_alpha`, built over `76f3279`)
- **Protocol:** `research/r58/R58_RESEARCH_PROTOCOL.json` — every layer, family,
  variant grid, cost, gate and threshold registered **before** any experiment
  ran; the four pre-experiment amendments are disclosed in §11
- **Campaign:** `r58_orthogonal_alpha_v1`
- **Research root (new, disclosed):** `D:\Stock_Prediction_app_data\r58_orthogonal_alpha`
- **Safety:** research only; no order, fill, broker, promotion, sleeve
  activation or operational-store write. The live `C:\Users\binis\paper_trader`
  checkout is unchanged at `54cecf7` with zero tracked modifications; the R56
  and R57 evidence roots are hash- and mtime-verified unmodified.

---

## 1. The one-sentence result

**The fundamental leg is not the victim and momentum is not the villain.** On 43
months of untouched, survivorship-safe out-of-sample data the incumbent 50/50
blend earned **+7.17%/yr net excess** while every fundamental-only construction
earned between **−1.15% and +0.65%** — the exact opposite of what the desk's
eight-session live forward sample says, and R58's central hypothesis is
therefore **rejected on the only sample large enough to carry the question.**

## 2. What R58 built that did not exist

R57 wrote that "the fundamental leg cannot be replicated on the 20-year panel
(no owned PIT fundamental history)". That was true of R57's panel and false of
the repository. Two owned assets had never been joined:

| asset | what it is |
|---|---|
| R57 Norgate panel | 1,897 S&P 500 Current & Past securities, PIT index membership, TOTALRETURN prices, delisted names retained |
| `sec_companyfacts_stage24.sqlite` | 1,615,843 XBRL facts, 846 CIKs, **real SEC `filed` dates**, 2009-04-15 .. 2026-07-31, restatements preserved as distinct observations |

Joined through the identity layer's RESOLVED CIK bridge, they produce
**PANEL-F: 885 symbols, 237 of them delisted, 842 CIKs**, covering **88.8% of
PIT S&P 500 member-days in 2010 rising to 98.7% in 2026**. This is the first
time this project can score a fundamental signal on a universe containing the
companies that died.

**The new capability that made it possible.** Stage 24 rejected 575,008
non-annual flow facts and used annual durations only. R58 uses the quarterly and
year-to-date facts through a `YTD_DIFF` construction — anchor on the latest
annual fact A, and where a fresher current-year YTD fact Y and the prior year's
same-length YTD fact P exist, return `A + Y − P`. That is what makes both
freshness (median observation age 74 days in the lockbox, against a live
operational leg frozen at 2026-05-22) and *change* signals possible at all.

### 2.1 Four reader defects, found and fixed before any experiment ran

The reader was validated against one company's actual filings before a single
score was computed. It failed four times, and every failure would have silently
corrupted every fundamental family:

1. **Stale synonym.** A ladder returning the first tag with any value kept
   serving `Revenues` for Apple years after ASC 606 — reporting $265.6bn of
   revenue in 2021 when the true figure was $365.8bn. Fixed: the freshest anchor
   wins, ties broken by ladder order.
2. **YTD telescoping.** Full-year facts also live in the YTD store, so an older
   anchor telescoped forward (`A_prior + FY − FY_prior == FY`) and made every
   year-on-year change identically zero. Fixed: full-year durations excluded
   from the YTD extension.
3. **Quarter-back prior anchor.** Many filers publish a genuine "twelve months
   ended \<quarter date\>" fact, so the adjacent annual entry is one *quarter*
   back. A year-on-year change was measuring a quarter. Fixed: the prior anchor
   is the annual period end closest to one year before, within 120 days, and is
   never freshened.
4. **52/53-week tolerance.** Apple's Q3 YTD is 272 days one year and 279 the
   next; a ±5-day match refused the YTD_DIFF path for every such filer. Fixed:
   ±10 days, which cannot confuse a 90-day quarter with a 181-day half.

All four are data-reader correctness, not threshold tuning; `tests/
test_release58_orthogonal_alpha.py` carries a regression for each.

## 3. What the operational fundamental leg actually is

The champion reads a frozen CSV. Measured, not assumed:

| property | measurement |
|---|---|
| Rows | 38,725 over 545 tickers |
| Pseudo-date collapse | **17,503 rows (45.2%) stamped on one date, 2016-06-23, covering 267 tickers, of which 17,236 are exact duplicates** |
| Survivorship | **544 of 545 tickers are still present in the final month**; one exit in ten years, where a real S&P 500 panel loses ~250 members |
| Staleness | last rebalance date **2026-05-22** — the live leg's scores cannot be fresher |

Any rank IC computed on that file is dominated by a single cross-section, and
its effective sample is a fraction of its row count. R58 therefore treats it as
the *object of study* and never as evidence. Stage 24 reached the same
conclusion about its survivorship (`SURVIVOR_BIASED_CURRENT_SNAPSHOT`); the
duplicate-row collapse is new here.

## 4. The campaign table — every family, every judged number

Judge: long-only equal-weight top-50 versus the equal-weight R58 eligible
universe, both charged 12.5bp/side on their own turnover, NEXT_CLOSE entry,
21-session cadence and horizon. Layers: DISCOVERY 2011-07→2017-12 (78),
VALIDATION 2018-01→2022-12 (60), **LOCKBOX 2023-01→2026-07 (43)**, embargoed at
each boundary. One variant per family selected on validation alone and persisted
(19:18:47Z) before the lockbox ran (19:21:26Z).

| id | family | selected | validation | **lockbox** | t | turn | verdict / failed gates |
|---|---|---|---|---|---|---|---|
| A1 | fundamental composite | 1:2 | +2.89% | **−1.15%** | −0.25 | 0.087 | materiality, sign-flip, halves, BH |
| A2 | FCF/assets alone | avg3 | +3.04% | **−0.29%** | −0.09 | 0.033 | materiality, sign-flip, halves, sector, BH |
| A3 | reversed accruals alone | level | +3.88% | **−1.10%** | −0.25 | 0.086 | materiality, sign-flip, halves, BH |
| A4 | composite, freshness-restricted | 550d | +1.69% | **+0.65%** | 0.15 | 0.079 | materiality, halves, BH |
| B2 | fundamental-heavy blend 75/25 | 0.75 | +2.47% | **+3.40%** | 0.79 | 0.129 | BH |
| B3 | momentum as a veto | tercile | +3.66% | +2.85% | — | — | **DATA_HOLD_COVERAGE** |
| B4 | regime-conditional momentum | vol_only | +1.37% | **+2.87%** | 0.74 | 0.297 | neighbour-sign, BH |
| B5 | composite + hold band | K=150 | +3.69% | **+1.05%** | 0.28 | 0.019 | materiality, halves, BH |
| C1 | profitability acceleration | stab | −1.70% | **+4.10%** | 1.45 | 0.180 | sign-flip, BH |
| C2 | accrual deterioration change | level | +1.89% | **−2.80%** | −0.86 | 0.144 | materiality, sign-flip, halves, drawdown, BH |
| C3 | working-capital build | avg3 | −1.84% | **+2.69%** | 0.83 | 0.084 | sign-flip, BH |
| C4 | R&D intensity | avg3 | +5.97% | +13.30% | — | 0.018 | **DATA_HOLD_COVERAGE** |
| C5 | post-filing drift | react5/42 | −3.89% | **−4.10%** | −0.99 | 0.480 | materiality, halves, turnover, BH |
| **B0** | **incumbent 50/50 shape** | 0.50 | **+0.05%** | **+7.17%** | 1.39 | 0.273 | *diagnostic reference — not an alpha claim* |

Smallest lockbox one-sided p among the thirteen FDR-counted families is **0.0732
(C1)**, against a rank-1 Benjamini–Hochberg threshold of **q/m = 0.00769**.
Nothing is close. Cumulative prosecuted-hypothesis burden through R57: ~302.

**B2, B4 and C1 failed only on multiple-testing.** That is a real observation and
it is not a licence: three families whose only surviving objection is that
thirteen were tested is exactly the pattern a discovery burden of 302 exists to
discount.

## 5. Track 1 — the fundamental rescue, answered

**Did fundamental-only beat the champion? No — it lost by 8.3 percentage points a
year in the lockbox.** A1 (−1.15%) against B0 (+7.17%) on the identical
universe, judge and costs. A1, A2 and A3 were *positive across six validation
years* (+2.9%, +3.0%, +3.9%) and all three flipped **negative** in the lockbox.
That is the same regime-instability signature R57 measured in eight price
families, now demonstrated for fundamentals on honest point-in-time data. It is
not a momentum problem and it is not a staleness problem: A4, which forces a
fresh observation, earns +0.65%.

### 5.1 Component attribution (Track 7)

Same universe, same judge, same costs; only the construct differs:

| construct | discovery | validation | **lockbox** | rank IC D/V/L |
|---|---|---|---|---|
| FCF/assets only | +0.57% | +2.26% | **+0.83%** | +0.020 / +0.027 / +0.004 |
| reversed accruals only | +0.47% | +3.88% | **−1.10%** | +0.018 / +0.016 / +0.001 |
| composite 1:1 | +0.84% | +1.68% | **+0.69%** | +0.020 / +0.026 / +0.004 |
| composite 2:1 (FCF-heavy) | +1.63% | +2.88% | **+0.79%** | +0.020 / +0.029 / +0.005 |
| composite 1:2 (accrual-heavy) | +1.08% | +2.89% | **−1.15%** | +0.019 / +0.022 / +0.003 |

**FCF/assets is the piece whose sign never flips; the accrual leg is the piece
that does.** Every FCF-weighted construction stays positive in all three layers
and every accrual-weighted one goes negative in the lockbox. This corroborates
Stage 24's standalone measurement (FCF rank IC t 2.23, accruals t 0.29) under a
lockbox Stage 24 never ran. The magnitudes, however, are small: +0.83%/yr with
t 0.24 is below the 1.5% materiality floor, so the finding is *which component
to keep if you keep one*, not evidence that it pays.

### 5.2 Why the incumbent won the lockbox (post-hoc diagnostic)

The momentum leg alone, on the R58 universe: discovery **−0.57%**, validation
**−2.09%**, lockbox **+8.08%**. B0's advantage is entirely the momentum leg in
the one window where momentum worked — the same +7–9%/yr, validation-negative
sign flip R57 rejected as family E1. **So "the momentum leg is destroying the
blend" and "the momentum leg carried the blend" are both true, in different
windows, which is precisely why neither is alpha.**

### 5.3 The live evidence still disagrees, and that is the finding

The desk's TRUE_FORWARD matured outcomes, re-read read-only (now **25 distinct
matured sessions**, up from R57's reading):

| component | h | n | buy side | sell side | rank IC |
|---|---|---|---|---|---|
| blend (champion) | 20 | 8 | −4.24pp (t −5.9) | −1.93pp (skill t 2.8) | −0.01 |
| fundamental leg | 20 | 8 | **+4.39pp (t 5.1)** | −2.49pp (skill t 17.1) | +0.20 |
| momentum leg | 20 | 8 | −0.62pp | **+8.58pp INVERTED** | −0.23 |

Eight matured sessions say fundamental-only should win. Forty-three months of
untouched out-of-sample data say it loses by 8.3pp/yr. **R58 does not resolve
that disagreement in favour of the number it likes** — it reports that the live
sample is far too small to overturn the historical one, and that the historical
one covers a window in which the incumbent's other leg happened to work.

## 6. Track 3 — information change, the class R57 never tested

Five families built from owned SEC facts with real filed dates. None survived,
and two are worth naming:

- **C1 profitability acceleration** produced the best BUY side of any judged
  family (**+5.72%/yr, t 1.90**, rank IC +0.017) and the smallest p in the
  campaign — but it was *validation-negative* and flipped sign into the lockbox.
- **C5 post-filing drift** — a pure information-timestamp signal using no
  accounting number at all — was **negative in every layer** (−0.76%, −5.21%,
  −4.10%) at 48% monthly turnover. Post-filing winners revert in this universe;
  the drift hypothesis is contradicted, not merely unsupported.

## 7. The two coverage-blocked families

The protocol's coverage gate (≥60% of the eligible universe scored on ≥90% of
lockbox dates) fired on two families, which therefore receive **no alpha
verdict**. Both are reported as labelled within-coverage diagnostics against a
benchmark restricted to the names they can score.

### 7.1 C4 R&D intensity — the campaign's most striking number, and a sector bet

Stage 24's only FDR survivor, never previously lockboxed. On its own universe
(214 of 491 eligible names) it looks extraordinary: **+2.63% / +5.27% /
+11.86%** across the three layers, sign-consistent, both lockbox halves positive
(+9.21%, +14.39%), rank IC positive throughout, **1.8% monthly turnover**.

Then the sector test:

| layer | excess | **excluding the largest sector** |
|---|---|---|
| discovery | +2.63% (t 1.17) | +2.49% (t 1.74) |
| validation | +5.27% (t 1.36) | +1.20% (t 0.38) |
| **lockbox** | **+11.86% (t 2.34)** | **−0.22% (t −0.06)** |

The lockbox book is **57.6% Information Technology and 23.4% Health Care**.
Removing the largest sector at each decision date removes the entire excess.
**R&D intensity in 2023–2026 is the AI capital-expenditure cycle wearing a
factor's name.** This also retrospectively qualifies Stage 24's survivor.

### 7.2 B3 momentum-as-a-veto — blocked by construction, not by data

B3 was the most robust *shape* in the campaign: sign-consistent in all three
layers (+1.44%, +4.01%, +2.64% restricted; +2.85% lockbox against the shared
benchmark), sign survives sector exclusion in all three, both lockbox halves
positive, drawdown no worse than its benchmark, 18% monthly turnover. Its
t-statistics are 0.95 / 1.51 / 0.98 — not significant — and it never received a
verdict because a veto excludes a third of the universe *by construction*, which
the coverage gate cannot distinguish from a data gap.

**That is a protocol lesson, recorded rather than patched:** the coverage gate
was written for missing data and mis-fires on deliberate exclusion. It was not
moved after the fact. The honest route for B3 is forward evidence, and §9 takes
it.

## 8. Tracks 5, 6 and 8

**Cross-asset conditioning (Track 5).** Regime variables are PIT by construction
from the R57 futures panel: the equal-risk cross-market composite's 126-session
trend, and the E-mini S&P 500's 21-session realised volatility against its own
252-session median. B4 (momentum weighted only in RISK-ON) earned **+2.87%** in
the lockbox against A1's −1.15% — conditioning helped — but B4 failed its
grid-neighbour sign gate and BH, and its selected variant was volatility-only.
**Cross-asset information improved the equity signal without rescuing it.**
FRED/ALFRED is confirmed genuinely point-in-time (149,234 vintage records, 12
series, `realtime_start` populated) but its per-series vintage start dates
(VIXCLS 2010-11, NFCI 2011-05, T10Y2Y 2014-01, credit spreads 2023-08) make it a
descriptive overlay, not a conditioner available at every decision date.

**BUY versus SELL (Track 6).** Reported separately for every family and never
blended. Strongest lockbox BUY side among judged families: **C1 +5.72%
(t 1.90)**, then B2 and B4 at +4.76%. Strongest SELL side: **C3 working-capital
build, −6.03% (skill t 1.55)** — the bottom decile underperformed by 6pp/yr,
while its BUY side managed +2.43%. C3 is a genuine sell-side signal with a weak
buy side, which is exactly the asymmetry the protocol forbids reporting as one
number. Among all families including the blocked ones, C4 leads both sides, and
§7.1 explains why that is not a factor.

**Calibration (Track 8): `CALIBRATION_NOT_ATTEMPTED_NO_QUALIFIED_SIGNAL`,
`expected_return_state = NOT_CALIBRATED`.** The protocol permits calibration
only after a family clears its gate. Zero cleared, so nothing was fitted.
R57 already *measured* that calibration cannot repair unstable ordering; refitting
on another set of rejected signals would consume sample and learn nothing.

## 9. Forward challengers — four frozen, four refused

Eligible session **2026-09-03**; signals use information through that close, the
position is effective at the next close, **zero forward observations exist at
freeze**, and back-fill is forbidden.

| challenger | information family | held / scored | why prospective |
|---|---|---|---|
| `R58_SHORT_VOLUME_PRESSURE_V1` | FINRA daily short-sale volume | 50 / 497 | owned history begins 2026-07-23 |
| `R58_DISCLOSURE_INTENSITY_V1` | SEC 8-K filing rate vs own baseline | 50 / 298 | owned history begins 2025-11-24 |
| `R58_FUND_MOMENTUM_VETO_V1` | PIT fundamentals + momentum veto | 50 / 297 | **post-hoc selection, disclosed** (§9.1) |
| `R58_FCF_PURE_V1` | PIT fundamentals | 50 / 445 | **CONTROL**, not a candidate |

**Refused rather than described** — a specification with no computable
cross-section is a wish, not a challenger:

| family | reason | measurement |
|---|---|---|
| INSIDER_FILING | `FIELD_UNPOPULATED` | the challenger was written, computed, and returned an **empty book**: `acquired_disposed` is populated on **195 of 28,002** records (0.7%) |
| NEWS_EVENT | `NOT_A_CROSS_SECTION` | 5,494 records over **7 tickers** |
| EARNINGS_EVENT | `NOT_A_CROSS_SECTION` | 34 records over 15 tickers |
| CORPORATE_ACTION | `NOT_A_CROSS_SECTION` | 8 records over 5 tickers |
| TRADING_HALT | `UNIVERSE_MISMATCH` | 411 tickers, overwhelmingly micro-cap |
| BEA / BLS macro | `TIMESTAMP_INSUFFICIENT` | `available_at` null with `RELEASE_LAG_UNKNOWN` |

### 9.1 The disclosure that matters

`R58_FUND_MOMENTUM_VETO_V1` has ample history and *was* prosecuted as family B3.
It did not pass. It is frozen because it was the most sign-stable, sector-robust
shape in the campaign — **a judgement made after seeing the lockbox, which is
selection bias, which is exactly why forward evidence is the only thing that can
settle it.** It carries no historical alpha claim. This follows the route R57
prescribed for its own F3 family.

## 10. Capital, the purchase gate, and what binds

**Capital frontier.** Zero families passed their gates, so the eligible
competitor set is unchanged: the incumbent strategy, SPY, cash. **The next
$1,000 and the next $10,000 both stay in CASH (or the incumbent book,
unchanged).** The largest lockbox number in the campaign loses its entire excess
to a sector control. Governed lane: `MANUAL_REVIEW_REQUIRED_NO_ECONOMIC_PROOF`,
unchanged. No capital moved; no proposal written to any operational store.

**Data purchase gate: NO DATASET IS RECOMMENDED FOR PURCHASE.** Two candidates
were taken through all twelve questions:

- **Point-in-time sector history** — `DO_NOT_BUY_YET`. It would let R58
  reproduce `composite_SN` rather than `composite_RAW` and would give a truly
  PIT sector-concentration gate. It fails at question 12: A1/A2/A3 did not fail
  marginally, they **flipped sign**, and a better normaliser does not repair a
  sign flip. Reopen condition: a family that is sign-stable and fails *only* on
  sector concentration. R58 has none.
- **Form 4 transaction detail** — `NO_PURCHASE_REQUIRED_BUILD_INSTEAD`, and this
  is the highest-value information action R58 found. The filings are already
  collected with excellent timestamps across 2,386 tickers; only the transaction
  table is unparsed. **Cost: $0.** It is a collector change in owned code.

**What binds, in order.** (1) SIGNAL — every family's edge is regime-unstable
across pre-registered layers, and R58 extends that from price factors to
fundamentals and to information-change signals on honest PIT data. (2)
INFORMATION — the owned frontier is not fully exhausted, but the unexhausted
part is *event and positioning* data whose owned history is months, not years.
(3) FORWARD EVIDENCE MATURITY — four R58 challengers, R56's six portfolios and
R46's signal challengers all need time no research act can accelerate. (4)
CALIBRATION — a consequence of (1). (5) Construction, turnover and costs —
real but second-order.

## 11. Disclosures

1. **Four pre-experiment protocol amendments**, all made before any score,
   portfolio, layer statistic or gate was computed, all recorded in the protocol
   itself: the YTD tolerance correction and three reader-defect fixes (§2.1);
   the uniform `level / avg3 / stab` variant grid replacing six bespoke ones that
   named quantities the owned store cannot produce without inventing data; and
   the coverage-blocked-family rule, registered *because* PANEL-F coverage had
   been measured and C4 was expected to hit it.
2. **PANEL-F coverage drift is residual survivorship exposure.** Coverage rises
   monotonically from 88.8% (2010) to 98.7% (2026) because a company that died
   early is less likely to have been CIK-resolved. The pre-registered diagnostic
   measures it: covered-minus-uncovered forward return is **+2.07%/yr (t 1.37,
   68 uncovered names)** in discovery, +4.44% (t 0.84, 22 names) in validation,
   +17.31% (t 2.63, **10 names**) in the lockbox. The lockbox estimate rests on a
   median of ten names and is correspondingly noisy. Because strategy *and*
   benchmark both live inside the covered set, R58's excesses are internally
   consistent and if anything measured against a harder benchmark — but the
   signals are demonstrated only on covered names, and generalisation to the
   full index is unproven.
3. **Sector history is not point-in-time.** The sector-exclusion checks — including
   the one that demolished C4 — use the *current* GICS classification, the same
   declared limitation R57 carried.
4. **`composite_RAW`, not `composite_SN`.** Without PIT sector history R58
   reproduces a construct that resembles the champion's fundamental leg rather
   than the leg itself.
5. **The challenger freeze was run twice**, minutes apart, to remove the insider
   challenger after it computed an empty book. No forward evidence existed at
   either moment; the discarded record was deleted.
6. **B3's coverage block is a gate mis-fire, not a data gap** (§7.2). The gate
   was left exactly where it was registered.

## 12. Deliverables, writes and verification

**Worktree.** Fifteen NEW files — `alpha_agent/r58/` (12 modules: `__init__`,
`fundamentals`, `panel_f`, `engine`, `families`, `regime`, `tournament`, `labs`,
`diagnostics`, `inventory`, `challengers`, `competition`),
`research/r58/R58_RESEARCH_PROTOCOL.json`,
`tests/test_release58_orthogonal_alpha.py` (30 tests) and this document — plus
exactly ONE modified tracked file, `PROJECT_STATE.md`. **No existing module,
test or configuration file was changed**, which is why the impacted regression
is narrow by construction rather than by choice.

**Data root (new, disclosed):** `D:\Stock_Prediction_app_data\r58_orthogonal_alpha`
— `panels/` (1 npz + meta), `results/` (8 artifacts), `challengers/` (4
immutable records). No operational store was touched.

**Verification.** 30/30 R58 tests pass. `scripts/audit_architecture.py --strict`
exits 0 with zero inventory drift. `git diff --check` clean. The live repo shows
zero tracked modifications and HEAD unchanged at `54cecf7`. R56 and R57 evidence
verified `R56_AND_R57_EVIDENCE_UNMODIFIED` by SHA-256 **and** by modification
times, all of which predate R58's protocol registration.

**One pre-existing failure found, not caused by R58:**
`test_release57_alpha_discovery.py::TestR56Immutability::test_all_six_records_hash_verify`
fails with `ImportError: cannot import name 'shadow_portfolio_evidence' from
'paper_trader.engine'`. This is the known git-worktree editable-install trap —
`import paper_trader` resolves to the live `C:` checkout (at `54cecf7`, which
predates R56) instead of the worktree that actually contains the module. The
remaining 25 R57 tests pass. R58's own tests import only `alpha_agent.r58` and
are immune by construction.

## 13. What runs next, in order of information value

1. **Parse the Form 4 transaction table** and freeze a net-insider prospective
   challenger. It is the only genuinely orthogonal family with real breadth and
   real timestamps, and it costs nothing.
2. **Let the frozen clocks run.** Four R58 challengers, six R56 portfolios and
   R46's signal challengers are the only machinery that can produce
   FORWARD_CONFIRMED evidence.
3. **A governed review of the blend weight** should now weigh §5.2 against the
   live ledger: the historical evidence does *not* support removing momentum,
   and the live evidence does. That tension is a governed decision, not a
   research claim.
4. **Widen the SEC companyfacts allowlist beyond 846 CIKs** to lift PANEL-F's
   2010–2015 coverage from 89% and shrink the residual survivorship exposure
   §11.2 measures. Owned data, free, bounded work.
