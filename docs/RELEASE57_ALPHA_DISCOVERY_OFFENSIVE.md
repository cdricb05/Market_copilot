# Release 57 — Alpha Discovery Offensive

**Verdict: `NO_ALPHA_FOUND_YET`.** Twelve pre-registered, economically distinct
families — eight equity, three futures, one combination — prosecuted on a
20-year survivorship-safe panel through an untouched lockbox: **12/12
`NO_ALPHA_EVIDENCE`**. Zero survive Benjamini–Hochberg at q=0.10; zero clear the
materiality-with-robustness gates. The score→return calibration attempt
**failed out of sample**, so `expected_return_state = NOT_CALIBRATED` is now a
*measured* answer, not an unattempted one. No R57 forward challenger was frozen,
because the pre-registered freeze gate was not met — and freezing anything
anyway would have been the exact failure mode this protocol exists to prevent.

- **Branch:** `r57-alpha-discovery-offensive` (created in place at `54cecf7`,
  carrying the complete uncommitted R56 tree — R56 was never committed and
  committing is forbidden, so the "worktree from the committed R56 branch" the
  brief expected could not exist; this is the stated deviation)
- **Protocol:** `research/r57/R57_RESEARCH_PROTOCOL.json` — every split,
  variant grid, cost, gate and threshold registered **before** any experiment
  ran; thresholds never moved
- **Campaign:** `r57_alpha_discovery_v1`
- **Research root (new, disclosed):** `D:\Stock_Prediction_app_data\r57_alpha_discovery`
- **Safety:** research only; no order, fill, broker, promotion, sleeve
  activation or operational-store write; the live system was GET/read-only
  throughout; the six R56 challenger records verified hash-identical after the
  campaign

---

## 1. The data this campaign stood on

| substrate | facts |
|---|---|
| Equity panel | Norgate **S&P 500 Current & Past**: 1,897 securities (1,236 delisted-suffixed), PIT membership via `index_constituent_timeseries` per security per day — 2,812,531 member-days over 5,601 sessions (≈502 members/day), TOTALRETURN prices for returns, unadjusted price×volume for the $5 / $10M-ADV floors. 824 symbols with no bars in-window (delisted before 2004) correctly absent. |
| Futures panel | Norgate **Continuous Futures**: 103 markets pass the mechanical rule (≥2,000 sessions, bar on/after 2026-08-01), both `&MKT` and `&MKT_CCB` roll methodologies, Delivery-Month roll detection, dollar P&L basis (percent arithmetic on back-adjusted series is forbidden by protocol). |
| Track-1 ledger | The desk's TRUE_FORWARD cross-sections and matured outcomes for the champion blend AND its two legs separately, read-only. |

Partition (SPY calendar): DISCOVERY 2006-01→2017-12 (143 monthly decisions),
VALIDATION 2018-01→2022-12 (59), **LOCKBOX 2023-01→2026-07/09 (43 monthly / 183
five-session / 921 daily marks)**, embargo `ceil(h/cadence)` decision dates at
each boundary. Selection was computed and persisted from validation before any
lockbox evaluation ran; artifact timestamps prove the ordering and
`tests/test_release57_alpha_discovery.py` asserts it.

---

## 2. Equity tournament — every family, every judged number

Judge (pre-registered): long-only EW top-50 vs the EW eligible-universe
benchmark, both charged 12.5bp/side on their own turnover, NEXT_CLOSE entry,
21-session horizon (5 for reversal). One variant per family reaches the lockbox,
chosen on validation alone.

| family | selected | validation ann net excess | **lockbox ann net excess** | t | n | failed gates |
|---|---|---|---|---|---|---|
| E1 XS momentum | mom_189_21 | −0.19% | **+9.27%** | 1.47 | 43 | sign-flip, BH, drawdown |
| E2 short reversal | rev_5 | −1.12% | **−1.16%** | −0.20 | 183 | materiality, halves, BH |
| E3 residual momentum | resid_126_21 | −1.01% | **+7.21%** | 1.67 | 43 | sign-flip, turnover-cap (41%/mo), BH |
| E4 sector-relative momentum | srel_126_21 | −2.74% | **+7.59%** | 1.30 | 43 | sign-flip, BH |
| E5 low risk | lowbeta_252 | **+1.16%** | **−4.90%** | −0.64 | 43 | sign-flip (other way), materiality, halves, BH |
| E6 idiosyncratic vol | idiovol_126 | **+2.06%** | **−2.93%** | −0.76 | 43 | sign-flip, materiality, halves, BH |
| E7 52-week-high proximity | hi52_252 | −9.05% | −5.24% | −0.85 | 43 | materiality, halves, turnover, BH |
| E8 liquidity | dvol_trend_63 | −1.36% | −3.63% | −0.77 | 43 | materiality, halves, drawdown, BH |
| E9 combo (E5+E6) | rank average | n/a | −5.48% | −0.81 | 43 | materiality, halves, sign, BH |

**The pattern IS the finding.** Momentum-type families (E1/E3/E4) were
*negative through five validation years* and +7–9%/yr in the lockbox; the
low-risk families flipped the opposite way. A selector honest enough to commit
on validation evidence could not have owned the lockbox winners, and a selector
that owns them because the lockbox looked good is doing test-set tuning. Under
BH across all 12 campaign tests, **nothing survives** (best one-sided p ≈ 0.05
needs ≤ 0.008 at rank 1). Context: in the lockbox the EW-universe benchmark
itself earned +14.15%/yr net; E1's book earned +23.4%/yr — a real gap that the
pre-registered gates correctly refuse to call evidence, because the identical
construction *lost* to the same benchmark for the five years before.

## 3. Futures tournament — native markets, both roll methodologies

Judge: annualised net Sharpe of the equal-risk, vol-targeted portfolio across
103 markets, 2bp/side + roll costs, weekly rebalance, NEXT_CLOSE. The
inverse-vol sizing rule required a uniform leverage bound (10× per-market
slice) after the first validation run produced million-contract positions in
degenerate low-vol markets; the bugged artifact was **discarded and disclosed**,
the bound applies to every variant of every family, and the lockbox had not
been touched. (A later cosmetic guard on a divide-by-zero warning in the same
code path is mathematically inert — the affected branch was never selected.)

| family | selected | validation Sharpe | **lockbox Sharpe** | lockbox ann | CCB-methodology Sharpe | halves | failed gates |
|---|---|---|---|---|---|---|---|
| F1 TS trend | tsmom_252_volscaled | +0.13 | **+0.22** | +0.37% | +0.05 | −0.34 / +0.82 | materiality, halves, neighbour-sign, BH |
| F2 channel breakout | donchian_50 | +0.38 | **−0.39** | −0.92% | −0.54 | −0.34 / −0.44 | sign-flip, materiality, halves, BH |
| F3 cross-market momentum | xsmom_252 | +0.11 | **+0.44** | +0.77% | +0.36 | +0.23 / +0.68 | neighbour-sign, BH |

F3 is the strongest honest futures result: lockbox Sharpe 0.44, positive in
both halves, sign-consistent across both roll methodologies — and it still
fails, because its variant neighbour (xsmom_126) was validation-negative and
nothing survives BH. The roll-methodology check did real work: F1's Sharpe
falls from 0.22 to 0.05 under the alternative continuous series, which is
exactly the contamination sensitivity the protocol was written to expose.

## 4. Track 1 — why the incumbent buy engine is weak (DIAGNOSTIC)

From the desk's own TRUE_FORWARD matured outcomes (full ~233-name scored
universe; **n = 8–25 sessions — directional only, no alpha vocabulary**):

| component | h | n | buy side (top decile − universe) | sell side (bottom decile − universe) | rank IC |
|---|---|---|---|---|---|
| blend (champion) | 20 | 8 | **−4.24pp** (t −5.9) | −1.93pp (sell-skill t 2.8) | −0.01 |
| fundamental leg (composite_sn) | 20 | 8 | **+4.39pp** (t 5.1) | **−2.49pp** (t 17.1) | +0.20 |
| momentum leg (mom_6_1) | 20 | 8 | −0.62pp | **+8.58pp — INVERTED** (t −7.3) | −0.23 |

On this tiny live window the fundamental leg is positive on BOTH sides while
the momentum leg is inverted on both (recent losers bounced hard), and the
50/50 blend inherits the damage: its top decile *underperformed* by 4.2pp over
20 sessions while its own fundamental component's top decile outperformed by
+4.4pp. The 20-year replication of the momentum leg's shape (126/21) shows the
long-run version of the same asymmetry: buy-side ann excess −1.5% in
validation, +4.5% (t 1.4) in the lockbox, sell-side skill never significant —
no stable buy-side edge in the S&P universe. Answer to "is SELL better than
BUY": yes for the fundamental leg on live data; the blend's failure is
concentrated on the BUY side and is currently driven by the momentum leg. What
follows operationally (blend weight, regime handling) is a GOVERNED model
review question, not an R57 research claim.

## 5. Calibration — the honest failure that answers Track 4

Target (fixed before lockbox): the strongest validation-positive family, E6
idiovol_126. Isotonic decile mapping with 0.25 shrinkage won validation (MAE
0.0015). **Lockbox: MAE 0.00282 vs 0.00276 for a zero forecast — worse than
predicting nothing — and decile ordering INVERTED (Kendall τ −0.47).**
`expected_return_state = NOT_CALIBRATED` therefore stands, now as a measured
result: the blocker isn't that nobody fit a calibrator, it's that the
underlying score ordering is not stable enough OOS to carry one.

## 6. Construction and turnover (diagnostics on rejected E1)

Construction, same signal, same costs: lockbox net excess EW-top25 **+12.7%**,
rank-weighted +10.4%, EW-top50 +9.3%, inverse-vol +4.4% — an 8.3pp construction
spread, all positive; construction matters, and none of it rescues a family
that was validation-negative. Signal instability dominates construction choice.

Turnover bands (K_out 75/100/150 vs none): one-way turnover per month fell
0.342 → 0.222 → 0.174 → 0.134 (−35% to −61%) while lockbox net excess stayed
flat (+9.3% ± 0.3pp) and validation barely moved. **After-cost OOS P&L was
held, not increased** — the honest answer to Track 6; the value of hysteresis
here is operational (less trading for the same result), which corroborates
R56's payback findings without being alpha.

## 7. Capital competition, next-dollar answers

Eligibility rule: only candidates that passed their historical gates compete.
**Zero qualified**, so the eligible set is unchanged: the incumbent strategy,
SPY, cash. If all capital were cash, the R57 research system allocates it
**nowhere new** — the next $1,000 and next $10,000 stay CASH pending a
qualified competitor (governed lane:
`MANUAL_REVIEW_REQUIRED_NO_ECONOMIC_PROOF`, unchanged and now with a measured
calibration failure behind it). Cash becomes preferable *immediately* for any
deployment that cannot name a positive after-cost expected edge; R57 found
none it could defend. Artifact: `capital_competition.json`.

## 8. R56 forward challengers

All six records re-verified hash-identical after the campaign
(`R56_RECORDS_IMMUTABLE: True`), zero forward observations — the first
completed session after inception has not yet closed. `FORWARD_PENDING` for
the research targets; the tournament continues untouched.

## 9. What binds, in order

1. **SIGNAL** — the largest measured constraint. Every family's edge is
   regime-unstable across pre-registered layers; nothing clears family-level
   FDR on top of this repository's ~289-hypothesis cumulative burden.
2. **INFORMATION** — the price-only information set is exhausted for stable
   cross-sectional edges (four prior campaigns said it; R57's 12/12 on a
   longer, cleaner panel confirms it). New *orthogonal* information remains
   the reopen condition.
3. **CALIBRATION** — now measured as unachievable on current signals, which is
   a consequence of (1), not an independent blocker.
4. **FORWARD EVIDENCE MATURITY** — everything already frozen (R46 signals, R56
   portfolios) needs time, and no research act can accelerate a clock.
5. Construction/turnover/costs — real but second-order: ~8pp construction
   spread and flat-P&L turnover reduction on an unstable signal.

## 10. Data purchase gate

**No purchase recommended.** No candidate dataset names an unresolved
hypothesis whose falsification a free/owned proxy could not attempt first; the
last gated purchase (Norgate Futures) was fully prosecuted here (103 native
markets, two roll methodologies) and yielded no qualifying family — evidence
the gate should keep demanding demonstrated incremental value.

## 11. Deliverables and writes

**Worktree (code/docs):** `alpha_agent/r57/` (9 modules: `__init__`, `panel`,
`futures`, `engine`, `families`, `tournament`, `futures_tournament`, `track1`,
`labs`, `competition`), `research/r57/R57_RESEARCH_PROTOCOL.json`,
`research/r57/R57_EXPERIMENT_REGISTRY.json`,
`tests/test_release57_alpha_discovery.py` (29 tests), this document,
`PROJECT_STATE.md`, and six evidence-citation updates inside
`api/alpha_opportunity_registry.py` (the minimal UI-facing integration — the
registry the ALPHA & CAPITAL view already renders now carries the R57
verdicts; no other UI change).

**Data root (new, disclosed):** `D:\Stock_Prediction_app_data\r57_alpha_discovery\`
— panels (2 npz + meta), results (11 artifacts), challengers (**empty, by
verdict**). No operational store was touched; the live repo shows zero tracked
modifications.

**Disclosures:** (a) the branch deviation described in the header; (b) one
bugged futures validation artifact discarded before any lockbox exposure;
(c) two lockbox stats were glimpsed during engine debugging before selection
persisted (`mom_252_21` in an engine smoke test, `tsmom_252_sign` in the
cost-explosion diagnosis) — selection is mechanical on validation alone and no
grid, threshold or rule changed afterwards, but the glimpses are on the record.

## 12. If R57 found nothing, what runs next

The highest-value independent experiments, in order:

1. **Let the frozen clocks run** — R46's 45 signal challengers and R56's six
   portfolio challengers are the only machinery that can produce
   FORWARD_CONFIRMED evidence, and both are live.
2. **Governed review of the momentum leg's blend weight** using Track 1's
   component asymmetry as the opening exhibit (manual, outside research).
3. **A pre-registered v2 futures protocol for F3 cross-market momentum
   alone** — the one family whose lockbox was positive in both halves under
   both roll methodologies; a single-hypothesis registration escapes the
   12-way BH burden honestly *only* by putting it on forward evidence, since
   the historical lockbox is now spent.
4. **Orthogonal information acquisition** through the purchase gate remains
   the only route to a genuinely new equity edge; nothing currently for sale
   passes.
