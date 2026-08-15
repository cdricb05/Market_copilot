# Stage 24 — Point-In-Time Fundamental Alpha & Research Gate Integrity

**Status:** research complete, no promotion proposed, zero operational mutations.
**Branch:** `stage19-controlled-rebalance` · **Base HEAD:** `0e1f5bb`
**Owner module:** [alpha_agent/stage24_pit_fundamental.py](../alpha_agent/stage24_pit_fundamental.py)
**CLI:** [scripts/run_stage24_pit_fundamental_alpha.py](../scripts/run_stage24_pit_fundamental_alpha.py)
**Index builder:** [scripts/build_stage24_pit_index.py](../scripts/build_stage24_pit_index.py)
**Research root:** `D:\Stock_Prediction_app_data\stage24_pit_fundamental_alpha`

---

## 1. What "point-in-time" actually means here

A backtest is honest only if, at every formation date, it uses **information that
existed on that date** and scores it against **the companies that were actually
investable then**. Two different lies are possible, and Stage 23 could rule out
neither:

**Look-ahead in the data.** A company's 2015 revenue is not one number. It is
first published in the FY2015 10-K (filed February 2016), republished as a
comparative in the FY2016 10-K, and occasionally *restated* in a 10-K/A years
later. A vendor snapshot hands you today's final restated figure and lets you
pretend you knew it in 2015. Stage 24 never does that: every fact carries the
SEC `filed` date it was published on, and a formation on date *D* can only read
facts with `filed <= D - 2 days`. A restatement published in 2018 is invisible to
a 2016 formation, permanently.

**Survivorship in the universe.** If you build a 2012 cross-section out of the
companies that are in the index *today*, you have quietly deleted every company
that went bankrupt, got acquired, or was dropped for poor performance. What
remains looks far more profitable than reality. Stage 24 takes eligibility from
the owned Norgate historical membership, in which delisted names keep both their
rows and their `TICKER-YYYYMM` identity.

---

## 2. What data we actually have

Everything below was already on disk. **No provider was called, no quota was
spent, and nothing was purchased.** The one new artefact is an index built
offline from the owned `companyfacts.zip` in 75 seconds.

| Source | Location | What it gives | Survivorship |
|---|---|---|---|
| SEC XBRL company facts (Stage-24 extended index) | `stage24_pit_fundamental_alpha\_index\sec_companyfacts_stage24.sqlite` | **1,615,843 facts**, 846 CIKs, filed **2009-04-15 → 2026-07-31**, 27 us-gaap tags | partially inclusive |
| Norgate historical index membership | frozen momentum monthly panel | 313 months, 2,728 symbols, **1,347 delisting-tagged (49.4 %)**, 2,029 observed exits | **SAFE** |
| Phase-10 historical identity (CIK bridge) | `alpha_agent\identity\historical_identity.sqlite` | 1,895 securities → 1,128 resolved CIKs | **SAFE** |
| Frozen Phase 10-L fundamental panel | Stock_Prediction_app_push research output | the OLD evidence, used only as the comparison reference | **BIASED** |

### The unlock

Phase 9.3's concept allowlist carried nine income-statement and balance-sheet
concepts and **no cash-flow concept at all**. That is precisely why `composite_sn`
had never been reconstructed point-in-time: both of its legs need cash flow from
operations, and one also needs capital expenditure. Stage 24 extended the
allowlist by nine concepts — `cash_flow_operations`, `capital_expenditure`,
`assets_current`, `liabilities_current`, `inventory`, `receivables`,
`depreciation_amortization`, `research_development`, `long_term_debt` — and
re-streamed the owned archive. Facts went from 924k to **1.62M**. Cash flow from
operations now covers **838 CIKs**; capital expenditure covers **664**.

The extension **adds to** `pit_fundamentals.CONCEPT_MAP` and a regression fails
the build if a Stage-24 key ever shadows a released one.

### A real defect found in the released period identity

`pit_fundamentals` keys a fiscal period as `"%s-%s" % (fy, fp)`. Against real SEC
company facts that is **wrong**, and measurably so: `fy`/`fp` label the **filing**,
not the fact. A FY2020 10-K carries the FY2019 comparatives *and* four quarterly
breakdowns, all tagged `fy=2020, fp=FY`. One key therefore collects six different
accounting periods.

Stage 24 keys on the fact's **own `period_end`**, and additionally requires flow
concepts to carry a fiscal-year duration (350–380 days). That single rule
discarded **575,008** 3-, 6- and 9-month facts that would otherwise have been
silently compared against annual figures. This is a data-correctness fix inside
Stage 24's reader; the released module is untouched and its own consumers are
unaffected.

---

## 3. What data we do NOT have

| Missing | Consequence | Cost to fix |
|---|---|---|
| **Point-in-time sector** | sector-neutral evidence is BLOCKED; `composite_sn` can only be PARTIALLY reconstructed | free — SEC filing headers carry assigned SIC per filing, but the owned `submissions.zip` has it only at entity level |
| **Point-in-time market cap** | every valuation ratio (book/market, earnings yield) is unrunnable | small — the owned parser emits monetary USD facts only, so share counts are dropped |
| **Historical analyst revisions** | the only untested orthogonal family stays blocked | vendor purchase (Steele/Intrinio) |
| **Pre-2009 fundamentals** | 951 of 1,895 securities delisted before XBRL existed and can never be scored | not fixable from SEC |
| **Identity for 1,657 panel symbols** | those names are absent from every PIT cross-section | free — work the existing unresolved backlog |

---

## 4. How survivorship is handled, honestly

The formation universe is survivorship-**safe**: it is the owned historical
membership, delisted names included, and 2,029 real exits are observed across the
panel.

The *fundamental coverage over* that universe is survivorship-**inclusive but not
complete**, and the honest denominator matters:

* 951 of 1,895 securities were delisted **before** XBRL company facts began
  (2009-04). They cannot have facts. Counting them as "missing" would overstate
  the gap.
* Of the 944 securities alive or delisted after that floor, **825 (87.4 %)** have
  PIT facts.
* Split out: **652 of 662 (98.5 %)** current names, versus **173 of 282 (61.3 %)**
  post-floor delisted names.

That 98.5 % vs 61.3 % asymmetry is the residual survivorship exposure, and it is
reported rather than hidden. It is a large improvement on the frozen panel, whose
universe contains **no** delisted names at all, but it is not zero.

---

## 5. Does `composite_sn` survive clean testing?

**Yes — weaker in rank information, stronger after costs, and for the first time
it can clear its own evidence gate.**

`composite_sn` is `z(fcf_to_assets, +1) + z(−operating_accruals)`, sector-neutral,
equal-weighted, quarterly, 63-day horizon. Stage 24 reproduces the orientation,
the equal-weight z blend, the cadence and the horizon. It **cannot** reproduce the
within-sector step, so the reconstruction targets `composite_raw` and is
classified `PARTIAL_PIT_RECONSTRUCTION` — explicitly **not** equivalent to the
champion.

| | OLD frozen panel | NEW PIT panel |
|---|---|---|
| Evidence class | survivor-biased current snapshot | **survivorship-safe point-in-time** |
| Window | 2016-06 → 2026-05 | 2010-01 → 2026-04 |
| Periods | 118 | 66 |
| Median names | 213 | **488** |
| rank IC | 0.0365 | 0.0231 |
| **rank IC t** | **3.26** | **2.03** |
| **spread t** | 1.14 | **2.10** |
| gross ann. | 4.33 % | 4.76 % |
| **net25** | 2.35 % | **4.45 %** |
| turnover | 0.99 | **0.16** |
| **released gate** | **DATA_HOLD** | **KEEP_FOR_RESEARCH** |

Read this carefully, because the headline is not the whole story:

* **Rank information genuinely weakens.** IC falls 37 % and its t-statistic falls
  from 3.26 to 2.03. Some of the frozen panel's apparent alpha *was* the bias.
* **Tradability genuinely improves.** Spread t nearly doubles and net-of-cost
  return nearly doubles, because turnover collapses from 0.99 to 0.16 — the
  frozen panel's per-ticker staggered rebalance dates were churning the book.
* **The gate flips for the right reason.** The frozen panel returns DATA_HOLD
  because `require_survivorship_safe = true` and it isn't. The PIT panel is, so
  its evidence can finally be judged — and it passes every threshold.

This resolves the single most important structural finding of Stage 23
(`DOMINANT_LEG_CANNOT_CLEAR_EVIDENCE_COMPLETENESS`). The leg that carries the
operational ranking information is no longer uncertifiable.

**The two legs separately** are both REJECTED, which is itself informative:

| leg | rank IC | IC t | spread t | net25 | gate |
|---|---|---|---|---|---|
| `fcf_to_assets` | 0.0335 | 2.23 | 1.04 | 2.68 % | REJECTED |
| `operating_accruals` | 0.0027 | 0.29 | 1.98 | 4.54 % | REJECTED |

FCF carries the **ranking**; accruals carry the **spread and the cost robustness**
and essentially no rank information. Neither clears the gate alone; the blend
does. That is the same complementarity pattern Stage 23 found between the
fundamental and momentum legs, one level down — and it is the strongest available
justification for leaving the released composite exactly as it is.

---

## 6. The eight hypotheses we tested

Pre-registered before evaluation, each with a fixed expected sign that was never
refit. Quarterly non-overlapping formation, 63-day forward return, 25 bps cost
reference, Benjamini-Hochberg over the fixed family of 8.

| Hypothesis | periods | names | rank IC | IC t | spread t | net25 | turnover | BH q | verdict |
|---|---|---|---|---|---|---|---|---|---|
| `s24_rnd_intensity` | 65 | 209 | **0.0594** | **2.86** | **3.13** | **13.5 %** | **0.06** | **0.034** | **KEEP_FOR_RESEARCH** |
| `s24_gross_profitability` | 65 | 326 | 0.0197 | 1.29 | 0.78 | 2.2 % | 0.08 | 0.391 | REJECTED |
| `s24_capital_efficiency` | 65 | 469 | 0.0152 | 1.29 | 1.52 | 3.6 % | 0.04 | 0.391 | REJECTED |
| `s24_margin_change` | 65 | 292 | 0.0132 | 1.23 | 1.47 | 3.5 % | 0.18 | 0.391 | REJECTED |
| `s24_profitability_change` | 65 | 430 | 0.0035 | 0.29 | 0.55 | 1.2 % | 0.18 | 0.773 | REJECTED |
| `s24_cashflow_momentum` | 66 | 551 | −0.0027 | −0.37 | −0.01 | −0.4 % | 0.16 | 0.773 | REJECTED |
| `s24_sales_growth` | 65 | 463 | −0.0108 | −0.66 | −1.08 | −3.9 % | 0.17 | 0.682 | REJECTED |
| `s24_asset_growth` | 66 | 552 | −0.0150 | −1.16 | −1.42 | −4.2 % | 0.18 | 0.391 | REJECTED |

**1 of 8 cleared the released gate; 1 of 8 survived FDR.** All eight — including
the seven nulls — are registered through the released candidate lifecycle, so
they cannot be rediscovered.

### Which failed, and what that tells us

* **Asset growth and sales growth came out with the WRONG SIGN.** Both were
  pre-registered negative (fast growers underperform). Both produced negative
  factor IC, meaning fast growers *out*performed over 2010–2026. We report this
  as a clean rejection rather than flipping the sign to manufacture a result.
* **Gross profitability (Novy-Marx) is directionally right but weak** here:
  IC 0.0197 at t = 1.29 over 65 quarters. Not nothing, not evidence.
* **Fundamental momentum is a well-powered null.** Change in CFO/assets is
  0.0000-ish at t = −0.37 over 66 survivorship-safe cross-sections. Do not
  re-open it.
* **Profitability change and margin change** both under-deliver relative to the
  levels they are differences of.

---

## 7. The one candidate that cleared: R&D intensity

`s24_rnd_intensity` = `ResearchAndDevelopmentExpense / Assets`, expected sign +1.

It is genuinely strong on the released contract: rank IC 0.0594 (t = 2.86),
spread t 3.13, gross 13.7 % annualised with **13.5 % net of 25 bps** because
turnover is only 0.06, subperiod consistency 1.00, BH q = 0.034. It is the
**first `KEEP_FOR_RESEARCH` candidate in the tournament's entire 70-candidate
history**.

It also **adds information** rather than restating what we run:

| vs baseline | cross-sec. rank corr | partial rank IC | t |
|---|---|---|---|
| `composite_sn_pit` | 0.348 | 0.0522 | **2.49** |
| `mom_6_1` | 0.067 | 0.0559 | **2.78** |
| `ensemble_pit_5050` | 0.283 | 0.0464 | **2.29** |

Folding it into the operational-shaped blend improves every headline number:
rank IC +0.0248, IC t +0.89, spread t +1.08, **net25 +7.39 %**, turnover −0.09.
It survives the PIT controls almost untouched — neutralising size and volatility
jointly leaves IC at 0.0591 (t = 3.33), i.e. **99.5 % retained**.

### Why this is not yet a reason to change anything

Three caveats, all of them load-bearing:

1. **Sector-neutral evidence is BLOCKED.** R&D intensity is mechanically
   concentrated in technology and biotech. Over 2010–2026 those sectors
   massively outperformed. We **cannot currently distinguish stock selection
   from a persistent sector bet**, and that is the single most likely
   explanation for a factor this strong.
2. **`SELECTION_ON_DISCLOSURE`.** Only issuers that tag an R&D line are scored —
   median 209 names versus 553 for the panel. Missing R&D is not read as zero
   (that would be an imputation), so the result generalises only to R&D
   reporters.
3. **One regime.** 65 quarters, all post-GFC, all in a secular growth/tech bull
   market.

The correct status is exactly what the released lifecycle assigned it:
`KEEP_FOR_RESEARCH`. That is a research state, not a promotion, and it is the
state a candidate must hold *before* a shadow book is even considered.

---

## 8. Neutralisation

Controls used are trailing and were observable at formation: `log_adv_dollar`
(size/liquidity) and `realized_vol_63d` (volatility), both from the frozen
survivorship-safe momentum panel.

| signal | raw IC | joint-neutral IC | t | retained |
|---|---|---|---|---|
| `composite_sn_pit` | 0.0231 | 0.0247 | 2.21 | 107 % |
| `ensemble_pit_5050` | 0.0322 | 0.0380 | 2.53 | 118 % |
| `mom_6_1` | 0.0253 | 0.0354 | 2.00 | 140 % |
| `s24_rnd_intensity` | 0.0594 | 0.0591 | 3.33 | 99.5 % |

**Nothing here is a size or volatility bet.** Every signal retains ≥ 99 % of its
information after joint neutralisation, and momentum and the ensemble actually
*improve*, meaning adverse size/vol exposure was diluting them.

**Sector: `BLOCKED_NO_POINT_IN_TIME_SECTOR`.** The owned Norgate GICS surface is
undated and the owned SEC submissions archive carries assigned SIC at entity
level without a per-filing effective date. `alpha_agent/pit_sector.py` already
implements the correct leakage-safe consumer; the observations it needs are not
on disk. Applying today's classification to a 2012 cross-section would inject a
classification look-ahead, so Stage 24 reports the wall and **never substitutes
the map**. A regression enforces this.

---

## 9. Research gate integrity — the drawdown concern was real

Stage 23 suspected the drawdown gate was length-sensitive. Stage 24 **did not
assume it** and instead measured it.

**What the metric actually is.** `signal_evaluation.evaluate_periods` computes
the drawdown of an **unnormalised cumulative SUM** of per-period long/short
spreads. `tournament.row_to_contract_metrics` then multiplies it by 100 and names
it `max_drawdown_pct`, which `classify_evidence` compares against
`keep_max_drawdown_pct = -35.0`.

**Two defects, both confirmed:**

1. **Units.** A sum of decimal spreads is not a percentage of capital. The tell
   is the published reading for `mom_6_1`: **−142.1 "percent"**. No portfolio can
   lose 142 % of itself. Under correct semantics the same series is **−87.0 %**.
2. **Unboundedness.** The running sum is a random walk, so its worst drawdown is
   unbounded below and grows with the number of periods. A genuine fraction
   cannot go below −100 %.

**The controlled proof.** Holding the per-period distribution *exactly fixed* and
varying only the number of periods:

| periods | V1 reported | passes −35 gate? | V2 (fraction of capital) |
|---|---|---|---|
| 24 | −17.7 | ✅ | −16.7 % |
| 192 | −30.9 | ✅ | −31.6 % |
| **384** | **−89.5** | **❌** | −64.9 % |
| 1536 | −89.5 | ❌ | −64.9 % |

The identical statistical process passes at 192 periods and fails at 384, purely
for being measured longer. **The concern was real.**

**The repair.** The project already implements the correct semantics in
`tournament.ShadowBook`, which measures `(v − peak) / peak` on a value series.
Stage 24 makes the research metric agree with the one the project already trusts:
`drawdown_v2_compounded_fraction` is the deepest peak-to-trough decline of the
compounded long/short equity curve, bounded below by −100 %.

**What was deliberately NOT done.** The threshold was not lowered. Historical
evidence was not rewritten. The **active contract version stays V1**, because
which drawdown a gate consumes is a *model governance* decision — activating V2
would retro-actively re-judge every candidate ever evaluated. Stage 24 ships the
repaired metric under an explicit version, measures its impact, and leaves the
decision to the gate owner.

**And the measured impact is nil.** On both Stage-23's 312-period evidence and
Stage-24's 66-period evidence, **no candidate's classification changes** under V2.
`mom_6_1` moves from an impossible −142.1 to a meaningful −87.0, and −87 still
breaches −35. So: the metric was broken, fixing it makes the numbers mean
something, and it rescues nobody. Whether −35 % is the right bar for a long/short
decile spread is a separate governance question Stage 24 does not answer.

---

## 10. What the autonomous research agent should do next

Stage 24 adds **no second agent, no second registry, no second experiment
contract**. It publishes a capability matrix the existing agent reads.

| Capability | State |
|---|---|
| `PIT_GROSS_PROFITABILITY`, `PIT_ACCRUALS`, `PIT_FREE_CASH_FLOW`, `PIT_ASSET_GROWTH`, `PIT_PROFITABILITY_CHANGE`, `PIT_SALES_HISTORY`, `PIT_FUNDAMENTAL_MOMENTUM` | **READY** |
| `SURVIVORSHIP_SAFE_FORMATION_UNIVERSE` | **READY** |
| `PIT_RND_INTENSITY` | PARTIAL (disclosure selection) |
| `PIT_SECTOR_HISTORY` | **BLOCKED** |
| `PIT_MARKET_CAP` | **BLOCKED** |
| `HISTORICAL_ANALYST_REVISIONS` | **BLOCKED** (waiting on Steele/Intrinio) |

Priority order:

1. **Acquire per-filing assigned SIC with acceptance timestamps from SEC.** Free,
   no vendor. It simultaneously unblocks sector-neutral evidence *and* a full
   `composite_sn` reconstruction *and* the one question that could kill or
   confirm the R&D result.
2. **Add share counts to the PIT concept allowlist**, unlocking market cap and
   every valuation ratio.
3. **Work the identity backlog** — 1,657 panel symbols currently resolve to no
   CIK and are silently absent from every cross-section.
4. **Historical analyst revisions** — still the only untested orthogonal family.
5. Do **not** re-open residual momentum, low-vol, vol-scaled momentum, the
   monthly liquidity family, or Stage-24's own seven nulls.

---

## 11. Steele / Intrinio

**No historical analyst data has arrived.** State stays `WAITING_FOR_DATA`. No
paid API was called, no quota spent, no schema invented.

What changes because of Stage 24: when the extract does arrive, an
analyst-revision candidate can be tested for **incremental** information over a
survivorship-safe point-in-time fundamental baseline, instead of over a
survivor-biased snapshot. The lane's state is unchanged; its value went up.

The pipeline that runs automatically on arrival is unchanged and lives in
`alpha_agent/analyst_revisions.py` (Stage 13A): importer → `pit_scan` → adequacy
gate → six frozen hypotheses under BH-FDR → the Stage-23 incremental test → the
same tournament lifecycle, still with no automatic promotion.

---

## 12. Portfolio relevance

Reported as `COUNTERFACTUAL_NOT_PROOF`. The canonical seam remains
`stage23_unified.build_decision_link` and its status remains
`INSUFFICIENT_FORWARD_EVIDENCE` (minimum 12 matured observations).

A counterfactual re-ranking of past holdings is only worth computing for a signal
that cleared the gate — ranking holdings by a rejected signal would dress a null
result as portfolio advice. `s24_rnd_intensity` now qualifies for that work, but
it should not be run until the sector question is settled, because a
sector-driven signal would produce a sector-driven "improvement".

No historical portfolio decision is rewritten.

---

## 13. Machine-readable outputs

Under `D:\Stock_Prediction_app_data\stage24_pit_fundamental_alpha\runs\<run_id>\`
with `latest.json` pointing at the newest run:

`pit_data_source_inventory` · `historical_universe_contract` ·
`pit_fundamental_coverage` · `composite_sn_pit_validation` ·
`fundamental_experiment_manifest` · `fundamental_candidate_results` ·
`alpha_incrementality_matrix` · `neutralization_results` ·
`research_gate_integrity` · `stage24_research_priority_queue` ·
`intrinio_parallel_status` · `portfolio_decision_relevance` · `stage24_summary`

Run directories are content-addressed: identical inputs reproduce an identical
`run_id`.

---

## 14. Safety

Research-only and read-only with respect to every operational store. No orders,
fills, signals, trade decisions, proposals, rebalance plans, Daily Close, model
promotion or champion replacement. No network, no provider call, no PostgreSQL,
no prediction service. The backend was never restarted.

Writes are confined to the Stage-24 research root, the Stage-24 SEC index, and
the existing research tournament registry.
`api.universe_scoring.AUTOMATIC_PROMOTION_ALLOWED` remains `False` and Stage 24
never references it.

---

## 15. Is any model promotion justified?

**No.**

* `fundamental_momentum_50_50_v1` is unchanged, and Stage 24 strengthens the case
  for leaving it alone: its fundamental leg now clears its own evidence gate on
  honest data, and the two legs of `composite_sn` are complements in the same way
  the fundamental and momentum legs are.
* `s24_rnd_intensity` cleared the released gate and survived FDR, which earns it
  `KEEP_FOR_RESEARCH` — a research state. It is not a challenger to the
  operational model until the sector question is answered, because the most
  likely competing explanation for its strength is one we currently cannot test.
* No manual promotion gate was reached and no automatic promotion is possible.
