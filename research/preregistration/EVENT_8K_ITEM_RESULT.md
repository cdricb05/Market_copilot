# SEC FORM 8-K ITEM-CODE EVENT AXIS — RESULT

**8-K EVENT-TYPE ALPHA = NO**

Preregistration: `EVENT_8K_ITEM_PREREGISTRATION.md`, commit `998a3f9`, frozen before
a single real-data forward return was computed (sha256 pinned by test).
Implementation committed at `f3766b8`, also before any result. Family
`DISCLOSURE_8K_ITEM_EVENT_V1`. Artifact: `results/event_8k_item.json`
(`artifact_hash f2f8a720…d086b`), produced by the canonical runner stage
`event_8k` (`ALPHA_RECOVERY_STAGE_OK`). Dollars spent: **0**.

---

## 1. Data

```
PREREGISTERED CELLS  ONE: RESTRUCTURING_OR_IMPAIRMENT
                     original 8-K, Item 2.05 or 2.06, neither 2.02 nor 2.01
                     sign NEGATIVE (frozen), horizons 5 / 21 / 63, m = 3
DATA COVERAGE        220,226 modern-era 8-K / 8-K/A filings, 2004-08-23 -> 2026-09-09
                     structured `items` populated 100%; 1,080 issuer histories
EVENT COUNTS         476 events (475 filings, 2 same-row-session collapses)
                     2011-07-19 -> 2026-07-28, 213 names; 389 selection / 87 lockbox
                     258 Item 2.05 only, 159 Item 2.06 only, 59 both
PIT MATCH RATE       1,130 of 1,897 panel rows carry a CIK (0 name matches, 0 route
                     disagreements); eligible-universe assessable share mean 94.3%,
                     worst 88.4%; 48.6-49.7% of decision sessions under 95%
PIT PROFILE          86% of events accepted outside the regular session
                     (216 after close, 190 pre-open, 67 intraday, 3 non-session days)
```

Cells considered and NOT frozen, all before any return (preregistration §3): 1.02 (73%
co-filed with a replacement agreement; 59% of the remainder is financing
housekeeping), 3.01 (mixed sign), 4.01 (29 informative periods at h=21; routine RFP
rotations), 5.02 interim-CEO rule (~65% precision on development documents), 2.04
(voluntary note redemptions), 4.02 / 1.03 / 1.05 (insufficient), 3.02 (not
directional).

## 2. Result per frozen horizon — the qualification input

Entry at the close of `t+1`, where `t` is the first session whose 16:00 ET close is
strictly after the SEC acceptance instant. "Signed" is the market-adjusted return in
the FROZEN direction (short the event name against SPY), paying the cost on entry and
exit.

| h | events | market-adj gross (observed) | signed gross | net 1bp | net 2bp | net 5bp | desk 12.5bp | t (mkt-adj) | Sharpe @1bp | max DD @1bp |
|---|---|---|---|---|---|---|---|---|---|---|
| 5 | 476 | **+4.78%/yr** | −4.78% | −5.79% | −6.79% | −9.82% | −17.38% | 0.55 | −0.21 | −0.66 |
| 21 | 476 | **+7.34%/yr** | −7.34% | −7.58% | −7.82% | −8.54% | −10.34% | 1.22 | −0.24 | −1.00 |
| 63 | 473 | **+2.78%/yr** | −2.78% | −2.86% | −2.94% | −3.18% | −3.78% | 0.77 | −0.09 | −1.00 |

The max-drawdown figure compounds overlapping per-session event windows — the same
construction as the 13D/G result — so it is a stress indicator, not a portfolio path.

Other returns, for completeness:

| h | raw | t | sector-adj (diagnostic) | t | equal-risk t | selection / lockbox per event (t) |
|---|---|---|---|---|---|---|
| 5 | +17.10%/yr | 1.63 | +6.54%/yr | 0.83 | −0.09 | +0.17% (0.34) / +0.14% (0.49) |
| 21 | +20.36%/yr | **2.99** | +7.82%/yr | 1.48 | 0.52 | +0.64% (1.09) / +0.74% (0.58) |
| 63 | +17.05%/yr | **4.11** | +3.02%/yr | 1.03 | 0.39 | +0.82% (0.62) / +0.98% (0.51) |

### Orthogonalised, incremental, multiplicity

Controls: the 10 owned US_EQUITY baseline dimensions + the owned 8-K intensity block
`DISCLOSURE_INTENSITY_LANGUAGE` + the incumbent `fundamental_momentum_50_50_v1`. The
signal is signed, so a positive number would mean the preregistered direction.

| h | rank IC raw (t) | rank IC orthogonalised (t) | orthogonalised L/S book net @1bp (t) | incremental vs incumbent @1bp (t) | @12.5bp (t) |
|---|---|---|---|---|---|
| 5 | −0.0000 (−0.02) | +0.0050 (0.72) | −1.10%/yr (−0.70) | +0.22%/yr (0.60) | −0.44%/yr (−1.21) |
| 21 | −0.0004 (−0.10) | **−0.0118** (−1.41) | −2.67%/yr (−1.52) | +0.92%/yr (1.58) | +0.77%/yr (1.33) |
| 63 | +0.0011 (0.27) | −0.0033 (−0.39) | −0.13%/yr (−0.11) | +0.32%/yr (0.87) | +0.27%/yr (0.74) |

`MULTIPLICITY = Benjamini-Hochberg, q = 0.10, m = 3 (one declared family).
p = 0.583 / 0.221 / 0.439. 0 of 3 rejected. The denominator was not reset.`

**The canonical panel scorer could not score this cell** — NO_RESPONSE at all three
horizons (§4). The orthogonalised and incremental numbers above are the frozen
descriptive measurements, which do not pass through the scorer's winsoriser.

## 3. Announcement versus post-publication

| h | announcement session, close t−1 → t, mkt-adj | t | forgone first post-publication session, close t → t+1 | t | prior 21-session run-up (post-hoc) | t |
|---|---|---|---|---|---|---|
| 5 / 21 | **−0.19%** per event | −1.38 | **−0.07%** per event | −0.76 | +0.14% per event | 0.63–0.80 |
| 63 | −0.21% per event | −1.51 | −0.11% per event | −1.23 | +0.09% per event | 0.80 |

```
ANNOUNCEMENT-SESSION EFFECT  = -0.19% per event, t -1.38: the right sign, tiny, not
                               significant. The disclosure barely moves the price.
POST-PUBLICATION EFFECT      = +4.8% / +7.3% / +2.8% per year market-adjusted at
                               h = 5 / 21 / 63, t 0.55 / 1.22 / 0.77: the WRONG sign
                               for the frozen NEGATIVE direction, and not significant.
```

This is not the 13D/G shape. There is no large move spent before entry to find: the
filing session, the forgone session and the prior run-up are all small and
insignificant. A restructuring or impairment reported outside an earnings release
simply carries little price-relevant surprise, and what follows is a mild,
insignificant REBOUND rather than the negative drift the hypothesis predicted. The
forgone session is −0.07%, so no same-day or first-session variant would change
anything — and none was scored.

## 4. Verdicts

```
FINAL VERDICT        = DATA_HOLD (frozen gate order, all three horizons)
MERITS READING       = NEED_MORE_EVIDENCE (the declared re-evaluation with both data
                       gates satisfied stops at the scorer gate - see below)
QUALIFICATION INPUT  = the post-entry market-adjusted return contradicts the frozen
                       NEGATIVE sign at every horizon -> the next frozen gate
                       (FROZEN_SIGN) is NO_EDGE, and |t| <= 1.22 would fail the
                       post-publication floor regardless
SIGN                 = CONTRADICTED. Not reversed. The cell closes.
CAPITAL ELIGIBLE     = NO
```

Gate by gate, in the preregistered order:

| gate | h=5 | h=21 | h=63 |
|---|---|---|---|
| 1 coverage (≤ 20% of sessions under 95%) | **FIRES** 48.6% | **FIRES** 49.2% | **FIRES** 49.7% |
| 2 missingness bias (≤ 1.5%/yr) | **FIRES** −6.74%/yr, t −2.99 | **FIRES** −6.34%/yr, t −2.75 | **FIRES** −6.23%/yr, t −3.31 |
| 3 effective periods / scorer | scorer NO_RESPONSE | scorer NO_RESPONSE | scorer NO_RESPONSE; 31 informative < 36 |
| 4 frozen sign (NEGATIVE) | **contradicted** (+) | **contradicted** (+) | **contradicted** (+) |
| 5 post-publication \|t\| ≥ 2 | 0.55 | 1.22 | 0.77 |
| 7 BH m = 3 | not rejected | not rejected | not rejected |
| 8 incremental t ≥ 2 | 0.60 | 1.58 | 0.87 |
| 9 materiality ≥ 1.5%/yr | +0.22% | +0.92% | +0.32% |

Both data gates were predicted to fire in the preregistration, before any return.
Every gate that measures economics fails as well, so no data repair could qualify
this cell.

**A wording note, not a gate change:** the merits `why` string reads "the canonical
scorer returned DATA_HOLD (the augmented arm's scores are identical to the baseline
arm's)". The scorer's own verdict is `NO_RESPONSE`; the frozen gate maps a scorer cell
without effective periods to NEED_MORE_EVIDENCE either way. The string is left as it
was committed before the results.

### The scorer blind spot this cell found

`alpha_agent.r63.sensitivity._fit_scaler` clips every feature to its own 1st and 99th
percentiles on each training fold. The signed event count is non-zero in **0.52%** of
scored rows at h=21/63 and **0.15%** at h=5 (2.6 and 0.8 names per decision date out
of ~500), so both percentiles are 0. The whole column is clipped to a constant, the
ridge receives a zero feature, and `run_cell` returns NO_RESPONSE by construction. The
13D/G cells were scorable because roughly 10% of rows carried an event. **Any event
family present in fewer than 1% of cross-sectional rows is invisible to the canonical
scorer.** That is recorded here and was not "fixed" after the results: the brief
forbids a second scorer, and changing the owner's winsorisation to rescue one cell is
exactly the move the discipline exists to prevent.

## 5. Where the information was lost

* **Already priced / no surprise.** The disclosure itself carries almost no measurable
  reaction (−0.19% on the announcement session, −0.07% on the next). Restructuring
  plans and impairments are typically foreshadowed in guidance, strategy updates or
  earlier results, so the 8-K records a decision the market already expected.
* **Market beta.** Raw returns are large and significant (+17 to +20%/yr, t up to 4.11)
  and are essentially all market: subtracting SPY leaves +2.8 to +7.3%/yr at t ≤ 1.22.
* **Wrong sign.** What remains after entry is a small rebound, not negative drift. Traded
  in the preregistered direction it loses money at every horizon and every cost.
* **Costs.** They only deepen the loss: the h=5 signed trade goes from −5.8%/yr at 1 bp
  to −17.4%/yr at the 12.5 bp desk rate.
* **Non-incremental.** Against the owned baseline, the owned 8-K intensity block and the
  incumbent, the orthogonalised rank IC is at best +0.005 and −0.012 at h=21 (wrong
  direction); the incremental value over the incumbent is under 1%/yr and never
  reaches t 2.
* **Multiplicity.** Nothing survives BH at m = 3; the smallest p is 0.22.
* **Instability.** Selection and lockbox agree in sign, and both are insignificant.
* **Data quality / survivorship.** Both data gates fire. The names that cannot be
  assessed earn 6.2–6.7%/yr LESS than the names that can (t −2.75 to −3.31), so the
  blind spot is concentrated in the worst outcomes. It cannot turn a wrong-signed,
  insignificant effect in the visible 94% into an edge.

## 6. What generalises

1. **Orthogonal is not valuable, confirmed a third way.** 13F was redundant and empty;
   13D/G was distinct but spent before entry; this cell is not even informative at
   announcement.
2. **A structured field beats text when it exists.** EDGAR's `items` column made the
   census exact and free. Every attempt to refine an Item with text (1.02, 3.01, 5.02)
   ran into heterogeneity that a deterministic rule could not clean.
3. **The S&P 500 is the wrong universe for rare distress events.** 3.01, 4.02, 1.03 and
   1.05 barely occur inside it, and the canonical scorer cannot see an event present
   in under 1% of rows even when it does occur.

## 7. Safety

No purchase, no subscription, no model promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no backfill, no live deployment. The live
checkout (`20598fa`) and the live next-open skew challenger were not touched. Research
only.
