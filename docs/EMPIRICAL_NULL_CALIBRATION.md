# Empirical-null calibration of mass discovery screens (R94)

Owner: `alpha_agent/r57/empirical_null.py` (the canonical statistical kernel package). Tests:
`tests/test_r94_empirical_null.py`, `tests/test_r94_amended_rule_prototype.py`. First consumer:
`research/agents/campaign_r94_calibrated_hedged_alpha_factory/r94_select.py` (campaign-local research
artifact, not committed; so is its hedged-basket engine test).

## Why it exists

R93 screened about 53,000 relationships with the analytic Newey-West t of a book's net return on
overlapping stride-1 decisions and Benjamini-Hochberg on the analytic p. With the targets deliberately
misaligned by at least one year, the screen produced the same exceedance rates as the real one. The
analytic t is therefore not a selection statistic in this estate. The R93 director retired it and ordered
that the next mass screen freeze empirical-null thresholds before reading real data.

## What the layer does

1. **Null**: `shift_targets()` circularly shifts the forward-return panel inside the discovery decision
   span by one offset common to every market and horizon (at least 260 sessions). The caller runs the
   identical screen code on the shifted targets. Weights and costs are target-free, so they are
   byte-identical between the real and the null run (asserted per relationship in `r94_select`).
2. **Empirical p** within a stratum (engine | panel | book | horizon): `(1 + #null >= s) / (1 + n_null)`.
3. **Empirical FDR q** (count-based, Tusher/Storey style): `FDR(c) = max(1, N_null(c)) / n_seeds / R(c)`,
   q = running minimum over thresholds at or below the row. The `max(1, .)` floor refuses to call a lone
   real row above the null maximum a discovery.
4. **Frozen thresholds**: `freeze_thresholds()` is a pure function of the null rows and the rule; its
   SHA-256 is carried into every calibrated row and every preregistration. `calibrate_frame()` refuses a
   tampered record. The selection rule must be written and hashed before the real screen is read.

## What it is not

It is not a qualification gate. The D/V/L verdict of a frozen candidate stays with the canonical gate
(`alpha_agent.r59` + `alpha_agent.agents_v2.runner` on non-overlapping books). The layer only decides
which discovery rows may ask for that verdict.

## R94 findings that bind the next consumer (director ruling, defects D1-D5)

- D1: one offset per seed means the effective null sample per stratum is about the number of seeds;
  tree strata with one seed are not calibrated for promotion.
- D2: rank-identical books (class-level or breadth-interacted features inside a single-group universe)
  inflate the discovery count; identical statistics must be de-duplicated before counting.
- D3: the overlapping statistic is ill-conditioned at long horizons (H126); non-overlapping cadence runs
  promote nothing there.
- D4: a stratum pools small and large universes; the null tests alignment, not timing - the static
  long/short bias is removed only by the timing-share rule.
- D5: promotion depends on the null construction (common offset vs per-market offset); an amended rule
  should require a row to pass both nulls.

Any change to the rule is a human decision; thresholds are hashed on null rows only before any screen.

## Amended-rule prototype (not applied to R94)

`dedupe_identical_statistics()` (D2) marks rank-identical books inside a stratum so each book counts once;
`calibrate_two_nulls()` (D5) promotes a row only if it passes the frozen rule under both null constructions
and reports the larger q. Tests: `tests/test_r94_amended_rule_prototype.py`. Sensitivity on the actual R94 screens
(R94_NULL_CALIBRATION.json, `addendum.amended_rule_sensitivity_engine_*`):

| Engine | Promotions, common-offset null only | Per-market null only | Amended rule (de-dup + both) |
|---|---|---|---|
| B (price state, 540 duplicate books) | 96 | 0 | 0 |
| C (cross-asset) | 0 | 8 | 0 |
| D (ML; per-market null 25.7% vs 12.6% at t >= 2.5) | 33 | 0 | 0 |
| E (hedged baskets, wave 1) | 5 | 6 | 0 |

Null-construction dependence runs both ways, which is defect D5 itself. Adopting the amended rule for R95 is
the human decision the R94 director named as the single next governed action.
