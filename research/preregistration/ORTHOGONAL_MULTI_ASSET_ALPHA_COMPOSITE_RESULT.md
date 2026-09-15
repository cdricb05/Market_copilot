# RESULT - ORTHOGONAL MULTI-ASSET ALPHA COMPOSITE OF FROZEN SLEEVES

**Mechanism:** `ORTHOGONAL_MULTI_ASSET_ALPHA_COMPOSITE_V1` (global candidate `GC_XA_ORTHOGONAL_MULTI_ASSET_COMPOSITE`)
**Run:** ALPHA_COMPOSITE_STRIKE_SEP15_V1, verdict 1. Executed 2026-09-15 21:11 UTC through the canonical handler
body `alpha_agent.r59.mechanisms.execute_job` (3.3 s).
**Preregistration:** `69d267f` (sha256 `5cd4858f...`). **Implementation:** `e76850f` (`7bac664d...`). **Pin:** `d35f1b5`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\orthogonal_multi_asset_alpha_composite.json`
(artifact_hash `7294db5e...caff9e`).
**Input identity:** `GLOBAL_FRONTIER:COMPLETE:51 candidates:eligible=GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW,GC_FX_XS_CARRY_DATED_CONTRACT`.
**ResearchMemory:** `HM_ORTHOGONAL_MULTI_ASSET_ALPHA_COMPOSITE_V1`, outcome `DATA_HOLD`.

## VERDICT: `DATA_HOLD` (gate 1, sleeve inventory). CAPITAL ELIGIBLE = NO.

The executor built the canonical global frontier from the committed catalog and the estate (state COMPLETE, 51
candidates including this composite, 127 of 127 owner identities reconciled) and re-derived the inventory under
the frozen reading of E1-E10.

| check | result |
|---|---|
| derived eligible set | `GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW`, `GC_FX_XS_CARRY_DATED_CONTRACT` - identical to the preregistered set (no drift) |
| selected sleeves | 2: SPY reversed skew (EQUITY_INDEX, HEDGER_DEMAND_PRESSURE), dated-contract FX carry (FX, RISK_TRANSFER_PREMIUM) |
| mechanism classes / asset classes | 2 / 2 |
| frozen minimum | 3 sleeves and 3 mechanism classes |
| fired | `SLEEVES_2_BELOW_3; MECHANISM_CLASSES_2_BELOW_3` |
| return paths loaded | none |
| composite weights, covariance, returns | never computed |
| qualification / confirmation windows | never formed; the confirmation is UNREAD |

Of the 29 other ranked candidates: 24 fail E2 (closed on their own untouched evidence, an executed kill, a
DATA_HOLD, or no qualifying history), 8 are in the closed PRICE_STATE_TRANSFORMATION class (E5), one needs
paid data behind a human purchase gate (E3/E4), one is the operational incumbent (E9), and none except the
two eligible sleeves has a declared per-session or per-period return path (E7/E8). R39/R40 machine shadows
additionally store month-end rows only.

### Disclosure: one reason code differs from the preregistration table

The preregistration listed `GC_EQIDX_SCHEDULED_EVENT_PREMIUM` (rank 7) as failing E2 because its mechanism
settled NO_EDGE earlier on 2026-09-15. In this branch's catalog its frontier evidence label reads
`GOOD_STRATEGY_INCOMPLETE_EVIDENCE` (built on the standalone 1994-2015 statistics, net +2.46 %/yr, t 3.37),
so the mechanical E2 reading did not flag it; it was excluded by E7/E8 alone (no declared return path).
The eligible set, the selection and the verdict are unaffected. The lesson for any re-preregistration: an
owner evidence label is not a substitute for the executed verdict of the claimed mechanism - E2 must also
read ResearchMemory settlements of members and closed families. The mechanism stays closed and is not
revisited.

## What this means

The estate does not hold three frozen, non-closed, economically distinct sleeves with recoverable return
paths. Only two sleeves survive their own evidence, and one of them (SPY reversed skew) rests on a sign found
post hoc with 5-session periods beginning 2022-09-09 - so even a third eligible sleeve could not have met the
frozen 2,268-session common-history floor while the rank-order rule selects SPY skew. The composite route is
therefore blocked by the estate itself, not by a construction choice: combining sleeves that failed on their own
untouched evidence would be a rescue, and it was not done.

**Not done, and never to be done under this preregistration:** a looser eligibility reading, adding a closed
sleeve (commodity carry, cross-asset trend, R39 WIDE), dropping SPY skew to lengthen history, a different
weighting rule, target, cap or lookback, or computing any composite return.

**For the frontier:** the candidate is held by owner evidence (`RESOLVE_DATA_HOLD`); it re-enters only when the
catalog data_version changes, which requires a new preregistration once a third eligible sleeve with a long
common history exists. The run continues to structural forced-flow research.
