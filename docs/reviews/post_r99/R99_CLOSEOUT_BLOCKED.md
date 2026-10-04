# R99 closeout — BLOCKED by the canonical guard

**Final status: `R99_NOT_COMPLETED`**

**R99 FOUND NO VALIDATED ALPHA.**

R99 (`R99_MULTI_ASSET_ALPHA_OFFENSIVE`) was a real multi-asset search. It produced no validated alpha. Its own deterministic gate, `r99_contract_guard.py`, does not authorise either finalisation or a hard stop. So R99 is closed as **not completed**. It is not complete and it did not hard-stop. No final report was generated, as the guard requires.

## The guard's decision at closeout

| | |
|---|---|
| Guard | `research/agents/campaign_r99_multi_asset_alpha_offensive/r99_contract_guard.py` (unchanged) |
| Evaluated | 2026-10-04T18:48:13Z |
| Decision | `R99_CONTINUE_REQUIRED` (exit 2) |
| Active research minutes | **174.7** (budget 180) |
| Valid hard-stop events | none (no `R99_HARD_STOP_EVENTS.json` exists) |
| Downstream pending | none: nothing awaits validation, skeptic, risk or ensemble |
| Remaining quota deficits | `EXPERIMENTS_PREREGISTERED` −10 (20 / 30), `EXPERIMENTS_MEASURED` −4 (20 / 24), `DIRECTIONAL_MEASURED` −5 (1 / 6), `EVENT_DRIVEN_MEASURED` −3 (1 / 4) |

### Why no stop condition applies

| Code | Condition in the guard | Status |
|---|---|---|
| H1 | Production mutation | **Not met.** Fingerprints match: R99 BEFORE = R99 AFTER = post-R99 BEFORE = post-R99 AFTER. That is 175 store files, 4 API bodies and 6 DB tables, with HEAD `8d6c366` unchanged. |
| H2 | Safety violation | **Not met.** There were no orders, fills, promotions, proposals or forward adoptions. |
| H3 | Research system unusable, no safe independent lane, after bounded repair | **Not met.** The research system is usable: the runner, books and gates all work, and PF1 is now repaired. The director's W11 judgement that the *owned-data frontier* is exhausted under R99's caps is a statement about research **opportunity**, not system usability. As instructed, "frontier exhausted" is not treated as H3. |
| H4 | Active ≥ 180 min with quotas incomplete | **Not met: 174.7 < 180.** |

### How the active clock was handled (no fabricated minutes)

The guard computes active minutes as wall-clock time since `started_at_utc`, minus the intervals declared in `R99_START_STATE.json.idle_exclusions`.

- R99's last work-log entry and its last guard run were both at **17:54:18Z**.
- This post-campaign review started about 48 minutes later.
- A bare guard run would have counted that review time as R99 research. It would then have mechanically granted **H4**, a hard stop paid for with fabricated active minutes.

To prevent that, one idle exclusion was recorded through the clock rule's own mechanism (`post_r99_closeout_guard.py`), and the guard was evaluated at the same instant:

```
tag POST_R99_CLOSEOUT_REVIEW   from 2026-10-04T17:54:18Z   to 2026-10-04T18:48:13Z   53.917 min
```

Copies of `R99_START_STATE.json` and `R99_CONTRACT_GUARD.json` taken before closeout are kept in this folder. No quota, counter source, result or verdict changed; `POST_R99_VERIFICATION.json` shows every non-clock counter identical before and after.

**Caveat for later readers.** The guard is a live-campaign gate. Any future run of it keeps accruing wall-clock time after 18:48:13Z and will eventually report H4. That later report would be an artifact of time passing, not research. The authoritative closeout is the 18:48:13Z evaluation recorded here.

### Why R99 was not "completed" by doing more work

The four deficits could only be closed by **new** preregistrations and measurements, including 5 more directional and 3 more event-driven experiments.

The director's W11 agenda ruling found that no further owned-data expression is admissible under R99's declared caps and standards. The remaining ready designs are all blocked:

| Design | Blocker |
|---|---|
| C34 | Cap-blocked |
| C52F | Needs new data certification and a new ID |
| X04 | Power-held for about 4.5 years |
| BK08, C53* | Cost- or G4-blocked |

Creating filler hypotheses to satisfy the quotas was forbidden, and none were created.

## What R99 genuinely accomplished (independently recounted)

Every counter below was recounted from the raw artifacts by `post_r99_verify.py`, which does not import the guard. Each was cross-checked against a second source:

- the measurement rows;
- the preregistration artifact;
- the 20 `PIPELINE_OK preregister` tokens;
- the read-only R59 research memory, which holds 20 R99 hypotheses, all `NO_ALPHA_EVIDENCE`.

**0 discrepancies.**

| Counter | Value |
|---|---|
| Fresh hypotheses screened | 169 |
| Formal gate assessments | 114 |
| Canonical preregistrations | 20 (1 refusal at the door) |
| Measured experiments | 20 |
| Measured asset areas | 5 (RATES 5, COMMODITIES 6, FX 3, EQUITY 3, INTERNATIONAL 3) |
| Hedged or cross-asset | 19 |
| Curve or spread | 9 |
| Distinct information families measured | 20 |
| Reached validation and lockbox | 2: R02 `H_d1181435_4f695f30a9e1`, C02 `H_1b9c733e_787575045691` |
| Skeptic verdicts | both **KILLED** |
| Skeptic survivors / risk-cleared / ensembles | **0 / 0 / 0** |
| Portfolio, order, fill, promotion or production change | **none** |

### Why the two lockbox candidates died

The cost-budget machine check was `NOT_EVALUABLE` because of PF1. It is now evaluable and **passes** for both (see `PF1_RERUN_R02_C02.json`). That changes neither verdict, because both candidates fail the canonical statistical gate independently:

- **R02** (US 2y policy-path premium, ZF/ZB curve). Lockbox net is +0.16%/yr against a 1.5% materiality floor, t 0.08 and p 0.47. It turns negative at 2× cost and in the second lockbox half.
- **C02** (WNGSR storage deviation, NG calendar spread). Lockbox net is −4.62%/yr, t −2.10. It fails sign consistency with validation, and the burden-corrected p is 1.0.

## Governance exceptions that qualify the R99 record

These are recorded in `R99_PROVENANCE_ERRATA.json`. No historical artifact was rewritten.

1. **C04 was measured under an admission that was withdrawn 22 seconds later.**
   - Its D layer was revealed at 15:46:12Z; W1A ruled it G1 FAIL / G7 FAIL at 15:46:34Z.
   - C04 stays in the guard's measured count but is **not a clean admissible experiment**.
   - Its outcome (NO_ALPHA_EVIDENCE at D) is unaffected. Removing it changes no quota from PASS to FAIL or back.
2. **Frozen-source hash drift.**
   - 19 preregistrations recorded **3** distinct `r99_cells.py` hashes, and none equals the final file. (R99's PF4 said 4; the fourth was actually R15's `frozen_rules_sha256` for a different file.)
   - `R99_EVENT_RULES.json`, which R15 binds, also drifted after R15's preregistration. R15's event source CSV is unchanged.
   - Behavioural evidence (exact Discovery reproduction and 19/19 truncation-lookahead passes) indicates the measured cells were unaffected, but the exact preregistered bytes cannot be reconstructed.
3. **Horizon dual representation.** The cell table shows h=21, but h=1 was executed. All preregistrations state h=1, so the returns are what was preregistered.

## R99 status line

```
R99_NOT_COMPLETED  |  R99 FOUND NO VALIDATED ALPHA  |  guard R99_CONTINUE_REQUIRED @ 174.7 active min
0 skeptic survivors | 0 risk-cleared | 0 ensembles | production PRESERVED
```
