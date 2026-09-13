# RESULT - PEER EARNINGS INFORMATION TRANSFER

**Mechanism:** `PEER_EARNINGS_INFORMATION_TRANSFER_V1`
**Selected and executed by:** the Alpha Agent, session 15 of the continuous loop, 2026-09-13. The
agent picked it up automatically after the executor was pinned; no human prompt chose it.
**Preregistration:** `e9a15ff`. **Implementation:** `86930a4`. **Pin:** `41c91b0`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\peer_earnings_information_transfer.json`
(artifact_hash `79650029...b3f32`).

## VERDICT: `DATA_HOLD` (gate 1, `DATA`). CAPITAL ELIGIBLE = NO.

**Why:** point-in-time SIC covers 86.4 % of qualification member-quarters, against the frozen floor of
90 %. CIK bridging covers 91.5 %. This is the outcome the preregistration stated at freeze.

**What was read:** `max_session_read = -1`. No traded name's return, IC, book or t-statistic was computed,
and the confirmation window is untouched.

## What this means

This is not evidence about the mechanism. The owned identity layer cannot yet place 8.5 % of
qualification member-quarters in an industry without using information from after the decision date.
Those member-quarters are mostly names that later left the index through acquisition.
- Substituting current SIC would misplace 1.5-4 % of members, and the kill rule forbids it.
- A lower coverage floor or a shorter window is also forbidden.

The agent records the hold as settled at catalog `data_version` 1. It re-runs the mechanism only if
the lead changes the data version, which requires two things first:
- an authoritative identity repair (95 AMBIGUOUS and 671 UNRESOLVED `cik_map` securities);
- a fresh census and a re-review of this executor.

The lead commissioned no such repair this session. The mechanism's class already has several closed
members, and its 3.38 %/yr gross hurdle makes the repair a poor use of effort ahead of the priced
purchase gate.

**For the frontier:** with this hold, every declared mechanism is settled, refused or human-gated.
The agent reports `NO_ELIGIBLE_MECHANISM`.
