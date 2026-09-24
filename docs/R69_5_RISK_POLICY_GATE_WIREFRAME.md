# R69.5 — risk-policy review: wireframe and acceptance criteria

Produced BEFORE any UI code, per the mandatory redesign workflow. This release
adds no screen, no tab and no navigation. It extends **one existing panel** —
Step 3 of the Proposal Decision Review, `_pdrRiskContribution()` — and adds one
sentence to the review's scope note. The cockpit layout, the sidebar, the header
and every other card are untouched.

## 1. SCAN — what is already there

`_pdrSelectedTarget()` renders Step 3 from the FROZEN selection
(`selection.selected_target_implementation`). Inside it, `_pdrRiskContribution()`
renders a before/after table (per-name cap, covariance names, breach counts, risk
on both bases, concentration, largest position, cash) plus a warning block when
`discharged_without_reduction` is non-empty.

Every number comes from the frozen block. The file derives nothing, and the
architecture audit forbids weight or cost arithmetic in this region.

What the panel could NOT say before this release:

- whether the target complies with the cap the **current book** was judged
  against — the only comparison that holds the denominator still;
- that two of the 2026-09-23 full target's four reference breaches are names the
  target **created** (ALAB raised 1.84% → 3.21%, SNDK added at 2.88%), which the
  "weight did not move" warning can never reach;
- that a token trim (AMD, −0.36pp) drops a name off that warning while leaving it
  far above the original limit;
- that approval is now **withheld** until the operator rules on the policy.

## 2. REVIEW — critique before coding

| Check | Verdict |
|---|---|
| vertical stacked layout | Unchanged — this is one existing card in the existing grid |
| empty / useless Overview cards | None added; Overview untouched |
| Daily Plan heavy scrolling at 1920×1080 | New content is ~3 rows + one banner inside a card the operator has already opened at Step 3; it does not lengthen the first screen |
| hidden safety status | Improved: the withheld state is a visible badge, not a silent pass |
| diagnostics on the main dashboard | None added; nothing moves out of Audit / Advanced |
| `alert()` / `confirm()` | Not used, and not introduced |
| blank buttons | **No button is added at all** (see §3) |
| Create Orders / automation | Not implemented, not enabled, not referenced |

**Explicit design decision: the ruling is not a button.** A policy decision that
can be cleared by reflex is not a decision. The acknowledgement is an API-level
object (`risk_policy_acknowledgement`) that must name the exact frozen book, the
exact reference limit and the exact instruments. The screen STATES the decision
and shows the three options; it does not offer a one-click way to discharge it.
This also keeps the release inside its safety boundary: the UI gains no new write
control, so no click on this screen can approve anything.

## 3. PLAN — the wireframe (1920×1080, inside Step 3)

```
┌─ Step 3 — the exact target you selected (FULL TARGET) ─────────────────────┐
│ [FROZEN AT SELECTION] [PREVIEW ONLY] [NO ORDERS] [MANUAL REVIEW]           │
│                                                                            │
│  Positions 20 │ Changes 24 │ Turnover 35.0% │ Cost $85.73 │ Cash 20.0% ... │
│                                                                            │
│  ┌── target rows (unchanged) ─────────────────────────────────────────┐    │
│  │ Ticker  Now     Target   Δ       Value     Action                   │   │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                            │
│  Risk contribution — current book vs selected target        (UNCHANGED)    │
│  ┌────────────────────────────┬──────────┬──────────┐                      │
│  │ Per-name risk cap (3/N)    │  12.00%  │  15.00%  │  ← already there     │
│  │ Names in covariance univ.  │    25    │    20    │                      │
│  │ Cap breaches               │     2    │     0    │                      │
│  │ Risk, invested basis       │  11.67%  │  15.47%  │                      │
│  │ Risk, capital basis        │  11.13%  │  12.37%  │                      │
│  │ Concentration (HHI)        │  0.0372  │  0.0329  │                      │
│  │ Largest position / Cash    │   ...    │   ...    │                      │
│  ├────────────────────────────┼──────────┴──────────┤                      │
│  │ NEW: Breaches at the 12.00% reference cap        │  2 → 4               │
│  │ NEW: Risk carried by those names                 │ 29.5% → 54.6%        │
│  │ NEW: NAV carried by those names                  │  9.1% → 14.8%        │
│  └──────────────────────────────────────────────────┴──────────────────┘    │
│                                                                            │
│  ┌─ NEW ── ⚠ RISK POLICY REVIEW REQUIRED ─────────────────[WITHHELD]──┐    │
│  │ This target clears its OWN per-name cap only because that cap rose │    │
│  │ from 12.00% to 15.00% when the covariance universe went 25 → 20.   │    │
│  │ Held at 12.00%, four names breach:                                 │    │
│  │                                                                    │    │
│  │   ALAB  7.43% → 14.34% of risk   weight 1.84% → 3.21%   CREATED    │    │
│  │   SNDK     —  → 13.95% of risk   weight    0 → 2.88%    CREATED    │    │
│  │   AMD  15.35% → 14.15% of risk   weight 5.02% → 4.66%   trim closed│    │
│  │                                                         29% of gap │    │
│  │   DDOG 14.15% → 12.13% of risk   weight unchanged 4.11%            │    │
│  │                                                                    │    │
│  │ The declared 3/N policy is UNCHANGED and correctly applied. No     │    │
│  │ threshold was moved and no exception was granted. Approval is      │    │
│  │ withheld until an operator rules:                                  │    │
│  │   • ACCEPT_AS_IS                                                   │    │
│  │   • JUDGE_AGAINST_THE_BEFORE_UNIVERSE                              │    │
│  │   • ADD_AN_ABSOLUTE_COMPANION_FLOOR                                │    │
│  │ See docs/R69_1_POLICY_ITEMS_FOR_MANUAL_REVIEW.md. The ruling is    │    │
│  │ recorded through the governed API, not from this screen.           │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                            │
│  Target identity 2d3707b5f597c275 · weights from … · rows from …           │
└────────────────────────────────────────────────────────────────────────────┘
```

And one line in the review's scope area (existing text block, no new card):

```
Scope: every dollar in all three targets sits in US_EQUITY or cash. This is a
comparison WITHIN that opportunity set, not a search of the cross-asset frontier.
```

## 4. Acceptance criteria

1. Step 3 renders the reference row group whenever the frozen block carries
   `risk_contribution.reference_compliance` in state `AVAILABLE`.
2. The `RISK POLICY REVIEW REQUIRED` banner renders **iff**
   `risk_contribution.policy_review.required` is `true`; it is absent otherwise.
3. Names the target created carry a distinct `CREATED` marker, sourced from
   `code === "OPENED_AGAINST_THE_REFERENCE_LIMIT"`.
4. The banner states that no threshold was changed and names the policy owner and
   the manual-review document.
5. No `alert()`, no `confirm()`, no new button, no new write path.
6. Every value is read from the frozen block. No arithmetic in `index.html`.
7. A selection whose block predates R69.5 renders the existing panel unchanged and
   shows a "verdict not published — re-select" note rather than implying
   compliance.
8. Console clean at 1920×1080; no horizontal scroll; the first screen is not
   lengthened (the panel is inside Step 3, which is already below the fold by
   design).
