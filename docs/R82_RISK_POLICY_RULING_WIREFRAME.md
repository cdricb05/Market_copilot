# R82 — the governed risk-policy RULING, and the panel that states it

Target viewport 1920x1080. This slice adds NO new page, NO new column and NO new
route to the cockpit layout: it changes what the existing **Step 3 — the exact
target you selected** card says once a ruling is on record. Nothing below scrolls
the Daily Plan further than R69.5 already did, because the ruled panel REPLACES
the unruled one rather than being appended to it.

## 1. SCAN — what exists today (read before any change)

| Concern | Owner | State found |
| --- | --- | --- |
| 3/N per-name cap | `engine.holding_opportunity_cost.risk_contribution_limit` | declared once; the limit follows the object judged |
| breach function | `engine.holding_opportunity_cost.risk_contribution_breaches` | the one callable both sides use |
| before/after sight line | `engine.selected_target.risk_contribution_comparison` | publishes both limits, both risk bases |
| reference comparison | `engine.selected_target.reference_limit_compliance` | holds the BEFORE cap still and re-asks |
| the named decision | `engine.selected_target.policy_review_state` | raises `RISK_POLICY_REVIEW_REQUIRED_DENOMINATOR_RELAXATION` |
| the approval gate | `api.portfolio_decision.record_decision` | withholds at `SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW` |
| the only door through it | `POLICY_REVIEW_ACK_TOKEN` | a per-request acknowledgement that **clears** the gate |
| panel | `api/ui/index.html` → `_pdrPolicyReview` | display only, no write control, three options as prose |

**The gap this slice closes.** The three courses R69.1 put to the operator exist
as three *strings* in `POLICY_REVIEW_OPTIONS`. Only one of them is reachable: the
single acknowledgement token, which on acceptance lets the approval through. That
is `ACCEPT_AS_IS` semantics under a neutral name. There is no way to record
`JUDGE_AGAINST_THE_BEFORE_UNIVERSE` — the ruling that says the breach must be
repaired by reducing the breaching name — because the only recordable outcome
unblocks the very approval that ruling refuses. The ack is also not *durable*: it
lives in the approval request, so a book nobody approved carries no ruling at all.

## 2. REVIEW — critique of the current panel

- OK: no `alert()`, no `confirm()`, no blank button, no write control (correct — a
  ruling that can be clicked by reflex is not a ruling).
- OK: safety badges present (`APPROVAL WITHHELD`, `NO THRESHOLD CHANGED`,
  `MANUAL REVIEW`); no Create Orders; no automation.
- OK: compact table, inside an existing card, not a vertical stack.
- DEFECT: **the three options read as equally available and none of them is.** The
  panel invites a ruling the API cannot accept.
- DEFECT: **there is no state after the ruling.** Once an operator rules, the panel
  has nothing to show: it either still demands a ruling or disappears. The ruling,
  the cap it binds and the obligations it reopens are invisible.
- DEFECT: **DDOG is missing from the headline warning.**
  `discharged_without_reduction` asks only "did the weight move?"; DDOG's moved by
  0.11 points of NAV, so the banner reads "1 BREACH DISCHARGED" for a book whose
  own attribution block says **two** names closed on the limit alone. The precise
  number is published one block away and the loud number is the narrow one.

## 3. PLAN — the ruled panel (wireframe, 1920x1080)

Unruled (unchanged, R69.5):

```
+--- Step 3 - the exact target you selected (MINIMUM REPAIR) ---------------------+
| [FROZEN AT SELECTION] [PREVIEW ONLY] [NO ORDERS] [MANUAL REVIEW]               |
| Positions 14 | Changes 12 | Turnover 21.19% | Cost $52.33 | Cash 46.92% | ...  |
| ... target rows ... risk-contribution table ... reference rows ...              |
| +-- WARN -----------------------------------------------------------------+    |
| | (!) RISK POLICY REVIEW REQUIRED  [APPROVAL WITHHELD] [MANUAL REVIEW]    |    |
| | Held at 12.00%, 2 name(s) breach:   <compact 4-col table>               |    |
| | Rule on it: ACCEPT_AS_IS / JUDGE_AGAINST_THE_BEFORE_UNIVERSE / FLOOR    |    |
| +------------------------------------------------------------------------+    |
+--------------------------------------------------------------------------------+
```

Ruled (NEW — replaces the block above in place, same card, same height budget):

```
+-- RULED -----------------------------------------------------------------------+
| (=) RISK POLICY RULED - JUDGE AGAINST THE BEFORE UNIVERSE                      |
| [RULING ON RECORD] [REFERENCE CAP BINDS] [APPROVAL UNAVAILABLE]                |
| [NO THRESHOLD CHANGED] [PREVIEW ONLY] [NO ORDERS] [MANUAL REVIEW]              |
|                                                                                |
| Ruled by <actor> - <ts> - ruling <ruling_id>                                   |
| Bound to selection <selection_id> - frozen book <impl_hash[:12]>               |
| Binding cap for THIS book: 12.00%  (the cap the 25-name current book was       |
| judged against). The target's own 3/N cap, 21.43% on 14 names, stays published  |
| and is NOT the cap this book is judged against.                                |
|                                                                                |
| Ticker | Risk share       | Weight           | Own cap  | RULED cap | Oblig.   |
| AMD    | 15.07% -> 20.33% | 5.11% -> 5.11%   | complies | BREACH    | OPEN     |
|        |                  | (unchanged)      | 21.43%   | +8.33pp   | REDUCE   |
| DDOG   | 15.28% -> 21.41% | 4.34% -> 4.24%   | complies | BREACH    | OPEN     |
|        |                  | (trimmed 0.11pp) | 21.43%   | +9.41pp   | REDUCE   |
|                                                                                |
| Obligations reopened by this ruling: 2   (0 on the target's own cap)           |
| APPROVAL UNAVAILABLE - the ruled cap binds and 2 obligations are open.         |
| Still available: REJECT - HOLD - select a target that complies at 12.00%.      |
| Scope: THIS frozen proposal and THIS selected target only. No declared         |
| threshold moved. No exception granted. No absolute companion floor created.    |
+--------------------------------------------------------------------------------+
```

### Layout rules honoured

- The block lives inside the existing Step 3 card in the central workflow cockpit;
  left sidebar, top status bar, KPI row, right-side action/safety panel and the
  Audit / Advanced diagnostics area are untouched.
- One badge row + three text lines + one compact 6-column table + two verdict
  lines: the ruled block is no taller than the unruled block it replaces, so the
  first screen still does not scroll at 1920x1080.
- No write control is added. The ruling is recorded through the governed API for
  the reason R69.5 already gave: a policy ruling must name the exact frozen book.
- No `alert()`, no `confirm()`, no Create Orders, no automation, no new approval
  path, and no weight arithmetic in the browser — every number is read.

## 4. Acceptance criteria

1. `JUDGE_AGAINST_THE_BEFORE_UNIVERSE` is recordable, durable and bound to the
   exact proposal / selection / frozen-book identity; a ruling that names another
   book, cap or instrument set is refused.
2. With that ruling on record, the binding cap for the frozen book is the BEFORE
   cap (12.00%), AMD and DDOG are named as OPEN obligations, and the panel says
   `APPROVAL UNAVAILABLE`.
3. The approval gate refuses the same target at a state distinct from "a ruling is
   missing", and an acknowledgement cannot override a recorded
   `JUDGE_AGAINST_THE_BEFORE_UNIVERSE`.
4. This release adds **no** new path to approval: the ruling writer can only ever
   make approval less available.
5. `ADD_AN_ABSOLUTE_COMPANION_FLOOR` is declared and REFUSED as not available in
   this release; no absolute threshold is invented anywhere.
6. The frozen 2026-09-25 proposal, its selection record and its
   `selected_target_implementation_hash` are byte-unchanged, and the daily cycle
   is not rerun.
7. `RISK_CONTRIBUTION_CAP`, the 3/N basis and every declared threshold are
   unchanged, and every number on the panel is produced by
   `engine.holding_opportunity_cost` — measured nowhere else.
