# Release 54.2.3.2 — Authoritative Decision / Proposal Supersession

Status: LANDED (hotfix on R54.2.3.1). Scope: one bounded decision-composition
repair plus its guard rails. Nothing here creates an order, a fill, a target, a
new route or any automation; paper-only, preview-first, manual-review boundaries
are unchanged.

## 1. The live contradiction (2026-09-02, ~23:51Z)

The Sep-2 normal Portfolio Cycle completed. Two research artifacts then stood
for the same session:

| artifact | produced by | at (UTC) | verdict |
| --- | --- | --- | --- |
| reassessment v1 `prs_..._74776dda34ac` | live EVENT cycle `evt_4456405791b07b2e` (non-governed) | 23:38:09 | PROPOSAL_READY (HOC `702c599e…`, scoring `24fb64a4…`) |
| proposal `reap_2026-09-02_..._dcf85725a02e` | the same event cycle | 23:38:15 | READY — 28 changes, 35% turnover, $85.69 |
| reassessment v2 `prs_..._029df5cdcda5` | governed DRC `drc_2026-09-02_15abfb01856f` | 23:51:50 | **CURRENT_NO_CHANGE** (HOC `a162fca9…`, scoring `acac7587…`) — supersedes v1 in the R54.2 version chain |

The governed manifest recorded `reallocation_proposal_state: NOT_REQUIRED` —
the cycle deliberately skipped the proposal step because the session's decision
is "no change". That skip left the event cycle's 23:38 proposal standing as the
proposal-index head for `(book, 2026-09-02)`, and every "current proposal" read
keys purely on that head:

* the decision lane derived `PROPOSAL_REVIEW_REQUIRED` (approvable, material);
* the canonical portfolio decision ranked review-required above the
  reassessment's CURRENT_NO_CHANGE;
* Today rendered **"REALLOCATE — 28 POSITIONS CHANGE"** with the stale
  economics and a live "Review reallocation" CTA — directly above its own
  narrative "No change is proposed";
* the governed-lane projection read its decision word from the stale proposal's
  outcome and published **CHANGE_RECOMMENDED stamped with the NO-CHANGE
  assessment's own 23:51:50 timestamp**;
* `record_decision` would have accepted an APPROVE on the superseded proposal
  (every existing guard passed: not withheld, not HOLD, material, hash current,
  corporate-action registry unchanged).

Why no invariant fired: the Release-29.3 binding invariant (I4) compares the
proposal's bound HOC hash against the assessment's **proposal_binding** — which
a CURRENT_NO_CHANGE assessment does not publish (no proposal is requested), so
the comparison arm was `None` and the check was structurally disarmed in exactly
the case that needed it.

## 2. The canonical authority rule

Owned by `api.portfolio_decision` (the ONE decision owner — no new owner):

    newer governed completed-session decision
        > older governed completed-session decision
        > any older proposal awaiting manual review

* A governed **intraday** decision participates only through the existing
  R54.1 contract (the gate + the one ordering function).
* A **non-governed / governance-withheld intraday research result never
  supersedes an authoritative governed decision** — its assessment may advance
  the R54.2 store head, but without decision authority it changes nothing.
* A proposal that is superseded remains **immutable, history-visible evidence**
  — and is never again current, reviewable-as-outstanding-work, or approvable.

### The ONE calculation

`api.portfolio_decision.assess_proposal_supersession(proposal_summary,
assessment)` — pure, fail-closed in both directions:

* supersedes only when EVERY link is proven: a proposal exists; an
  authoritative assessment exists **and `is_governed` is True**; its decision
  is conclusive (`CURRENT_NO_CHANGE` / `PROPOSAL_READY`); and the direction is
  newer-onto-older (a later session always; the same session when its
  conclusion is CURRENT_NO_CHANGE, or when it requested a proposal from
  provably different, not-older evidence);
* never supersedes on: no assessment observed, authority unproven, a blocked or
  inconclusive state, an older session/stamp, or an unprovable evidence
  direction — the standing review keeps its status.

`load_decision_supersession(...)` is the bounded loader: reassessment store
head (`load_latest_assessment_pointer`, a pure index read) + decision-authority
proof from **either** the governed DRC manifest
(`daily_research_cycle.load_governed_manifest_reference` — a compact pure file
read of the run manifest that must bind the head's reassessment hash) **or** a
persisted R54.1 governed record binding the head's hash. Hermetic callers
supply `assessment=`/explicit store dirs; unresolvable authority → NOT
superseded.

`resolve_decision_authority(...)` is the canonical selector every surface
echoes: `current_authoritative_decision_id / _session / _decision_type`,
`current_reviewable_proposal_id` (nullable), `superseded_proposal_ids`,
`supersession_reason`.

### Where the verdict is consumed (never recomputed)

| surface | change |
| --- | --- |
| `api.reallocation_proposal` | read state `SUPERSEDED_BY_NEWER_DECISION` (payload `superseded`/`superseded_by`; approvable/executable forced False; history message); summary publishes `reallocation_proposal_generated_at`; constrained composition renders the supersession headline |
| `api.portfolio_decision` lane | `PROPOSAL_SUPERSEDED_BY_NEWER_DECISION` outranks review/hold/withhold **and a recorded decision**; current-work economics go quiet (`one_way_turnover` etc. → None; materiality zeroed) while the numbers move to the explicit `superseded_proposal` history block |
| `record_decision` | server-enforced refusal (`status PROPOSAL_SUPERSEDED_BY_NEWER_DECISION`, nothing written) naming the newer decision, its session and artifact id — recomputed on the live endpoint path, so a direct POST cannot slip past a browser rendering |
| governed projection | the DRC-governed decision word comes from the governed ASSESSMENT first: `CURRENT_NO_CHANGE` projects the new governed word `GD_NO_CHANGE`; a superseded proposal's outcome/hash never enter the governed identity |
| `api.workflow_state` | consumes the ONE calculation (assessment view = the canonical reassessment summary + the hoisted Release-29.5 governed flag); overrides `rp_state` to `REALLOCATION_PROPOSAL_SUPERSEDED` (never approvable); CPD echoes the verdict and falls through to `NO_CHANGE`; new semantic invariant `NO_CHANGE_DECISION_WITH_REVIEWABLE_PROPOSAL`; composes the `decision_authority` selector block |
| `api.operator_presentation` | the superseded proposal's allocation rows never re-enter the Today hero count (`positions_changing` fallback gated); the decision summary frames the target `SUPERSEDED_HISTORY_ONLY`, no approval CTA |
| `api.active_manager_state` | `decision_authority.authoritative_selector` echoes the selector verbatim |
| UI | renders the new states/chips verbatim; still performs no evidence-hash comparison and decides nothing |

### Hermetic seam rule

The default (production-store) resolution runs on production-default reads —
the live routes — or when a caller supplies the sibling store roots
(`reassessment_dir` / `drc_dir`). A hermetic caller that redirected only the
proposal store keeps its constructed world untouched; it opts in by passing the
sibling dirs or an explicit `supersession=` verdict. This is the Stage-22
`actions_dir` precedent applied to the new stores, and it is what keeps unit
tests from ever depending on the operator's live reassessment head.

## 3. What did NOT change

* No proposal artifact, reassessment artifact, decision record or ledger row
  was modified or deleted — supersession is a **read-composition verdict**;
  history is immutable and stays visible (Audit shows the superseded proposal
  with the superseding decision/session/reason).
* No new route: `POST /v1/operations/portfolio-decision/record` keeps its exact
  request model; there is no supersede/backfill/force endpoint and no operator
  date input.
* Stage-19 execution precedence, the R54.1 gate and ordering function, the
  R54.2 version chain, corporate-action staleness (Stage 19.1) and the
  withhold/HOLD guards (R29.3/R47) are all unchanged — supersession sits
  beside them as one more fail-closed reason a proposal is not current.
* Order/fill/broker/automation surface: none, still.

## 4. Live verification (read-only)

Through the repaired composition against the production stores (no restart, no
write): the workflow composes `CPD NO_CHANGE / lane
PROPOSAL_SUPERSEDED_BY_NEWER_DECISION / RPS SUPERSEDED — HISTORY ONLY /
consistency CONSISTENT`; the authority selector answers
`prs_2026-09-02_alpha_paper_book_1_029df5cdcda5 / 2026-09-02 /
CURRENT_NO_CHANGE / reviewable None / superseded
[reap_2026-09-02_alpha_paper_book_1_dcf85725a02e]`; the Today hero renders
**HOLD CURRENT PORTFOLIO** with 0 positions changing and no CTA; the
reallocation read returns SUPERSEDED/not-approvable; `record_decision` refuses
with the newer decision named; the governed read now answers
`CURRENT_NO_CHANGE · GOVERNED_DAILY_CYCLE · 2026-09-02T23:51:50Z`. The running
8001 backend serves the pre-R54.2.3.x runtime until the canonical restart.

## 5. Guard rails

* `scripts/audit_architecture.py --strict` —
  `check_release54_2_3_2_decision_supersession` (16 blocking fields: one
  calculation, refusing write path, rendering reads, consuming workflow +
  selector + invariant, assessment-first projection, verbatim presentation,
  no UI derivation, no new route).
* `tests/test_release54_2_3_2_decision_supersession.py` — 26 tests covering
  the twenty Phase-H proofs plus the direction table, vocabularies, selector,
  projection, wiring, UI and route statics.
