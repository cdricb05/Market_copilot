"""
alpha_agent.report_renderer — deterministic EXECUTIVE research brief (HTML+text).

Renders the Stage 4 research report from a fully-computed *model* dict as a
plain-English executive email whose primary audience is the human operator, not
the machine. Every numeric value originates in deterministic Python upstream;
this module performs NO LLM call, opens NO network/DB connection, reads NO clock
and introduces NO randomness. Identical models render byte-identical output. All
dynamic values are HTML-escaped. The email body is produced here at zero
LLM-token cost.

Executive report structure (Stage 7.1 — email is the primary product):
   1. BOTTOM LINE                     6. MODEL HEALTH
   2. ACTION TODAY                    7. RISK AND SHADOW PORTFOLIOS
   3. PORTFOLIO SCORECARD             8. HISTORICAL DATA READINESS
   4. WHAT CHANGED SINCE LAST REPORT  9. SOURCE / AGENT HEALTH
   5. RESEARCH DECISIONS             10. TECHNICAL APPENDIX

Plain English only in the main sections. Internal terminal tokens, raw run IDs,
raw source record IDs, local file paths, schema versions and provider
implementation details live ONLY in the TECHNICAL APPENDIX. Every signed number
carries an explicit ``+``/``-`` sign so meaning never depends on colour alone.
"""
from __future__ import annotations

import hashlib
from typing import Any

from .runtime_contracts import canonical_json, html_escape

REPORT_SCHEMA_VERSION = "2.0.0"

_HEADER_NAVY = "#0b2545"
_HEADER_NAVY_2 = "#12345c"
_ACCENT = "#1f6feb"

# --------------------------------------------------------------------------- #
# Canonical safety flags. These describe FIXED safety invariants only. The
# automatic-research-schedule state is NOT a fixed invariant — it is derived
# separately from the real Windows scheduled-task states (see schedule_status)
# and is never hardcoded to "ON".
# --------------------------------------------------------------------------- #
STATUS_FLAGS = ("TRADING AUTOMATION: OFF", "BROKER EXECUTION: OFF",
                "PAPER ONLY")

# --------------------------------------------------------------------------- #
# Automatic-research-schedule state (Stage 7.2). Derived from the real Windows
# scheduled-task states upstream; the renderer only presents the resolved value.
# We NEVER claim the schedule is On unless a task is verified Enabled; unknown
# or uncollectable states resolve to "Not verified", never to "On".
# --------------------------------------------------------------------------- #
SCHEDULE_OFF = "Off"
SCHEDULE_ON = "On"
SCHEDULE_UNVERIFIED = "Not verified"


def schedule_status(task_states: Any) -> str:
    """Resolve the canonical automatic-research-schedule state from a mapping of
    ``{task_name: "Enabled"|"Disabled"|None}``. All present and Disabled -> Off;
    any verified Enabled -> On; any missing/unknown/uncollectable -> Not verified.
    This is the ONE place email, API and UI derive the schedule state; it is
    never hardcoded to On."""
    if not task_states or not isinstance(task_states, dict):
        return SCHEDULE_UNVERIFIED
    norm = []
    for v in task_states.values():
        s = str(v).strip().lower() if v is not None else None
        if s not in ("enabled", "disabled"):
            return SCHEDULE_UNVERIFIED
        norm.append(s)
    if not norm:
        return SCHEDULE_UNVERIFIED
    if any(s == "enabled" for s in norm):
        return SCHEDULE_ON
    return SCHEDULE_OFF


def schedule_flag_text(state: Any) -> str:
    """Header-chip text for the resolved schedule state (upper-case, explicit)."""
    s = str(state or SCHEDULE_UNVERIFIED)
    return "AUTOMATIC SCHEDULE: %s" % s.upper()


# The research verdict — stated EXACTLY ONCE in the executive body (Stage 7.2
# requirement). Any equivalent restatement must reuse this exact string so the
# once-only guarantee is enforceable by a deterministic test.
VERDICT_SENTENCE = ("We do not yet have enough evidence to trust or replace the "
                    "current model.")

_DISPOSITION_VERDICT = {
    "NEED_MORE_EVIDENCE": VERDICT_SENTENCE,
    "CHAMPION_CONFIRMED_NO_CHANGE":
        "The current model still looks genuine and needs no change.",
    "CHAMPION_CONFIRMED_RISK_OVERLAY_REQUIRED":
        "The current model still looks genuine, but its risk needs better "
        "control.",
    "CHAMPION_REJECTED":
        "The current model is no longer defensible and should be replaced.",
}


def verdict_sentence(disposition: Any) -> str:
    """The single plain-English research verdict for a disposition. Defaults to
    the not-enough-evidence verdict when the disposition is unknown/absent."""
    return _DISPOSITION_VERDICT.get(str(disposition or ""), VERDICT_SENTENCE)

# Jargon that must never appear in the plain-English executive body (Stage 7.2).
# It may still appear in the compact audit appendix. Lower-cased for scanning.
FORBIDDEN_JARGON = (
    "point-in-time-safe", "point-in-time", "survivorship-aware",
    "survivorship-free", "rank-ic", "t-statistic", "t-stat", "reconstruction",
    "recovery campaign", "overlay", "forward scale", "manual de-risk preview",
    "de-risk preview", "provider classification", "machine token",
    "data hold token", "data_hold", "keep_for_research", "rank ic",
)

ACTION_NO_TRADE = "NO TRADE — CONTINUE PAPER TRACKING"
ACTION_MANUAL_REVIEW = "MANUAL REVIEW REQUIRED"
ACTION_DATA_ATTENTION = "DATA / AGENT ATTENTION REQUIRED"
ACTION_SHADOW_READY = "SHADOW CANDIDATE READY FOR REVIEW"
ACTION_STATES = (ACTION_NO_TRADE, ACTION_MANUAL_REVIEW, ACTION_DATA_ATTENTION,
                 ACTION_SHADOW_READY)


# --------------------------------------------------------------------------- #
# WS3 — plain-English translation of machine outcomes. The raw token is shown
# ONLY in the technical appendix; the main body always uses these sentences.
# --------------------------------------------------------------------------- #
TRANSLATIONS = {
    "ALPHA_AGENT_STAGE5_DATA_HOLD":
        "No experiment ran because the required historical data is not yet "
        "available.",
    "DATA_HOLD":
        "No experiment ran because the required historical data is not yet "
        "available.",
    "REJECT_WEAK_EVIDENCE":
        "Rejected: the result was too weak to distinguish from noise.",
    "REJECT_INSTABILITY":
        "Rejected: the result did not hold up consistently across time "
        "periods.",
    "REJECT_COST_SENSITIVITY":
        "Rejected: realistic trading costs erased the result.",
    "REJECT_LEAKAGE_RISK":
        "Rejected: the result risked relying on information that would not "
        "have been available at the time.",
    "EXPERIMENT_FAILED":
        "The experiment could not complete and produced no usable result.",
    "NEED_MORE_DATA":
        "More data is needed before this idea can be judged.",
    "UNVERIFIABLE_COMPONENT":
        "Cannot yet be validated with the historical data currently "
        "available.",
    "PARTIAL_RECONSTRUCTION":
        "Only partly reproducible from the history we currently own.",
    "EXACT_RECONSTRUCTION":
        "Fully reproducible from the history we currently own.",
    "KEEP_FOR_RESEARCH":
        "Passed the first research gate; still not approved for the "
        "portfolio.",
    "NEED_MORE_EVIDENCE":
        "The current model can be neither confirmed nor rejected yet.",
    "CHAMPION_CONFIRMED_RISK_OVERLAY_REQUIRED":
        "The current model still looks genuine, but its risk needs better "
        "control.",
    "CHAMPION_CONFIRMED_NO_CHANGE":
        "The current model still looks genuine and needs no change.",
    "CHAMPION_REJECTED":
        "The current model is no longer defensible.",
    "WITHHELD_NO_ROBUST_EVIDENCE":
        "No de-risking change is recommended — the evidence is not strong "
        "enough.",
    "PREVIEW_AVAILABLE":
        "A read-only de-risking preview is available for manual review.",
}


def translate(token: Any) -> str:
    """Plain-English sentence for a machine token; unknown tokens pass through."""
    if token is None:
        return ""
    return TRANSLATIONS.get(str(token), str(token))


# --------------------------------------------------------------------------- #
# Deterministic value formatting. Every signed value carries an explicit sign
# using an ASCII "+"/"-" so the meaning never depends on colour alone (WS2).
# --------------------------------------------------------------------------- #
def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_money(value: Any, *, dash: str = "Not available") -> str:
    n = _num(value)
    if n is None:
        return dash
    return "${:,.2f}".format(n)


def fmt_signed_money(value: Any, *, dash: str = "Not available") -> str:
    n = _num(value)
    if n is None:
        return dash
    sign = "+" if n >= 0 else "-"
    return "%s$%s" % (sign, "{:,.2f}".format(abs(n)))


def fmt_pct(value: Any, *, dash: str = "Not available", signed: bool = True,
            suffix: str = "%") -> str:
    n = _num(value)
    if n is None:
        return dash
    if signed:
        sign = "+" if n >= 0 else "-"
        return "%s%s%s" % (sign, "{:.2f}".format(abs(n)), suffix)
    return "%s%s" % ("{:.2f}".format(n), suffix)


def fmt_pp(value: Any, *, dash: str = "Not available") -> str:
    return fmt_pct(value, dash=dash, signed=True, suffix=" pp")


def fmt_cash_pct(fraction: Any, *, dash: str = "Not available") -> str:
    """Plain cash-allocation percentage from a fraction (0.603 -> '60.3%').
    Cash is a level, never a change, so it carries NO leading '+'/'-' sign."""
    n = _num(fraction)
    if n is None:
        return dash
    return "{:.1f}%".format(n * 100.0)


def fmt_pp_from_return(fraction: Any, *, dash: str = "Not available") -> str:
    """Percentage-points string from a fractional return difference (0.0053 ->
    '+0.53 pp'). Relative benchmark performance is always in percentage points,
    never a bare '%'."""
    n = _num(fraction)
    if n is None:
        return dash
    return fmt_pp(n * 100.0)


def fmt_int(value: Any, *, dash: str = "0") -> str:
    n = _num(value)
    if n is None:
        return dash
    return "{:,}".format(int(round(n)))


def _fmt_stat(value: Any, *, dash: str = "n/a") -> str:
    n = _num(value)
    if n is None:
        return dash
    return "{:.2f}".format(n)


def _fmt_ret(value: Any, *, dash: str = "Not available") -> str:
    """Format a fractional return (0.12 → +12.00%) with an explicit sign."""
    n = _num(value)
    if n is None:
        return dash
    sign = "+" if n >= 0 else "-"
    return "%s%s%%" % (sign, "{:.2f}".format(abs(n) * 100.0))


def _fmt_num(value: Any, *, dash: str = "n/a") -> str:
    if value is None:
        return dash
    try:
        return "%.4f" % float(value)
    except (TypeError, ValueError):
        return str(value)


def _sign_class(value: Any) -> str:
    n = _num(value)
    if n is None:
        return "neutral"
    if n > 0:
        return "pos"
    if n < 0:
        return "neg"
    return "neutral"


# --------------------------------------------------------------------------- #
# Canonical scorecard — the SINGLE rounding/value source shared by the email,
# the observatory API payload and the UI (WS2 reconciliation). Given a
# paper-book context dict it returns raw numbers plus their canonical signed
# strings so every surface renders identical text.
# --------------------------------------------------------------------------- #
_SCORECARD_FIELDS = (
    ("nav", "Net asset value (NAV)", "money", False),
    ("daily_pnl", "Profit / loss today", "money", True),
    ("daily_return_pct", "Return today", "pct", True),
    ("cumulative_pnl", "Profit / loss since inception", "money", True),
    ("cumulative_return_pct", "Return since inception", "pct", True),
    ("drawdown_pct", "Drawdown from peak", "pct", True),
    ("spy_cumulative_pct", "SPY return since inception", "pct", True),
    ("cumulative_excess_pp", "Ahead of / behind SPY", "pp", True),
)


def _fmt_kind(value: Any, kind: str, signed: bool) -> str:
    if kind == "money":
        return fmt_signed_money(value) if signed else fmt_money(value)
    if kind == "pp":
        return fmt_pp(value)
    return fmt_pct(value, signed=signed)


def scorecard(paper_book: dict | None) -> dict:
    """Canonical portfolio scorecard: {rows, raw, formatted}. One rounding
    source for email / API / UI. Missing values render as 'Not available'."""
    pb = paper_book or {}
    rows: list[dict] = []
    formatted: dict[str, str] = {}
    raw: dict[str, Any] = {}
    for key, label, kind, signed in _SCORECARD_FIELDS:
        val = pb.get(key)
        s = _fmt_kind(val, kind, signed)
        raw[key] = val
        formatted[key] = s
        rows.append({"key": key, "label": label, "value": s,
                     "sign": _sign_class(val) if signed else "neutral"})
    return {"rows": rows, "raw": raw, "formatted": formatted}


# --------------------------------------------------------------------------- #
# Executive derivations (pure functions of the model — deterministic, testable).
# --------------------------------------------------------------------------- #
def derive_action_today(model: dict) -> tuple[str, str]:
    """Exactly one ACTION TODAY state + a plain-English reason. A HOLD is never
    presented as proof that absolute risk is acceptable."""
    if model.get("attention") or model.get("degraded"):
        return (ACTION_DATA_ATTENTION,
                "A data source or agent component needs attention before the "
                "next research cycle can be fully trusted.")
    if model.get("shadow_ready"):
        return (ACTION_SHADOW_READY,
                "A shadow research candidate has cleared its first gate and is "
                "ready for your manual review. Nothing has been promoted or "
                "traded.")
    if model.get("manual_review"):
        return (ACTION_MANUAL_REVIEW,
                "A portfolio-level check wants a human look. No automated "
                "action has been or will be taken.")
    return (ACTION_NO_TRADE,
            "No rebalance is due and paper tracking simply continues. This is "
            "not a statement that the portfolio's absolute risk is acceptable "
            "— only that no action is triggered today.")


def _money_verb(pnl: Any) -> str:
    n = _num(pnl)
    if n is None:
        return "no reported"
    if n < 0:
        return "lost"
    return "made"  # >= 0: "made money … +$0.00" reads correctly at break-even


def _money_direction_clause(pnl: Any, ret_pct: Any) -> str | None:
    """A single period-correct money clause, e.g. 'down $443.45 (-0.45%)'. The
    dollar magnitude carries no sign (the word supplies direction); the percent
    keeps its explicit sign. The dollar and the percent ALWAYS describe the SAME
    period, so a today figure can never be paired with a since-inception figure
    in one statement (Stage 7.2 defect #1)."""
    n = _num(pnl)
    if n is None:
        return None
    if n < 0:
        word = "down"
    elif n > 0:
        word = "up"
    else:
        word = "flat at"
    return "%s %s (%s)" % (word, fmt_money(abs(n)), fmt_pct(ret_pct))


def derive_bottom_line(model: dict) -> list[str]:
    """<=4 short plain-English sentences. The money sentence pairs each dollar
    figure with the percent from the SAME period (today with today, since-launch
    with since-launch) so periods are never mixed in one metric statement. The
    research VERDICT is NOT stated here — it appears exactly once, in the
    research-progress section."""
    sc = (model.get("scorecard") or {}).get("raw") or {}
    sentences: list[str] = []

    today = _money_direction_clause(sc.get("daily_pnl"),
                                    sc.get("daily_return_pct"))
    since = _money_direction_clause(sc.get("cumulative_pnl"),
                                    sc.get("cumulative_return_pct"))
    if today and since:
        sentences.append("The paper portfolio is %s today and %s since launch."
                         % (today, since))
    elif since:
        sentences.append("The paper portfolio is %s since launch." % since)
    elif today:
        sentences.append("The paper portfolio is %s today." % today)
    else:
        sentences.append("Portfolio valuation is not available in this report.")

    spy = _num(sc.get("spy_cumulative_pct"))
    exc = _num(sc.get("cumulative_excess_pp"))
    if spy is not None and exc is not None:
        spy_word = "down" if spy < 0 else ("up" if spy > 0 else "flat")
        rel = "better" if exc >= 0 else "worse"
        cum_pnl = _num(sc.get("cumulative_pnl"))
        tail = (", though it is still losing money"
                if (cum_pnl is not None and cum_pnl < 0 and exc >= 0) else "")
        sentences.append(
            "SPY is %s %s over the same period, so the portfolio has performed "
            "%s percentage points %s%s." % (
                spy_word, fmt_pct(abs(spy), signed=False),
                fmt_pct(abs(exc), signed=False, suffix=""), rel, tail))
    elif exc is not None:
        rel = "ahead of" if exc >= 0 else "behind"
        sentences.append("Versus SPY the portfolio is %s by %s percentage "
                         "points." % (rel, fmt_pct(abs(exc), signed=False,
                                                   suffix="")))

    action, _reason = derive_action_today(model)
    if action == ACTION_NO_TRADE:
        sentences.append("No action is required today; the paper portfolio is "
                         "unchanged.")
    else:
        sentences.append("Action needed today: %s." % action)
    return sentences[:4]


def what_changed(model: dict) -> list[str]:
    """Plain-English deltas versus the previous report, with meaningless zero
    changes suppressed. When every material figure is below the display
    threshold and nothing else moved, a single plain sentence is returned
    instead of a wall of '$0.00 / 0.00% / 0.00 pp' lines (Stage 7.2 defect #6)."""
    cur = (model.get("scorecard") or {}).get("raw") or {}
    prior = model.get("prior") or {}
    if not prior:
        return ["This is the first report on record, so there is no prior "
                "report to compare against."]
    prev = prior.get("scorecard") or {}
    out: list[str] = []

    def _delta_money(key, label):
        a, b = _num(cur.get(key)), _num(prev.get(key))
        if a is None or b is None:
            return
        d = a - b
        if abs(round(d, 2)) < 0.005:  # rounds to $0.00 -> not shown
            return
        out.append("%s changed by %s since the last report (now %s)." % (
            label, fmt_signed_money(d), fmt_signed_money(a)))

    def _delta_pct(key, label, pp=False):
        a, b = _num(cur.get(key)), _num(prev.get(key))
        if a is None or b is None:
            return
        d = a - b
        if abs(round(d, 2)) < 0.005:  # rounds to 0.00% / 0.00 pp -> not shown
            return
        f = fmt_pp if pp else fmt_pct
        out.append("%s moved %s (now %s)." % (label, f(d), f(a)))

    _delta_money("nav", "NAV")
    _delta_pct("cumulative_return_pct", "Return since inception")
    _delta_pct("cumulative_excess_pp", "Standing versus SPY", pp=True)

    prev_disp = prior.get("disposition")
    cur_disp = (model.get("recovery_readiness") or {}).get("disposition")
    if cur_disp and prev_disp and cur_disp != prev_disp:
        # Plain-English only — never print the raw disposition token in the body.
        out.append("The research verdict changed to: %s." % translate(cur_disp))

    if not out:
        return ["No meaningful portfolio or research change occurred since the "
                "previous report."]
    return out


# --------------------------------------------------------------------------- #
# Stage 9 tournament changes (WS10): reports flag ONLY meaningful tournament
# changes (a new retained candidate, a rejection, a newly-activated shadow book,
# a forward-evidence milestone, a new blocker). An unchanged leaderboard emits
# nothing, so a report never repeats a static tournament standing. The lines are
# read from ``model['tournament_changes']`` so a caller with no tournament state
# renders exactly as before (no new noise).
# --------------------------------------------------------------------------- #
_TOURNAMENT_NONMATERIAL = (
    "No meaningful tournament change", "No tournament activity")


def material_tournament_changes(lines) -> list:
    """Filter a raw change feed down to material lines only."""
    out = []
    for ln in (lines or []):
        s = str(ln).strip()
        if not s or any(s.startswith(p) for p in _TOURNAMENT_NONMATERIAL):
            continue
        out.append(s)
    return out


def tournament_change_lines(model: dict) -> list:
    """Material tournament changes for the report (empty unless the model carries
    real retained/rejected/shadow-book changes)."""
    return material_tournament_changes((model or {}).get("tournament_changes"))


def _tournament_changes_default(config_path):
    from . import tournament as _tt
    p = config_path
    if p is None:
        from pathlib import Path as _P
        p = str(_P(__file__).resolve().parents[1] / "configs" / "alpha_agent"
                / "stage9_tournament.json")
    return _tt.meaningful_tournament_changes(config_path=p)


def attach_tournament_changes(model: dict, *, config_path=None,
                              loader=None) -> dict:
    """Populate ``model['tournament_changes']`` from the read-only tournament
    change feed (best-effort; never raises). The production report path calls
    this so the report surfaces only meaningful tournament changes."""
    try:
        fn = loader or (lambda: _tournament_changes_default(config_path))
        model["tournament_changes"] = list(fn() or [])
    except Exception:  # noqa: BLE001 - reporting must never break on research state
        pass
    return model


# --------------------------------------------------------------------------- #
# Canonical research-decision reconciliation (Stage 7.2 defect #3). Exactly one
# summary, with counts that add up. Stage 7 recovery ideas that were EVALUATED
# are kept strictly separate from Stage 5 studies that COULD NOT RUN for lack of
# data — the two are never conflated (defect / test #6).
# --------------------------------------------------------------------------- #
_RECOVERY_CATEGORY = {
    "REJECT_WEAK_EVIDENCE": "rejected as no better than chance",
    "REJECT_INSTABILITY": "rejected for not holding up over time",
    "REJECT_COST_SENSITIVITY": "rejected because trading costs erased the edge",
    "REJECT_LEAKAGE_RISK": "rejected for relying on information that was not "
                           "available at the time",
    "KEEP_FOR_RESEARCH": "kept for further study (not used and not promoted)",
    "DATA_HOLD": "could not run because the required data was missing",
    "ALPHA_AGENT_STAGE5_DATA_HOLD":
        "could not run because the required data was missing",
    "EXPERIMENT_FAILED": "could not complete and produced no usable result",
    "NEED_MORE_DATA": "needs more data before it can be judged",
}
_RETAIN_TOKENS = {"KEEP_FOR_RESEARCH"}
_COULD_NOT_RUN_TOKENS = {"DATA_HOLD", "ALPHA_AGENT_STAGE5_DATA_HOLD",
                         "EXPERIMENT_FAILED", "NEED_MORE_DATA"}


def research_decision_reconciliation(recovery: dict | None,
                                     stage5_could_not_run: Any = None) -> dict:
    """One canonical, exactly-reconciling research-decision summary from the
    latest recovery evaluation. Every category is described in plain English and
    NO raw machine token is ever placed in a category phrase (unknown tokens fall
    back to a generic phrase). ``evaluated`` == sum of every category count when
    the engine emitted one decision per idea; both are exposed so a test can
    prove reconciliation. Stage 5 'could not run' studies are reported on a
    SEPARATE field, never added into ``evaluated``."""
    rec = recovery or {}
    counts = rec.get("campaign_decision_counts") or {}
    rejected = retained = could_not_run = other = 0
    categories: list[dict] = []
    for token in sorted(counts):
        c = int(_num(counts.get(token)) or 0)
        if c <= 0:
            continue
        phrase = _RECOVERY_CATEGORY.get(token)
        if phrase is None:
            other += c
            phrase = "recorded under another outcome"
        elif token in _RETAIN_TOKENS:
            retained += c
        elif token in _COULD_NOT_RUN_TOKENS:
            could_not_run += c
        else:
            rejected += c
        categories.append({"count": c, "token": token, "phrase": phrase})
    accounted = rejected + retained + could_not_run + other
    # Canonical count (Stage 8 count-consistency fix): an EVALUATED recovery idea
    # is any recorded decision, so the outcome categories partition the evaluated
    # set exactly and 'evaluated' == 'accounted' ALWAYS reconciles. 'completed'
    # is the narrower count of experiments that actually ran
    # (campaign_experiments); it is surfaced separately and is NEVER used as the
    # headline 'evaluated' number (the prior bug reported 'completed' as
    # 'evaluated' while the breakdown summed the full partition).
    completed = _num(rec.get("campaign_experiments"))
    completed = int(completed) if completed is not None else accounted
    evaluated = accounted
    s5 = _num(stage5_could_not_run)
    return {
        "evaluated": evaluated,
        "completed": completed,
        "rejected": rejected,
        "retained": retained,
        "could_not_run": could_not_run,
        "other": other,
        "promoted": 0,               # Stage 7 promotes nothing, ever.
        "accounted": accounted,
        "reconciles": bool(evaluated == accounted),
        "categories": categories,
        "stage5_could_not_run": int(s5) if s5 is not None else None,
    }


def research_progress(model: dict) -> dict:
    """Plain-English research-progress block. States the verdict EXACTLY ONCE
    (as the first line) and then the reconciling breakdown. Never emits a bare
    '3 experiment result(s)' style line without naming the category."""
    rec = model.get("recovery_readiness") or {}
    recon = research_decision_reconciliation(rec, model.get("stage5_could_not_run"))
    verdict = verdict_sentence(rec.get("disposition"))
    lines: list[str] = [verdict]
    ev = recon["evaluated"]
    if ev > 0:
        lines.append("%s recovery idea(s) were evaluated and none was strong "
                     "enough to use." % fmt_int(ev))
        parts = ["%s %s" % (fmt_int(c["count"]), c["phrase"])
                 for c in recon["categories"]]
        if parts:
            lines.append("Of these: " + "; ".join(parts) + ".")
        lines.append("No model was promoted.")
    else:
        lines.append("No new recovery ideas were evaluated this cycle, and no "
                     "model was promoted.")
    s5 = recon.get("stage5_could_not_run")
    if s5:
        lines.append("Separately, a different experiment engine had %s "
                     "study(ies) that could not run because the required "
                     "historical data is missing." % fmt_int(s5))
    return {"verdict": verdict, "lines": lines, "reconciliation": recon}


# Plain-English shadow names — deliberately free of the word "overlay" and other
# jargon so they read cleanly in the executive body.
_SHADOW_PLAIN = {
    "CURRENT_CONTROL": "Current book",
    "MARKET_REGIME_CASH_OVERLAY": "Cash-in-downturns shadow",
    "PORTFOLIO_VOL_TARGET_20": "Lower-risk shadow",
}


def risk_experiments(model: dict) -> dict:
    """Read-only risk-shadow block: a three-row table (return / cash / vs SPY in
    percentage points / days) plus a plain-English interpretation. Realized
    volatility is explained from the observation count, never shown as a bare
    'Not available'. Cash is a level and carries no +/- sign."""
    fs = model.get("risk_and_shadow") or {}
    shadows = fs.get("shadows") or []
    rows: list[dict] = []
    max_obs = 0
    lowrisk = None
    for s in shadows:
        obs = int(_num(s.get("observations")) or 0)
        max_obs = max(max_obs, obs)
        rows.append({
            "name": _SHADOW_PLAIN.get(s.get("overlay"), s.get("overlay")),
            "return": _fmt_ret(s.get("cumulative_return")),
            "cash": fmt_cash_pct(s.get("cash")),
            "vs_spy": fmt_pp_from_return(s.get("spy_excess")),
            "obs": fmt_int(obs),
        })
        if s.get("overlay") == "PORTFOLIO_VOL_TARGET_20":
            lowrisk = s
    lines: list[str] = []
    if rows:
        if lowrisk is not None and lowrisk.get("cash") is not None:
            lines.append("The lower-risk shadow lost less mainly because it held "
                         "about %s cash." % fmt_cash_pct(lowrisk.get("cash")))
        if max_obs < 20:
            lines.append("Only %s trading day(s) are available so far, so "
                         "realized volatility cannot be measured yet and it is "
                         "too early to conclude that any approach is better."
                         % fmt_int(max_obs))
        lines.append("No shadow portfolio changes the active paper portfolio.")
    return {"rows": rows[:3], "lines": lines, "observations": max_obs}


# NOT-READY readiness labels -> jargon-free plain sentences for the body.
_READINESS_PLAIN = {
    "Point-in-time fundamentals":
        "We lack historical financial-statement data showing when each figure "
        "first became public, so the fundamental part of the model cannot be "
        "safely tested without risking the use of information that was not "
        "available at the time.",
    "Earnings history":
        "There is not enough earnings-announcement history with reliable timing "
        "to test earnings-driven ideas.",
    "Historical sector classifications":
        "Historical sector groupings are missing, so sector-balanced ideas "
        "cannot be tested yet.",
    "Price history":
        "Historical price data is incomplete.",
    "Historical membership":
        "Historical index-membership data is incomplete.",
}


def data_system_issues(model: dict) -> list[str]:
    """Exception-only list of issues currently preventing research, in plain
    English. Empty when nothing is wrong."""
    issues: list[str] = []
    for r in (model.get("historical_readiness_summary") or []):
        if str(r.get("status", "")).upper().startswith("READY"):
            continue
        label = str(r.get("label", ""))
        issues.append(_READINESS_PLAIN.get(
            label, ("%s is not available yet." % label) if label
            else "A required historical dataset is not available yet."))
    nr = model.get("news_rss") or {}
    failed = [str(f) for f in (nr.get("failed_feeds") or []) if str(f).strip()]
    if failed:
        issues.append("Some news sources are currently unavailable: %s."
                      % ", ".join(failed))
    return issues


def safety_lines(model: dict) -> list[tuple[str, str]]:
    """Canonical, clearly-separated safety facts. The automatic-schedule state is
    the resolved value from the real task states, never hardcoded to On."""
    return [
        ("Automatic research schedule",
         str(model.get("schedule_status") or SCHEDULE_UNVERIFIED)),
        ("Trading automation", "Off"),
        ("Broker execution", "Off"),
        ("Portfolio", "Paper only"),
    ]


_PORTFOLIO_ROW_KEYS = ("nav", "daily_pnl", "cumulative_pnl",
                       "spy_cumulative_pct", "cumulative_excess_pp")


def portfolio_rows(model: dict) -> list[dict]:
    """The five headline scorecard rows for the compact email table. Values come
    straight from the canonical scorecard so email/API/UI render identical
    strings."""
    sc = model.get("scorecard") or scorecard(model.get("paper_book"))
    by = {r.get("key"): r for r in sc.get("rows", [])}
    return [by[k] for k in _PORTFOLIO_ROW_KEYS if k in by]


def audit_appendix_items(model: dict) -> list[tuple[str, str]]:
    """Exactly the five compact audit lines allowed after the separator. No local
    file paths, no per-stage run-id list, no provider classification, no tokens
    beyond a single latest-research-run reference."""
    rec = model.get("recovery_readiness") or {}
    sh = model.get("source_agent_health") or {}
    nr = model.get("news_rss") or {}
    ledgers = "unchanged" if sh.get("ledgers_unchanged", True) else "CHANGED"
    feeds = "%s of %s healthy" % (fmt_int(nr.get("healthy")),
                                  fmt_int(nr.get("enabled")))
    run_ref = rec.get("run_id") or "none on record"
    age = sh.get("stage7_age_note")
    latest_run = run_ref if not age else "%s (%s)" % (run_ref, age)
    sched = str(model.get("schedule_status") or SCHEDULE_UNVERIFIED)
    return [
        ("Generated", str(model.get("generated_at", "") or "n/a")),
        ("Market data through",
         str(model.get("market_data_through") or "Not available")),
        ("Latest research run", latest_run),
        ("Data quality", "operational ledgers %s; news feeds %s" %
         (ledgers, feeds)),
        ("Safety", "automatic schedule %s; trading automation off; broker "
         "execution off; paper only" % sched),
    ]


# Marker that separates the plain-English executive body from the audit
# appendix. Body-only scans (jargon / machine tokens) split on this marker.
BODY_APPENDIX_SEPARATOR = "AUDIT APPENDIX"


# --------------------------------------------------------------------------- #
# HTML rendering.
# --------------------------------------------------------------------------- #
def _css() -> str:
    """Compact, mobile-safe, dark-mode-tolerant CSS. Meaning never depends on
    colour alone: every signed number keeps an explicit +/- and every status
    carries a word (Off / On / yes / no)."""
    css = """
body { margin:0; padding:0; background:#eef2f7;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
  color:#1b2733; -webkit-text-size-adjust:100%%; }
.wrap { max-width:640px; margin:0 auto; background:#ffffff; }
.header { background:%(navy)s; color:#ffffff; padding:20px 22px; }
.header h1 { margin:0; font-size:19px; line-height:1.25; letter-spacing:0.2px; }
.header .sub { margin:5px 0 0; font-size:12px; color:#cfe0f5; }
.flags { padding:10px 22px 2px; background:#0b2545; }
.flag { display:inline-block; font-size:11px; font-weight:700; color:#ffffff;
  background:#12406e; border:1px solid #2f6fb0; padding:4px 9px;
  border-radius:6px; margin:0 6px 8px 0; letter-spacing:0.3px; }
.flag.off { background:#3a1d1d; border-color:#7a3b3b; }
.section { padding:16px 22px; border-top:1px solid #e7edf5; }
.section h2 { font-size:13px; text-transform:uppercase; letter-spacing:0.5px;
  color:%(navy)s; margin:0 0 9px; }
.section p { font-size:14px; line-height:1.5; margin:0 0 6px; }
.bottomline { background:#f4f8ff; }
.bottomline p { font-size:15px; line-height:1.55; margin:0 0 7px; }
.action { font-size:15px; font-weight:800; padding:10px 12px; border-radius:8px;
  display:block; }
.action.hold { background:#e6f4ea; color:#14622c; border:1px solid #ace0bd; }
.action.review { background:#fff3d6; color:#7a4f00; border:1px solid #f0d089; }
.action.attention { background:#fdeaea; color:#a01d13; border:1px solid #f4a9a4; }
.action.shadow { background:#e7f0fb; color:#0b4a9e; border:1px solid #a9cbf5; }
.action .why { display:block; font-size:12px; font-weight:500; margin-top:5px; }
table.kv { width:100%%; border-collapse:collapse; font-size:13px; }
table.kv td, table.kv th { padding:6px 6px; border-bottom:1px solid #eef2f8;
  vertical-align:top; text-align:left; }
table.kv td.k, table.kv th { color:#5b6b7c; font-weight:600; }
table.kv td.v { color:#1b2733; font-weight:700; text-align:right;
  white-space:nowrap; }
.pos { color:#1a7f37; }
.neg { color:#b42318; }
.neutral { color:#5b6b7c; }
ul.clean { margin:2px 0 0; padding-left:18px; font-size:14px; }
ul.clean li { margin:5px 0; line-height:1.5; }
.safety { list-style:none; margin:8px 0 0; padding:0; font-size:13px; }
.safety li { margin:4px 0; }
.safety b { color:#5b6b7c; font-weight:600; }
.muted { color:#5b6b7c; font-size:12px; }
.appendix { background:#f7f9fc; }
.appendix h2 { color:#5b6b7c; }
.foot { padding:14px 22px; background:#f2f5fa; color:#5b6b7c; font-size:11px;
  border-top:1px solid #e4eaf2; }
@media (max-width:480px) {
  .header, .flags, .section, .foot { padding-left:16px; padding-right:16px; }
  table.kv td, table.kv th { font-size:12px; }
}
@media (prefers-color-scheme: dark) {
  body { background:#0d1117; color:#dfe6ef; }
  .wrap { background:#12161d; }
  .section { border-top-color:#232a34; }
  .bottomline { background:#141d2b; }
  .section p, ul.clean, .safety { color:#dfe6ef; }
  .section h2 { color:#7fb0ff; }
  table.kv td, table.kv th { border-bottom-color:#232a34; }
  table.kv td.k, table.kv th, .muted, .safety b { color:#93a1b5; }
  table.kv td.v { color:#eef2f8; }
  .pos { color:#3fb950; } .neg { color:#f85149; }
  .appendix { background:#0f141b; } .foot { background:#0f141b; color:#93a1b5; }
  .action.hold { background:#122015; color:#7ee2a0; border-color:#1f5e34; }
  .action.review { background:#241d0c; color:#e9c46a; border-color:#6b5320; }
  .action.attention { background:#25120f; color:#ff8a80; border-color:#6b2b26; }
  .action.shadow { background:#0f1c2e; color:#8ec2ff; border-color:#204a7a; }
}
"""
    return css % {"navy": _HEADER_NAVY}


_BADGE_KIND = {"safe": "safe", "info": "info", "warn": "warn", "crit": "crit"}


def _kv_row(k: str, v: str, cls: str = "") -> str:
    vv = html_escape(v)
    if cls:
        vv = '<span class="%s">%s</span>' % (html_escape(cls), vv)
    return '<tr><td class="k">%s</td><td class="v">%s</td></tr>' % (
        html_escape(k), vv)


def _action_css(action: str) -> str:
    return {ACTION_NO_TRADE: "hold", ACTION_MANUAL_REVIEW: "review",
            ACTION_DATA_ATTENTION: "attention",
            ACTION_SHADOW_READY: "shadow"}.get(action, "hold")


def render_html(model: dict) -> str:
    """Render the deterministic COMPACT executive HTML brief from *model*.

    Six plain-English sections — Bottom line / Your action today / Portfolio
    today / Research progress / Risk experiments / Data and system issues —
    followed by a clear separator and a five-line audit appendix. The email
    carries no local file path, no per-stage run-id list and no provider
    internals; those live in the API/UI observatory instead."""
    e = html_escape
    m = model
    date = m.get("cycle_date", "")
    label = m.get("cycle_label", "")
    parts: list[str] = []
    parts.append("<!doctype html>")
    parts.append('<html lang="en"><head><meta charset="utf-8">')
    parts.append('<meta name="viewport" content="width=device-width,'
                 'initial-scale=1">')
    parts.append('<meta name="color-scheme" content="light dark">')
    parts.append("<title>%s</title>" % e(m.get("subject", "Alpha Agent")))
    parts.append("<style>%s</style></head><body><div class=\"wrap\">" % _css())

    # One clear headline.
    title = m.get("report_title") or "Alpha Agent Executive Research Brief"
    parts.append('<div class="header"><h1>%s</h1>'
                 '<p class="sub">%s &middot; %s cycle &middot; generated %s</p>'
                 '</div>' % (e(title), e(date), e(label),
                             e(m.get("generated_at", ""))))

    # Safety flag band. The schedule state is the RESOLVED value (Off / On /
    # Not verified) from the real task states — never a hardcoded ON. Each flag
    # carries an explicit word so meaning survives without colour.
    parts.append('<div class="flags">')
    parts.append('<span class="flag off">%s</span>' %
                 e(schedule_flag_text(m.get("schedule_status"))))
    for f in m.get("status_flags", STATUS_FLAGS):
        cls = "flag off" if "OFF" in f else "flag"
        parts.append('<span class="%s">%s</span>' % (cls, e(f)))
    parts.append('</div>')

    # 1. BOTTOM LINE.
    parts.append('<div class="section bottomline"><h2>1. Bottom line</h2>')
    for s in derive_bottom_line(m):
        parts.append('<p>%s</p>' % e(s))
    parts.append('</div>')

    # 2. YOUR ACTION TODAY — one action card.
    action, reason = derive_action_today(m)
    parts.append('<div class="section"><h2>2. Your action today</h2>'
                 '<div class="action %s">%s<span class="why">%s</span></div>'
                 '</div>' % (_action_css(action), e(action), e(reason)))

    # 3. PORTFOLIO TODAY — one compact five-row table + a change note (zero
    # changes collapse to a single plain sentence).
    parts.append('<div class="section"><h2>3. Portfolio today</h2>'
                 '<table class="kv">')
    for row in portfolio_rows(m):
        parts.append(_kv_row(row.get("label", ""), row.get("value", ""),
                             row.get("sign", "")))
    parts.append('</table>')
    parts.append('<p class="muted" style="margin-top:8px;">%s</p>' %
                 e(" ".join(what_changed(m))))
    parts.append('</div>')

    # 4. RESEARCH PROGRESS — the verdict is stated here exactly once.
    rp = research_progress(m)
    parts.append('<div class="section"><h2>4. Research progress</h2>')
    for ln in rp["lines"]:
        parts.append('<p>%s</p>' % e(ln))
    tchanges = tournament_change_lines(m)
    if tchanges:
        parts.append('<p class="muted" style="margin-top:6px;">'
                     '<b>Alpha tournament changes:</b></p><ul class="clean">')
        for ln in tchanges:
            parts.append('<li>%s</li>' % e(ln))
        parts.append('</ul>')
    parts.append('</div>')

    # 5. RISK EXPERIMENTS — one three-row shadow table + plain interpretation.
    rx = risk_experiments(m)
    parts.append('<div class="section"><h2>5. Risk experiments</h2>')
    if rx["rows"]:
        parts.append('<table class="kv"><tr><th>Shadow</th><th>Return</th>'
                     '<th>Cash</th><th>vs SPY</th><th>Days</th></tr>')
        for r in rx["rows"]:
            parts.append('<tr><td class="k">%s</td><td class="v">%s</td>'
                         '<td class="v">%s</td><td class="v">%s</td>'
                         '<td class="v">%s</td></tr>' % (
                             e(r["name"]), e(r["return"]), e(r["cash"]),
                             e(r["vs_spy"]), e(r["obs"])))
        parts.append('</table>')
    else:
        parts.append('<p class="muted">No completed shadow observations yet; '
                     'forward tracking begins at the next eligible close.</p>')
    for ln in rx["lines"]:
        parts.append('<p>%s</p>' % e(ln))
    parts.append('</div>')

    # 6. DATA AND SYSTEM ISSUES — exception-only, plus the safety state.
    parts.append('<div class="section"><h2>6. Data and system issues</h2>')
    issues = data_system_issues(m)
    if issues:
        parts.append('<ul class="clean">')
        for it in issues:
            parts.append('<li>%s</li>' % e(it))
        parts.append('</ul>')
    else:
        parts.append('<p>No data or system issues to report.</p>')
    parts.append('<ul class="safety">')
    for lbl, val in safety_lines(m):
        parts.append('<li><b>%s:</b> %s</li>' % (e(lbl), e(val)))
    parts.append('</ul>')
    parts.append('</div>')

    # Clear separator + compact audit appendix (five lines only; no paths).
    parts.append('<div class="section appendix"><h2>%s</h2>'
                 '<table class="kv">' % e(BODY_APPENDIX_SEPARATOR))
    for lbl, val in audit_appendix_items(m):
        parts.append(_kv_row(lbl, val))
    parts.append('</table></div>')

    parts.append('<div class="foot">Alpha Agent research automation only '
                 '&middot; paper portfolio only &middot; no live orders '
                 '&middot; trading automation off. Rendered deterministically '
                 'at zero LLM-token cost.</div>')
    parts.append('</div></body></html>')
    return "\n".join(parts)


# --------------------------------------------------------------------------- #
# Plain-text rendering (deterministic mirror of the HTML executive brief).
# --------------------------------------------------------------------------- #
def render_text(model: dict) -> str:
    """Complete plain-text mirror of the compact HTML brief (same six sections,
    same separator, same five-line appendix)."""
    m = model
    lines: list[str] = []
    title = m.get("report_title") or "Alpha Agent Executive Research Brief"
    lines.append(title)
    lines.append("%s | %s cycle | generated %s" % (
        m.get("cycle_date", ""), m.get("cycle_label", ""),
        m.get("generated_at", "")))
    lines.append("=" * 60)
    lines.append(schedule_flag_text(m.get("schedule_status")))
    lines.append(" | ".join(m.get("status_flags", STATUS_FLAGS)))
    lines.append("")

    lines.append("1. BOTTOM LINE")
    for s in derive_bottom_line(m):
        lines.append("  " + s)
    lines.append("")

    action, reason = derive_action_today(m)
    lines.append("2. YOUR ACTION TODAY")
    lines.append("  >> %s" % action)
    lines.append("     %s" % reason)
    lines.append("")

    lines.append("3. PORTFOLIO TODAY")
    for row in portfolio_rows(m):
        lines.append("  %-30s : %s" % (row.get("label", ""),
                                       row.get("value", "")))
    lines.append("  " + " ".join(what_changed(m)))
    lines.append("")

    lines.append("4. RESEARCH PROGRESS")
    for ln in research_progress(m)["lines"]:
        lines.append("  " + ln)
    tchanges = tournament_change_lines(m)
    if tchanges:
        lines.append("  Alpha tournament changes:")
        for ln in tchanges:
            lines.append("    - " + ln)
    lines.append("")

    lines.append("5. RISK EXPERIMENTS")
    rx = risk_experiments(m)
    if rx["rows"]:
        lines.append("  %-22s %-10s %-8s %-10s %s" % (
            "Shadow", "Return", "Cash", "vs SPY", "Days"))
        for r in rx["rows"]:
            lines.append("  %-22s %-10s %-8s %-10s %s" % (
                str(r["name"])[:22], r["return"], r["cash"], r["vs_spy"],
                r["obs"]))
    else:
        lines.append("  No completed shadow observations yet.")
    for ln in rx["lines"]:
        lines.append("  " + ln)
    lines.append("")

    lines.append("6. DATA AND SYSTEM ISSUES")
    issues = data_system_issues(m)
    if issues:
        for it in issues:
            lines.append("  - " + it)
    else:
        lines.append("  No data or system issues to report.")
    for lbl, val in safety_lines(m):
        lines.append("  %s: %s" % (lbl, val))
    lines.append("")

    lines.append(BODY_APPENDIX_SEPARATOR)
    lines.append("-" * 60)
    for lbl, val in audit_appendix_items(m):
        lines.append("  %-22s : %s" % (lbl, val))
    lines.append("")
    return "\n".join(lines)


def report_manifest(model: dict, html_body: str, text_body: str) -> dict:
    """Immutable, deterministic manifest describing the rendered report.

    Contains NO secret and NO raw article body — only content hashes, the
    reconciled numeric model and file identities.
    """
    html_sha = hashlib.sha256(html_body.encode("utf-8")).hexdigest()
    text_sha = hashlib.sha256(text_body.encode("utf-8")).hexdigest()
    action, _reason = derive_action_today(model)
    return {
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "cycle_label": model.get("cycle_label"),
        "cycle_date": model.get("cycle_date"),
        "subject": model.get("subject"),
        "degraded": bool(model.get("degraded")),
        "generated_at": model.get("generated_at"),
        "html_sha256": html_sha,
        "text_sha256": text_sha,
        "model_sha256": hashlib.sha256(
            canonical_json(model).encode("utf-8")).hexdigest(),
        "action_today": action,
        "status_flags": list(model.get("status_flags", STATUS_FLAGS)),
        "schedule_status": str(model.get("schedule_status")
                               or SCHEDULE_UNVERIFIED),
        "market_data_through": model.get("market_data_through"),
        "research_reconciliation": research_decision_reconciliation(
            model.get("recovery_readiness"), model.get("stage5_could_not_run")),
        "scorecard": (model.get("scorecard")
                      or scorecard(model.get("paper_book"))).get("formatted"),
        "kpis": model.get("kpis"),
        "paper_book": model.get("paper_book"),
        "news_rss": model.get("news_rss"),
        "llm": model.get("llm"),
        "evidence": model.get("evidence"),
        "experiment": model.get("experiment"),
        "historical_readiness": model.get("historical_readiness"),
        "recovery_readiness": model.get("recovery_readiness"),
        "forward_shadows": (model.get("risk_and_shadow") or {}).get("shadows"),
        "email_llm_tokens": 0,
    }
