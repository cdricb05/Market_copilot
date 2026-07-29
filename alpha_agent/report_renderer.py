"""
alpha_agent.report_renderer — deterministic friendly report (HTML + text).

Renders the Stage 4 research report from a fully-computed *model* dict. Every
numeric value originates in deterministic Python upstream; this module performs
NO LLM call, opens NO network/DB connection, reads NO clock and introduces NO
randomness. Identical models render byte-identical output. All dynamic values
are HTML-escaped. The email body is produced here at zero LLM-token cost.

The report structure (matching the approved contract):
  1. Alpha Agent header            7. Paper-book context
  2. Status badges                 8. News/RSS coverage
  3. Executive summary             9. LLM usage and safety
  4. KPI cards                    10. Current operating action
  5. Material events              11. Run IDs and local evidence paths
  6. Research conclusions
"""
from __future__ import annotations

import hashlib
from typing import Any

from .runtime_contracts import canonical_json, html_escape

REPORT_SCHEMA_VERSION = "1.0.0"

_HEADER_NAVY = "#0b2545"
_HEADER_NAVY_2 = "#12345c"
_ACCENT = "#1f6feb"


# --------------------------------------------------------------------------- #
# Deterministic value formatting.
# --------------------------------------------------------------------------- #
def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_money(value: Any, *, dash: str = "n/a") -> str:
    n = _num(value)
    if n is None:
        return dash
    return "${:,.2f}".format(n)


def fmt_signed_money(value: Any, *, dash: str = "n/a") -> str:
    n = _num(value)
    if n is None:
        return dash
    sign = "+" if n >= 0 else "−"
    return "%s$%s" % (sign, "{:,.2f}".format(abs(n)))


def fmt_pct(value: Any, *, dash: str = "n/a", signed: bool = True,
            suffix: str = "%") -> str:
    n = _num(value)
    if n is None:
        return dash
    if signed:
        sign = "+" if n >= 0 else "−"
        return "%s%s%s" % (sign, "{:.2f}".format(abs(n)), suffix)
    return "%s%s" % ("{:.2f}".format(n), suffix)


def fmt_pp(value: Any, *, dash: str = "n/a") -> str:
    return fmt_pct(value, dash=dash, signed=True, suffix=" pp")


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


def _fmt_ret(value: Any, *, dash: str = "n/a") -> str:
    """Format a fractional return (0.12 → +12.00%)."""
    n = _num(value)
    if n is None:
        return dash
    sign = "+" if n >= 0 else "−"
    return "%s%s%%" % (sign, "{:.2f}".format(abs(n) * 100.0))


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
# HTML rendering.
# --------------------------------------------------------------------------- #
def _css() -> str:
    css = """
body { margin:0; padding:0; background:#eef2f7;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
  color:#1b2733; }
.wrap { max-width:820px; margin:0 auto; background:#ffffff; }
.header { background:linear-gradient(135deg,%(navy)s 0%%,%(navy2)s 100%%);
  color:#ffffff; padding:22px 28px; }
.header h1 { margin:0; font-size:20px; letter-spacing:0.3px; }
.header .sub { margin:4px 0 0; font-size:13px; color:#cfe0f5; }
.badges { padding:14px 28px 4px; }
.badge { display:inline-block; font-size:11px; font-weight:700;
  padding:4px 10px; border-radius:12px; margin:0 6px 8px 0; letter-spacing:0.4px; }
.badge.safe { background:#e6f4ea; color:#1a7f37; border:1px solid #ace0bd; }
.badge.info { background:#e7f0fb; color:#0b4a9e; border:1px solid #a9cbf5; }
.badge.warn { background:#fbece6; color:#9a3b12; border:1px solid #f3c2a8; }
.badge.crit { background:#fdeaea; color:#b42318; border:1px solid #f4a9a4; }
.section { padding:16px 28px; border-top:1px solid #eef2f7; }
.section h2 { font-size:14px; text-transform:uppercase; letter-spacing:0.6px;
  color:%(navy)s; margin:0 0 10px; }
.kpis { display:flex; flex-wrap:wrap; gap:10px; }
.kpi { flex:1 1 120px; min-width:120px; background:#f7f9fc;
  border:1px solid #e4eaf2; border-radius:8px; padding:10px 12px; }
.kpi .v { font-size:20px; font-weight:700; color:%(navy)s; }
.kpi .l { font-size:11px; color:#5b6b7c; text-transform:uppercase;
  letter-spacing:0.4px; margin-top:2px; }
table.kv { width:100%%; border-collapse:collapse; font-size:13px; }
table.kv td { padding:5px 8px; border-bottom:1px solid #f0f3f8; vertical-align:top; }
table.kv td.k { color:#5b6b7c; width:46%%; }
table.kv td.v { color:#1b2733; font-weight:600; }
.pos { color:#1a7f37; }
.neg { color:#b42318; }
.neutral { color:#5b6b7c; }
ul.clean { margin:4px 0 0; padding-left:18px; font-size:13px; }
ul.clean li { margin:3px 0; }
.evt { border:1px solid #e4eaf2; border-radius:8px; padding:10px 12px;
  margin-bottom:8px; background:#fbfcfe; font-size:13px; }
.evt .t { font-weight:700; color:%(navy)s; }
.evt .m { font-size:11px; color:#5b6b7c; margin-top:3px; }
.muted { color:#5b6b7c; font-size:12px; }
.foot { padding:16px 28px; background:#f7f9fc; color:#5b6b7c; font-size:11px;
  border-top:1px solid #e4eaf2; }
code { background:#f0f3f8; padding:1px 4px; border-radius:4px; font-size:12px;
  word-break:break-all; }
"""
    return css % {"navy": _HEADER_NAVY, "navy2": _HEADER_NAVY_2}


_BADGE_KIND = {"safe": "safe", "info": "info", "warn": "warn", "crit": "crit"}


def _kv_row(k: str, v: str, cls: str = "") -> str:
    vv = html_escape(v)
    if cls:
        vv = '<span class="%s">%s</span>' % (html_escape(cls), vv)
    return '<tr><td class="k">%s</td><td class="v">%s</td></tr>' % (
        html_escape(k), vv)


def render_html(model: dict) -> str:
    """Render the deterministic HTML report body from *model*."""
    e = html_escape
    label = model.get("cycle_label", "")
    date = model.get("cycle_date", "")
    parts: list[str] = []
    parts.append("<!doctype html>")
    parts.append('<html lang="en"><head><meta charset="utf-8">')
    parts.append('<meta name="viewport" content="width=device-width,'
                 'initial-scale=1">')
    parts.append("<title>%s</title>" % e(model.get("subject", "Alpha Agent")))
    parts.append("<style>%s</style></head><body><div class=\"wrap\">" % _css())

    # 1. Header.
    title = model.get("report_title") or "Alpha Agent Research Report"
    parts.append('<div class="header"><h1>%s</h1>'
                 '<p class="sub">%s &middot; %s cycle &middot; generated %s</p>'
                 '</div>' % (e(title), e(date), e(label),
                             e(model.get("generated_at", ""))))

    # 2. Badges.
    parts.append('<div class="badges">')
    for b in model.get("badges", []):
        kind = _BADGE_KIND.get(str(b.get("kind")), "info")
        parts.append('<span class="badge %s">%s</span>' %
                     (kind, e(b.get("text", ""))))
    parts.append('</div>')

    # 3. Executive summary.
    parts.append('<div class="section"><h2>Executive summary</h2>'
                 '<p style="font-size:13px;margin:0;">%s</p></div>' %
                 e(model.get("executive_summary", "")))

    # 4. KPI cards.
    k = model.get("kpis", {})
    kpi_cards = [
        ("Records considered", fmt_int(k.get("records_considered"))),
        ("Selected events", fmt_int(k.get("selected_events"))),
        ("Accepted analyses", fmt_int(k.get("accepted_analyses"))),
        ("Hypotheses", fmt_int(k.get("hypotheses"))),
        ("Queue entries", fmt_int(k.get("queue_entries"))),
        ("Feed health", e(k.get("feed_health", "n/a"))),
    ]
    parts.append('<div class="section"><h2>Key metrics</h2><div class="kpis">')
    for lbl, val in kpi_cards:
        parts.append('<div class="kpi"><div class="v">%s</div>'
                     '<div class="l">%s</div></div>' % (val, e(lbl)))
    parts.append('</div></div>')

    # 5. Material events.
    parts.append('<div class="section"><h2>Material events</h2>')
    events = model.get("material_events", [])
    if events:
        for ev in events:
            parts.append(
                '<div class="evt"><div class="t">%s</div>'
                '<div>%s</div>'
                '<div class="m">Entity: %s &middot; Materiality: %s '
                '&middot; Sources: %s</div></div>' % (
                    e(ev.get("headline", ev.get("entity", "Event"))),
                    e(ev.get("summary", "")),
                    e(ev.get("entity", "n/a")),
                    e(ev.get("materiality", "n/a")),
                    e(", ".join(str(s) for s in ev.get("source_ids", []))
                      or "n/a")))
    else:
        parts.append('<p class="muted">No material events selected this '
                     'cycle.</p>')
    parts.append('</div>')

    # 6. Research conclusions.
    r = model.get("research", {})
    parts.append('<div class="section"><h2>Research conclusions</h2>')
    if r.get("no_new_action"):
        parts.append('<p style="font-size:13px;margin:0;">'
                     '<strong>NO NEW RESEARCH ACTION.</strong> %s</p>' %
                     e(r.get("notes", "")))
    else:
        hyps = r.get("new_hypotheses", [])
        if hyps:
            parts.append('<p class="muted">New hypotheses:</p>'
                         '<ul class="clean">')
            for h in hyps:
                parts.append('<li>%s</li>' % e(h))
            parts.append('</ul>')
        holds = r.get("data_holds", [])
        if holds:
            parts.append('<p class="muted">Data holds:</p><ul class="clean">')
            for h in holds:
                parts.append('<li>%s</li>' % e(h))
            parts.append('</ul>')
        parts.append('<p class="muted">Rejected duplicate proposals: %s.</p>' %
                     fmt_int(r.get("rejected_duplicates")))
        if r.get("notes"):
            parts.append('<p style="font-size:13px;">%s</p>' %
                         e(r.get("notes")))
    parts.append('</div>')

    # 6b. Experiment & Evidence (Stage 5).
    exp = model.get("experiment")
    parts.append('<div class="section"><h2>Experiment &amp; Evidence</h2>')
    if not exp:
        parts.append('<p class="muted">Stage 5 experiment engine not run this '
                     'cycle.</p>')
    else:
        parts.append('<table class="kv">')
        parts.append(_kv_row("Stage 5 run", exp.get("run_id") or "n/a"))
        parts.append(_kv_row("Outcome", "%s (%s)" % (exp.get("terminal"),
                                                     exp.get("status"))))
        parts.append(_kv_row("Hypotheses considered",
                             fmt_int(exp.get("hypotheses_considered"))))
        parts.append(_kv_row(
            "Experiments started / completed / failed",
            "%s / %s / %s" % (fmt_int(exp.get("experiments_started")),
                              fmt_int(exp.get("experiments_completed")),
                              fmt_int(exp.get("experiments_failed")))))
        parts.append(_kv_row("Data gaps", fmt_int(exp.get("data_gaps"))))
        parts.append(_kv_row("Duplicates rejected",
                             fmt_int(exp.get("duplicates_rejected"))))
        parts.append(_kv_row("KEEP_FOR_RESEARCH",
                             fmt_int(exp.get("keep_for_research"))))
        dc = exp.get("decision_counts") or {}
        if dc:
            parts.append(_kv_row("Evidence decisions", ", ".join(
                "%s=%s" % (k, v) for k, v in sorted(dc.items()))))
        st = exp.get("strongest")
        if st:
            parts.append(_kv_row("Strongest result", "%s [%s] rank-IC t %s" % (
                st.get("hypothesis_id"), st.get("template"),
                _fmt_stat(st.get("rank_ic_t")))))
        wk = exp.get("weakest")
        if wk and wk is not st:
            parts.append(_kv_row("Weakest result", "%s [%s] rank-IC t %s" % (
                wk.get("hypothesis_id"), wk.get("template"),
                _fmt_stat(wk.get("rank_ic_t")))))
        bc = exp.get("benchmark_comparison")
        if bc:
            parts.append(_kv_row("Benchmark excess (annualized)",
                                 _fmt_ret(bc.get("excess_annualized")),
                                 _sign_class(bc.get("excess_annualized"))))
            parts.append(_kv_row("Champion", exp.get("champion_model")
                                 or "n/a"))
        cs = exp.get("cost_sensitivity")
        if cs:
            parts.append(_kv_row("Cost flips sign",
                                 "YES" if cs.get("flips_sign") else "NO",
                                 "neg" if cs.get("flips_sign") else "pos"))
        parts.append(_kv_row("Next research action",
                             exp.get("next_action") or "n/a"))
        parts.append('</table>')
        parts.append('<p class="muted">KEEP_FOR_RESEARCH is not model '
                     'promotion; every result awaits human review. Stage 5 is '
                     'research-only and creates no order, signal or trade '
                     'decision.</p>')
    parts.append('</div>')

    # 7. Paper-book context.
    pb = model.get("paper_book", {})
    parts.append('<div class="section"><h2>Paper-book context</h2>'
                 '<table class="kv">')
    parts.append(_kv_row("Valuation date", pb.get("valuation_date") or "n/a"))
    parts.append(_kv_row("NAV", fmt_money(pb.get("nav"))))
    parts.append(_kv_row("Daily P&L", fmt_signed_money(pb.get("daily_pnl")),
                         _sign_class(pb.get("daily_pnl"))))
    parts.append(_kv_row("Daily return", fmt_pct(pb.get("daily_return_pct")),
                         _sign_class(pb.get("daily_return_pct"))))
    parts.append(_kv_row("Cumulative P&L",
                         fmt_signed_money(pb.get("cumulative_pnl")),
                         _sign_class(pb.get("cumulative_pnl"))))
    parts.append(_kv_row("Cumulative return",
                         fmt_pct(pb.get("cumulative_return_pct")),
                         _sign_class(pb.get("cumulative_return_pct"))))
    parts.append(_kv_row("Drawdown", fmt_pct(pb.get("drawdown_pct")),
                         _sign_class(pb.get("drawdown_pct"))))
    parts.append(_kv_row("SPY daily return", fmt_pct(pb.get("spy_daily_pct")),
                         _sign_class(pb.get("spy_daily_pct"))))
    parts.append(_kv_row("Daily excess vs SPY",
                         fmt_pp(pb.get("daily_excess_pp")),
                         _sign_class(pb.get("daily_excess_pp"))))
    parts.append(_kv_row("Cumulative excess vs SPY",
                         fmt_pp(pb.get("cumulative_excess_pp")),
                         _sign_class(pb.get("cumulative_excess_pp"))))
    parts.append(_kv_row("Matured evidence", fmt_int(pb.get("matured_evidence"))))
    parts.append(_kv_row("Pending evidence", fmt_int(pb.get("pending_evidence"))))
    parts.append(_kv_row("Risk-gate state", pb.get("risk_gate") or "n/a"))
    parts.append('</table></div>')

    # 8. News/RSS coverage.
    nr = model.get("news_rss", {})
    parts.append('<div class="section"><h2>News / RSS coverage</h2>'
                 '<table class="kv">')
    parts.append(_kv_row("Enabled / healthy feeds",
                         "%s / %s" % (fmt_int(nr.get("enabled")),
                                      fmt_int(nr.get("healthy")))))
    parts.append(_kv_row("Records collected (new/total)",
                         "%s / %s" % (fmt_int(nr.get("records_new")),
                                      fmt_int(nr.get("records_total")))))
    parts.append(_kv_row("Event clusters", fmt_int(nr.get("clusters"))))
    parts.append(_kv_row("Newest source item", nr.get("newest_item") or "n/a"))
    parts.append(_kv_row("Company-direct coverage",
                         nr.get("company_direct") or "sparse"))
    parts.append(_kv_row("Generalized RSS/Atom",
                         nr.get("generalized_status") or "n/a"))
    failed = nr.get("failed_feeds", [])
    parts.append(_kv_row("Failed feeds",
                         (", ".join(str(f) for f in failed) if failed
                          else "none"),
                         "neg" if failed else "pos"))
    parts.append('</table></div>')

    # 9. LLM usage and safety.
    llm = model.get("llm", {})
    parts.append('<div class="section"><h2>LLM usage and safety</h2>'
                 '<table class="kv">')
    parts.append(_kv_row("Provider", llm.get("provider") or "n/a"))
    parts.append(_kv_row("Classification", llm.get("classification") or "n/a"))
    parts.append(_kv_row("LLM invoked this cycle",
                         "YES" if llm.get("invoked") else "NO"))
    if llm.get("skipped_reason"):
        parts.append(_kv_row("LLM skipped", llm.get("skipped_reason"), "neg"))
    parts.append(_kv_row("LLM calls", fmt_int(llm.get("calls"))))
    parts.append(_kv_row("Tokens (in / out)",
                         "%s / %s" % (fmt_int(llm.get("tokens_in")),
                                      fmt_int(llm.get("tokens_out")))))
    parts.append(_kv_row("Estimated cost", llm.get("cost") or "UNAVAILABLE"))
    parts.append(_kv_row("Email formatting LLM tokens", "0 (deterministic)"))
    parts.append('</table></div>')

    # 10. Current operating action.
    parts.append('<div class="section"><h2>Current operating action</h2>'
                 '<p style="font-size:13px;margin:0;">%s</p>'
                 '<p class="muted">Trading automation remains OFF. This report '
                 'is research-only and never creates orders, fills, signals or '
                 'trade decisions.</p></div>' %
                 e(model.get("operating_action", "")))

    # 11. Run IDs and evidence paths.
    ev = model.get("evidence", {})
    parts.append('<div class="section"><h2>Run IDs and local evidence</h2>'
                 '<table class="kv">')
    for lbl, key in (("Stage 4 runtime run", "run_id"),
                     ("Stage 1 registry run", "stage1_run_id"),
                     ("Stage 2 ingestion run", "stage2_run_id"),
                     ("Stage 3.5 news/RSS run", "stage35_run_id"),
                     ("Stage 3 director run", "stage3_run_id")):
        parts.append('<tr><td class="k">%s</td><td class="v"><code>%s</code>'
                     '</td></tr>' % (e(lbl), e(ev.get(key) or "n/a")))
    for p in ev.get("evidence_paths", []):
        parts.append('<tr><td class="k">%s</td><td class="v"><code>%s</code>'
                     '</td></tr>' % (e(p.get("label", "path")),
                                     e(p.get("path", ""))))
    parts.append('</table></div>')

    parts.append('<div class="foot">Alpha Agent Stage 4 &middot; research '
                 'automation only &middot; paper portfolio only &middot; '
                 'no live orders &middot; automation off. Schema %s.</div>' %
                 e(REPORT_SCHEMA_VERSION))
    parts.append('</div></body></html>')
    return "\n".join(parts)


# --------------------------------------------------------------------------- #
# Plain-text rendering (deterministic fallback).
# --------------------------------------------------------------------------- #
def render_text(model: dict) -> str:
    lines: list[str] = []
    title = model.get("report_title") or "Alpha Agent Research Report"
    lines.append(title)
    lines.append("%s | %s cycle | generated %s" % (
        model.get("cycle_date", ""), model.get("cycle_label", ""),
        model.get("generated_at", "")))
    lines.append("=" * 64)
    lines.append("Badges: " + " | ".join(
        b.get("text", "") for b in model.get("badges", [])))
    lines.append("")
    lines.append("EXECUTIVE SUMMARY")
    lines.append(model.get("executive_summary", ""))
    lines.append("")
    k = model.get("kpis", {})
    lines.append("KEY METRICS")
    lines.append("  Records considered : %s" % fmt_int(k.get("records_considered")))
    lines.append("  Selected events    : %s" % fmt_int(k.get("selected_events")))
    lines.append("  Accepted analyses  : %s" % fmt_int(k.get("accepted_analyses")))
    lines.append("  Hypotheses         : %s" % fmt_int(k.get("hypotheses")))
    lines.append("  Queue entries      : %s" % fmt_int(k.get("queue_entries")))
    lines.append("  Feed health        : %s" % (k.get("feed_health") or "n/a"))
    lines.append("")
    lines.append("MATERIAL EVENTS")
    events = model.get("material_events", [])
    if events:
        for ev in events:
            lines.append("  - %s [%s]" % (ev.get("summary", ""),
                                          ev.get("entity", "n/a")))
            lines.append("    materiality=%s sources=%s" % (
                ev.get("materiality", "n/a"),
                ", ".join(str(s) for s in ev.get("source_ids", [])) or "n/a"))
    else:
        lines.append("  (none selected this cycle)")
    lines.append("")
    r = model.get("research", {})
    lines.append("RESEARCH CONCLUSIONS")
    if r.get("no_new_action"):
        lines.append("  NO NEW RESEARCH ACTION. %s" % r.get("notes", ""))
    else:
        for h in r.get("new_hypotheses", []):
            lines.append("  + hypothesis: %s" % h)
        for h in r.get("data_holds", []):
            lines.append("  ~ data hold: %s" % h)
        lines.append("  rejected duplicates: %s" %
                     fmt_int(r.get("rejected_duplicates")))
        if r.get("notes"):
            lines.append("  %s" % r.get("notes"))
    lines.append("")
    exp = model.get("experiment")
    lines.append("EXPERIMENT & EVIDENCE (STAGE 5)")
    if not exp:
        lines.append("  (Stage 5 experiment engine not run this cycle)")
    else:
        lines.append("  Run / outcome     : %s / %s (%s)" % (
            exp.get("run_id") or "n/a", exp.get("terminal"), exp.get("status")))
        lines.append("  Hypotheses cons.  : %s" %
                     fmt_int(exp.get("hypotheses_considered")))
        lines.append("  Exp start/done/fail: %s / %s / %s" % (
            fmt_int(exp.get("experiments_started")),
            fmt_int(exp.get("experiments_completed")),
            fmt_int(exp.get("experiments_failed"))))
        lines.append("  Data gaps / dupes : %s / %s" % (
            fmt_int(exp.get("data_gaps")),
            fmt_int(exp.get("duplicates_rejected"))))
        lines.append("  KEEP_FOR_RESEARCH : %s" %
                     fmt_int(exp.get("keep_for_research")))
        dc = exp.get("decision_counts") or {}
        if dc:
            lines.append("  Decisions         : %s" % ", ".join(
                "%s=%s" % (k, v) for k, v in sorted(dc.items())))
        st = exp.get("strongest")
        if st:
            lines.append("  Strongest         : %s [%s] rank-IC t %s" % (
                st.get("hypothesis_id"), st.get("template"),
                _fmt_stat(st.get("rank_ic_t"))))
        bc = exp.get("benchmark_comparison")
        if bc:
            lines.append("  Benchmark excess  : %s (champion %s)" % (
                _fmt_ret(bc.get("excess_annualized")),
                exp.get("champion_model") or "n/a"))
        cs = exp.get("cost_sensitivity")
        if cs:
            lines.append("  Cost flips sign   : %s" %
                         ("YES" if cs.get("flips_sign") else "NO"))
        lines.append("  Next action       : %s" % (exp.get("next_action")
                                                    or "n/a"))
        lines.append("  (KEEP_FOR_RESEARCH is not promotion; human review "
                     "only; research-only.)")
    lines.append("")
    pb = model.get("paper_book", {})
    lines.append("PAPER-BOOK CONTEXT")
    lines.append("  Valuation date     : %s" % (pb.get("valuation_date") or "n/a"))
    lines.append("  NAV                : %s" % fmt_money(pb.get("nav")))
    lines.append("  Daily P&L / return : %s / %s" % (
        fmt_signed_money(pb.get("daily_pnl")),
        fmt_pct(pb.get("daily_return_pct"))))
    lines.append("  Cumulative P&L/ret : %s / %s" % (
        fmt_signed_money(pb.get("cumulative_pnl")),
        fmt_pct(pb.get("cumulative_return_pct"))))
    lines.append("  Drawdown           : %s" % fmt_pct(pb.get("drawdown_pct")))
    lines.append("  SPY daily return   : %s" % fmt_pct(pb.get("spy_daily_pct")))
    lines.append("  Excess daily / cum : %s / %s" % (
        fmt_pp(pb.get("daily_excess_pp")),
        fmt_pp(pb.get("cumulative_excess_pp"))))
    lines.append("  Evidence mat/pend  : %s / %s" % (
        fmt_int(pb.get("matured_evidence")),
        fmt_int(pb.get("pending_evidence"))))
    lines.append("  Risk-gate state    : %s" % (pb.get("risk_gate") or "n/a"))
    lines.append("")
    nr = model.get("news_rss", {})
    lines.append("NEWS / RSS COVERAGE")
    lines.append("  Enabled / healthy  : %s / %s" % (
        fmt_int(nr.get("enabled")), fmt_int(nr.get("healthy"))))
    lines.append("  Records new/total  : %s / %s" % (
        fmt_int(nr.get("records_new")), fmt_int(nr.get("records_total"))))
    lines.append("  Clusters           : %s" % fmt_int(nr.get("clusters")))
    lines.append("  Newest item        : %s" % (nr.get("newest_item") or "n/a"))
    lines.append("  Generalized RSS    : %s" % (nr.get("generalized_status") or "n/a"))
    failed = nr.get("failed_feeds", [])
    lines.append("  Failed feeds       : %s" % (
        ", ".join(str(f) for f in failed) if failed else "none"))
    lines.append("")
    llm = model.get("llm", {})
    lines.append("LLM USAGE AND SAFETY")
    lines.append("  Provider           : %s (%s)" % (
        llm.get("provider") or "n/a", llm.get("classification") or "n/a"))
    lines.append("  Invoked / calls    : %s / %s" % (
        "YES" if llm.get("invoked") else "NO", fmt_int(llm.get("calls"))))
    if llm.get("skipped_reason"):
        lines.append("  LLM skipped        : %s" % llm.get("skipped_reason"))
    lines.append("  Tokens in / out    : %s / %s" % (
        fmt_int(llm.get("tokens_in")), fmt_int(llm.get("tokens_out"))))
    lines.append("  Estimated cost     : %s" % (llm.get("cost") or "UNAVAILABLE"))
    lines.append("  Email LLM tokens   : 0 (deterministic)")
    lines.append("")
    lines.append("CURRENT OPERATING ACTION")
    lines.append("  %s" % model.get("operating_action", ""))
    lines.append("  Trading automation remains OFF; research-only.")
    lines.append("")
    ev = model.get("evidence", {})
    lines.append("RUN IDS AND LOCAL EVIDENCE")
    lines.append("  runtime  : %s" % (ev.get("run_id") or "n/a"))
    lines.append("  stage1   : %s" % (ev.get("stage1_run_id") or "n/a"))
    lines.append("  stage2   : %s" % (ev.get("stage2_run_id") or "n/a"))
    lines.append("  stage3.5 : %s" % (ev.get("stage35_run_id") or "n/a"))
    lines.append("  stage3   : %s" % (ev.get("stage3_run_id") or "n/a"))
    for p in ev.get("evidence_paths", []):
        lines.append("  %s: %s" % (p.get("label", "path"), p.get("path", "")))
    lines.append("")
    return "\n".join(lines)


def report_manifest(model: dict, html_body: str, text_body: str) -> dict:
    """Immutable, deterministic manifest describing the rendered report.

    Contains NO secret and NO raw article body — only content hashes, the
    reconciled numeric model and file identities.
    """
    html_sha = hashlib.sha256(html_body.encode("utf-8")).hexdigest()
    text_sha = hashlib.sha256(text_body.encode("utf-8")).hexdigest()
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
        "kpis": model.get("kpis"),
        "paper_book": model.get("paper_book"),
        "news_rss": model.get("news_rss"),
        "llm": model.get("llm"),
        "evidence": model.get("evidence"),
        "experiment": model.get("experiment"),
        "email_llm_tokens": 0,
    }
