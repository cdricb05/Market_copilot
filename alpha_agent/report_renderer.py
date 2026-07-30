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
# Canonical status flags + Action-Today vocabulary (WS6 / WS1).
# --------------------------------------------------------------------------- #
STATUS_FLAGS = ("RESEARCH SCHEDULE: ON", "TRADING AUTOMATION: OFF",
                "BROKER EXECUTION: OFF", "PAPER ONLY")

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


def derive_bottom_line(model: dict) -> list[str]:
    """<=4 short plain-English sentences answering the four bottom-line
    questions: money, vs SPY, usable candidate, action required."""
    sc = (model.get("scorecard") or {}).get("raw") or {}
    sentences: list[str] = []

    cum_pnl = sc.get("cumulative_pnl")
    cum_ret = sc.get("cumulative_return_pct")
    if cum_pnl is None and cum_ret is None:
        sentences.append("Portfolio valuation is not available in this "
                         "report.")
    else:
        # A plain verb answers 'made or lost money?'; the signed value keeps its
        # explicit sign without a double negative in the prose.
        sentences.append(
            "The paper book has %s money since inception: %s (%s today)." % (
                _money_verb(cum_pnl), fmt_signed_money(cum_pnl),
                fmt_pct(sc.get("daily_return_pct"))))

    exc = sc.get("cumulative_excess_pp")
    n_exc = _num(exc)
    if n_exc is None:
        sentences.append("Its position versus SPY is not available.")
    else:
        sentences.append("It is %s SPY by %s." % (
            "ahead of" if n_exc >= 0 else "behind",
            fmt_pp(abs(n_exc))))

    if model.get("shadow_ready"):
        sentences.append("Research has found a candidate worth your review; it "
                         "is a paper-only shadow and nothing was promoted.")
    else:
        disp = (model.get("recovery_readiness") or {}).get("disposition")
        tail = (" — " + translate(disp)) if disp else "."
        sentences.append("Research has not yet found a usable new candidate%s"
                         % (tail if tail != "." else "."))

    action, _reason = derive_action_today(model)
    required = "No" if action == ACTION_NO_TRADE else "Yes"
    sentences.append("Action required: %s — %s." % (required, action))
    return sentences[:4]


def what_changed(model: dict) -> list[str]:
    """Plain-English deltas versus the previous report (WS1 §4)."""
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
        out.append("%s changed by %s since the last report (now %s)." % (
            label, fmt_signed_money(a - b), fmt_signed_money(a)))

    def _delta_pp(key, label, kind="pct"):
        a, b = _num(cur.get(key)), _num(prev.get(key))
        if a is None or b is None:
            return
        f = fmt_pp if kind == "pp" else fmt_pct
        out.append("%s moved %s (now %s)." % (label, f(a - b), f(a)))

    _delta_money("nav", "NAV")
    _delta_pp("cumulative_return_pct", "Return since inception")
    _delta_pp("cumulative_excess_pp", "Standing versus SPY", kind="pp")

    prev_disp = prior.get("disposition")
    cur_disp = (model.get("recovery_readiness") or {}).get("disposition")
    if cur_disp and prev_disp and cur_disp != prev_disp:
        out.append("The research disposition changed from '%s' to '%s' (%s)." %
                   (prev_disp, cur_disp, translate(cur_disp)))
    elif cur_disp and not prev_disp:
        out.append("A research disposition is now on record: %s." %
                   translate(cur_disp))

    if model.get("no_new_evidence"):
        out.append("No new research evidence since the prior report.")
    if not out:
        out.append("No material change since the last report.")
    return out


# --------------------------------------------------------------------------- #
# HTML rendering.
# --------------------------------------------------------------------------- #
def _css() -> str:
    css = """
body { margin:0; padding:0; background:#eef2f7;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
  color:#1b2733; }
.wrap { max-width:840px; margin:0 auto; background:#ffffff; }
.header { background:linear-gradient(135deg,%(navy)s 0%%,%(navy2)s 100%%);
  color:#ffffff; padding:22px 28px; }
.header h1 { margin:0; font-size:20px; letter-spacing:0.3px; }
.header .sub { margin:4px 0 0; font-size:13px; color:#cfe0f5; }
.flags { padding:12px 28px 2px; background:#0b2545; }
.flag { display:inline-block; font-size:11px; font-weight:800; color:#ffffff;
  background:#12406e; border:1px solid #2f6fb0; padding:4px 10px;
  border-radius:6px; margin:0 6px 8px 0; letter-spacing:0.5px; }
.flag.off { background:#3a1d1d; border-color:#7a3b3b; }
.badges { padding:12px 28px 4px; }
.badge { display:inline-block; font-size:11px; font-weight:700;
  padding:4px 10px; border-radius:12px; margin:0 6px 8px 0; letter-spacing:0.4px; }
.badge.safe { background:#e6f4ea; color:#1a7f37; border:1px solid #ace0bd; }
.badge.info { background:#e7f0fb; color:#0b4a9e; border:1px solid #a9cbf5; }
.badge.warn { background:#fbece6; color:#9a3b12; border:1px solid #f3c2a8; }
.badge.crit { background:#fdeaea; color:#b42318; border:1px solid #f4a9a4; }
.section { padding:16px 28px; border-top:1px solid #eef2f7; }
.section h2 { font-size:14px; text-transform:uppercase; letter-spacing:0.6px;
  color:%(navy)s; margin:0 0 10px; }
.bottomline { background:#f4f8ff; }
.bottomline p { font-size:15px; line-height:1.5; margin:0 0 6px; }
.action { font-size:16px; font-weight:800; padding:10px 12px; border-radius:8px;
  display:inline-block; }
.action.hold { background:#e6f4ea; color:#14622c; border:1px solid #ace0bd; }
.action.review { background:#fff3d6; color:#8a5a00; border:1px solid #f0d089; }
.action.attention { background:#fdeaea; color:#b42318; border:1px solid #f4a9a4; }
.action.shadow { background:#e7f0fb; color:#0b4a9e; border:1px solid #a9cbf5; }
.kpis { display:flex; flex-wrap:wrap; gap:10px; }
.kpi { flex:1 1 150px; min-width:150px; background:#f7f9fc;
  border:1px solid #e4eaf2; border-radius:8px; padding:10px 12px; }
.kpi .v { font-size:18px; font-weight:700; color:%(navy)s; }
.kpi .l { font-size:11px; color:#5b6b7c; text-transform:uppercase;
  letter-spacing:0.4px; margin-top:2px; }
table.kv { width:100%%; border-collapse:collapse; font-size:13px; }
table.kv td, table.kv th { padding:5px 8px; border-bottom:1px solid #f0f3f8;
  vertical-align:top; text-align:left; }
table.kv td.k, table.kv th { color:#5b6b7c; }
table.kv td.v { color:#1b2733; font-weight:600; }
.pos { color:#1a7f37; }
.neg { color:#b42318; }
.neutral { color:#5b6b7c; }
ul.clean { margin:4px 0 0; padding-left:18px; font-size:13px; }
ul.clean li { margin:4px 0; line-height:1.45; }
.evt { border:1px solid #e4eaf2; border-radius:8px; padding:10px 12px;
  margin-bottom:8px; background:#fbfcfe; font-size:13px; }
.evt .t { font-weight:700; color:%(navy)s; }
.evt .m { font-size:11px; color:#5b6b7c; margin-top:3px; }
.muted { color:#5b6b7c; font-size:12px; }
.ready { color:#1a7f37; font-weight:700; }
.notready { color:#b42318; font-weight:700; }
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


def _action_css(action: str) -> str:
    return {ACTION_NO_TRADE: "hold", ACTION_MANUAL_REVIEW: "review",
            ACTION_DATA_ATTENTION: "attention",
            ACTION_SHADOW_READY: "shadow"}.get(action, "hold")


def render_html(model: dict) -> str:
    """Render the deterministic executive HTML report body from *model*."""
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

    # Header.
    title = model.get("report_title") or "Alpha Agent Executive Research Brief"
    parts.append('<div class="header"><h1>%s</h1>'
                 '<p class="sub">%s &middot; %s cycle &middot; generated %s</p>'
                 '</div>' % (e(title), e(date), e(label),
                             e(model.get("generated_at", ""))))

    # Prominent status flags (WS6) — replaces the generic AUTOMATION OFF badge.
    parts.append('<div class="flags">')
    for f in model.get("status_flags", STATUS_FLAGS):
        cls = "flag off" if "OFF" in f else "flag"
        parts.append('<span class="%s">%s</span>' % (cls, e(f)))
    parts.append('</div>')

    # Conditional badges (degraded / llm skipped / coverage).
    parts.append('<div class="badges">')
    for b in model.get("badges", []):
        kind = _BADGE_KIND.get(str(b.get("kind")), "info")
        parts.append('<span class="badge %s">%s</span>' %
                     (kind, e(b.get("text", ""))))
    parts.append('</div>')

    # 1. BOTTOM LINE.
    parts.append('<div class="section bottomline"><h2>1. Bottom line</h2>')
    for s in derive_bottom_line(model):
        parts.append('<p>%s</p>' % e(s))
    parts.append('</div>')

    # 2. ACTION TODAY.
    action, reason = derive_action_today(model)
    parts.append('<div class="section"><h2>2. Action today</h2>'
                 '<div class="action %s">%s</div>'
                 '<p style="font-size:13px;margin:8px 0 0;">%s</p></div>' % (
                     _action_css(action), e(action), e(reason)))

    # 3. PORTFOLIO SCORECARD.
    sc = model.get("scorecard") or scorecard(model.get("paper_book"))
    parts.append('<div class="section"><h2>3. Portfolio scorecard</h2>'
                 '<table class="kv">')
    for row in sc.get("rows", []):
        parts.append(_kv_row(row.get("label", ""), row.get("value", ""),
                             row.get("sign", "")))
    parts.append('</table></div>')

    # 4. WHAT CHANGED SINCE THE LAST REPORT.
    parts.append('<div class="section"><h2>4. What changed since the last '
                 'report</h2><ul class="clean">')
    for b in what_changed(model):
        parts.append('<li>%s</li>' % e(b))
    parts.append('</ul></div>')

    # 5. RESEARCH DECISIONS.
    rd = model.get("research_decisions") or {}
    parts.append('<div class="section"><h2>5. Research decisions</h2>')
    parts.append('<p style="font-size:13px;margin:0 0 8px;">%s</p>' %
                 e(rd.get("summary", "No research decisions this cycle.")))
    items = rd.get("items") or []
    if items:
        parts.append('<ul class="clean">')
        for it in items:
            parts.append('<li><strong>%s</strong> — %s</li>' % (
                e(it.get("label", "")), e(it.get("plain", ""))))
        parts.append('</ul>')
    # Up to three material events, each with a one-sentence why.
    events = (model.get("material_events") or [])[:3]
    if events:
        parts.append('<p class="muted" style="margin-top:10px;">Most material '
                     'events this cycle:</p>')
        for ev in events:
            parts.append('<div class="evt"><div class="t">%s</div>'
                         '<div>%s</div><div class="m">Why it matters: %s</div>'
                         '</div>' % (
                             e(ev.get("headline", ev.get("entity", "Event"))),
                             e(ev.get("summary", "")),
                             e(ev.get("mechanism")
                               or "Flagged as potentially relevant to held or "
                                  "watched names.")))
    parts.append('</div>')

    # 6. MODEL HEALTH (Alpha Recovery).
    parts.append(_render_model_health_html(model, e))

    # 7. RISK AND SHADOW PORTFOLIOS.
    parts.append(_render_shadows_html(model, e))

    # 8. HISTORICAL DATA READINESS.
    parts.append(_render_historical_html(model, e))

    # 9. SOURCE / AGENT HEALTH.
    parts.append(_render_source_health_html(model, e))

    # 10. TECHNICAL APPENDIX.
    parts.append(_render_appendix_html(model, e))

    parts.append('<div class="foot">Alpha Agent Stage 4 &middot; research '
                 'automation only &middot; paper portfolio only &middot; '
                 'no live orders &middot; trading automation off. Report '
                 'rendered deterministically at zero LLM-token cost.</div>')
    parts.append('</div></body></html>')
    return "\n".join(parts)


def _render_model_health_html(model: dict, e) -> str:
    rec = model.get("recovery_readiness")
    p = ['<div class="section"><h2>6. Model health (Alpha Recovery)</h2>']
    if not rec:
        p.append('<p class="muted">No Stage 7 recovery package yet.</p></div>')
        return "\n".join(p)
    champ = rec.get("champion_model") or "the current champion"
    p.append('<p style="font-size:13px;margin:0 0 6px;">Champion model: '
             '<strong>%s</strong>.</p>' % e(champ))
    p.append('<p style="font-size:13px;margin:0 0 6px;">Verdict: %s</p>' %
             e(translate(rec.get("disposition"))))
    if rec.get("disposition_rationale"):
        p.append('<p class="muted">%s</p>' % e(rec.get("disposition_rationale")))
    classes = rec.get("reconstruction_classes") or []
    if classes:
        p.append('<ul class="clean">')
        for c in classes:
            p.append('<li>%s: %s</li>' % (
                e(_pretty_component(c.get("component"))),
                e(translate(c.get("class")))))
        p.append('</ul>')
    p.append('<p class="muted">No model has been promoted; the champion '
             'remains paper-only regardless of this verdict.</p>')
    p.append('</div>')
    return "\n".join(p)


def _pretty_component(name: Any) -> str:
    return {
        "portfolio_construction": "How the portfolio is built",
        "selection_signal_price_leg": "The price-momentum selection signal",
        "fundamental_leg_point_in_time": "The fundamental selection signal",
    }.get(str(name), str(name).replace("_", " ").capitalize())


_SHADOW_PRETTY = {
    "CURRENT_CONTROL": "Current book (control)",
    "MARKET_REGIME_CASH_OVERLAY": "Market-regime cash overlay",
    "PORTFOLIO_VOL_TARGET_20": "Volatility target 20%",
}


def _render_shadows_html(model: dict, e) -> str:
    fs = model.get("risk_and_shadow") or {}
    shadows = fs.get("shadows") or []
    p = ['<div class="section"><h2>7. Risk and shadow portfolios</h2>']
    if fs.get("note"):
        p.append('<p style="font-size:13px;margin:0 0 8px;">%s</p>' %
                 e(fs.get("note")))
    if not shadows:
        p.append('<p class="muted">Forward shadow tracking has no completed '
                 'observations yet; it begins at the next eligible close.</p>')
    else:
        p.append('<table class="kv"><tr><th>Shadow portfolio</th>'
                 '<th>Return since start</th><th>Drawdown</th>'
                 '<th>Realized vol</th><th>Cash</th><th>vs SPY</th>'
                 '<th>Obs</th></tr>')
        for s in shadows:
            p.append('<tr><td class="v">%s</td><td>%s</td><td>%s</td><td>%s</td>'
                     '<td>%s</td><td>%s</td><td>%s</td></tr>' % (
                         e(_SHADOW_PRETTY.get(s.get("overlay"),
                                              s.get("overlay"))),
                         e(_fmt_ret(s.get("cumulative_return"))),
                         e(_fmt_ret(s.get("drawdown"))),
                         e(_fmt_ret(s.get("realized_vol"))
                           if s.get("realized_vol") is not None
                           else "Not available"),
                         e(fmt_pct((s.get("cash") or 0) * 100.0)
                           if s.get("cash") is not None else "Not available"),
                         e(_fmt_ret(s.get("spy_excess"))),
                         e(fmt_int(s.get("observations")))))
        p.append('</table>')
    p.append('<p class="muted">No shadow portfolio changes the active paper '
             'portfolio. These are read-only research shadows: no target, '
             'order, signal, decision or fill is created.</p>')
    p.append('</div>')
    return "\n".join(p)


_READINESS_ITEMS = ("Price history", "Historical membership",
                    "Point-in-time fundamentals", "Earnings history",
                    "Historical sector classifications")


def _render_historical_html(model: dict, e) -> str:
    rows = model.get("historical_readiness_summary") or []
    p = ['<div class="section"><h2>8. Historical data readiness</h2>']
    if not rows:
        p.append('<p class="muted">No historical-data readiness summary this '
                 'cycle.</p></div>')
        return "\n".join(p)
    p.append('<table class="kv">')
    for r in rows:
        ready = str(r.get("status", "")).upper().startswith("READY")
        cls = "ready" if ready else "notready"
        val = '<span class="%s">%s</span> — %s' % (
            cls, e(r.get("status", "")), e(r.get("note", "")))
        p.append('<tr><td class="k">%s</td><td class="v">%s</td></tr>' % (
            e(r.get("label", "")), val))
    p.append('</table>')
    if model.get("historical_gap_note"):
        p.append('<p class="muted">%s</p>' % e(model.get("historical_gap_note")))
    p.append('</div>')
    return "\n".join(p)


def _render_source_health_html(model: dict, e) -> str:
    sh = model.get("source_agent_health") or {}
    nr = model.get("news_rss") or {}
    p = ['<div class="section"><h2>9. Source / agent health</h2>',
         '<table class="kv">']
    p.append(_kv_row("News/RSS feeds healthy",
                     "%s of %s" % (fmt_int(nr.get("healthy")),
                                   fmt_int(nr.get("enabled")))))
    failed = nr.get("failed_feeds") or []
    p.append(_kv_row("Feeds needing attention",
                     ", ".join(str(f) for f in failed) if failed else "none",
                     "neg" if failed else "pos"))
    p.append(_kv_row("Research provider running",
                     "yes" if sh.get("provider_ok", True) else "attention",
                     "pos" if sh.get("provider_ok", True) else "neg"))
    p.append(_kv_row("Operational ledgers unchanged",
                     "yes" if sh.get("ledgers_unchanged", True) else "NO",
                     "pos" if sh.get("ledgers_unchanged", True) else "neg"))
    if sh.get("stage7_age_note"):
        p.append(_kv_row("Latest research verdict age",
                         sh.get("stage7_age_note")))
    p.append('</table></div>')
    return "\n".join(p)


def _render_appendix_html(model: dict, e) -> str:
    p = ['<div class="section"><h2>10. Technical appendix</h2>',
         '<p class="muted">Machine identifiers, tokens, file paths and provider '
         'internals — for audit only; not needed to act on this report.</p>']

    # Recovery / disposition raw tokens.
    rec = model.get("recovery_readiness")
    if rec:
        p.append('<table class="kv">')
        p.append(_kv_row("Stage 7 recovery run", rec.get("run_id") or "n/a"))
        p.append(_kv_row("Recovery disposition (token)",
                         rec.get("disposition") or "n/a"))
        p.append(_kv_row("Champion price-leg rank-IC t",
                         _fmt_num(rec.get("champion_rank_ic_t"))))
        for c in (rec.get("reconstruction_classes") or []):
            p.append(_kv_row("Reconstruction: %s" % c.get("component"),
                             c.get("class")))
        p.append(_kv_row("Campaign experiments / keep-for-research",
                         "%s / %s" % (rec.get("campaign_experiments"),
                                      rec.get("campaign_keep_for_research"))))
        p.append(_kv_row("Manual de-risk preview status",
                         rec.get("manual_preview_status") or "n/a"))
        p.append(_kv_row("Promotion allowed now", "no"))
        p.append('</table>')

    # Experiment & Evidence (Stage 5) — full machine detail in the appendix.
    exp = model.get("experiment")
    p.append('<h3 style="font-size:12px;color:%s;margin:12px 0 4px;">'
             'Experiment &amp; Evidence (Stage 5)</h3>' % _HEADER_NAVY)
    if not exp:
        p.append('<p class="muted">Stage 5 experiment engine not run this '
                 'cycle.</p>')
    else:
        p.append('<table class="kv">')
        p.append(_kv_row("Stage 5 run", exp.get("run_id") or "n/a"))
        p.append(_kv_row("Outcome token", "%s (%s)" % (exp.get("terminal"),
                                                       exp.get("status"))))
        p.append(_kv_row("KEEP_FOR_RESEARCH",
                         fmt_int(exp.get("keep_for_research"))))
        dc = exp.get("decision_counts") or {}
        if dc:
            p.append(_kv_row("Evidence decisions", ", ".join(
                "%s=%s" % (k, v) for k, v in sorted(dc.items()))))
        p.append('</table>')
        p.append('<p class="muted">KEEP_FOR_RESEARCH is not model promotion; '
                 'every result awaits human review.</p>')

    # Historical Data & Experiment Readiness (Stage 6) — corrected window +
    # universe (raw machine detail; the plain-English summary is section 8).
    hr = model.get("historical_readiness")
    p.append('<h3 style="font-size:12px;color:%s;margin:12px 0 4px;">'
             'Historical Data &amp; Experiment Readiness (Stage 6)</h3>'
             % _HEADER_NAVY)
    if not hr:
        p.append('<p class="muted">No Stage 6 historical backfill package '
                 'yet.</p>')
    else:
        p.append('<table class="kv">')
        p.append(_kv_row("Stage 6 run", hr.get("run_id") or "n/a"))
        p.append(_kv_row("Backfill window", _window_str(hr)))
        p.append(_kv_row("Universe (price / membership)", _universe_str(hr)))
        p.append(_kv_row("Records acquired", fmt_int(hr.get("records_written"))))
        p.append(_kv_row("Data version", hr.get("data_version") or "n/a"))
        p.append('</table>')

    # Extra material events beyond the first three.
    extra = (model.get("material_events") or [])[3:]
    if extra:
        p.append('<p class="muted">Additional events (lower information):</p>'
                 '<ul class="clean">')
        for ev in extra:
            p.append('<li>%s [%s] sources: %s</li>' % (
                e(ev.get("summary", "")), e(ev.get("entity", "n/a")),
                e(", ".join(str(s) for s in ev.get("source_ids", []))
                  or "n/a")))
        p.append('</ul>')

    # LLM + provider.
    llm = model.get("llm") or {}
    p.append('<table class="kv">')
    p.append(_kv_row("Research provider", llm.get("provider") or "n/a"))
    p.append(_kv_row("Provider classification",
                     llm.get("classification") or "n/a"))
    p.append(_kv_row("LLM invoked / calls", "%s / %s" % (
        "yes" if llm.get("invoked") else "no", fmt_int(llm.get("calls")))))
    p.append(_kv_row("Tokens in / out", "%s / %s" % (
        fmt_int(llm.get("tokens_in")), fmt_int(llm.get("tokens_out")))))
    p.append(_kv_row("Estimated LLM cost", llm.get("cost") or "UNAVAILABLE"))
    p.append(_kv_row("Email formatting LLM tokens", "0 (deterministic)"))
    p.append('</table>')

    # Run ids + evidence paths.
    ev = model.get("evidence") or {}
    p.append('<table class="kv">')
    for lbl, key in (("Stage 4 runtime run", "run_id"),
                     ("Stage 1 registry run", "stage1_run_id"),
                     ("Stage 2 ingestion run", "stage2_run_id"),
                     ("Stage 3.5 news/RSS run", "stage35_run_id"),
                     ("Stage 3 director run", "stage3_run_id")):
        p.append('<tr><td class="k">%s</td><td class="v"><code>%s</code>'
                 '</td></tr>' % (e(lbl), e(ev.get(key) or "n/a")))
    for path in ev.get("evidence_paths", []):
        p.append('<tr><td class="k">%s</td><td class="v"><code>%s</code>'
                 '</td></tr>' % (e(path.get("label", "path")),
                                 e(path.get("path", ""))))
    p.append(_kv_row("Report schema version", REPORT_SCHEMA_VERSION))
    p.append('</table></div>')
    return "\n".join(p)


def _window_str(hr: dict) -> str:
    ds, de = hr.get("date_start"), hr.get("date_end")
    if ds and de:
        return "%s through %s" % (ds, de)
    return "Not available (window not recorded in package)"


def _universe_str(hr: dict) -> str:
    price = hr.get("universe_full_size")
    memb = hr.get("universe_size")
    if price or memb:
        return "%s priced tickers (survivorship-free) / %s current members" % (
            fmt_int(price), fmt_int(memb))
    return "Not available (coverage not recorded in package)"


# --------------------------------------------------------------------------- #
# Plain-text rendering (deterministic mirror of the HTML executive brief).
# --------------------------------------------------------------------------- #
def render_text(model: dict) -> str:
    lines: list[str] = []
    title = model.get("report_title") or "Alpha Agent Executive Research Brief"
    lines.append(title)
    lines.append("%s | %s cycle | generated %s" % (
        model.get("cycle_date", ""), model.get("cycle_label", ""),
        model.get("generated_at", "")))
    lines.append("=" * 66)
    lines.append(" | ".join(model.get("status_flags", STATUS_FLAGS)))
    extra = [b.get("text", "") for b in model.get("badges", [])]
    if extra:
        lines.append("Flags: " + " | ".join(extra))
    lines.append("")

    lines.append("1. BOTTOM LINE")
    for s in derive_bottom_line(model):
        lines.append("  " + s)
    lines.append("")

    action, reason = derive_action_today(model)
    lines.append("2. ACTION TODAY")
    lines.append("  >> %s" % action)
    lines.append("     %s" % reason)
    lines.append("")

    sc = model.get("scorecard") or scorecard(model.get("paper_book"))
    lines.append("3. PORTFOLIO SCORECARD")
    for row in sc.get("rows", []):
        lines.append("  %-30s : %s" % (row.get("label", ""),
                                       row.get("value", "")))
    lines.append("")

    lines.append("4. WHAT CHANGED SINCE THE LAST REPORT")
    for b in what_changed(model):
        lines.append("  - " + b)
    lines.append("")

    rd = model.get("research_decisions") or {}
    lines.append("5. RESEARCH DECISIONS")
    lines.append("  " + rd.get("summary", "No research decisions this cycle."))
    for it in (rd.get("items") or []):
        lines.append("   - %s: %s" % (it.get("label", ""), it.get("plain", "")))
    for ev in (model.get("material_events") or [])[:3]:
        lines.append("   * %s [%s]" % (ev.get("summary", ""),
                                       ev.get("entity", "n/a")))
    lines.append("")

    rec = model.get("recovery_readiness")
    lines.append("6. MODEL HEALTH (ALPHA RECOVERY)")
    if not rec:
        lines.append("  No Stage 7 recovery package yet")
    else:
        lines.append("  Champion : %s" % (rec.get("champion_model") or "n/a"))
        lines.append("  Verdict  : %s" % translate(rec.get("disposition")))
        for c in (rec.get("reconstruction_classes") or []):
            lines.append("   - %s: %s" % (_pretty_component(c.get("component")),
                                          translate(c.get("class"))))
        lines.append("  No model promoted; champion stays paper-only.")
    lines.append("")

    fs = model.get("risk_and_shadow") or {}
    lines.append("7. RISK AND SHADOW PORTFOLIOS")
    if fs.get("note"):
        lines.append("  " + fs.get("note"))
    for s in (fs.get("shadows") or []):
        lines.append("   - %-28s ret %s dd %s vol %s cash %s vsSPY %s obs %s" % (
            _SHADOW_PRETTY.get(s.get("overlay"), s.get("overlay")),
            _fmt_ret(s.get("cumulative_return")), _fmt_ret(s.get("drawdown")),
            (_fmt_ret(s.get("realized_vol"))
             if s.get("realized_vol") is not None else "Not available"),
            (fmt_pct((s.get("cash") or 0) * 100.0)
             if s.get("cash") is not None else "Not available"),
            _fmt_ret(s.get("spy_excess")), fmt_int(s.get("observations"))))
    lines.append("  No shadow portfolio changes the active paper portfolio.")
    lines.append("")

    lines.append("8. HISTORICAL DATA READINESS")
    for r in (model.get("historical_readiness_summary") or []):
        lines.append("  %-32s : %s — %s" % (
            r.get("label", ""), r.get("status", ""), r.get("note", "")))
    if model.get("historical_gap_note"):
        lines.append("  " + model.get("historical_gap_note"))
    lines.append("")

    sh = model.get("source_agent_health") or {}
    nr = model.get("news_rss") or {}
    lines.append("9. SOURCE / AGENT HEALTH")
    lines.append("  Feeds healthy       : %s of %s" % (
        fmt_int(nr.get("healthy")), fmt_int(nr.get("enabled"))))
    failed = nr.get("failed_feeds") or []
    lines.append("  Feeds attention     : %s" % (
        ", ".join(str(f) for f in failed) if failed else "none"))
    lines.append("  Ledgers unchanged   : %s" % (
        "yes" if sh.get("ledgers_unchanged", True) else "NO"))
    if sh.get("stage7_age_note"):
        lines.append("  Research verdict age: %s" % sh.get("stage7_age_note"))
    lines.append("")

    lines.append("10. TECHNICAL APPENDIX")
    ev = model.get("evidence") or {}
    lines.append("  runtime run : %s" % (ev.get("run_id") or "n/a"))
    if rec:
        lines.append("  stage7 run  : %s" % (rec.get("run_id") or "n/a"))
        lines.append("  disposition : %s" % (rec.get("disposition") or "n/a"))
        for c in (rec.get("reconstruction_classes") or []):
            lines.append("  recon %s: %s" % (c.get("component"),
                                             c.get("class")))
    exp = model.get("experiment")
    lines.append("  --- EXPERIMENT & EVIDENCE (STAGE 5) ---")
    if exp:
        lines.append("  stage5 run : %s (%s)" % (exp.get("run_id") or "n/a",
                                                 exp.get("terminal")))
        lines.append("  keep_for_research: %s" %
                     fmt_int(exp.get("keep_for_research")))
    else:
        lines.append("  Stage 5 experiment engine not run this cycle")
    hr = model.get("historical_readiness")
    lines.append("  --- HISTORICAL DATA & EXPERIMENT READINESS (STAGE 6) ---")
    if hr:
        lines.append("  stage6 run  : %s" % (hr.get("run_id") or "n/a"))
        lines.append("  window      : %s" % _window_str(hr))
        lines.append("  universe    : %s" % _universe_str(hr))
    else:
        lines.append("  No Stage 6 historical backfill package yet")
    for p in ev.get("evidence_paths", []):
        lines.append("  %s: %s" % (p.get("label", "path"), p.get("path", "")))
    lines.append("  schema      : %s" % REPORT_SCHEMA_VERSION)
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
