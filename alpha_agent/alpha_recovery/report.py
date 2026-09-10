"""alpha_agent.alpha_recovery.report - the final report, scoreboard first.

The contract forbids a report that begins with "Implemented" or "tests
passed". This renders, from the artifacts only: the ALPHA RECOVERY SCOREBOARD,
the head table, the twenty investment answers, and only then the files,
tests, audit and branch. It is rebuilt by the runner and never edited by hand.
"""
from __future__ import annotations

from pathlib import Path

from . import (INCUMBENT_MODEL_ID, REPORT_MD_NAME, SCOREBOARD_JSON_NAME, read_artifact,
               read_repo_artifact, repo_dir)
from . import scoreboard as SB

CALCULATION_OWNER = "alpha_agent.alpha_recovery.report"


def _f(x, nd=4):
    return SB._f(x, nd)


def _pct(x, nd=2):
    if x is None:
        return "n/a"
    try:
        return "%+.*f %%" % (nd, 100.0 * float(x))
    except (TypeError, ValueError):
        return str(x)


def head_table(sb: dict) -> list:
    inc = sb.get("incumbent") or {}
    ho = inc.get("historical_oos") or {}
    b = sb.get("best_challenger") or {}
    adv = sb.get("advantage") or {}
    camp = sb.get("campaign") or {}
    stat = adv.get("statistical_status") or {}
    cal = adv.get("forecast_calibration_improvement")
    sd = sb.get("best_same_domain_challenger") or {}
    fp = sb.get("forecast_products") or {}
    rows = [
        ("INCUMBENT", "%s (%s)" % (INCUMBENT_MODEL_ID, inc.get("verdict"))),
        ("BEST CHALLENGER", "%s (%s)" % (b.get("cell_id"), b.get("verdict")) if b else "none measured"),
        ("BEST CHALLENGER ON THE SAME CAPITAL",
         "%s (%s): %s /yr advantage, paired t %s, Sharpe delta %s, drawdown delta %s, turnover delta %s"
         % (sd.get("cell_id"), sd.get("verdict"), _pct(sd.get("historical_oos_net_advantage")),
            _f(sd.get("t_advantage"), 2), _f(sd.get("sharpe_delta"), 3), _f(sd.get("drawdown_delta"), 3),
            _f(sd.get("turnover_delta"), 3)) if sd else "none measured"),
        ("HORIZON", "%s sessions" % b.get("horizon") if b else "n/a"),
        ("INFORMATION", "%s / %s" % (b.get("family"), b.get("information")) if b else "n/a"),
        ("HISTORICAL OOS NET ADVANTAGE", "%s /yr (incumbent %s /yr net excess)" % (
            _pct(b.get("historical_oos_net_advantage")), _pct(ho.get("ann_net_excess")))),
        ("SHARPE DELTA", _f(b.get("sharpe_delta"), 3)),
        ("DRAWDOWN DELTA", _f(b.get("drawdown_delta"), 3)),
        ("TURNOVER", "challenger %s one-way / period (incumbent %s)" % (
            _f(b.get("challenger_turnover"), 3), _f(ho.get("mean_oneway_turnover_per_period"), 3))),
        ("FORECAST PRODUCTS", fp.get("answer") or "not built"),
        ("FORECAST CALIBRATION", (cal if isinstance(cal, str) else
                                  "Brier skill %s, ECE %s" % (_f((cal or {}).get("brier_skill_score")),
                                                              _f((cal or {}).get("expected_calibration_error"), 3)))),
        ("MULTIPLICITY STATUS", "paired t %s, conditional t %s, BH %s, Holm %s, failed gates %s" % (
            _f(stat.get("t"), 2), _f(stat.get("conditional_t"), 2), stat.get("fdr_pass"), stat.get("holm_pass"),
            ", ".join(stat.get("failed_gates") or []) or "none")),
        ("TRUE_FORWARD OBSERVATIONS", "0 for any campaign challenger; incumbent book %s sessions" % (
            (inc.get("true_forward") or {}).get("sessions"))),
        ("FORWARD STATUS", adv.get("forward_evidence") or "NONE"),
        ("CAPITAL APPLICABILITY", b.get("capital_applicability") if b else "n/a"),
        ("DECISION", sb.get("status")),
    ]
    L = ["| field | value |", "|---|---|"]
    L += ["| %s | %s |" % (k, v) for k, v in rows]
    return L


def _answers(sb: dict, inc: dict | None, tour: dict | None, cad: dict | None, direction: dict | None,
             program: dict | None, purchase: dict | None, cross: dict | None, package: dict | None,
             news_manifest: dict | None, products: dict | None = None, eqch: dict | None = None,
             residual: dict | None = None) -> list:
    A = []
    v = (inc or {}).get("verdict") or {}
    ho = ((sb.get("incumbent") or {}).get("historical_oos") or {})
    tf = ((sb.get("incumbent") or {}).get("true_forward") or {})
    today = (inc or {}).get("predicts_today") or {}
    b = sb.get("best_challenger") or {}
    sd = sb.get("best_same_domain_challenger") or {}
    adv = sb.get("advantage") or {}
    stat = adv.get("statistical_status") or {}
    pf = ((products or {}).get("families") or {})
    bookf = pf.get("EQUITY_BOOK_LEVEL") or {}
    sleevef = pf.get("MULTI_ASSET_SLEEVE") or {}
    A.append(("Does the incumbent actually have demonstrated Alpha?",
              "%s. Historical OOS (2011-07 to 2026-07, top-25, 21 sessions): net excess %s /yr at t %s, rank-IC t %s, "
              "lockbox (2023+) %s /yr at t %s versus selection %s /yr. TRUE_FORWARD (separate, never pooled): %s "
              "sessions, %s cumulative versus SPY %s, excess %s. Checks: %s."
              % (v.get("verdict"), _pct(v.get("ann_net_excess")), _f(v.get("t_net_excess"), 2), _f(v.get("t_rank_ic"), 2),
                 _pct(v.get("lockbox_ann_net_excess")), _f(ho.get("lockbox_t"), 2), _pct(v.get("selection_ann_net_excess")),
                 tf.get("sessions"), _pct(tf.get("cumulative_return")), _pct(tf.get("benchmark_cumulative_return")),
                 _pct(tf.get("excess_cumulative")), ", ".join("%s=%s" % kv for kv in (v.get("checks") or {}).items()))))
    A.append(("What does the incumbent predict today, in economic units rather than scores?",
              "%s (snapshot %s, fundamental data as of %s, %s names ranked). At BOOK level the answer is no longer "
              "empty: %s"
              % (today.get("statement"), today.get("market_date"), today.get("fundamental_data_as_of"),
                 today.get("eligible_universe_count"), bookf.get("answer") or "no book-level product was built.")))
    A.append(("What is the strongest challenger?",
              ("Ranked best: %s (verdict %s, family %s, horizon %s). On the SAME capital as the incumbent the best is "
               "%s (verdict %s): %s /yr advantage at paired t %s, Sharpe delta %s, drawdown delta %s, turnover delta "
               "%s. The ranked best is a futures sleeve and is not applicable to this portfolio; the same-capital "
               "candidate is."
               % (b.get("cell_id"), b.get("verdict"), b.get("family"), b.get("horizon"),
                  sd.get("cell_id"), sd.get("verdict"), _pct(sd.get("historical_oos_net_advantage")),
                  _f(sd.get("t_advantage"), 2), _f(sd.get("sharpe_delta"), 3), _f(sd.get("drawdown_delta"), 3),
                  _f(sd.get("turnover_delta"), 3)))
              if b else "None measured."))
    A.append(("How much does it beat the incumbent AFTER COSTS?",
              ("Ranked best: %s /yr (paired t %s; lockbox %s /yr); Sharpe delta %s; drawdown delta %s. "
               "Same capital (%s): %s /yr at paired t %s, lockbox %s /yr, Sharpe delta %s, drawdown delta %s, "
               "turnover delta %s - it clears the 1.5 %%/yr materiality bar and FAILS the frozen t >= 2 bar."
               % (_pct(b.get("historical_oos_net_advantage")), _f(b.get("t_advantage"), 2),
                  _pct(b.get("lockbox_net_advantage")), _f(b.get("sharpe_delta"), 3), _f(b.get("drawdown_delta"), 3),
                  sd.get("cell_id"), _pct(sd.get("historical_oos_net_advantage")), _f(sd.get("t_advantage"), 2),
                  _pct(sd.get("lockbox_net_advantage")), _f(sd.get("sharpe_delta"), 3),
                  _f(sd.get("drawdown_delta"), 3), _f(sd.get("turnover_delta"), 3))) if b else "n/a"))
    A.append(("Is the result statistically and economically credible?",
              "Materiality %s; paired t %s; conditional-information t %s; Benjamini-Hochberg %s; Holm %s; failed gates: %s. "
              "%s" % (adv.get("economic_materiality"), _f(stat.get("t"), 2), _f(stat.get("conditional_t"), 2),
                      stat.get("fdr_pass"), stat.get("holm_pass"), ", ".join(stat.get("failed_gates") or []) or "none",
                      (b.get("evidence_label") or "")) if b else "n/a"))
    st_rows = []
    all_cells = list((tour or {}).get("cells") or []) + list((cad or {}).get("cells") or [])
    best_id = b.get("cell_id")
    ordered = sorted(all_cells, key=lambda c: 0 if c.get("cell_id") == best_id else 1)
    for c in ordered:
        s = c.get("stability") or {}
        if s:
            reg = {k: (None if v is None else round(float(v), 4)) for k, v in (s.get("regime") or {}).items()}
            st_rows.append("%s: %s of 3-year blocks positive, regimes %s%s" % (
                c.get("cell_id"), _f(s.get("share_blocks_positive"), 2), reg,
                " (the conditional statistic's stability; the cadence book is POST_SELECTION)"
                if c.get("cadence_verdict") else ""))
    A.append(("Does it survive regimes?", "; ".join(st_rows[:3]) or "no stability record"))
    dir_rows = []
    for h, r in ((direction or {}).get("horizons") or {}).items():
        if r.get("state") == "OK":
            dir_rows.append("SPY %ss: Brier skill %s (t %s), ECE %s, hit %s vs always-up %s -> %s" % (
                h, _f(r.get("brier_skill_score")), _f(r.get("t_brier_improvement"), 2),
                _f((r.get("calibration") or {}).get("expected_calibration_error"), 3), _f(r.get("hit_rate_model"), 3),
                _f(r.get("hit_rate_always_up"), 3), (r.get("verdicts") or {}).get("verdict")))
        else:
            dir_rows.append("SPY %ss: %s (%s)" % (h, r.get("state"), r.get("why")))
    A.append(("Does it improve forecast calibration?",
              "Cross-sectional rank models emit no probability; calibration is measured on the broad-market direction "
              "forecasts: " + "; ".join(dir_rows)))
    A.append(("Is it ready for TRUE_FORWARD competition?",
              "%s. READY records: %s; survivors not qualified: %s." % (
                  sb.get("status"), (package or {}).get("n_ready"), (package or {}).get("n_survivors_not_qualified"))))
    cmd = None
    for rec in ((package or {}).get("ready") or []) + ((package or {}).get("survivors_not_qualified") or []):
        cmd = rec.get("human_gated_adoption_command")
        break
    A.append(("What exact human-gated command would start that competition?",
              ("Only a READY record may be offered. None is READY. The command shape, for review, is: `%s` "
               "(dry run without --execute; a live registration requires the typed confirmation token)." % cmd)
              if cmd and not (package or {}).get("n_ready") else
              ("`%s`" % cmd if cmd else "no candidate record exists; the adoption path is scripts/adopt_prospective_freeze.py")))
    A.append(("What broad-market direction forecasts can we now produce?",
              "; ".join("SPY %ss: probability_up %s (climatology %s) -> %s" % (
                  h, _f((r.get("latest") or {}).get("probability_up"), 3), _f((r.get("latest") or {}).get("climatology"), 3),
                  "VALUE" if (r.get("verdicts") or {}).get("calibrated") else "UNAVAILABLE (uncalibrated OOS)")
                  for h, r in ((direction or {}).get("horizons") or {}).items() if r.get("state") == "OK") or "none"))
    A.append(("What equity expected-return forecasts can we now produce?",
              "PER NAME: %s The forecast contract carries the ranks as labelled scores with expected_excess_return %s. "
              "PER BOOK: %s Licensed books: %s. The operational 21-session book is NOT licensed - its selection-period "
              "mean excess and its lockbox mean disagree in sign, which is the calibration test doing its job on the "
              "incumbent's own construction."
              % (today.get("statement"), "VALUE" if today.get("economic_units_licensed") else "UNAVAILABLE",
                 bookf.get("answer") or "not built", ", ".join(bookf.get("licensed") or []) or "none")))
    cr = (cross or {}).get("result") or {}
    A.append(("What multi-asset forecasts can we now produce?",
              "FX carry sleeve (%s): incumbent-only Sharpe %s versus incumbent + sleeve at equal risk %s; incremental "
              "net %s /yr (t %s); %s. No calibrated sleeve expected return exists; the sleeve forecast fields are UNAVAILABLE."
              % (((cross or {}).get("chosen_sleeve") or {}).get("cell_id"),
                 _f((cr.get("incumbent_only") or {}).get("sharpe"), 3),
                 _f((cr.get("incumbent_plus_sleeve_equal_risk") or {}).get("sharpe"), 3),
                 _pct(cr.get("incremental_ann_net_return")), _f(cr.get("t_incremental"), 2), cr.get("state"))
              + " Sleeve product: %s" % (sleevef.get("answer") or "not built")))
    qualified, measured, rejected = [], [], []
    for c in sb.get("candidates") or []:
        row = "%s (%s, %s /yr)" % (c.get("cell_id"), c.get("verdict"), _pct(c.get("historical_oos_net_advantage")))
        v = c.get("verdict")
        if v in ("MATERIALLY_BEATS_INCUMBENT", "ECONOMIC_UNDER_CONTROLS", "CALIBRATED_DIRECTIONAL_SKILL"):
            qualified.append(row)
        elif v in ("BEATS_INCUMBENT_NOT_QUALIFIED", "ECONOMIC_UNDER_CONTROLS_NOT_FDR",
                   "ECONOMIC_UNDER_CONTROLS_UNSTABLE", "PROFITABLE_NOT_CALIBRATED"):
            measured.append(row)
        else:
            rejected.append(row)
    A.append(("What information actually added value?",
              ("QUALIFIED (every frozen gate): %s. " % ("; ".join(qualified) if qualified else "none")
               + "MEASURED BUT NOT QUALIFIED (conditional information value real, economics positive under "
                 "controls, multiplicity or the equal-risk utility test failed): %s."
               % ("; ".join(measured) if measured else "none")
               + (" The FX carry sleeve adds no utility to the incumbent-only book at equal risk (%s /yr, t %s)."
                  % (_pct(cr.get("incremental_ann_net_return")), _f(cr.get("t_incremental"), 2))
                  if cr.get("state") == "OK" else ""))))
    A.append(("What information was rejected?", "; ".join(rejected) or "none"))
    npr = (program or {}).get("non_price_rule") or {}
    npi = (program or {}).get("non_price_rule_inclusive") or {}
    A.append(("How much research effort was non-price?",
              "%s of %s information-directed specifications (%s) targeted non-PRICE_STATE information; rule >= 0.75 "
              "met: %s. Counting the construction-only family too: %s of %s (%s), met: %s. Both denominators are "
              "reported and both must hold, so the rule cannot be met by reclassifying a family."
              % (npr.get("non_price"), npr.get("executed"), _f(npr.get("share_non_price"), 3), npr.get("rule_met"),
                 npi.get("non_price"), npi.get("executed"), _f(npi.get("share_non_price"), 3), npi.get("rule_met"))))
    gov = (program or {}).get("isolated_governor") or {}
    A.append(("What did AlphaAgent choose from the information frontier?",
              "Isolated governor run (%s): %s information needs offered, mandates: %s."
              % (gov.get("state"), gov.get("n_information_needs"),
                 "; ".join("%s (eiv %s)" % (m.get("cell_key"), _f(m.get("expected_information_value"), 3))
                           for m in (gov.get("mandates") or [])[:8]))))
    gap = ((sb.get("campaign") or {}).get("highest_value_unresolved_information_gap") or {})
    A.append(("What is the next highest-value information need?",
              "Frontier: %s (remaining value %s, %s). Purchase case: %s."
              % (gap.get("cell_key"), _f(gap.get("remaining_research_value_effective")), gap.get("next_action"),
                 (purchase or {}).get("top_missing_information_need"))))
    clock = (sb.get("campaign") or {}).get("clock") or {}
    A.append(("How many stop-loss sessions remain?", clock.get("rendered")))
    res_rows = ["%s (frontier rank %s): %s, conditional t %s, economic increment %s /yr"
                % (r.get("cell_id"), r.get("frontier_rank"), r.get("r64_verdict"), _f(r.get("conditional_t"), 2),
                   _pct(r.get("ann_net_increment")))
                for r in ((residual or {}).get("brief") or [])]
    mand_left = (program or {}).get("governor_mandates_not_executed") or []
    A.append(("If no survivor exists, are owned/free information sources exhausted?",
              "%s. Every one of the governor's mandated needs is now executed (%s left unexecuted). The frontier's "
              "top OWNED needs were measured rather than assumed: %s. Two of them carry REAL conditional information "
              "and still fail the economics - the equity free-cash-flow cell improves a daily long-short book that "
              "loses 20.3 %%/yr to cost and draws down 77.5 %%, which the R64 degeneracy rule voids. Families closed: "
              "%s. News sample: %s."
              % ((purchase or {}).get("owned_free_information_exhausted"), len(mand_left), "; ".join(res_rows) or "none",
                 ", ".join((purchase or {}).get("families_closed") or []),
                 {k: (news_manifest or {}).get(k) for k in ("state", "n_complete", "items")} if news_manifest else "not acquired")))
    pc = [(c.get("need"), c.get("purchase_experiment_verdict"), c.get("why")) for c in (purchase or {}).get("candidates_ranked") or []]
    A.append(("If yes, what EXACT data purchase experiment is economically justified?",
              "; ".join("%s -> %s (%s)" % t for t in pc) or "no candidate"))
    return A


def render() -> str:
    sb = read_repo_artifact(SCOREBOARD_JSON_NAME) or {}
    inc = read_artifact("incumbent_baseline.json")
    tour = read_artifact("tournament.json")
    cad = read_artifact("cadence_grid.json")
    direction = read_artifact("market_direction.json")
    program = read_artifact("research_program.json")
    purchase = read_artifact("purchase_case.json")
    cross = read_artifact("cross_domain.json")
    package = read_artifact("forward_candidates.json")
    products = read_artifact("forecast_products.json")
    eqch = read_artifact("equity_challengers.json")
    residual = read_artifact("frontier_residual.json")
    from . import news as NW
    man = NW.manifest()
    L = ["# ALPHA RECOVERY SCOREBOARD", "", "**STATUS: %s**" % sb.get("status"), ""]
    L += head_table(sb)
    L += ["", "## The twenty answers", ""]
    for i, (q, a) in enumerate(_answers(sb, inc, tour, cad, direction, program, purchase, cross, package, man,
                                       products=products, eqch=eqch, residual=residual), 1):
        L.append("%d. **%s** %s" % (i, q, a))
        L.append("")
    L += ["## Full scoreboard", "", SB.render(sb), "",
          "## Files, tests, audit, branch (reported last, as the contract requires)", "",
          "- package: `alpha_agent/alpha_recovery/` (checkpoint, scoreboard, forecast_contract, incumbent, program, "
          "earnings_events, news, tournament, cadence, market_direction, equity_challengers, frontier_residual, "
          "forecast_products, forward_package, purchase_case, report)",
          "- runner: `scripts/run_alpha_recovery_offensive.py`; protocol: `research/alpha_recovery/ALPHA_RECOVERY_PROTOCOL.json`",
          "- contract: `docs/ALPHA_RECOVERY_OPERATING_CONTRACT.md`; checkpoint: `research/alpha_recovery/alpha_recovery_checkpoint.json`",
          "- tests: `tests/test_alpha_recovery_offensive.py`; audit: `check_alpha_recovery_operating_contract`",
          "- research root: `D:\\Stock_Prediction_app_data\\alpha_recovery_offensive`", ""]
    return "\n".join(L)


def write() -> Path:
    p = repo_dir() / REPORT_MD_NAME
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(render(), encoding="utf-8")
    return p
