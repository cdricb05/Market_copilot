"""
scripts/run_alpha_recovery.py — Alpha Agent Stage 7 CLI (alpha recovery).

Deterministic, strictly read-only recovery driver. Inventories the Stage 1..6
evidence, reconstructs the operational champion on survivorship-aware Norgate
history (owned, point-in-time-safe; the fundamental leg is UNVERIFIABLE and is
never fabricated), runs the fixed risk-overlay tournament on the current held-
name set, runs the bounded owned-price alpha campaign, synthesises the single
recovery disposition, produces a read-only manual de-risking preview only when
robust evidence supports it, and writes the immutable Stage 7 package.

It NEVER creates an order / fill / signal / trade decision, promotes a model,
mutates any portfolio / Alpha Paper Book / target / operational ledger, writes
Paper Trader PostgreSQL, calls the prediction service or a Daily Close, runs the
LLM, executes LLM-authored code, installs a package, or creates a scheduled task.

Modes:
    --mode observatory   read-only evidence inventory only (writes nothing)
    --mode dry-run       inventory + campaign plan only (writes nothing)
    --mode recovery      full recovery + immutable package
    --mode verify        read-only verification of the latest package

Terminal lines (exactly one):
    ALPHA_AGENT_STAGE7_READY
    ALPHA_AGENT_STAGE7_PARTIAL
    ALPHA_AGENT_STAGE7_VERIFIED
    ALPHA_AGENT_STAGE7_DRY_RUN
    ALPHA_AGENT_STAGE7_NEED_MORE_EVIDENCE
    ALPHA_AGENT_STAGE7_BLOCKED — <reason>

Example (Windows PowerShell):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      scripts\\run_alpha_recovery.py `
      --config configs\\alpha_agent\\stage7_alpha_recovery.json `
      --mode recovery --json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import champion_forensics as cf  # noqa: E402
from paper_trader.alpha_agent import evidence_observatory as eo  # noqa: E402
from paper_trader.alpha_agent import experiment_runner as er  # noqa: E402
from paper_trader.alpha_agent import risk_overlay_research as ro  # noqa: E402
from paper_trader.alpha_agent import runtime as rt  # noqa: E402


# --------------------------------------------------------------------------- #
# Read-only data acquisition (owned Stage 6 backfill + Norgate benchmark).
# --------------------------------------------------------------------------- #
def _load_price_panel(cfg: dict) -> dict:
    ing = cfg.get("ingestion_root") or (cfg.get("stage_roots") or {}).get("stage2")
    bounds = cfg.get("bounds") or {}
    store = er.NormalizedStore(str(ing),
                               max_files=int(bounds.get("max_files", 12000)),
                               max_symbols=int(bounds.get("max_symbols", 600)))
    return store.price_panel()


def _read_sector_map(cfg: dict) -> dict:
    ing = cfg.get("ingestion_root") or (cfg.get("stage_roots") or {}).get("stage2")
    root = Path(str(ing)) / "normalized" / "SECURITY_IDENTITY"
    out: dict[str, str] = {}
    if not root.exists():
        return out
    for f in sorted(root.rglob("*.jsonl"))[:4000]:
        try:
            text = f.read_text(encoding="utf-8-sig")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            tkr = rec.get("ticker")
            pay = rec.get("normalized_payload") or {}
            sec = pay.get("gics_sector") or pay.get("classification")
            if tkr and sec:
                out[str(tkr)] = str(sec)
    return out


def _fetch_benchmark(cfg: dict):
    """Read-only Norgate fetch of the SPY (or $SPX) benchmark as [(date, close)].
    Returns (series_or_None, source_label)."""
    try:
        from paper_trader.alpha_agent import historical_backfill as hb
    except Exception:  # noqa: BLE001
        return None, "UNAVAILABLE"
    adapter = hb.NorgateHistoricalAdapter({})
    ok, _ = adapter.available()
    if not ok:
        return None, "NORGATE_UNAVAILABLE"
    start = (cfg.get("date_range") or {}).get("start", "2015-01-01")
    for sym in (cfg.get("benchmark_symbol", "SPY"),
                cfg.get("benchmark_fallback_symbol", "$SPX")):
        try:
            arr = adapter._price_series(sym, start, None)
            if arr is None:
                continue
            names, rows = adapter._recarray_rows(arr)
            date_k = next((n for n in names if n.lower() in ("date", "datetime")),
                          None)
            close_k = next((n for n in names if n.lower() == "close"), None)
            if not date_k or not close_k:
                continue
            series = [(str(r[date_k])[:10], float(r[close_k])) for r in rows
                      if r.get(close_k) is not None]
            if series:
                return series, sym
        except Exception:  # noqa: BLE001
            continue
    return None, "UNAVAILABLE"


def _read_holdings(cfg: dict, recon: dict):
    """Read the CURRENT Alpha Paper Book #1 holdings read-only. Falls back to the
    reconstructed champion Top-N proxy when the operational book is unreadable in
    this environment. Returns (holdings, source, book_context, forward_context)."""
    champ = cfg.get("champion") or {}
    defaults = champ.get("operational_context_defaults") or {}
    count = int(champ.get("target_position_count", 25))
    invested = float(defaults.get("invested_pct", 95.3)) / 100.0
    book_context = {
        "champion_model": champ.get("model_id"),
        "book_name": champ.get("book_name"),
        "nav": defaults.get("nav"),
        "cumulative_pnl": defaults.get("cumulative_pnl"),
        "cumulative_pnl_pct": defaults.get("cumulative_pnl_pct"),
        "holdings_count": defaults.get("holdings_count"),
        "invested_pct": defaults.get("invested_pct"),
        "spy_cumulative_return": defaults.get("spy_cumulative_return"),
        "excess_vs_spy_pct": defaults.get("excess_vs_spy_pct"),
        "portfolio_realized_vol": defaults.get("portfolio_realized_vol"),
        "risk_checks_status": defaults.get("risk_checks_status"),
    }
    forward_context = {"observations": int(defaults.get("forward_observations",
                                                        0) or 0)}
    tickers: list[str] = []
    source = "RECONSTRUCTED_CHAMPION_PROXY"
    try:
        from paper_trader.api import operational_book as ob  # noqa: PLC0415
        payload = ob.load_operational_book()
        holdings_map = payload.get("holdings") or {}
        tickers = [str(t) for t in holdings_map.keys()]
        if payload.get("nav") is not None:
            book_context["nav"] = payload.get("nav")
            book_context["holdings_count"] = payload.get("holdings_count")
            inv = payload.get("invested")
            nav = payload.get("nav")
            if inv and nav:
                invested = float(inv) / float(nav)
                book_context["invested_pct"] = round(invested * 100.0, 2)
        if tickers:
            source = "OPERATIONAL_BOOK_EQUAL_WEIGHT"
    except Exception:  # noqa: BLE001 — read-only best effort; fall back below
        tickers = []
    if not tickers:
        tickers = list(recon.get("reconstructed_top_names") or [])[:count]
    tickers = tickers[:count] or tickers
    w = (invested / len(tickers)) if tickers else 0.0
    holdings = [{"ticker": t, "weight": w} for t in tickers]
    return holdings, source, book_context, forward_context


# --------------------------------------------------------------------------- #
# Deterministic markdown report (no LLM).
# --------------------------------------------------------------------------- #
def _report_md(*, run_id, inventory, recon, overlay, campaign, disposition,
               manual_preview, gaps, gate, book_context, holdings_source) -> str:
    f = recon.get("forensics") or {}
    L = []
    L.append("# Alpha Agent — Stage 7 Recovery Report")
    L.append("")
    L.append("Run: `%s`" % run_id)
    L.append("")
    L.append("SAFETY: RESEARCH ONLY · READ-ONLY · NO MODEL PROMOTION · NO ORDERS "
             "· PAPER ONLY · AUTOMATION OFF")
    L.append("")
    L.append("## Recovery disposition")
    L.append("")
    L.append("**%s**" % disposition.get("disposition"))
    L.append("")
    L.append(disposition.get("rationale", ""))
    L.append("")
    L.append("No operational model or holding changes in this phase.")
    L.append("")
    L.append("## Champion autopsy — fundamental_momentum_50_50_v1")
    L.append("")
    for c in (recon.get("classification") or []):
        L.append("- **%s** — %s: %s" % (c.get("class"), c.get("component"),
                                        c.get("detail")))
    L.append("")
    L.append("Price-leg forensics (survivorship-free, monthly, Top-%s):" %
             (recon.get("construction_match") or {}).get("target_position_count",
                                                         25))
    L.append("")
    L.append("- periods: %s, rank-IC t: %s, gross ann: %s, net ann: %s" % (
        f.get("periods"), _r(f.get("rank_ic_t")),
        _r(f.get("gross_annualized_return")), _r(f.get("net_annualized_return"))))
    L.append("- annualised vol: %s, max drawdown: %s, market beta: %s (%s)" % (
        _r(f.get("annualized_vol")), _r(f.get("max_drawdown")),
        _r(f.get("market_beta")), f.get("beta_benchmark")))
    L.append("- top-5 contribution share: %s, equal-dollar⇒unequal risk: %s" % (
        _r(f.get("top5_contribution_share")),
        f.get("equal_dollar_implies_unequal_risk")))
    L.append("")
    L.append("## Risk-overlay tournament (current control unchanged)")
    L.append("")
    L.append("Holdings source: %s · %s" % (holdings_source,
             overlay.get("evaluation_note", "")))
    L.append("")
    L.append("| overlay | net ann | vol | max dd | worst 20d | SPY excess | cash |")
    L.append("|---|---|---|---|---|---|---|")
    for r in (overlay.get("overlays") or []):
        L.append("| %s | %s | %s | %s | %s | %s | %s |" % (
            r.get("overlay"), _r(r.get("net_annualized_return")),
            _r(r.get("annualized_vol")), _r(r.get("max_drawdown")),
            _r(r.get("worst_20d")), _r(r.get("spy_excess_annualized")),
            _r(r.get("avg_cash_weight"))))
    L.append("")
    L.append("Manual de-risking preview: **%s**" % manual_preview.get("status"))
    L.append("")
    L.append("## Bounded alpha campaign (owned, point-in-time-safe)")
    L.append("")
    L.append("Experiments completed: %s · KEEP_FOR_RESEARCH: %s · held: %s" % (
        campaign.get("experiments_completed"),
        campaign.get("keep_for_research"), len(campaign.get("held") or [])))
    L.append("")
    for r in (campaign.get("results") or []):
        L.append("- %s → **%s** (rank-IC t %s)" % (
            r.get("label") or r.get("feature"), r.get("decision"),
            _r(r.get("rank_ic_t"))))
    L.append("")
    L.append("## Remaining data gaps")
    L.append("")
    for g in (gaps or []):
        L.append("- %s: %s" % (g.get("gap") or g.get("family") or "gap",
                               g.get("detail") or g.get("statement") or ""))
    L.append("")
    L.append("## Future-promotion checklist (nothing promoted now)")
    L.append("")
    for it in (gate.get("requirements") or []):
        mark = {True: "[x]", False: "[ ]", None: "[~]"}[it.get("met")]
        L.append("- %s %s" % (mark, it.get("requirement")))
    L.append("")
    L.append("Promotion allowed now: %s · Shadow challenger allowed now: %s" % (
        gate.get("promotion_allowed_now"),
        gate.get("shadow_challenger_allowed_now")))
    L.append("")
    return "\n".join(L)


def _r(v):
    if v is None:
        return "n/a"
    try:
        return "%.4f" % float(v)
    except (TypeError, ValueError):
        return str(v)


def _build_gaps(cfg, recon, campaign) -> list:
    gaps = []
    gaps.append({"gap": "POINT_IN_TIME_FUNDAMENTALS",
                 "detail": "Owned fundamentals are a current snapshot with no "
                 "lookahead-free per-period filing dates; the champion's "
                 "fundamental selection leg is UNVERIFIABLE historically "
                 "(Stage 6). Acquiring true PIT fundamentals is the primary "
                 "unblock."})
    for h in (campaign.get("held") or []):
        gaps.append({"gap": h.get("gap"), "family": h.get("feature"),
                     "detail": h.get("detail")})
    # Fold in the latest Stage 6 remaining gaps if present.
    s6root = (cfg.get("stage_roots") or {}).get("stage6")
    if s6root:
        latest = eo._read_json(Path(s6root) / "latest.json") or {}
        rid = latest.get("run_id")
        if rid:
            for g in eo._read_jsonl(Path(s6root) / "runs" / rid /
                                    "unresolved_data_gaps.jsonl"):
                gaps.append({"gap": "STAGE6_%s_%s" % (g.get("provider"),
                             g.get("family")), "detail": g.get("detail")})
    return gaps


# --------------------------------------------------------------------------- #
# Orchestration.
# --------------------------------------------------------------------------- #
def _run_recovery(cfg: dict, *, mode: str, as_of_arg: str) -> dict:
    ledger_fp = lambda: rt.fingerprint_ledgers(cfg)
    ledgers_before = ledger_fp()

    panel = _load_price_panel(cfg)
    if not panel:
        return {"terminal": eo.BLOCKED,
                "status": "no historical price panel under ingestion_root "
                "(run Stage 6 backfill first)"}
    as_of = as_of_arg
    if as_of in (None, "latest"):
        as_of = max(d for s in panel.values() for d, _ in s)

    sector_map = _read_sector_map(cfg)
    spy_series, spy_source = _fetch_benchmark(cfg)
    champ = cfg.get("champion") or {}
    top_n = int((cfg.get("bounds") or {}).get("reconstruction_top_n", 25))
    cost_bps = float((cfg.get("overlays") or {}).get("cost_bps", 10.0))
    gates = cfg.get("gates") or {}

    recon = cf.reconstruct_champion(
        panel, policy=champ, sector_map=sector_map, spy_series=spy_series,
        benchmark_source=("SPY" if spy_series else "EQUAL_WEIGHT_UNIVERSE_PROXY"),
        top_n=top_n, cost_bps=cost_bps)

    holdings, holdings_source, book_context, forward_context = \
        _read_holdings(cfg, recon)

    return_panel = ro.build_return_panel(panel)
    spy_returns = ro.series_to_returns(spy_series) if spy_series else \
        _market_proxy(return_panel)
    overlay = ro.run_overlay_tournament(
        holdings, return_panel, spy_returns=spy_returns, spy_levels=spy_series,
        sector_map=sector_map, specs=ro.default_overlay_specs(cfg),
        cost_bps=cost_bps, holdings_source=holdings_source)

    champ_returns = [p.get("book_return") for p in (recon.get("per_period") or [])]
    from paper_trader.alpha_agent import experiment_contracts as ec
    camp_gates = dict(ec.DEFAULT_GATES)
    for k, v in gates.items():
        if k in camp_gates and isinstance(v, (int, float)):
            camp_gates[k] = float(v)
    campaign = er.run_price_factor_campaign(
        panel, gates=camp_gates, cost_bps=cost_bps,
        champion_returns=champ_returns, spy_series=spy_series,
        bounds=cfg.get("bounds") or {})

    disposition = ro.synthesize_disposition(
        champion_recon=recon, overlay=overlay, campaign=campaign,
        forward_context=forward_context, gates=gates)
    manual_preview = ro.build_manual_risk_preview(
        overlay=overlay, disposition=disposition, holdings=holdings)
    promo_ev = ro.promotion_evidence(campaign=campaign, champion_recon=recon,
                                     overlay=overlay)
    gate = eo.promotion_checklist(promo_ev)
    gaps = _build_gaps(cfg, recon, campaign)

    inventory = eo.build_evidence_inventory(cfg, book_context=book_context,
                                            today=as_of)
    ev_fp = eo.evidence_fingerprint(inventory)
    run_id = eo.stage7_run_id(as_of, eo.config_hash(cfg), ev_fp)
    # Reflect the fresh recovery into the stage7 block + top-level disposition.
    inventory["stages"]["stage7"] = {
        "status": "PRESENT", "label": eo._STAGE_LABEL["stage7"],
        "run_id": run_id, "run_dir": str(Path(cfg["recovery_root"]) / "runs"
                                          / run_id),
        "as_of": as_of, "recovery_disposition": disposition["disposition"],
        "stale": False}
    inventory["recovery_disposition"] = disposition["disposition"]

    campaign_out = dict(campaign)
    campaign_out["overlay_specs"] = ro.default_overlay_specs(cfg)

    report_md = _report_md(run_id=run_id, inventory=inventory, recon=recon,
                           overlay=overlay, campaign=campaign,
                           disposition=disposition,
                           manual_preview=manual_preview, gaps=gaps, gate=gate,
                           book_context=book_context,
                           holdings_source=holdings_source)

    overlay_ok = overlay.get("status") == "OK"
    campaign_ok = (campaign.get("experiments_completed") or 0) > 0
    recon_ok = (recon.get("forensics") or {}).get("periods", 0) > 0
    if overlay_ok and campaign_ok and recon_ok:
        terminal = eo.READY
    elif recon_ok:
        terminal = eo.PARTIAL
    else:
        terminal = eo.NEED_MORE_EVIDENCE_TERMINAL

    stage7_input = {
        "as_of": as_of, "mode": mode, "config_hash": eo.config_hash(cfg),
        "champion_model": champ.get("model_id"),
        "holdings_source": holdings_source,
        "benchmark_source": spy_source,
        "sector_map_symbols": len(sector_map),
        "panel_symbols": len(panel),
        "bounds": cfg.get("bounds"),
        "terminal": terminal,
        "safety": {"research_and_read_only": True, "no_orders": True,
                   "no_promotion": True, "no_ledger_mutation": True,
                   "paper_only": True},
    }

    write = (mode == eo.MODE_RECOVERY)
    pkg = ro.assemble_and_write(
        cfg, run_id=run_id, as_of=as_of, stage7_input=stage7_input,
        inventory=inventory, champion_recon=recon, overlay=overlay,
        campaign=campaign_out, disposition=disposition,
        manual_preview=manual_preview, gaps=gaps, promotion=gate,
        report_md=report_md, write=write)

    ledgers_after = ledger_fp()
    ledgers_unchanged = (ledgers_before == ledgers_after)
    if not ledgers_unchanged:
        return {"terminal": eo.BLOCKED,
                "status": "operational ledger changed during recovery run"}

    return {
        "terminal": terminal, "run_id": run_id, "run_dir": pkg["run_dir"],
        "disposition": disposition["disposition"],
        "holdings_source": holdings_source, "benchmark_source": spy_source,
        "overlay_status": overlay.get("status"),
        "experiments_completed": campaign.get("experiments_completed"),
        "keep_for_research": campaign.get("keep_for_research"),
        "manual_preview_status": manual_preview.get("status"),
        "ledgers_unchanged": ledgers_unchanged,
        "champion_rank_ic_t": (recon.get("forensics") or {}).get("rank_ic_t"),
        "inventory": inventory, "written": write,
    }


def _market_proxy(return_panel: dict) -> dict:
    """Equal-weight universe daily return as a benchmark proxy when SPY is
    unavailable."""
    by_date: dict[str, list] = {}
    for _, rmap in return_panel.items():
        for d, r in rmap.items():
            by_date.setdefault(d, []).append(r)
    return {d: sum(v) / len(v) for d, v in by_date.items() if v}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Alpha Agent Stage 7 alpha-recovery engine.")
    ap.add_argument("--config", required=True)
    ap.add_argument("--mode", required=True, choices=list(eo.MODES))
    ap.add_argument("--as-of", dest="as_of", default="latest")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    try:
        cfg = eo.load_config(args.config)
    except eo.ConfigError as exc:
        print("%s — %s" % (eo.BLOCKED, exc))
        return 1

    try:
        if args.mode == eo.MODE_VERIFY:
            result = ro.verify_recovery(
                cfg, ledger_fingerprint=lambda: rt.fingerprint_ledgers(cfg))
        elif args.mode == eo.MODE_OBSERVATORY:
            inv = eo.observatory_payload(cfg=cfg, today=args.as_of if
                                         args.as_of != "latest" else None)
            if args.json:
                print(json.dumps(inv, indent=1, sort_keys=True, default=str))
            hs = inv.get("highlights") or {}
            print("OBSERVATORY records=%s analyses=%s experiments=%s gaps=%s "
                  "disposition=%s" % (
                      hs.get("data_collected_records"),
                      hs.get("analyses_completed"),
                      hs.get("experiments_completed"), hs.get("data_gaps"),
                      inv.get("recovery_disposition")))
            print(eo.READY)
            return 0
        else:
            result = _run_recovery(cfg, mode=args.mode, as_of_arg=args.as_of)
    except Exception as exc:  # noqa: BLE001 — one clean BLOCKED line
        print("%s — %s: %s" % (eo.BLOCKED, type(exc).__name__,
                               eo.redact(str(exc))))
        return 1

    if args.json:
        payload = {k: v for k, v in result.items() if k != "inventory"}
        print(json.dumps(payload, indent=1, sort_keys=True, default=str))

    terminal = result.get("terminal")
    if args.mode == eo.MODE_DRY_RUN and terminal in (eo.READY, eo.PARTIAL):
        terminal = eo.DRY_RUN
    if result.get("disposition"):
        print("RECOVERY_DISPOSITION %s" % result.get("disposition"))
    reason = result.get("status") or result.get("disposition") or ""
    if terminal in (eo.BLOCKED, eo.PARTIAL, eo.NEED_MORE_EVIDENCE_TERMINAL):
        print("%s — %s" % (terminal, reason))
    else:
        print(terminal)
    return 1 if terminal == eo.BLOCKED else 0


if __name__ == "__main__":
    raise SystemExit(main())
