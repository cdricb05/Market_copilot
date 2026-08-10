#!/usr/bin/env python
r"""Intrinio TRIAL alpha evaluation campaign (research-only, offline).

Evaluates the PRE-REGISTERED Intrinio-supported fundamental signal family over
the acquired trial data using the RELEASED evaluation machinery — no parallel
research framework:

  * cross-sections     -> alpha_agent.signal_evaluation.evaluate_periods
  * gate thresholds    -> alpha_agent.tournament.classify_evidence
                          (configs/alpha_agent/stage9_tournament.json)
  * owned baseline     -> alpha_agent.fundamental_evidence.evaluate_fundamental_evidence
                          over the owned SEC companyfacts PIT store
  * orthogonality      -> alpha_agent.orthogonality
  * multiple testing   -> alpha_agent.analyst_revisions._bh_fdr (BH-FDR)
  * price panel        -> alpha_agent.historical_price_panel (owned Norgate
                          MARKET_BAR, survivorship-safe, assetid-anchored;
                          2015+ main tree plus the pre-2015 extension)

PRE-REGISTERED FAMILY (size 6, fixed BEFORE evaluation; signs documented):
  intrinio_fcf_to_assets      (+)  TTM(CFO - |capex|) / assets      [champion family]
  intrinio_operating_accruals (-)  TTM(NI - CFO) / assets (Sloan)   [champion family]
  intrinio_gross_profitability(+)  TTM gross profit / assets        [owned overlap]
  intrinio_asset_growth       (-)  yoy assets change                [owned overlap]
  intrinio_earnings_momentum  (+)  yoy change in TTM NI / assets
  intrinio_equity_ratio       (+)  total equity / assets            [owned overlap]

PIT DISCIPLINE: formation at date d uses only fundamentals whose measured
availability (reported filing_date, else original first_calculable) is <= d.
VALUE CONSTRAINT (measured, documented): Intrinio bulk values are the LATEST
standardized values (restatements folded in) — the restatement bias is measured
against the owned SEC as-first-reported PIT store and reported alongside every
result. Trial data is TRIAL-licensed research-only: this campaign never writes
the tournament registry, an operational ledger, or any portfolio state.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import analyst_revisions as _ar          # noqa: E402
from paper_trader.alpha_agent import fundamental_evidence as _fev      # noqa: E402
from paper_trader.alpha_agent import orthogonality as _orth            # noqa: E402
from paper_trader.alpha_agent import signal_evaluation as _sev         # noqa: E402
from paper_trader.alpha_agent import tournament as _tour               # noqa: E402
from paper_trader.alpha_agent.historical_identity import IdentityStore  # noqa: E402
from paper_trader.alpha_agent.historical_price_panel import (           # noqa: E402
    build_assetid_price_panel, build_cik_to_assetid)

TRIAL_ROOT = Path(r"D:\Stock_Prediction_app_data\provider_trials\intrinio")
IDENTITY_DB = Path(r"D:\Stock_Prediction_app_data\alpha_agent\identity\historical_identity.sqlite")
INGESTION_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion")
PRE2015_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\pre2015\ingestion")
STAGE9_CFG = _REPO / "configs" / "alpha_agent" / "stage9_tournament.json"
OUT_DIR = Path(r"D:\Temp\paper_trader_intrinio_live_trial_handoff")

HORIZON = 63
FAMILY = {
    # signal -> (expected_sign, rationale)
    "intrinio_fcf_to_assets": (1, "champion family long leg: free-cash-flow yield on assets"),
    "intrinio_operating_accruals": (-1, "champion family short leg: Sloan operating accruals"),
    "intrinio_gross_profitability": (1, "Novy-Marx quality; owned-SEC overlap control"),
    "intrinio_asset_growth": (-1, "Cooper-Gulen-Schill investment; owned-SEC overlap control"),
    "intrinio_earnings_momentum": (1, "fundamental momentum: yoy TTM earnings change scaled by assets"),
    "intrinio_equity_ratio": (1, "balance-sheet quality; owned-SEC overlap control"),
}
FAMILY_SIZE = len(FAMILY)
FDR_ALPHA = 0.05


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# Intrinio PIT-constrained fundamental store (from the acquired census + tags)
# --------------------------------------------------------------------------- #
class IntrinioFundamentals:
    """Per-CIK quarterly periods with measured availability.

    period: {end, avail, values{tag}} sorted by end. avail = min reported
    filing_date for that period end, else min original first_calculable.
    """

    def __init__(self, meta_dir: Path, tags_dir: Path, *, max_securities=None):
        self.by_cik: dict[str, list] = {}
        self.assetid_by_cik: dict[str, str] = {}
        n = 0
        for tp in sorted(tags_dir.glob("*.json")):
            rec = json.loads(tp.read_text(encoding="utf-8"))
            cik = rec.get("cik")
            if not cik:
                continue
            mp = meta_dir / tp.name
            if not mp.is_file():
                continue
            meta = json.loads(mp.read_text(encoding="utf-8"))
            avail = self._availability(meta.get("fundamentals") or [])
            values = defaultdict(dict)
            for tag, series in (rec.get("series") or {}).items():
                for pt in series or []:
                    d, v = pt.get("date"), pt.get("value")
                    if d and v is not None:
                        values[d][tag] = float(v)
            periods = []
            for end in sorted(values):
                a = avail.get(end)
                if a is None:
                    continue        # no measured availability -> never usable
                periods.append({"end": end, "avail": a, "values": values[end]})
            if periods:
                self.by_cik[cik] = periods
                aid = (meta.get("security") or {}).get("norgate_assetid")
                if aid:
                    self.assetid_by_cik[cik] = str(aid)
                n += 1
            if max_securities and n >= max_securities:
                break

    @staticmethod
    def _availability(funds: list) -> dict:
        rep = defaultdict(list)
        fc = defaultdict(list)
        for f in funds:
            end = f.get("end_date")
            if not end:
                continue
            if f.get("statement_code") not in ("income_statement",
                                               "balance_sheet_statement",
                                               "cash_flow_statement",
                                               "calculations"):
                continue
            if f.get("type") == "reported" and f.get("filing_date"):
                rep[end].append(str(f["filing_date"])[:10])
            if f.get("first_calculable"):
                fc[end].append(str(f["first_calculable"])[:10])
        out = {}
        for end in set(rep) | set(fc):
            cand = rep.get(end) or fc.get(end)
            if cand:
                a = min(cand)
                if a > end:          # availability must postdate the period end
                    out[end] = a
        return out

    def periods_available(self, cik: str, as_of: str) -> list:
        return [p for p in self.by_cik.get(cik, []) if p["avail"] <= as_of]


def _ttm(periods: list, tag: str, *, end_idx: int) -> float | None:
    """Sum of the last 4 quarterly values of ``tag`` ending at periods[end_idx],
    requiring 4 values whose period ends span <= 380 days (true trailing year)."""
    vals, ends = [], []
    i = end_idx
    while i >= 0 and len(vals) < 4:
        v = periods[i]["values"].get(tag)
        if v is None:
            return None
        vals.append(v)
        ends.append(periods[i]["end"])
        i -= 1
    if len(vals) < 4:
        return None
    d0 = _dt.date.fromisoformat(ends[-1])
    d1 = _dt.date.fromisoformat(ends[0])
    if (d1 - d0).days > 380:
        return None
    return sum(vals)


def _yoy_idx(periods: list, end_idx: int) -> int | None:
    """Index of the comparable period ~1 year before periods[end_idx]."""
    target = _dt.date.fromisoformat(periods[end_idx]["end"])
    for j in range(end_idx - 1, -1, -1):
        d = _dt.date.fromisoformat(periods[j]["end"])
        dd = (target - d).days
        if 320 <= dd <= 420:
            return j
        if dd > 420:
            break
    return None


def compute_intrinio_signal(periods: list, signal: str, as_of: str) -> float | None:
    """Compute one pre-registered signal from the availability-filtered periods."""
    if not periods:
        return None
    i = len(periods) - 1
    p = periods[i]
    assets = p["values"].get("totalassets")
    if assets is None or assets <= 0:
        return None
    if signal == "intrinio_equity_ratio":
        eq = p["values"].get("totalequity")
        return (eq / assets) if eq is not None else None
    if signal == "intrinio_asset_growth":
        j = _yoy_idx(periods, i)
        if j is None:
            return None
        a0 = periods[j]["values"].get("totalassets")
        if a0 is None or a0 == 0:
            return None
        return (assets - a0) / abs(a0)
    if signal == "intrinio_gross_profitability":
        gp = _ttm(periods, "totalgrossprofit", end_idx=i)
        return (gp / assets) if gp is not None else None
    cfo = _ttm(periods, "netcashfromoperatingactivities", end_idx=i)
    ni = _ttm(periods, "netincome", end_idx=i)
    if signal == "intrinio_fcf_to_assets":
        capex = _ttm(periods, "purchaseofplantpropertyandequipment", end_idx=i)
        if cfo is None or capex is None:
            return None
        return (cfo - abs(capex)) / assets
    if signal == "intrinio_operating_accruals":
        if ni is None or cfo is None:
            return None
        return (ni - cfo) / assets
    if signal == "intrinio_earnings_momentum":
        j = _yoy_idx(periods, i)
        if j is None or ni is None:
            return None
        ni0 = _ttm(periods, "netincome", end_idx=j)
        if ni0 is None:
            return None
        return (ni - ni0) / assets
    return None


# --------------------------------------------------------------------------- #
def quarterly_formation_dates(panel: dict, *, start: str, horizon: int) -> list:
    all_dates = sorted({d for bars in panel.values() for d, _ in bars})
    if not all_dates:
        return []
    last_ok = all_dates[-1 - horizon] if len(all_dates) > horizon else None
    out = []
    y0 = int(start[:4])
    y1 = int(all_dates[-1][:4])
    for y in range(y0, y1 + 1):
        for m in (1, 4, 7, 10):
            anchor = "%04d-%02d-01" % (y, m)
            if anchor < start:
                continue
            nxt = next((d for d in all_dates if d >= anchor), None)
            if nxt and (last_ok is None or nxt <= last_ok):
                out.append(nxt)
    return sorted(set(out))


def _p_from_t(t: float | None) -> float:
    if t is None:
        return 1.0
    return max(1e-12, 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(t) / math.sqrt(2.0)))))


def _series_corr(kept_a, ls_a, kept_b, ls_b):
    m_b = dict(zip(kept_b, ls_b))
    pairs = [(a, m_b[k]) for k, a in zip(kept_a, ls_a) if k in m_b]
    if len(pairs) < 8:
        return None, len(pairs)
    return _orth.factor_correlation([x for x, _ in pairs], [y for _, y in pairs]), len(pairs)


def main() -> int:
    ap = argparse.ArgumentParser(description="Intrinio TRIAL alpha campaign (offline).")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--max-securities", type=int, default=None)
    ap.add_argument("--start", default="2010-01-01")
    ap.add_argument("--skip-owned-baseline", action="store_true")
    args = ap.parse_args()

    cfg = json.loads(STAGE9_CFG.read_text(encoding="utf-8"))
    print("[1/7] loading Intrinio fundamentals (census + tags) ...")
    fnd = IntrinioFundamentals(TRIAL_ROOT / "fundamentals" / "metadata",
                               TRIAL_ROOT / "fundamentals" / "tags",
                               max_securities=args.max_securities)
    print("   ciks with usable periods:", len(fnd.by_cik))

    print("[2/7] building owned survivorship-safe price panel (2009+) ...")
    panel = build_assetid_price_panel(PRE2015_ROOT)
    main_panel = build_assetid_price_panel(INGESTION_ROOT)
    for aid, bars in main_panel.items():
        panel.setdefault(aid, [])
        panel[aid] = sorted(set(panel[aid]) | set(bars), key=lambda p: p[0])
    id_store = IdentityStore(IDENTITY_DB)
    cik_to_assetid = build_cik_to_assetid(id_store)
    print("   panel assetids:", len(panel), " cik->assetid:", len(cik_to_assetid))

    dates = quarterly_formation_dates(panel, start=args.start, horizon=HORIZON)
    print("   formation dates:", len(dates), dates[0] if dates else None,
          "->", dates[-1] if dates else None)

    print("[3/7] building Intrinio cross-sections ...")
    price_idx = {a: _fev._price_index(b) for a, b in panel.items()}
    periods_by_signal = {s: [] for s in FAMILY}
    coverage_rows = []
    for d in dates:
        per_sig_names = {s: [] for s in FAMILY}
        n_avail = 0
        for cik, aid in fnd.assetid_by_cik.items():
            if aid not in price_idx:
                continue
            avail_periods = fnd.periods_available(cik, d)
            if not avail_periods:
                continue
            # stale-data guard: newest available period must be < 400 days old
            newest = _dt.date.fromisoformat(avail_periods[-1]["end"])
            if (_dt.date.fromisoformat(d) - newest).days > 400:
                continue
            n_avail += 1
            dts, closes = price_idx[aid]
            fwd = _fev._forward_return(dts, closes, d, HORIZON)
            if fwd is None:
                continue
            for sig, (sign, _r) in FAMILY.items():
                v = compute_intrinio_signal(avail_periods, sig, d)
                if v is not None:
                    per_sig_names[sig].append((aid, float(v) * sign, float(fwd)))
        coverage_rows.append({"as_of": d, "names_with_available_fundamentals": n_avail,
                              **{s: len(per_sig_names[s]) for s in FAMILY}})
        for s in FAMILY:
            if per_sig_names[s]:
                periods_by_signal[s].append({"as_of": d, "names": per_sig_names[s]})

    print("[4/7] evaluating the pre-registered family through the canonical engine ...")
    results = {}
    ls_series = {}
    for sig in FAMILY:
        res = _sev.evaluate_periods(periods_by_signal[sig], horizon_days=HORIZON,
                                    feature=sig)
        row, series = res["row"], res["series"]
        metrics = _tour.row_to_contract_metrics(row, survivorship_safe=True)
        gate = _tour.classify_evidence(metrics, cfg)
        results[sig] = {"row": {k: row.get(k) for k in (
            "periods", "universe", "rank_ic_mean", "rank_ic_t",
            "rank_ic_positive_ratio", "decile_spread_mean", "spread_t",
            "oos_ic_mean", "gross_annualized_return", "net_annualized_return",
            "cost_erosion_ratio", "cost_flips_sign", "turnover", "max_drawdown",
            "subperiod_consistency", "regime_consistency")},
            "gate": gate, "expected_sign": FAMILY[sig][0],
            "rationale": FAMILY[sig][1]}
        ls_series[sig] = (series.get("dates") or [], series.get("ls") or [])

    print("[5/7] BH-FDR over the pre-registered family (n=%d) ..." % FAMILY_SIZE)
    pvals = {s: _sev.approx_two_sided_pvalue(
        results[s]["row"].get("rank_ic_t"),
        int(results[s]["row"].get("periods") or 0)) or 1.0 for s in FAMILY}
    fdr = _ar._bh_fdr(pvals, family_size=FAMILY_SIZE, alpha=FDR_ALPHA)

    print("[6/7] owned-SEC baseline + redundancy + restatement bias ...")
    owned = {}
    redundancy = {}
    restatement = {}
    if not args.skip_owned_baseline:
        from paper_trader.alpha_agent import fundamental_readiness as _fr
        sec_store = _fr.open_companyfacts_pit_store(cfg)
        overlap = {"intrinio_gross_profitability": "gross_profitability",
                   "intrinio_asset_growth": "asset_growth",
                   "intrinio_equity_ratio": "balance_sheet_quality"}
        for isig, osig in overlap.items():
            row = _fev.evaluate_fundamental_evidence(
                sec_store, panel, osig, cik_to_ticker=cik_to_assetid,
                horizon_days=HORIZON, rebalance_dates=dates)
            om = _tour.row_to_contract_metrics(row, survivorship_safe=True)
            og = _tour.classify_evidence(om, cfg)
            okept, ols = [], []
            osecs = _fev.build_cross_sections(sec_store, panel, osig,
                                              cik_to_ticker=cik_to_assetid,
                                              rebalance_dates=dates, horizon=HORIZON)
            for p in osecs:
                s = _fev._long_short_spread(p["names"], quantile=0.10,
                                            min_names=_fev._MIN_DECILE_NAMES)
                if s is not None:
                    okept.append(p["as_of"])
                    ols.append(s)
            owned[osig] = {"row": {k: row.get(k) for k in (
                "periods", "universe", "rank_ic_mean", "rank_ic_t", "spread_t",
                "decile_spread_mean", "net_annualized_return", "turnover")},
                "gate": og}
            corr, n_over = _series_corr(ls_series[isig][0], ls_series[isig][1],
                                        okept, ols)
            redundancy[isig] = {"owned_counterpart": osig,
                                "ls_series_correlation": corr,
                                "overlap_periods": n_over}
        # restatement bias: Intrinio LATEST values vs SEC AS-FIRST-REPORTED
        # (earliest-filed observation per (cik, concept, period_end)). Balance
        # tags compare point values; NOTE netincome/totalrevenue in Intrinio tag
        # series are QUARTERLY values while SEC facts may be quarterly or YTD, so
        # the flow comparison keeps only exact period_end matches whose SEC
        # fiscal period is a quarter (fy-Qn key), best-effort measured evidence.
        tagmap = {"totalassets": "assets", "netincome": "net_income",
                  "totalrevenue": "revenue"}
        first_reported = defaultdict(dict)  # (cik, concept) -> {period_end: value}
        sample_ciks = set(list(fnd.by_cik.keys())[:400])
        for (cik, concept, fk), obs_list in sec_store._obs.items():
            if cik not in sample_ciks or concept not in tagmap.values():
                continue
            if concept != "assets" and "-Q" not in str(fk):
                continue
            first = min(obs_list, key=lambda o: o.available_at)
            pe = str(getattr(first, "period_end", "") or "")[:10]
            if pe:
                first_reported[(cik, concept)].setdefault(pe, first.value)
        diffs = {"totalassets": [], "netincome": [], "totalrevenue": []}
        for cik in sample_ciks:
            for p in fnd.by_cik.get(cik, [])[-12:]:
                for itag, concept in tagmap.items():
                    v_i = p["values"].get(itag)
                    if v_i is None:
                        continue
                    v_s = first_reported.get((cik, concept), {}).get(p["end"])
                    try:
                        v_s = float(v_s)
                    except (TypeError, ValueError):
                        continue
                    if v_s == 0:
                        continue
                    diffs[itag].append(abs(v_i - v_s) / max(abs(v_s), abs(v_i), 1e-9))
        def _q(xs, q):
            if not xs:
                return None
            s = sorted(xs)
            return s[min(len(s) - 1, int(q * len(s)))]
        restatement = {itag: {"n": len(xs), "p50_rel_diff": _q(xs, .5),
                              "p90_rel_diff": _q(xs, .9),
                              "gt_1pct": sum(1 for x in xs if x > 0.01) / len(xs) if xs else None}
                       for itag, xs in diffs.items()}

    print("[7/7] orthogonality within family + composite champion proxy ...")
    comp_kept, comp_ls = ls_series["intrinio_fcf_to_assets"]
    cross_corr = {}
    for sig in FAMILY:
        if sig == "intrinio_fcf_to_assets":
            continue
        c, n_o = _series_corr(ls_series[sig][0], ls_series[sig][1], comp_kept, comp_ls)
        cross_corr[sig] = {"corr_vs_fcf_leg": c, "overlap": n_o}

    campaign = {
        "campaign": "intrinio_trial_alpha_evaluation",
        "generated_at": _now_iso(),
        "license_state": "TRIAL", "research_use_only": True,
        "engine": {"evaluator": "alpha_agent.signal_evaluation.evaluate_periods",
                   "gates": "alpha_agent.tournament.classify_evidence@stage9_tournament.json",
                   "fdr": "alpha_agent.analyst_revisions._bh_fdr",
                   "horizon_days": HORIZON, "family_size": FAMILY_SIZE,
                   "fdr_alpha": FDR_ALPHA},
        "universe": {"ciks_usable": len(fnd.by_cik),
                     "panel_assetids": len(panel),
                     "formation_dates": len(dates),
                     "first_formation": dates[0] if dates else None,
                     "last_formation": dates[-1] if dates else None},
        "pit_constraint": ("values are latest-restated (measured); availability "
                           "from reported filing_date else original "
                           "first_calculable; formation uses avail <= d"),
        "results": {s: {k: v for k, v in r.items() if not k.startswith("_")}
                    for s, r in results.items()},
        "bh_fdr": fdr, "p_values": pvals,
        "owned_sec_baseline": owned,
        "redundancy_vs_owned": redundancy,
        "restatement_bias_vs_sec_first_reported": restatement,
        "family_cross_correlation": cross_corr,
        "coverage_by_period": coverage_rows,
        "safety": {"tournament_registry_written": False,
                   "operational_ledger_written": False,
                   "portfolio_mutated": False, "orders_created": False,
                   "model_promoted": False},
    }
    out = TRIAL_ROOT / "campaign"
    out.mkdir(parents=True, exist_ok=True)
    stamp = _now_iso().replace(":", "").replace("-", "")[:15]
    p1 = out / ("campaign_%s.json" % stamp)
    p1.write_text(json.dumps(campaign, indent=2, sort_keys=True, default=str),
                  encoding="utf-8")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    p2 = OUT_DIR / "alpha_campaign_result.json"
    p2.write_text(json.dumps(campaign, indent=2, sort_keys=True, default=str),
                  encoding="utf-8")
    if args.json:
        slim = dict(campaign)
        slim.pop("coverage_by_period", None)
        print(json.dumps(slim, indent=1, sort_keys=True, default=str))
    else:
        for s, r in results.items():
            row = r["row"]
            print("%-32s periods=%-3s univ=%-4s ic=%-8s ic_t=%-7s net25=%-9s -> %s"
                  % (s, row.get("periods"), row.get("universe"),
                     ("%.4f" % row["rank_ic_mean"]) if row.get("rank_ic_mean") is not None else "-",
                     ("%.2f" % row["rank_ic_t"]) if row.get("rank_ic_t") is not None else "-",
                     ("%.4f" % row["net_annualized_return"]) if row.get("net_annualized_return") is not None else "-",
                     r["gate"].get("target_state")))
        print("BH-FDR survivors:", fdr)
    print("wrote:", p1)
    print("wrote:", p2)
    print("INTRINIO_TRIAL_CAMPAIGN_DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
