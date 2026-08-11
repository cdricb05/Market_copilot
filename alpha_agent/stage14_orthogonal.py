"""
alpha_agent.stage14_orthogonal — Stage 14 orthogonal alpha discovery campaign
(bounded, preregistered, operator-run).

Executes the SIX hypotheses frozen in
``D:\\Temp\\paper_trader_stage14_orthogonal_alpha_discovery\\STAGE14_PREREGISTRATION.md``
through the RELEASED machinery only:

  * Lane A (3): the existing frozen Stage 9.4 catalogue candidates
    ``overnight_persistence`` / ``overnight_intraday_divergence`` /
    ``gap_concentration`` — measurable now because the owned Norgate
    MARKET_BAR store carries the adjusted Open on every record. The stale
    ``DATA_HOLD_REQUIRES_INTRADAY_OPEN_DATA`` blocker is resolved by DATA
    (an open-aware panel read), never by approximation.
  * Lane B (2): S&P 500 membership-transition event studies (addition
    reversal / deletion rebound) from owned PIT membership spans, evaluated
    with the released Stage 12 event-time statistics (monthly cohorts,
    overlap-corrected Newey-West clustered t) and Stage 12 adequacy gates.
  * Lane C (1): cross-sectional same-calendar-month seasonality from the
    owned close panel.

Scoring, gates, FDR, orthogonality and registry lifecycle are the released
components: ``experiment_runner.score_experiment`` + ``experiment_contracts.
DEFAULT_GATES``, ``stage9_4.enrich_price_factor_row``, ``tournament.
row_to_contract_metrics`` / ``classify_evidence`` / ``ingest_completed_
experiments``, ``selection_controls.benjamini_hochberg``,
``orthogonality.partial_rank_ic``, ``stage12_event_study.overlap_diagnostics``.
This module adds NO new gate, NO new statistic and NO promotion path.

Research-only: writes are limited to the D: research roots (ResearchQueue,
tournament registry, evidence JSON). Never touches an operational ledger,
order, fill, book, target or model registration.

Pure stdlib.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from . import autonomous_research as ar
from . import experiment_contracts as ec
from . import experiment_runner as er
from . import historical_price_panel as hpp
from . import orthogonality as orth
from . import price_factors as pf
from . import selection_controls as sctl
from . import signal_evaluation as sev
from . import stage9_4 as s94
from . import stage12_event_study as s12ev
from . import tournament as tt

STAGE14_VERSION = "stage14-orthogonal-1.0.0"
ORIGIN = "stage14-orthogonal"
LANE_PREFIX = "stage14.orthogonal."
FAMILY_OVERNIGHT = "stage14_overnight"
FAMILY_MEMBERSHIP = "stage14_membership_event"
FAMILY_SEASONALITY = "stage14_seasonality"
FAMILY_ALL = "stage14_all"
FDR_ALPHA = 0.05
EVENT_COST_ROUND_TRIP = 0.0050          # 50 bps, mandate cost_bps_round_trip
EVENT_HORIZONS = (5, 20, 63)            # primary 63 (frozen)
EVENT_PRIMARY_HORIZON = 63
EVENT_SPAN_MERGE_GAP_SESSIONS = 5
INDEX_NAME = "S&P 500"

# The six frozen hypotheses (mirrors STAGE14_PREREGISTRATION.md; the Lane A
# entries measure the EXISTING Stage 9.4 catalogue candidates verbatim).
HYPOTHESES = (
    {"id": "S14-A1", "feature": "overnight_persistence",
     "family": FAMILY_OVERNIGHT, "kind": "cross_sectional",
     "window": 252, "min_obs": 126, "expected_sign": 1,
     "horizon_days": 21, "rebalance": "monthly",
     "candidate_id": "c9_pricemomentu_e28ffecf4f"},
    {"id": "S14-A2", "feature": "overnight_intraday_divergence",
     "family": FAMILY_OVERNIGHT, "kind": "cross_sectional",
     "window": 252, "min_obs": 126, "expected_sign": 1,
     "horizon_days": 21, "rebalance": "monthly",
     "candidate_id": "c9_pricemomentu_be1eb51ec8"},
    {"id": "S14-A3", "feature": "gap_concentration",
     "family": FAMILY_OVERNIGHT, "kind": "cross_sectional",
     "window": 126, "min_obs": 63, "expected_sign": 1,
     "horizon_days": 21, "rebalance": "monthly",
     "candidate_id": "c9_pricemomentu_8a40c702bf"},
    {"id": "S14-B1", "feature": "sp500_addition_reversal",
     "family": FAMILY_MEMBERSHIP, "kind": "membership_event",
     "side": "ADDITION", "expected_sign": -1},
    {"id": "S14-B2", "feature": "sp500_deletion_rebound",
     "family": FAMILY_MEMBERSHIP, "kind": "membership_event",
     "side": "DELETION", "expected_sign": 1},
    {"id": "S14-C1", "feature": "same_month_seasonality",
     "family": FAMILY_SEASONALITY, "kind": "cross_sectional",
     "window": None, "min_obs": 5, "expected_sign": 1,
     "horizon_days": 21, "rebalance": "monthly", "candidate_id": None,
     "lookback_years": 10, "exclusion_sessions": 252},
)


# --------------------------------------------------------------------------- #
# Panel assembly (owned Norgate open/close, main + pre2015, merged by assetid).
# --------------------------------------------------------------------------- #
def build_merged_open_close_panel(ingestion_roots) -> dict:
    """``{assetid: [(date, adj_open, adj_close)]}`` merged across the owned
    stores (earlier stores first; first record per (assetid, date) wins)."""
    merged: dict = {}
    for root in ingestion_roots:
        part = hpp.build_assetid_open_close_panel(root)
        for aid, series in part.items():
            by_date = merged.setdefault(aid, {})
            for d, o, c in series:
                if d not in by_date:
                    by_date[d] = (o, c)
    out: dict = {}
    for aid, by_date in merged.items():
        out[aid] = [(d, v[0], v[1]) for d, v in sorted(by_date.items())]
    return out


def close_panel_from_open_close(oc_panel: dict) -> dict:
    """The close-only view ``{assetid: [(date, close)]}`` of the open/close
    panel (index-aligned with the per-name overnight arrays)."""
    return {aid: [(d, c) for d, _o, c in series]
            for aid, series in oc_panel.items()}


def build_overnight_arrays(oc_panel: dict) -> dict:
    """Per-assetid aligned overnight / intraday simple-return arrays, indexed
    exactly like the close-only series: at index j (j >= 1),
    ``on[j] = open_j/close_{j-1} - 1`` and ``in[j] = close_j/open_j - 1``.
    Index 0 (and any position whose inputs are missing/nonpositive) is None —
    invalid sessions are skipped by the calculators, never imputed."""
    out: dict = {}
    for aid, series in oc_panel.items():
        n = len(series)
        on: list = [None] * n
        intr: list = [None] * n
        for j in range(n):
            _d, o, c = series[j]
            if o and o > 0 and c and c > 0:
                intr[j] = c / o - 1.0
                if j >= 1:
                    c_prev = series[j - 1][2]
                    if c_prev and c_prev > 0:
                        on[j] = o / c_prev - 1.0
        out[aid] = (on, intr)
    return out


# --------------------------------------------------------------------------- #
# Lane A: frozen overnight calculators (value_fn for the released builder).
# --------------------------------------------------------------------------- #
def _var(vals: list) -> Optional[float]:
    sd = ec.stdev(vals, sample=True)
    return None if sd is None else sd * sd


def make_overnight_value_fn(overnight_arrays: dict):
    """A ``value_fn`` for ``price_factors.build_factor_cross_sections``:
    computes the three frozen overnight features from owned opens under the
    released leakage contract (window strictly through the formation index)."""
    spec_by_feature = {h["feature"]: h for h in HYPOTHESES
                       if h["family"] == FAMILY_OVERNIGHT}

    def value_fn(feature, *, name_key, name_dates, name_closes, idx,
                 mret_by_date):
        h = spec_by_feature.get(feature)
        arrays = overnight_arrays.get(name_key)
        if h is None or arrays is None or idx < 1:
            return None
        on, intr = arrays
        lo = max(0, idx - int(h["window"]) + 1)
        on_w = [v for v in on[lo:idx + 1] if v is not None]
        in_w = [v for v in intr[lo:idx + 1] if v is not None]
        if feature == "overnight_persistence":
            if len(on_w) < h["min_obs"]:
                return None
            return ec.mean(on_w)
        if feature == "overnight_intraday_divergence":
            if len(on_w) < h["min_obs"] or len(in_w) < h["min_obs"]:
                return None
            m_on, m_in = ec.mean(on_w), ec.mean(in_w)
            if m_on is None or m_in is None:
                return None
            return m_on - m_in
        if feature == "gap_concentration":
            if len(on_w) < h["min_obs"] or len(in_w) < h["min_obs"]:
                return None
            v_on, v_in = _var(on_w), _var(in_w)
            if v_on is None or v_in is None or (v_on + v_in) <= 0:
                return None
            return v_on / (v_on + v_in)
        return None

    return value_fn


# --------------------------------------------------------------------------- #
# Lane C: frozen same-calendar-month seasonality calculator (close-only).
# --------------------------------------------------------------------------- #
def make_seasonality_value_fn(lookback_years: int = 10,
                              exclusion_sessions: int = 252,
                              min_obs: int = 5):
    """Same-calendar-month (annual-lag) seasonality: at formation t the factor
    is the mean of the name's TARGET-month monthly returns over the prior
    ``lookback_years`` calendar years, where TARGET is the calendar month
    AFTER month(date(t)) (pure calendar arithmetic — no future data), using
    only months whose month-end session index is <= idx - exclusion_sessions
    (momentum/reversal guard). Per-name month structure is cached."""
    cache: dict = {}

    def _month_structure(name_key, name_dates, name_closes):
        got = cache.get(name_key)
        if got is not None and got[0] == len(name_dates):
            return got[1]
        # Last session index of every calendar month present in the series.
        month_end_idx: dict = {}
        for i, d in enumerate(name_dates):
            month_end_idx[str(d)[:7]] = i
        months = sorted(month_end_idx)
        # Monthly return keyed by month: close(month_end)/close(prev_month_end)-1
        rets: dict = {}
        for k in range(1, len(months)):
            m0, m1 = months[k - 1], months[k]
            c0 = name_closes[month_end_idx[m0]]
            c1 = name_closes[month_end_idx[m1]]
            if c0 and c0 > 0 and c1 is not None:
                rets[m1] = (c1 / c0 - 1.0, month_end_idx[m1])
        cache[name_key] = (len(name_dates), (month_end_idx, rets))
        return month_end_idx, rets

    def value_fn(feature, *, name_key, name_dates, name_closes, idx,
                 mret_by_date):
        if feature != "same_month_seasonality" or idx < 1:
            return None
        _me, rets = _month_structure(name_key, name_dates, name_closes)
        fd = str(name_dates[idx])
        f_year, f_month = int(fd[:4]), int(fd[5:7])
        target_month = 1 if f_month == 12 else f_month + 1
        target_year_base = f_year + 1 if f_month == 12 else f_year
        obs = []
        for m1, (r, end_idx) in rets.items():
            if int(m1[5:7]) != target_month:
                continue
            if end_idx > idx - exclusion_sessions:
                continue
            if int(m1[:4]) < target_year_base - lookback_years:
                continue
            obs.append(r)
        if len(obs) < min_obs:
            return None
        return ec.mean(obs)

    return value_fn


# --------------------------------------------------------------------------- #
# Cross-sectional scoring: the RELEASED Stage 5 scorer on a prebuilt build.
# (Mirrors price_factors.run_new_price_factor_campaign for one prebuilt
#  ``built`` dict so the champion join can reuse the same cross-sections.)
# --------------------------------------------------------------------------- #
def score_cross_sectional(feature: str, built: dict, *,
                          horizon_days: int = 21, rebalance: str = "monthly",
                          cost_grid=None, cost_bps: float = 10.0,
                          champion_returns=None, min_periods: int = 12) -> dict:
    cost_grid = cost_grid or [5, 10, 25, 50]
    if built["periods"] < min_periods:
        return {"feature": feature, "label": feature,
                "periods": built["periods"], "rank_ic_t": None,
                "decision": ec.NEED_MORE_DATA}
    spec = {"template": "campaign_price_factor", "feature": feature,
            "study_kind": "cross_sectional_rank", "horizon_days": horizon_days,
            "rebalance": rebalance, "benchmark": "equal_weight_universe",
            "transaction_cost_bps": cost_bps}
    ppy = ec.periods_per_year_for(rebalance)
    metrics = er.score_experiment(spec, built, gates=ec.DEFAULT_GATES,
                                  cost_grid=cost_grid, periods_per_year=ppy,
                                  champion_returns=champion_returns,
                                  missing_data_rate=0.0)
    metrics["feature"] = feature
    metrics["label"] = feature
    metrics["market_features"] = built["market_features"]
    metrics["_periods_series"] = {
        "rank_ic": [ec.spearman(f, r) for f, r in built["cross_sections"]],
        "spread": [ec._decile_spread_one(f, r, 10)
                   for f, r in built["cross_sections"]],
        "portfolio_returns": built["portfolio_returns"],
        "turnovers": built["turnovers"],
        "market_features": built["market_features"]}
    metrics["decision"] = ec.evaluate_evidence(
        metrics, ec.DEFAULT_GATES)["decision"]
    return metrics


# --------------------------------------------------------------------------- #
# Champion comparison (signal-level, released partial_rank_ic).
# --------------------------------------------------------------------------- #
def load_champion_controls(champion_panel_csv, momentum_panel_csv) -> dict:
    """``{"composite_sn": {(YYYY-MM, TICKER): score},
         "mom_6_1": {(YYYY-MM, TICKER): score}}`` from the frozen Phase 10-L
    sector-neutral scored panel and the momentum monthly panel. Missing files
    yield empty maps (recorded honestly as CHAMPION_JOIN_UNAVAILABLE)."""
    import csv
    champ: dict = {}
    mom: dict = {}
    cp = Path(champion_panel_csv)
    if cp.exists():
        with open(cp, "r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                m = (row.get("rebalance_date") or "")[:7]
                tkr = (row.get("ticker") or "").upper()
                try:
                    v = float(row.get("composite_sn"))
                except (TypeError, ValueError):
                    continue
                if m and tkr:
                    champ[(m, tkr)] = v
    mp = Path(momentum_panel_csv)
    if mp.exists():
        with open(mp, "r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                m = (row.get("month") or "")[:7]
                tkr = (row.get("ticker") or "").upper()
                try:
                    v = float(row.get("mom_6_1"))
                except (TypeError, ValueError):
                    continue
                if m and tkr:
                    mom[(m, tkr)] = v
    return {"composite_sn": champ, "mom_6_1": mom}


def _series_t(vals: list) -> Optional[float]:
    clean = [v for v in vals if v is not None]
    n = len(clean)
    if n < 3:
        return None
    m = ec.mean(clean)
    sd = ec.stdev(clean, sample=True)
    if m is None or not sd:
        return None
    return m / (sd / (n ** 0.5))


def champion_comparison(built: dict, controls: dict,
                        ticker_maps_by_date: dict) -> dict:
    """Per-period signal-level comparison vs the champion:
      * mean cross-sectional Spearman corr of the candidate factor vs
        composite_sn (champion-overlap periods only) and vs mom_6_1;
      * released ``orthogonality.partial_rank_ic`` of candidate vs forward
        returns controlling BOTH composite_sn and mom_6_1, with the
        per-period series t.
    Names join per (formation month, PIT-effective ticker)."""
    champ_map = controls.get("composite_sn") or {}
    mom_map = controls.get("mom_6_1") or {}
    corr_champ_series: list = []
    corr_mom_series: list = []
    partial_series: list = []
    n_join_champ = 0
    n_join_mom = 0
    xs = built.get("cross_sections") or []
    names_by_period = built.get("cross_section_names") or []
    dates = built.get("formation_dates") or []
    for k in range(min(len(xs), len(names_by_period), len(dates))):
        fac_vals, fwd_vals = xs[k]
        names = names_by_period[k]
        month = str(dates[k])[:7]
        tmap = ticker_maps_by_date.get(str(dates[k])) or {}
        cand_c, champ_c = [], []
        cand_m, mom_c = [], []
        cand_p, fwd_p, ctl_champ, ctl_mom = [], [], [], []
        for i, aid in enumerate(names):
            tkr = (tmap.get(str(aid)) or "").upper()
            if not tkr:
                continue
            cv = champ_map.get((month, tkr))
            mv = mom_map.get((month, tkr))
            if cv is not None:
                cand_c.append(fac_vals[i])
                champ_c.append(cv)
            if mv is not None:
                cand_m.append(fac_vals[i])
                mom_c.append(mv)
            if cv is not None and mv is not None:
                cand_p.append(fac_vals[i])
                fwd_p.append(fwd_vals[i])
                ctl_champ.append(cv)
                ctl_mom.append(mv)
        if len(cand_c) >= 10:
            corr_champ_series.append(ec.spearman(cand_c, champ_c))
            n_join_champ += len(cand_c)
        if len(cand_m) >= 10:
            corr_mom_series.append(ec.spearman(cand_m, mom_c))
            n_join_mom += len(cand_m)
        if len(cand_p) >= 10:
            partial_series.append(orth.partial_rank_ic(
                cand_p, fwd_p, [ctl_champ, ctl_mom]))
    corr_champ = [v for v in corr_champ_series if v is not None]
    corr_mom = [v for v in corr_mom_series if v is not None]
    partial = [v for v in partial_series if v is not None]
    return {
        "join_basis": "(formation month, PIT-effective ticker)",
        "corr_vs_champion_mean": ec.mean(corr_champ) if corr_champ else None,
        "corr_vs_champion_periods": len(corr_champ),
        "corr_vs_momentum_mean": ec.mean(corr_mom) if corr_mom else None,
        "corr_vs_momentum_periods": len(corr_mom),
        "partial_rank_ic_mean": ec.mean(partial) if partial else None,
        "partial_rank_ic_t": _series_t(partial),
        "partial_rank_ic_periods": len(partial),
        "joined_name_periods_champion": n_join_champ,
        "joined_name_periods_momentum": n_join_mom,
        "status": ("OK" if partial else "CHAMPION_JOIN_UNAVAILABLE"),
    }


# --------------------------------------------------------------------------- #
# Lane B: membership spans -> transition events -> event study.
# --------------------------------------------------------------------------- #
def read_membership_spans(ingestion_roots, *, index_name: str = INDEX_NAME):
    """Owned INDEX_MEMBERSHIP_SPAN records grouped per store: for each root the
    LATEST capture (max retrieved_at) is used — an older capture's member_to is
    truncated at ITS capture date and would fabricate spurious deletions.
    Returns a list of per-store dicts:
      {"root", "spans": {assetid: [(member_from, member_to)]},
       "window_start", "window_end"}"""
    out = []
    for root in ingestion_roots:
        base = Path(root) / "normalized" / "UNIVERSE_MEMBERSHIP"
        if not base.exists():
            continue
        by_capture: dict = {}
        for f in sorted(base.rglob("*.jsonl")):
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
                if rec.get("source_id") != "norgate_local":
                    continue
                pay = rec.get("normalized_payload") or {}
                if pay.get("index_name") != index_name:
                    continue
                aid = rec.get("security_id")
                mf, mt = pay.get("member_from"), pay.get("member_to")
                cap = str(rec.get("retrieved_at") or "")
                if not (aid and mf and mt):
                    continue
                by_capture.setdefault(cap, {}).setdefault(
                    str(aid), set()).add((str(mf)[:10], str(mt)[:10]))
        if not by_capture:
            continue
        latest = max(by_capture)
        spans = {aid: sorted(v) for aid, v in by_capture[latest].items()}
        all_from = [s[0] for v in spans.values() for s in v]
        all_to = [s[1] for v in spans.values() for s in v]
        out.append({"root": str(root), "spans": spans,
                    "window_start": min(all_from), "window_end": max(all_to),
                    "capture": latest})
    return out


def merge_spans_across_stores(stores, session_index: dict):
    """Union the per-store spans per assetid; merge same-assetid spans whose
    gap is <= EVENT_SPAN_MERGE_GAP_SESSIONS owned sessions (store-boundary
    abutments and micro-churn are not add/drop events). ``session_index`` maps
    date -> global session ordinal. Returns
    ``{assetid: [(from, to)]}`` (merged, sorted) plus the per-store boundary
    dates used for event exclusion."""
    def _ord(d, side):
        if d in session_index:
            return session_index[d]
        keys = sorted(session_index)
        if side == "from":                      # first session >= d
            for k in keys:
                if k >= d:
                    return session_index[k]
            return None
        for k in reversed(keys):                # last session <= d
            if k <= d:
                return session_index[k]
        return None

    union: dict = {}
    for st in stores:
        for aid, spans in st["spans"].items():
            union.setdefault(aid, set()).update(spans)
    merged: dict = {}
    for aid, spans in union.items():
        ordered = sorted(spans)
        acc = []
        for s in ordered:
            if not acc:
                acc.append(list(s))
                continue
            prev = acc[-1]
            if s[0] <= prev[1]:                 # overlapping spans: extend
                prev[1] = max(prev[1], s[1])
                continue
            o_prev = _ord(prev[1], "to")
            o_next = _ord(s[0], "from")
            if o_prev is not None and o_next is not None and \
                    (o_next - o_prev) <= EVENT_SPAN_MERGE_GAP_SESSIONS + 1:
                prev[1] = max(prev[1], s[1])
            else:
                acc.append(list(s))
        merged[aid] = [(a, b) for a, b in acc]
    boundaries = {"starts": {st["window_start"] for st in stores},
                  "ends": {st["window_end"] for st in stores}}
    return merged, boundaries


def extract_membership_events(merged_spans: dict, boundaries: dict):
    """ADDITION events (member_from strictly inside the observed history) and
    DELETION events (member_to strictly before the latest observed date).
    Store-boundary artifacts are excluded per the frozen contract."""
    additions, deletions = [], []
    starts, ends = boundaries["starts"], boundaries["ends"]
    for aid, spans in merged_spans.items():
        for mf, mt in spans:
            if mf not in starts:
                additions.append({"assetid": aid, "date": mf})
            if mt not in ends and mt not in starts:
                deletions.append({"assetid": aid, "date": mt})
    additions.sort(key=lambda e: (e["date"], e["assetid"]))
    deletions.sort(key=lambda e: (e["date"], e["assetid"]))
    return additions, deletions


def run_membership_event_study(events, side, expected_sign, merged_spans,
                               close_panel, *, horizons=EVENT_HORIZONS,
                               cost_round_trip=EVENT_COST_ROUND_TRIP) -> dict:
    """The frozen Lane B event study: entry at the first owned session close
    STRICTLY AFTER the event date; abnormal = event-name return minus the
    equal-weight return of PIT S&P 500 members over the same window; monthly
    cohorts; released overlap-corrected clustered t. DATA_HOLD via the released
    Stage 12 adequacy thresholds. Read-only."""
    calendar = sorted({d for s in close_panel.values() for d, _c in s})
    cal_index = {d: i for i, d in enumerate(calendar)}
    px = {aid: {d: c for d, c in series} for aid, series in close_panel.items()}

    def _members_on(date):
        out = []
        for aid, spans in merged_spans.items():
            for mf, mt in spans:
                if mf <= date <= mt:
                    out.append(aid)
                    break
        return out

    per_h: dict = {h: {"events": [], "unmeasurable": 0} for h in horizons}
    excluded_no_forward = 0
    for ev in events:
        d = ev["date"]
        pos = None
        for i in range(cal_index.get(d, _first_ge(calendar, d)),
                       len(calendar)):
            if calendar[i] > d:
                pos = i
                break
        if pos is None:
            excluded_no_forward += 1
            continue
        entry_date = calendar[pos]
        members = _members_on(entry_date)
        for h in horizons:
            end_pos = pos + h
            if end_pos >= len(calendar):
                per_h[h]["unmeasurable"] += 1
                continue
            end_date = calendar[end_pos]
            series = px.get(ev["assetid"]) or {}
            p0, p1 = series.get(entry_date), series.get(end_date)
            if not p0 or p1 is None or p0 <= 0:
                per_h[h]["unmeasurable"] += 1
                continue
            bench = []
            for aid in members:
                ms = px.get(aid) or {}
                b0, b1 = ms.get(entry_date), ms.get(end_date)
                if b0 and b0 > 0 and b1 is not None:
                    bench.append(b1 / b0 - 1.0)
            if len(bench) < 20:
                per_h[h]["unmeasurable"] += 1
                continue
            abnormal = (p1 / p0 - 1.0) - (sum(bench) / len(bench))
            per_h[h]["events"].append(
                {"assetid": ev["assetid"], "event_date": d,
                 "entry_date": entry_date, "abnormal": abnormal})

    result = {"side": side, "expected_sign": expected_sign,
              "n_events_input": len(events),
              "excluded_no_forward_calendar": excluded_no_forward,
              "horizons": {}}
    for h in horizons:
        evs = per_h[h]["events"]
        cohorts: dict = {}
        for e in evs:
            cohorts.setdefault(e["event_date"][:7], []).append(e["abnormal"])
        cohort_months = sorted(cohorts)
        cohort_series = [sum(v) / len(v) for m in cohort_months
                         for v in [cohorts[m]]]
        diag = s12ev.overlap_diagnostics(cohort_series, h) \
            if cohort_series else {"t_clustered": None, "n_cohorts": 0}
        gross_mean = (sum(e["abnormal"] for e in evs) / len(evs)) if evs else None
        signed_gross = (expected_sign * gross_mean) if gross_mean is not None \
            else None
        net_signed = (signed_gross - cost_round_trip) if signed_gross is not None \
            else None
        years: dict = {}
        for e in evs:
            years.setdefault(e["event_date"][:4], []).append(e["abnormal"])
        yr_rows = {y: {"n": len(v), "mean_abnormal": sum(v) / len(v)}
                   for y, v in sorted(years.items())}
        elig = [y for y, r in yr_rows.items() if r["n"] >= 3]
        consistent = [y for y in elig
                      if (expected_sign * yr_rows[y]["mean_abnormal"]) > 0]
        subperiod = (len(consistent) / len(elig)) if elig else None
        distinct_issuers = len({e["assetid"] for e in evs})
        adequacy = None
        if len(cohort_series) < s12ev.MIN_COHORTS:
            adequacy = "DATA_HOLD_INSUFFICIENT_COHORTS(%d<%d)" % (
                len(cohort_series), s12ev.MIN_COHORTS)
        elif len(evs) < s12ev.MIN_EVENTS:
            adequacy = "DATA_HOLD_INSUFFICIENT_EVENTS(%d<%d)" % (
                len(evs), s12ev.MIN_EVENTS)
        result["horizons"][h] = {
            "n_events_measured": len(evs),
            "n_unmeasurable": per_h[h]["unmeasurable"],
            "distinct_issuers": distinct_issuers,
            "n_cohorts": len(cohort_series),
            "gross_mean_abnormal": gross_mean,
            "signed_gross_mean_abnormal": signed_gross,
            "net_signed_mean_abnormal_50bps": net_signed,
            "clustered_t": diag.get("t_clustered"),
            "t_naive": diag.get("t_naive"),
            "overlap_diagnostics": diag,
            "per_year": yr_rows,
            "subperiod_consistency": subperiod,
            "adequacy_blocker": adequacy,
        }
    prim = result["horizons"].get(EVENT_PRIMARY_HORIZON, {})
    t = prim.get("clustered_t")
    signed_t = (expected_sign * t) if t is not None else None
    passes = (prim.get("adequacy_blocker") is None and t is not None
              and abs(t) >= 2.0 and signed_t is not None and signed_t > 0
              and (prim.get("net_signed_mean_abnormal_50bps") or 0) > 0
              and (prim.get("subperiod_consistency") is not None
                   and prim["subperiod_consistency"] >= 0.60))
    result["primary_horizon"] = EVENT_PRIMARY_HORIZON
    result["primary_clustered_t"] = t
    result["primary_n_cohorts"] = prim.get("n_cohorts")
    result["passes_frozen_gates"] = bool(passes)
    result["evidence_status"] = (
        tt.EVIDENCE_INCOMPLETE if prim.get("adequacy_blocker")
        else (tt.EVIDENCE_COMPLETE_STRONG if passes
              else tt.EVIDENCE_COMPLETE_WEAK))
    return result


def _first_ge(calendar, d):
    lo, hi = 0, len(calendar)
    while lo < hi:
        mid = (lo + hi) // 2
        if calendar[mid] < d:
            lo = mid + 1
        else:
            hi = mid
    return lo


# --------------------------------------------------------------------------- #
# FDR over the frozen families.
# --------------------------------------------------------------------------- #
def stage14_fdr(primary_stats: list) -> dict:
    """``primary_stats`` items: {"id", "family", "t", "n"}. BH-FDR within each
    lane family AND over the full stage14 family (declared sensitivity pass).
    A hypothesis without a measurable t contributes no p-value (recorded)."""
    def _p(item):
        t, n = item.get("t"), item.get("n")
        if t is None or not n or n < 2:
            return None
        return sev.approx_two_sided_pvalue(t, int(n) - 1)

    rows = []
    for it in primary_stats:
        rows.append({**it, "pvalue": _p(it)})
    out = {"alpha": FDR_ALPHA, "rows": rows, "families": {}}
    fams = sorted({r["family"] for r in rows})
    for fam in fams + [FAMILY_ALL]:
        members = [r for r in rows
                   if fam == FAMILY_ALL or r["family"] == fam]
        pvals = [r["pvalue"] for r in members if r["pvalue"] is not None]
        ids = [r["id"] for r in members if r["pvalue"] is not None]
        qvals = sctl.benjamini_hochberg(pvals) if pvals else []
        out["families"][fam] = {
            "family_size_declared": len(members),
            "tested": len(pvals),
            "q_by_id": {ids[i]: qvals[i] for i in range(len(ids))},
            "survivors": [ids[i] for i in range(len(ids))
                          if qvals[i] is not None and qvals[i] < FDR_ALPHA],
        }
    return out


__all__ = [
    "STAGE14_VERSION", "ORIGIN", "LANE_PREFIX", "HYPOTHESES",
    "FAMILY_OVERNIGHT", "FAMILY_MEMBERSHIP", "FAMILY_SEASONALITY",
    "FAMILY_ALL", "FDR_ALPHA", "EVENT_HORIZONS", "EVENT_PRIMARY_HORIZON",
    "build_merged_open_close_panel", "close_panel_from_open_close",
    "build_overnight_arrays", "make_overnight_value_fn",
    "make_seasonality_value_fn", "score_cross_sectional",
    "load_champion_controls", "champion_comparison",
    "read_membership_spans", "merge_spans_across_stores",
    "extract_membership_events", "run_membership_event_study", "stage14_fdr",
]
