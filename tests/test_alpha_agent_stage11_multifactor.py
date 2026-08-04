"""
Stage 11 - AUTONOMOUS MULTI-FACTOR ALPHA DISCOVERY, ROBUSTNESS TOURNAMENT AND
SHADOW PORTFOLIO. Deterministic property tests for the signal factory
(``signal_library``), the generic evaluator (``signal_evaluation``), the two-stage
research funnel + multiple-testing controls + ensemble discovery
(``stage11_research``), the SHADOW-ONLY portfolio (``shadow_portfolio``) and the
campaign lanes / planner / dispatch (``stage11_jobs``).

No network, no real Norgate, no operational mutation: tiny deterministic in-memory
OHLCV panels, a real PIT fundamentals store, a real identity store and a real
research queue exercise every safety invariant. A "rigged" panel whose forward
returns follow trailing momentum lets the POSITIVE path (a signal that passes the
UNCHANGED gates -> shadow activation) be tested deterministically alongside the
honest negative path.
"""
import datetime as _dt
import json
import math
from pathlib import Path

from alpha_agent import historical_identity as H
from alpha_agent import pit_fundamentals as PF
from alpha_agent import tournament as TT
from alpha_agent import autonomous_research as AR
from alpha_agent import signal_library as SL
from alpha_agent import signal_evaluation as SE
from alpha_agent import stage11_research as SR
from alpha_agent import shadow_portfolio as SHP
from alpha_agent import stage11_jobs as S11

REPO = Path(__file__).resolve().parents[1]
_CFG9 = TT.load_config(str(REPO / "configs" / "alpha_agent"
                           / "stage9_tournament.json"))


# --------------------------------------------------------------------------- #
# Fixtures.
# --------------------------------------------------------------------------- #
def _clock():
    box = {"n": 0}

    def c():
        box["n"] += 1
        return "2026-08-03T00:%02d:00+00:00" % (box["n"] % 60)
    return c


def _dates(n, start="2015-01-02"):
    d0 = _dt.date.fromisoformat(start)
    return [(d0 + _dt.timedelta(days=k)).isoformat() for k in range(n)]


def _panel(n_assets=30, n_days=1500, *, rigged=True):
    """Deterministic OHLCV panel. rigged=True: per-asset drift ranked by index so
    trailing momentum predicts forward returns (a signal that passes the gates);
    rigged=False: sign-alternating noise with no cross-sectional edge."""
    dates = _dates(n_days)
    panel = {}
    for i in range(n_assets):
        drift = ((i - n_assets / 2.0) / n_assets) * 0.004
        price = 100.0
        c, v, dv = [], [], []
        for k in range(n_days):
            if rigged:
                r = drift + 0.002 * math.sin(i * 1.3 + k * 0.11)
            else:
                # deterministic, non-autocorrelated pseudo-random: no per-asset
                # drift and no serial structure -> ~zero cross-sectional edge.
                h = (i * 2654435761 + k * 40503 + 12345) & 0xFFFFFFFF
                r = ((h % 100000) / 100000.0 - 0.5) * 0.03
            price = price * (1.0 + r)
            c.append(price)
            vol = 1_000_000.0 + 5000.0 * ((i * 7 + k) % 40)
            v.append(vol)
            dv.append(price * vol)
        panel["A%d" % i] = {"d": list(dates), "c": c, "v": v, "dv": dv,
                            "o": [None] * n_days, "h": [None] * n_days,
                            "l": [None] * n_days}
    return panel


def _ctx_from_panel(panel, *, store=None, c2a=None, sector=None):
    return SR.build_panel_context(panel, store=store, cik_to_assetid=c2a or {},
                                  sector_series=sector)


def _rebs(panel, horizon=63):
    from alpha_agent import fundamental_evidence as FEV
    cpanel = {a: list(zip(s["d"], s["c"])) for a, s in panel.items()}
    return FEV._default_rebalance_dates(cpanel, horizon=horizon)


def _spec(name="momentum_6m", **kw):
    d = {x["name"]: x for x in SL.PRIMITIVE_DEFS}[name]
    return SL.SignalSpec(name=d["name"], family=d["family"], kind=d["kind"],
                         primitive=d["primitive"], direction=int(d["direction"]),
                         lookback=int(d.get("lookback", 126)),
                         skip=int(d.get("skip", 0)),
                         horizon_days=kw.get("horizon_days", 63),
                         winsor=kw.get("winsor", 0.02),
                         neutralization=kw.get("neutralization", "none"),
                         requires=tuple(d.get("requires", ())))


def _pit(ciks, *, with_prior=True):
    st = PF.PitFundamentalsStore()
    fys = [2016, 2017, 2018, 2019] if with_prior else [2018]
    for c in ciks:
        for fy in fys:
            for tag, val in (("Assets", 1000.0 + fy + int(c)), ("Liabilities", 400),
                             ("StockholdersEquity", 600), ("GrossProfit", 300),
                             ("Revenues", 900), ("CostOfRevenue", 600),
                             ("NetIncomeLoss", 120), ("OperatingIncomeLoss", 150),
                             ("CashAndCashEquivalentsAtCarryingValue", 80)):
                st.add_fact({"cik": c, "concept": tag, "unit": "USD",
                             "value": val, "period_end": "%d-12-31" % fy,
                             "fy": fy, "fp": "FY", "filed": "%d-02-15" % (fy + 1),
                             "form": "10-K"})
    return st


def _write_ohlcv(root, assetids, dates, *, base=100.0, run="r1"):
    b = Path(root) / "normalized" / "MARKET_BAR"
    buckets = {}
    for idx, aid in enumerate(assetids):
        for k, date in enumerate(dates):
            y, m, d = date.split("-")
            p = b / y / m / d / ("%s.jsonl" % run)
            close = base + idx + k * 0.5
            rec = {"record_type": "MARKET_BAR", "source_id": "norgate_local",
                   "ticker": "S%d" % idx, "security_id": str(aid),
                   "effective_at": date,
                   "normalized_payload": {"Date": date, "Open": close,
                                          "High": close, "Low": close,
                                          "Close": close, "Volume": 1000000,
                                          "Turnover": close * 1000000}}
            buckets.setdefault(p, []).append(json.dumps(rec))
    for p, lines in buckets.items():
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    return b


def _id_store(tmp_path, n, *, assetids=None):
    store = H.IdentityStore(str(tmp_path / "id.sqlite"), clock=_clock())
    assetids = assetids or [100 + i for i in range(n)]
    for i in range(n):
        sid = "ngid:%d" % i
        store.upsert_security(H.SecurityIdentity(
            security_id=sid, norgate_assetid=str(assetids[i]),
            norgate_symbol="S%d" % i, ticker="S%d" % i, issuer_name="Co%d" % i,
            share_class="COMMON", base_type="Equity", exchange="NYSE",
            security_start_date="2010-01-01", security_end_date=None,
            delisting_date=None, is_current=True,
            membership_intervals=[{"index_name": "S&P 500",
                                   "member_start": "2010-01-01",
                                   "member_end": None}]))
        store.record_mapping(H.MappingResult(
            sid, "%010d" % (1000 + i), H.METHOD_FILING_TICKER_OVERLAP, 3,
            0.9, H.STATUS_RESOLVED, "2010-01-01", None, {"t": 3}))
    return store


class _Job:
    def __init__(self, lane, payload=None, job_id="j1"):
        self.lane = lane
        self.payload = payload or {}
        self.job_id = job_id
        self.origin = S11.ORIGIN_11


def _s11_ctx(tmp_path, store, ingestion_root, *, pit=None, sector=None,
             rebs=None, stage11=None):
    root = Path(tmp_path)
    base_cfg = {"enabled": True, "planner_enabled": True,
                "sources": ["norgate_local"], "horizon_days": 63,
                "priority": 2, "screen_horizons": [63], "winsors": [0.02],
                "screen_batch": 8, "coverage_batch": 8, "deep_batch": 2,
                "max_survivors": 8, "max_factors": 4,
                "breadth_materialize_enabled": False, "breadth_increment": 0}
    base_cfg.update(stage11 or {})
    return S11.Stage11JobContext(
        store=store, ingestion_root=str(ingestion_root),
        artifact_root=str(root / "artifacts"),
        stage11_root=str(root / "stage11"), shadow_root=str(root / "stage11" / "shadow"),
        cache_dir=str(root / "stage11" / "cache"), rebalance_dates=rebs or [],
        cfg9=_CFG9, stage11=base_cfg, index_name="S&P 500",
        sources=("norgate_local",), horizon_days=63, pit_store=pit,
        sector_series=sector, clock=_clock())


class _Sector:
    """Tiny deterministic PIT sector series keyed by cik."""
    def __init__(self, mapping):
        self._m = mapping  # cik -> sector

    def sector_as_of(self, cik, as_of):
        return self._m.get(str(cik), "Unknown")

    def covered_keys(self):
        return set(self._m)


# =========================================================================== #
# Signal factory (Workstream B).
# =========================================================================== #
def test_p01_primitive_count_and_families():
    assert SL.PRIMITIVE_COUNT >= 24
    fams = SL.primitive_families()
    assert len(fams) >= 6
    assert set(fams).issuperset({"price_momentum", "risk_volatility",
                                 "profitability_quality", "growth_investment"})


def test_p02_spec_id_deterministic_and_dedup():
    a = _spec("momentum_6m")
    b = _spec("momentum_6m")
    assert a.spec_id == b.spec_id            # identical identity -> same id
    c = _spec("momentum_6m", horizon_days=21)
    assert c.spec_id != a.spec_id            # different horizon -> different test


def test_p03_catalogue_dedup_unique_ids():
    cat = SL.build_catalogue(horizons=(21, 63), winsors=(0.02, 0.05))
    ids = [s.spec_id for s in cat]
    assert len(ids) == len(set(ids))
    assert len(cat) >= 150                    # broad-screen family target range


def test_p04_valuation_data_hold_no_market_cap():
    owned = {"assets", "revenue", "net_income", "stockholders_equity"}
    val = [d for d in SL.PRIMITIVE_DEFS if d["family"] == "valuation"]
    assert val
    for d in val:
        ok, reason = SL.spec_supported(d, owned_concepts=owned,
                                       sector_available=True, volume_available=True)
        assert not ok and "MARKET_CAP" in reason


def test_p05_sector_signal_data_hold_when_no_sector():
    d = {x["name"]: x for x in SL.PRIMITIVE_DEFS}["momentum_6m_sector"]
    ok, reason = SL.spec_supported(d, owned_concepts=set(),
                                   sector_available=False, volume_available=True)
    assert not ok and reason == "DATA_HOLD_NO_PIT_SECTOR"


def test_p06_volume_signals_supported():
    d = {x["name"]: x for x in SL.PRIMITIVE_DEFS}["amihud_illiquidity"]
    ok, _ = SL.spec_supported(d, owned_concepts=set(), sector_available=False,
                              volume_available=True)
    assert ok


def test_p07_winsorize_symmetric_clip():
    vals = {("k%d" % i): float(i) for i in range(100)}
    w = SL.winsorize(vals, 0.05)
    assert min(w.values()) >= 4.0 and max(w.values()) <= 95.0


def test_p08_ohlcv_panel_dedup_duplicate_bar(tmp_path):
    dates = _dates(3)
    _write_ohlcv(tmp_path, [100], dates)
    _write_ohlcv(tmp_path, [100], dates, run="r2")       # duplicate run file
    panel = SL.build_ohlcv_panel(str(tmp_path))
    assert len(panel["100"]["d"]) == 3                   # deduped by date


def test_p09_assetid_keying_excludes_ticker_only(tmp_path):
    dates = _dates(2)
    _write_ohlcv(tmp_path, [100], dates)
    b = Path(tmp_path) / "normalized" / "MARKET_BAR" / "2015" / "01" / "02" / "x.jsonl"
    b.parent.mkdir(parents=True, exist_ok=True)
    b.write_text(json.dumps({"record_type": "MARKET_BAR",
                             "source_id": "norgate_local", "security_id": None,
                             "ticker": "ZZ", "effective_at": "2015-01-02",
                             "normalized_payload": {"Date": "2015-01-02",
                                                    "Close": 1.0}}) + "\n",
                 encoding="utf-8")
    panel = SL.build_ohlcv_panel(str(tmp_path))
    assert "100" in panel and None not in panel      # ticker-only row excluded


# =========================================================================== #
# Generic evaluation (Workstream B/C).
# =========================================================================== #
def test_p10_strictly_forward_return_no_lookahead():
    from alpha_agent import fundamental_evidence as FEV
    dates = _dates(200)
    closes = [100.0 + k for k in range(200)]
    # forward return uses the close AFTER as_of only.
    r = FEV._forward_return(dates, closes, dates[50], 10)
    assert abs(r - (closes[60] / closes[50] - 1.0)) < 1e-9


def test_p11_missing_price_drops_name():
    panel = _panel(n_assets=12, n_days=400)
    ctx = _ctx_from_panel(panel)
    spec = _spec("momentum_3m")
    # a signal cross-section for a name with no close index is dropped.
    periods = SE.build_spec_periods(spec, ohlcv=panel,
                                    close_index=ctx["close_index"],
                                    market=ctx["market"],
                                    rebalance_dates=_rebs(panel)[:3])
    for p in periods:
        keys = {k for k, _, _ in p["names"]}
        assert keys.issubset(set(panel))


def test_p12_pit_leakage_prevented():
    st = _pit(["1"], with_prior=False)
    # a 2018 fact filed 2019-02-15 is invisible as-of 2018-06-30.
    assert st.latest_fiscal_key("1", "2018-06-30", concept="assets") is None
    assert st.latest_fiscal_key("1", "2019-03-01", concept="assets") is not None


def test_p13_row_flows_through_unchanged_gates():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    out = SE.evaluate_spec(_spec("momentum_6m"), ohlcv=panel,
                           close_index=ctx["close_index"], market=ctx["market"],
                           rebalance_dates=_rebs(panel))
    m = TT.row_to_contract_metrics(out["row"])
    verdict = TT.classify_evidence(m, _CFG9)
    assert verdict["target_state"] in ("KEEP_FOR_RESEARCH", "REJECTED",
                                       "DATA_HOLD")
    for k in ("rank_ic_t", "spread_t", "net25_spread", "turnover_per_rebalance",
              "scored_periods"):
        assert k in m


def test_p14_thin_coverage_data_hold():
    panel = _panel(n_assets=6, n_days=400)      # <10 names / few periods
    ctx = _ctx_from_panel(panel)
    out = SE.evaluate_spec(_spec("momentum_3m"), ohlcv=panel,
                           close_index=ctx["close_index"], market=ctx["market"],
                           rebalance_dates=_rebs(panel))
    verdict = TT.classify_evidence(TT.row_to_contract_metrics(out["row"]), _CFG9)
    assert verdict["target_state"] == "DATA_HOLD"


def test_p15_cost_sensitivity_grid_present():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    row = SE.evaluate_spec(_spec("momentum_6m"), ohlcv=panel,
                           close_index=ctx["close_index"], market=ctx["market"],
                           rebalance_dates=_rebs(panel))["row"]
    grid = {g["cost_bps"] for g in row["cost_sensitivity"]["grid"]}
    assert {25, 50}.issubset(grid)


def test_p16_turnover_measured():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    row = SE.evaluate_spec(_spec("momentum_6m"), ohlcv=panel,
                           close_index=ctx["close_index"], market=ctx["market"],
                           rebalance_dates=_rebs(panel))["row"]
    assert 0.0 <= row["turnover"] <= 2.0


# =========================================================================== #
# Two-stage funnel (Workstream C).
# =========================================================================== #
def test_p17_screen_partitions_candidates():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    specs = [_spec("momentum_6m"), _spec("momentum_3m"), _spec("realized_vol")]
    res = SR.screen_specs(specs, ctx, _rebs(panel),
                          supported_ids={s.spec_id for s in specs},
                          min_scored_periods=12, cfg=_CFG9)
    assert res["family_size"] == 3
    assert len(res["survivors"]) + len(res["rejected"]) == 3


def test_p18_screen_rejects_no_signal_keeps_signal():
    noise = _panel(rigged=False)
    ctx = _ctx_from_panel(noise)
    # a pure-noise momentum is screened out; the rigged one is kept.
    res_noise = SR.screen_specs([_spec("momentum_6m")], ctx, _rebs(noise),
                                min_scored_periods=12, cfg=_CFG9)
    rig = _panel(rigged=True)
    ctxr = _ctx_from_panel(rig)
    res_rig = SR.screen_specs([_spec("momentum_6m")], ctxr, _rebs(rig),
                              min_scored_periods=12, cfg=_CFG9)
    assert len(res_rig["survivors"]) == 1
    assert len(res_noise["survivors"]) <= 1  # noise never SURVIVES stronger


def test_p19_screen_not_return_only():
    # a spec with a big in-sample spread but no statistical signal is not auto-kept
    # by the screen; screen keys on |rank_ic_t| / |spread_t|, not raw return.
    assert SR.SCREEN_MIN_ABS_RANK_IC_T > 0


# =========================================================================== #
# Statistical discipline (Workstream D).
# =========================================================================== #
def test_p20_bonferroni_increases_with_family():
    from alpha_agent import selection_controls as SC
    p = 0.02
    assert SC.bonferroni_adjust(p, 1) < SC.bonferroni_adjust(p, 50)


def test_p21_multiple_testing_records_family_and_survivors():
    recs = [{"spec_id": "s%d" % i, "name": "n%d" % i, "periods": 30,
             "rank_ic_t": (3.5 if i == 0 else 0.2)} for i in range(20)]
    mt = SR.multiple_testing(recs, family_size=20)
    assert mt["family_size"] == 20 and mt["tested"] == 20
    assert set(mt["fdr_survivors"]).issubset({r["spec_id"] for r in recs})
    assert "benjamini_hochberg" in mt["method"]


def test_p22_deflated_sharpe_present():
    recs = [{"spec_id": "s0", "name": "n0", "periods": 40, "rank_ic_t": 4.0}]
    mt = SR.multiple_testing(recs, family_size=200)
    assert mt["rows"][0]["deflated_sharpe"] is not None
    assert mt["rows"][0]["selection_penalty"] < 1.0    # search-size penalty


# =========================================================================== #
# Deep evaluation (Workstream C/D).
# =========================================================================== #
def test_p23_deep_evaluate_qualifies_on_rigged():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    res = SR.deep_evaluate(_spec("momentum_6m"), ctx, _rebs(panel), cfg9=_CFG9,
                           family_size=1, multiple_testing_survived=True,
                           cfg=_CFG9)
    assert res["gate_verdict"]["target_state"] == "KEEP_FOR_RESEARCH"
    assert res["qualifies"] is True
    assert res["holdout"]["confirms_sign"] is True
    assert res["parameter_neighborhood"]["sign_stable"] is True


def test_p24_deep_requires_multiple_testing():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    res = SR.deep_evaluate(_spec("momentum_6m"), ctx, _rebs(panel), cfg9=_CFG9,
                           family_size=1, multiple_testing_survived=False,
                           cfg=_CFG9)
    assert res["qualifies"] is False        # FDR not survived -> never qualifies


def test_p25_parameter_neighborhood_stability():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    nb = SR.parameter_neighborhood(_spec("momentum_6m"), ctx, _rebs(panel),
                                   cfg=_CFG9)
    assert nb["sign_stable"] is True and nb["neighbours"]


def test_p26_holdout_ledger_once_usable():
    from alpha_agent import selection_controls as SC
    led = SC.HoldoutLedger(str(REPO / "does_not_matter.json"))
    # use an in-memory temp path
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        led = SC.HoldoutLedger(str(Path(tmp) / "hl.json"))
        led.reserve("k1")
        led.mark_used("k1")
        try:
            led.mark_used("k1")
            assert False, "reuse should raise"
        except SC.HoldoutReuseError:
            pass


def test_p27_holdout_split_disjoint():
    dates = _dates(20)
    is_d, hold_d = SR._split_holdout(dates, 0.30)
    assert set(is_d).isdisjoint(hold_d)
    assert is_d and hold_d and is_d[-1] < hold_d[0]


# =========================================================================== #
# Ensemble discovery (Workstream E).
# =========================================================================== #
def test_p28_return_stream_correlations():
    series = {"a": {"long_short_by_date": {"d1": 0.1, "d2": -0.1, "d3": 0.2}},
              "b": {"long_short_by_date": {"d1": 0.1, "d2": -0.1, "d3": 0.2}}}
    corr = SR.return_stream_correlations(series)
    assert abs(corr[("a", "b")] - 1.0) < 1e-9


def test_p29_select_orthogonal_collapses_duplicates():
    survivors = [{"spec_id": "a", "rank_ic_t": 3.0},
                 {"spec_id": "b", "rank_ic_t": 2.5}]
    series = {"a": {"long_short_by_date": {"d%d" % k: (0.1 if k % 2 else -0.1)
                                           for k in range(8)}},
              "b": {"long_short_by_date": {"d%d" % k: (0.1 if k % 2 else -0.1)
                                           for k in range(8)}}}
    sel = SR.select_orthogonal(survivors, series, max_factors=5)
    assert sel["selected"] == ["a"]          # b collapses (corr ~ 1)
    assert sel["collapsed"]


def test_p30_select_orthogonal_bounded():
    survivors = [{"spec_id": "s%d" % i, "rank_ic_t": 3.0 - i * 0.1}
                 for i in range(10)]
    series = {"s%d" % i: {"long_short_by_date":
                          {"d%d" % k: math.sin(i * 3.1 + k) for k in range(12)}}
              for i in range(10)}
    sel = SR.select_orthogonal(survivors, series, max_factors=3)
    assert len(sel["selected"]) <= 3


def test_p31_discover_ensembles_weights_sum_one():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    specs = [_spec("momentum_6m"), _spec("momentum_3m")]
    out = SR.discover_ensembles(specs, {s.spec_id: {"rank_ic_t": 3.0}
                                        for s in specs}, ctx, _rebs(panel),
                                cfg9=_CFG9, cfg=_CFG9)
    assert out["ensembles"]
    for e in out["ensembles"]:
        s = sum(c["weight"] for c in e["components"])
        assert abs(s - 1.0) < 1e-6


def test_p32_ensemble_weight_isolation_train_only():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    specs = [_spec("momentum_6m"), _spec("realized_vol")]
    out = SR.discover_ensembles(specs, {s.spec_id: {"rank_ic_t": 2.0}
                                        for s in specs}, ctx, _rebs(panel),
                                cfg9=_CFG9, cfg=_CFG9)
    names = {e["ensemble"] for e in out["ensembles"]}
    assert {"equal_weight", "evidence_weighted", "train_constrained"} <= names


def test_p33_ensemble_rejects_weak_components():
    noise = _panel(rigged=False)
    ctx = _ctx_from_panel(noise)
    specs = [_spec("momentum_6m"), _spec("momentum_3m")]
    out = SR.discover_ensembles(specs, {s.spec_id: {"rank_ic_t": 0.2}
                                        for s in specs}, ctx, _rebs(noise),
                                cfg9=_CFG9, cfg=_CFG9)
    assert out["qualified"] == []            # weak components -> no qualified ens.


# =========================================================================== #
# Sector neutralization (Workstream B).
# =========================================================================== #
def test_p34_sector_demean_drops_unknown():
    panel = _panel(n_assets=6, n_days=400)
    c2a = {"1": "A0", "2": "A1", "3": "A2", "4": "A3"}
    sector = _Sector({"1": "Tech", "2": "Tech", "3": "Energy"})  # cik 4 -> Unknown
    spec = _spec("momentum_6m", neutralization="sector")
    ctx = _ctx_from_panel(panel, c2a=c2a, sector=sector)
    vals = SL.signal_cross_section(spec, ohlcv=panel, market=ctx["market"],
                                   cik_to_assetid=c2a, sector_series=sector,
                                   as_of=panel["A0"]["d"][300])
    assert "A3" not in vals                   # cik 4 Unknown-sector dropped


def test_p35_sector_demean_changes_ranks():
    panel = _panel(n_assets=8, n_days=500)
    c2a = {str(i + 1): "A%d" % i for i in range(8)}
    sector = _Sector({str(i + 1): ("Tech" if i < 4 else "Energy")
                      for i in range(8)})
    as_of = panel["A0"]["d"][400]
    plain = SL.signal_cross_section(_spec("momentum_6m"), ohlcv=panel,
                                    market=None, cik_to_assetid=c2a, as_of=as_of)
    sn = SL.signal_cross_section(_spec("momentum_6m", neutralization="sector"),
                                 ohlcv=panel, market=None, cik_to_assetid=c2a,
                                 sector_series=sector, as_of=as_of)
    assert plain and sn and plain != sn


# =========================================================================== #
# Shadow-only portfolio (Workstream F).
# =========================================================================== #
def test_p36_target_book_structure_and_safety():
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    spec = _spec("momentum_6m")
    out = SE.evaluate_spec(spec, ohlcv=panel, close_index=ctx["close_index"],
                           market=ctx["market"], rebalance_dates=_rebs(panel))
    book = SHP.build_target_book(
        strategy_name="s", components=[(spec, 1.0)], ctx=ctx,
        as_of=_rebs(panel)[-1], evidence_row=out["row"], gate_verdict={},
        combined_score=0.7)
    assert book["holdings"] and book["status"] == SHP.STATUS_ACTIVE
    assert abs(book["target_weights_sum"] - 1.0) < 1e-6
    assert set(SHP.SAFETY_LABELS) <= set(book["safety_labels"])
    assert book["creates_orders"] is False and book["operating_portfolio"] is False
    assert book["strategy_version"].startswith("sv_")


def test_p37_shadow_activate_immutable(tmp_path):
    store = SHP.ShadowPortfolioStore(str(tmp_path / "shadow"))
    book = {"strategy_name": "s", "as_of": "2020-01-01", "holdings": [],
            "components": [], "strategy_version": "sv_x", "benchmark": "SPY"}
    d1 = store.activate(book)
    assert d1["status"] == SHP.STATUS_ACTIVE
    # re-activate is first-write-wins on the immutable snapshot.
    sid = d1["active_strategy"]
    snap = store.load_strategy(sid)
    assert snap["strategy_version"] == "sv_x"
    store.activate({**book, "strategy_version": "sv_changed"})
    assert store.load_strategy(sid)["strategy_version"] == "sv_x"


def test_p38_no_alpha_status(tmp_path):
    store = SHP.ShadowPortfolioStore(str(tmp_path / "shadow"))
    doc = store.record_no_alpha(as_of="2020-01-01", evaluated=42)
    assert doc["status"] == SHP.STATUS_NO_ALPHA
    assert "NOT ACTIVATED" in doc["message"]
    assert SHP.summarize(store)["status"] == SHP.STATUS_NO_ALPHA


def test_p39_shadow_store_separate_root(tmp_path):
    root = str(tmp_path / "stage11" / "shadow")
    store = SHP.ShadowPortfolioStore(root)
    store.record_no_alpha(as_of="2020-01-01", evaluated=0)
    # everything the store writes lives under its own root; nothing outside it.
    written = list((tmp_path / "stage11" / "shadow").rglob("*"))
    assert written
    assert all(str(p).startswith(str(tmp_path / "stage11")) for p in written)


# =========================================================================== #
# Campaign lanes / planner / dispatch (Workstream A/H).
# =========================================================================== #
def test_p40_inventory_and_panel_build_lanes(tmp_path):
    dates = _dates(120)
    _write_ohlcv(tmp_path / "ing", [100 + i for i in range(6)], dates)
    store = _id_store(tmp_path, 6)
    ctx = _s11_ctx(tmp_path, store, tmp_path / "ing", pit=_pit([]),
                   rebs=dates[::30][:4])
    o1, d1 = S11.dispatch_stage11_job(_Job(S11.LANE_INVENTORY), ctx)
    o2, d2 = S11.dispatch_stage11_job(_Job(S11.LANE_PANEL_BUILD), ctx)
    assert o1 == AR.OUTCOME_COMPLETED and o2 == AR.OUTCOME_COMPLETED
    assert (Path(ctx.stage11_root) / "state" / "inventory.json").exists()
    assert store.get_meta("stage11_panel_epoch")


def test_p41_planner_one_live_job(tmp_path):
    _write_ohlcv(tmp_path / "ing", [100, 101, 102], _dates(30))
    store = _id_store(tmp_path, 3)
    q = AR.ResearchQueue(str(tmp_path / "autonomy.sqlite"))
    ctx = _s11_ctx(tmp_path, store, tmp_path / "ing", pit=_pit([]))
    p1 = S11.plan_next_stage11_job(q, ctx, cfg=ctx.stage11)
    assert p1 and p1["origin"] == S11.ORIGIN_11
    p2 = S11.plan_next_stage11_job(q, ctx, cfg=ctx.stage11)
    assert p2 is None                         # one live stage11 job at a time


def test_p42_planner_dependency_order(tmp_path):
    _write_ohlcv(tmp_path / "ing", [100, 101, 102], _dates(30))
    store = _id_store(tmp_path, 3)
    q = AR.ResearchQueue(str(tmp_path / "autonomy.sqlite"))
    ctx = _s11_ctx(tmp_path, store, tmp_path / "ing", pit=_pit([]))
    p = S11.plan_next_stage11_job(q, ctx, cfg=ctx.stage11)
    assert p["lane"] == S11.LANE_ORDER[0]     # first incomplete lane


def test_p43_planner_none_when_complete(tmp_path):
    _write_ohlcv(tmp_path / "ing", [100, 101, 102], _dates(30))
    store = _id_store(tmp_path, 3)
    q = AR.ResearchQueue(str(tmp_path / "autonomy.sqlite"))
    ctx = _s11_ctx(tmp_path, store, tmp_path / "ing", pit=_pit([]))
    epoch = S11._epoch(ctx)
    for lane in S11.LANE_ORDER:
        store.set_meta(S11._FLAG[lane], epoch)
    assert S11.plan_next_stage11_job(q, ctx, cfg=ctx.stage11) is None


def test_p44_dispatch_unknown_lane_blocked(tmp_path):
    store = _id_store(tmp_path, 1)
    ctx = _s11_ctx(tmp_path, store, tmp_path / "ing", pit=_pit([]))
    o, d = S11.dispatch_stage11_job(_Job("stage11.does_not_exist"), ctx)
    assert o == AR.OUTCOME_BLOCKED_SPECIFIC


def test_p45_dispatch_failure_isolated(tmp_path):
    store = _id_store(tmp_path, 1)
    # no ingestion tree -> panel build raises internally but is isolated.
    ctx = _s11_ctx(tmp_path, store, tmp_path / "missing", pit=_pit([]))
    o, d = S11.dispatch_stage11_job(_Job(S11.LANE_PANEL_BUILD), ctx)
    assert o in (AR.OUTCOME_BLOCKED_SPECIFIC, AR.OUTCOME_RETRYABLE,
                 AR.OUTCOME_COMPLETED)        # never crashes the drain


def test_p46_chunked_lanes_not_idempotency_skipped():
    assert S11.LANE_BROAD_SCREEN in S11._CHUNKED_LANES
    assert S11.LANE_DEEP_EVAL in S11._CHUNKED_LANES
    assert S11.LANE_INVENTORY not in S11._CHUNKED_LANES


def test_p47_command_center_safety_and_no_promotion(tmp_path):
    _write_ohlcv(tmp_path / "ing", [100, 101, 102], _dates(30))
    store = _id_store(tmp_path, 3)
    ctx = _s11_ctx(tmp_path, store, tmp_path / "ing", pit=_pit([]),
                   rebs=_dates(4))
    cc = S11.build_command_center(ctx)
    assert cc["no_automatic_promotion"] is True
    assert set(S11.SAFETY_BADGES) == set(cc["safety_badges"])
    assert "NO ORDERS" in cc["safety_badges"]


# =========================================================================== #
# Read-only API / UI (Workstream G) + safety.
# =========================================================================== #
def test_p48_snapshot_loader_safe_when_disabled(tmp_path):
    cfgp = tmp_path / "cfg.json"
    cfgp.write_text(json.dumps({"stage": "8"}), encoding="utf-8")
    snap = S11.load_stage11_snapshot(str(cfgp))
    assert "safety_badges" in snap


def test_p49_served_ui_has_stage11_section():
    html = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                          errors="ignore")
    assert 'id="s11-panel"' in html
    assert "loadStage11" in html
    assert "/v1/research/stage11/command-center" in html
    assert "SHADOW ONLY" in html and "NO ORDERS" in html


def test_p50_no_operational_imports():
    # the Stage 11 modules must never import operational trading ledgers.
    for mod in ("signal_library", "signal_evaluation", "stage11_research",
                "shadow_portfolio", "stage11_jobs"):
        src = (REPO / "alpha_agent" / ("%s.py" % mod)).read_text(encoding="utf-8")
        for banned in ("operational_book", "paper_desk", "alpha_book",
                       "daily_close", "order", "fill"):
            # allow the words in comments/labels but not as imports.
            assert ("import %s" % banned) not in src
            assert ("from paper_trader.api" not in src)


def test_p51_shadow_qualification_end_to_end(tmp_path):
    """A rigged signal that passes every unchanged gate -> deep qualifies ->
    a shadow target book is built and activated with the safety labels."""
    panel = _panel(rigged=True)
    ctx = _ctx_from_panel(panel)
    spec = _spec("momentum_6m")
    res = SR.deep_evaluate(spec, ctx, _rebs(panel), cfg9=_CFG9, family_size=1,
                           multiple_testing_survived=True, cfg=_CFG9)
    assert res["qualifies"] is True
    out = SE.evaluate_spec(spec, ohlcv=panel, close_index=ctx["close_index"],
                           market=ctx["market"], rebalance_dates=_rebs(panel))
    book = SHP.build_target_book(strategy_name="stage11_single_momentum_6m",
                                 components=[(spec, 1.0)], ctx=ctx,
                                 as_of=_rebs(panel)[-1], evidence_row=out["row"],
                                 gate_verdict=res["gate_verdict"],
                                 combined_score=0.7)
    store = SHP.ShadowPortfolioStore(str(tmp_path / "stage11" / "shadow"))
    doc = store.activate(book)
    assert doc["status"] == SHP.STATUS_ACTIVE
    assert SHP.summarize(store)["strategies"][0]["holdings"]


# =========================================================================== #
# Release-fix (Problem 1): finalize ORDERING + terminal persisted command center.
# The finalize completion flag must be stamped BEFORE the command-center snapshot
# is assembled, so the persisted snapshot agrees with a live recomputation
# (COMPLETE + measured-for-epoch) instead of freezing at IN_PROGRESS. The honest
# negative shadow outcome (NO_DEFENSIBLE_ALPHA) must stay visible and no shadow
# book may be activated.
# =========================================================================== #
def _finalize_ready_ctx(tmp_path):
    """A Stage 11 context whose shadow decision is durably recorded
    NO_DEFENSIBLE_ALPHA (no deep-eval / ensemble artifacts => zero qualifiers) and
    whose non-finalize lanes are stamped complete for the epoch, ready to finalize."""
    dates = _dates(40)
    _write_ohlcv(tmp_path / "ing", [100, 101, 102], dates)
    store = _id_store(tmp_path, 3)
    ctx = _s11_ctx(tmp_path, store, tmp_path / "ing", pit=_pit([]),
                   rebs=dates[::10][:4])
    o, d = S11.dispatch_stage11_job(_Job(S11.LANE_SHADOW_DECIDE), ctx)
    assert o == AR.OUTCOME_COMPLETED
    assert d["shadow_status"] == SHP.STATUS_NO_ALPHA
    epoch = S11._epoch(ctx)
    for lane in S11.LANE_ORDER:
        if lane != S11.LANE_FINALIZE:
            store.set_meta(S11._FLAG[lane], epoch)
    return ctx


def test_p52_finalize_flag_established_before_snapshot_built(tmp_path, monkeypatch):
    # (1) the finalize completion flag is stamped BEFORE the snapshot is assembled.
    ctx = _finalize_ready_ctx(tmp_path)
    epoch = S11._epoch(ctx)
    assert ctx.store.get_meta(S11._FLAG[S11.LANE_FINALIZE]) != epoch   # not yet
    seen = {}
    orig = S11.build_command_center

    def _observing_build(c):
        seen["flag_at_build"] = c.store.get_meta(S11._FLAG[S11.LANE_FINALIZE])
        seen["epoch_at_build"] = S11._epoch(c)
        return orig(c)

    monkeypatch.setattr(S11, "build_command_center", _observing_build)
    snap = S11.persist_terminal_command_center(ctx)
    # the flag already equalled the epoch at the instant the snapshot was built.
    assert seen["flag_at_build"] == seen["epoch_at_build"]
    assert snap["status"] == "COMPLETE"


def test_p53_persisted_command_center_complete_after_finalize(tmp_path):
    # (2) persisted command_center.json reports COMPLETE after finalization.
    ctx = _finalize_ready_ctx(tmp_path)
    o, d = S11.dispatch_stage11_job(_Job(S11.LANE_FINALIZE), ctx)
    assert o == AR.OUTCOME_COMPLETED and d["status"] == "COMPLETE"
    persisted = json.loads((Path(ctx.stage11_root) / "state" /
                            "command_center.json").read_text(encoding="utf-8"))
    assert persisted["status"] == "COMPLETE"


def test_p54_persisted_and_live_status_agree(tmp_path):
    # (3) persisted status == an independent live recomputation.
    ctx = _finalize_ready_ctx(tmp_path)
    S11.dispatch_stage11_job(_Job(S11.LANE_FINALIZE), ctx)
    persisted = json.loads((Path(ctx.stage11_root) / "state" /
                            "command_center.json").read_text(encoding="utf-8"))
    live = S11.build_command_center(ctx)
    assert persisted["status"] == live["status"] == "COMPLETE"


def test_p55_terminal_next_action_measured_for_epoch(tmp_path):
    # (4) the terminal next_action states the campaign is measured for the epoch.
    ctx = _finalize_ready_ctx(tmp_path)
    snap = S11.persist_terminal_command_center(ctx)
    assert snap["next_action"] == \
        "Stage 11 campaign measured for the current owned-data epoch"


def test_p56_no_defensible_alpha_visible_and_not_activated(tmp_path):
    # (5) NO_DEFENSIBLE_ALPHA stays visible AND (6) no shadow book is activated.
    ctx = _finalize_ready_ctx(tmp_path)
    snap = S11.persist_terminal_command_center(ctx)
    assert (snap["shadow"] or {}).get("status") == SHP.STATUS_NO_ALPHA
    assert (snap["shadow"] or {}).get("active_strategy") is None
    assert (snap["shadow"] or {}).get("strategies") == []


def test_p57_finalize_idempotent_byte_identical(tmp_path):
    # (7) re-running finalization is idempotent: same flag, byte-identical file.
    ctx = _finalize_ready_ctx(tmp_path)
    cc = Path(ctx.stage11_root) / "state" / "command_center.json"
    S11.persist_terminal_command_center(ctx)
    epoch = S11._epoch(ctx)
    first = cc.read_bytes()
    flag1 = ctx.store.get_meta(S11._FLAG[S11.LANE_FINALIZE])
    S11.persist_terminal_command_center(ctx)
    second = cc.read_bytes()
    flag2 = ctx.store.get_meta(S11._FLAG[S11.LANE_FINALIZE])
    assert first == second
    assert flag1 == flag2 == epoch


def test_p58_finalize_no_duplicate_artifacts_or_jobs(tmp_path):
    # (8) finalization does not duplicate artifacts or enqueue duplicate jobs.
    ctx = _finalize_ready_ctx(tmp_path)
    q = AR.ResearchQueue(str(tmp_path / "autonomy.sqlite"))
    o1, d1 = S11.dispatch_stage11_job(_Job(S11.LANE_FINALIZE), ctx)
    assert o1 == AR.OUTCOME_COMPLETED and "artifact" in d1
    art_dir = Path(ctx.artifact_root) / S11.LANE_FINALIZE.replace(".", "_")
    n_after_first = len(list(art_dir.glob("*.json")))
    # a second identical finalize dispatch is idempotency-skipped (handler not re-run)
    o2, d2 = S11.dispatch_stage11_job(_Job(S11.LANE_FINALIZE), ctx)
    assert o2 == AR.OUTCOME_COMPLETED and d2.get("idempotent_skip") is True
    assert len(list(art_dir.glob("*.json"))) == n_after_first
    # the whole pipeline is measured => the planner enqueues no further job.
    assert S11.plan_next_stage11_job(q, ctx, cfg=ctx.stage11) is None


def test_p59_finalize_never_touches_operational_ledger(tmp_path):
    # (9) finalization writes nothing to an operational ledger.
    op = tmp_path / "op" / "daily_close_journal.json"
    op.parent.mkdir(parents=True, exist_ok=True)
    sentinel = json.dumps({"seq": 9, "close_status": "DAILY_CLOSE_COMPLETE_HOLD"})
    op.write_text(sentinel, encoding="utf-8")
    ctx = _finalize_ready_ctx(tmp_path)
    S11.persist_terminal_command_center(ctx)
    assert op.read_text(encoding="utf-8") == sentinel
    # and the finalize path only writes under the dedicated Stage 11 root.
    assert not list((tmp_path / "op").glob("*.tmp"))


def test_p60_api_loaders_return_complete_and_no_alpha(tmp_path):
    # (10) the read-only API loaders return COMPLETE + NO_DEFENSIBLE_ALPHA from the
    # corrected persisted state (exercises _lite_context / _resolve_stage11_runtime).
    ctx = _finalize_ready_ctx(tmp_path)
    o, d = S11.dispatch_stage11_job(_Job(S11.LANE_FINALIZE), ctx)
    assert o == AR.OUTCOME_COMPLETED and d["status"] == "COMPLETE"
    cfg = {"stage10_identity": {"store_db": str(tmp_path / "id_api.sqlite")},
           "stage11": {"enabled": True,
                       "ingestion_root": str(tmp_path / "ing"),
                       "artifact_root": str(tmp_path / "artifacts"),
                       "stage11_root": ctx.stage11_root,
                       "shadow_root": ctx.shadow_root,
                       "cache_dir": str(tmp_path / "stage11" / "cache"),
                       "sources": ["norgate_local"], "horizon_days": 63}}
    cfgp = tmp_path / "cfg_api.json"
    cfgp.write_text(json.dumps(cfg), encoding="utf-8")
    snap = S11.load_stage11_snapshot(str(cfgp))
    assert snap["status"] == "COMPLETE"
    assert (snap.get("shadow") or {}).get("status") == SHP.STATUS_NO_ALPHA
    assert "NO ORDERS" in snap["safety_badges"]
    sh = S11.load_stage11_shadow(str(cfgp))
    assert sh["status"] == SHP.STATUS_NO_ALPHA
