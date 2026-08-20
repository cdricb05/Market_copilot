"""alpha_agent.r31.snapshot - the ONE Release 31 data snapshot owner.

Builds, caches and fingerprints the frozen point-in-time panel every candidate in
the campaign is fitted and judged on. Candidate generation and candidate
evaluation both read THIS cache, so a result can never be produced against a
quietly different sample.

Ownership: this module builds NO feature mathematics of its own. Features, the
point-in-time contract and the delisting-safe label all come from the released
Release-30 owners (``alpha_agent.release30_panel`` and the cross-section
assembly in ``alpha_agent.release30_forecast_research``), whose PIT rules are
already asserted by ``tests/test_release30_return_forecast.py``. What this module
adds is: a content-hashed freeze, a declared per-family manifest, and the
SURVIVORSHIP MEASUREMENT that decides which sample may carry a verdict.

The survivorship measurement is not a formality. The owned PIT fundamental store
covers a bounded set of SEC filers, and its coverage of names that stopped
trading is far below its coverage of names still trading. A fundamental-family
result measured on that sub-sample is measured where the losers are missing,
which is exactly the bias that inflates fundamental factor returns. The campaign
therefore measures the skew, declares it, and refuses to let the fundamental
sample carry a superiority verdict on its own.
"""
from __future__ import annotations

import warnings
from pathlib import Path
from typing import Optional

import numpy as np

from .. import release30_forecast_research as _rf
from .. import release30_panel as _rp
from .. import stage24_pit_fundamental as _s24
from .. import r31
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r31.snapshot"
MANIFEST_SCHEMA = "r31_data_snapshot_manifest/1"
MANIFEST_NAME = "data_snapshot_manifest.json"
CACHE_NAME = "panel_snapshot.npz"

#: Feature order is FROZEN here. Every artifact records it, and a candidate spec
#: hash binds it, so a reordered feature list is a different snapshot.
PRICE_FEATURES = _rp.PRICE_FEATURE_NAMES
FUNDAMENTAL_FEATURES = _rp.FUNDAMENTAL_FEATURE_NAMES
ALL_FEATURES = PRICE_FEATURES + FUNDAMENTAL_FEATURES


# --------------------------------------------------------------------------- #
# Build + cache
# --------------------------------------------------------------------------- #
def build_cache(*, campaign_id: str = _contract.CAMPAIGN_ID,
                price_panel_path=None, force: bool = False) -> Path:
    """Build the frozen panel cache once. Returns the cache path.

    The cache is a flat row store: every decision-date cross-section is stored
    contiguously with an offset index, which keeps a fold slice a pure array view
    and makes the whole campaign's data access deterministic.
    """
    out = r31.campaign_dir(campaign_id) / CACHE_NAME
    if out.exists() and not force:
        return out

    panel = _rp.load_price_panel(price_panel_path)
    store = _s24.Stage24PitStore()
    store.load()
    bridge = _s24.IdentityBridge()
    bridge.load()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ds = _rf.build_dataset(panel, with_fundamentals=True, store=store,
                               bridge=bridge, horizons=_contract.HORIZONS,
                               step_days=_contract.STEP_SESSIONS)

    n_sec = len(ds.sections)
    if n_sec == 0:
        raise RuntimeError("R31 snapshot produced no cross-section")

    counts = np.array([s.X.shape[0] for s in ds.sections], dtype=np.int64)
    offsets = np.concatenate([[0], np.cumsum(counts)]).astype(np.int64)
    total = int(offsets[-1])
    n_feat = len(ds.feature_names)

    X = np.empty((total, n_feat), dtype=np.float64)
    adv = np.empty(total, dtype=np.float64)
    has_fund = np.empty(total, dtype=bool)
    sym_idx = np.empty(total, dtype=np.int32)
    labels = {h: np.empty(total, dtype=np.float64)
              for h in list(_contract.HORIZONS) + [_contract.STEP_SESSIONS]}
    trunc = {h: np.empty(total, dtype=bool) for h in labels}

    for k, s in enumerate(ds.sections):
        lo, hi = int(offsets[k]), int(offsets[k + 1])
        X[lo:hi] = s.X
        adv[lo:hi] = s.adv
        has_fund[lo:hi] = s.has_fundamentals
        sym_idx[lo:hi] = s.cols.astype(np.int32)
        for h in labels:
            labels[h][lo:hi] = s.labels[h]
            trunc[h][lo:hi] = s.truncated[h]

    payload = {
        "X": X, "adv": adv, "has_fund": has_fund, "sym_idx": sym_idx,
        "offsets": offsets,
        "dates": np.array([s.date for s in ds.sections], dtype="U10"),
        "session_index": np.array([s.t for s in ds.sections], dtype=np.int64),
        "feature_names": np.array(list(ds.feature_names), dtype="U40"),
        "symbols": panel.symbols.astype("U16"),
    }
    for h in labels:
        payload["label_%d" % h] = labels[h]
        payload["trunc_%d" % h] = trunc[h]

    out.parent.mkdir(parents=True, exist_ok=True)
    # ``savez_compressed`` appends ".npz" unless the name already ends with it,
    # so the temp name must carry that suffix itself or the rename target moves.
    tmp = out.with_name(out.stem + ".building.npz")
    with open(tmp, "wb") as fh:
        np.savez_compressed(fh, **payload)
    tmp.replace(out)
    return out


# --------------------------------------------------------------------------- #
# Read
# --------------------------------------------------------------------------- #
class Snapshot:
    """The frozen panel, read back. Immutable by convention: nothing mutates it."""

    def __init__(self, path):
        self.path = Path(path)
        z = np.load(self.path, allow_pickle=False)
        self.X = z["X"]
        self.adv = z["adv"]
        self.has_fund = z["has_fund"]
        self.sym_idx = z["sym_idx"]
        self.offsets = z["offsets"]
        self.dates = [str(d) for d in z["dates"]]
        self.session_index = z["session_index"]
        self.feature_names = tuple(str(f) for f in z["feature_names"])
        self.symbols = np.array([str(s) for s in z["symbols"]])
        self.labels = {}
        self.truncated = {}
        for h in list(_contract.HORIZONS) + [_contract.STEP_SESSIONS]:
            key = "label_%d" % h
            if key in z:
                self.labels[int(h)] = z[key]
                self.truncated[int(h)] = z["trunc_%d" % h]
        self.content_hash = r31.sha_file(self.path)

    # -- geometry ---------------------------------------------------------- #
    @property
    def n_sections(self) -> int:
        return len(self.dates)

    def rows(self, k: int) -> slice:
        return slice(int(self.offsets[k]), int(self.offsets[k + 1]))

    def feature_index(self, names) -> np.ndarray:
        idx = {n: i for i, n in enumerate(self.feature_names)}
        return np.array([idx[n] for n in names], dtype=np.int64)

    # -- samples ----------------------------------------------------------- #
    def sample_mask(self, sample: str, k: int) -> np.ndarray:
        """Row mask for one cross-section under one declared sample geometry."""
        r = self.rows(k)
        if sample == _contract.SAMPLE_PRICE_FULL:
            return np.ones(r.stop - r.start, dtype=bool)
        if sample == _contract.SAMPLE_FUND_MATCHED:
            return self.has_fund[r]
        raise ValueError("unknown sample %r" % (sample,))

    def sample_sections(self, sample: str) -> list:
        """Indices of cross-sections that are a valid cross-section under ``sample``."""
        out = []
        for k in range(self.n_sections):
            if int(self.sample_mask(sample, k).sum()) >= _contract.MIN_CROSS_SECTION:
                out.append(k)
        return out

    def block(self, sample: str, k: int, feature_names, horizon: int) -> tuple:
        """``(X, y, adv, sym_idx)`` for one cross-section.

        ``y`` is the forward EXCESS return over the cross-sectional equal-weight
        mean of THIS sample's own eligible names, demeaned per date. Features are
        re-rank-normalised WITHIN the sample, because a percentile computed over a
        wider set is not the percentile the model would see when deployed on the
        narrower one.
        """
        r = self.rows(k)
        m = self.sample_mask(sample, k)
        cols = self.feature_index(feature_names)
        Xs = self.X[r][m][:, cols]
        if sample == _contract.SAMPLE_FUND_MATCHED:
            Xs = np.column_stack([_rf.rank_normalise(Xs[:, j])
                                  for j in range(Xs.shape[1])])
        y = self.labels[int(horizon)][r][m]
        finite = np.isfinite(y)
        mu = float(y[finite].mean()) if finite.any() else 0.0
        return Xs, y - mu, self.adv[r][m], self.sym_idx[r][m]

    def holding_returns(self, sample: str, k: int, hold: int) -> tuple:
        """``(raw_return, benchmark)`` over the REBALANCE window, not the label
        horizon.

        A book is rebalanced every ``hold`` sessions whatever horizon the model
        forecasts, so its realised economics are the ``hold``-session return. The
        benchmark is the equal-weight mean of the SAME eligible cross-section -
        the owned universe's own return. The panel carries no index security, so
        the campaign benchmarks against the universe it actually chooses from and
        says so, rather than substituting a proxy it does not own.
        """
        r = self.rows(k)
        m = self.sample_mask(sample, k)
        raw = self.labels[int(hold)][r][m]
        finite = np.isfinite(raw)
        bench = float(raw[finite].mean()) if finite.any() else 0.0
        return raw, bench


def load(campaign_id: str = _contract.CAMPAIGN_ID) -> Snapshot:
    return Snapshot(r31.campaign_dir(campaign_id) / CACHE_NAME)


# --------------------------------------------------------------------------- #
# Survivorship measurement
# --------------------------------------------------------------------------- #
def survivorship_report(*, price_panel_path=None) -> dict:
    """Measure PIT fundamental coverage of SURVIVING vs DELISTED names.

    A ratio materially above 1.0 means the fundamental sub-sample systematically
    omits names that stopped trading, and any factor return measured on it is
    biased upward by an unknown amount. The campaign reports the number rather
    than asserting the sample is clean.
    """
    panel = _rp.load_price_panel(price_panel_path)
    store = _s24.Stage24PitStore()
    store.load()
    bridge = _s24.IdentityBridge()
    bridge.load()
    covered = set(store.covered_ciks())

    finite = np.isfinite(panel.close)
    last_t = np.where(finite.any(axis=0), finite.shape[0] - 1
                      - np.argmax(finite[::-1], axis=0), -1)
    alive = last_t >= (panel.n_dates - 5)
    dead = (last_t >= 0) & ~alive
    ever_member = panel.member.any(axis=0)

    def _cov(mask) -> tuple:
        idx = np.nonzero(mask & ever_member)[0]
        hit = 0
        for j in idx:
            cik = bridge.cik_for(str(panel.symbols[j]))
            if cik is not None and cik in covered:
                hit += 1
        return int(idx.size), int(hit)

    n_alive, h_alive = _cov(alive)
    n_dead, h_dead = _cov(dead)
    r_alive = h_alive / max(n_alive, 1)
    r_dead = h_dead / max(n_dead, 1)
    ratio = r_alive / r_dead if r_dead > 0 else None

    return {
        "measurement": "PIT_FUNDAMENTAL_COVERAGE_BY_SURVIVAL_STATUS",
        "covered_ciks": len(covered),
        "panel_last_date": panel.iso(panel.n_dates - 1),
        "still_trading": {"names": n_alive, "covered": h_alive,
                          "coverage": round(r_alive, 4)},
        "delisted": {"names": n_dead, "covered": h_dead,
                     "coverage": round(r_dead, 4)},
        "coverage_ratio_alive_over_delisted": (None if ratio is None
                                               else round(ratio, 3)),
        "verdict": ("FUNDAMENTAL_SAMPLE_SURVIVORSHIP_LIMITED"
                    if (ratio or 0.0) > 1.25 else "FUNDAMENTAL_SAMPLE_BALANCED"),
        "consequence": (
            "the fundamental-matched sample may be measured and reported, but it "
            "may not on its own establish a superiority verdict; the primary "
            "verdict is carried by the survivorship-free price sample"),
    }


# --------------------------------------------------------------------------- #
# Manifest
# --------------------------------------------------------------------------- #
def _family(*, name, owner, source, path, pit_semantics, publication_semantics,
            membership_semantics, delisted_handling, missingness,
            eligible_transformations, coverage_start=None, coverage_end=None) -> dict:
    return {"family": name, "owner": owner, "source": source,
            "file": r31.file_fingerprint(path) if path else None,
            "coverage_start": coverage_start, "coverage_end": coverage_end,
            "pit_semantics": pit_semantics,
            "publication_effective_date_semantics": publication_semantics,
            "historical_membership_semantics": membership_semantics,
            "inactive_delisted_handling": delisted_handling,
            "missingness": missingness,
            "eligible_transformations": list(eligible_transformations)}


def build_manifest(snap: Snapshot, *, campaign_id: str = _contract.CAMPAIGN_ID,
                   survivorship: Optional[dict] = None,
                   price_panel_path=None) -> dict:
    price_path = Path(price_panel_path or _rp.DEFAULT_PRICE_PANEL)
    surv = survivorship if survivorship is not None else survivorship_report(
        price_panel_path=price_panel_path)

    fund_sections = snap.sample_sections(_contract.SAMPLE_FUND_MATCHED)
    price_sections = snap.sample_sections(_contract.SAMPLE_PRICE_FULL)

    families = [
        _family(
            name="OWNED_DAILY_PRICE_LIQUIDITY_MEMBERSHIP",
            owner="alpha_agent.release30_panel.load_price_panel",
            source="Norgate Russell 1000 Current & Past (owned), Phase-24 daily panel",
            path=price_path,
            coverage_start=snap.dates[0] if snap.dates else None,
            coverage_end=snap.dates[-1] if snap.dates else None,
            pit_semantics="every feature at decision index t reads rows <= t only",
            publication_semantics="daily close, known at the session it stamps",
            membership_semantics="the panel's own per-day PIT membership mask; "
                                 "never current membership",
            delisted_handling="delisted securities RETAINED; a name that stops "
                              "trading inside a forward window is measured to its "
                              "LAST OWNED CLOSE, never dropped and never carried "
                              "forward at a stale price",
            missingness="NaN outside listing; a name missing any price feature is "
                        "excluded from that cross-section only",
            eligible_transformations=(
                "ratio", "log", "rolling mean", "rolling std", "rolling max",
                "drawdown", "beta vs equal-weight PIT member mean",
                "per-date cross-sectional rank")),
        _family(
            name="OWNED_PIT_FUNDAMENTALS",
            owner="alpha_agent.stage24_pit_fundamental.Stage24PitStore",
            source="SEC companyfacts (owned, bounded)",
            path=None,
            coverage_start=(snap.dates[fund_sections[0]] if fund_sections else None),
            coverage_end=(snap.dates[fund_sections[-1]] if fund_sections else None),
            pit_semantics="a fact is visible only when FILED by the as-of date "
                          "minus the released reporting lag (%d days)"
                          % int(_s24.REPORTING_LAG_DAYS),
            publication_semantics="SEC 'filed' date is the effective date; period "
                                  "end is NOT used as the visibility date",
            membership_semantics="inherited from the price panel's PIT mask",
            delisted_handling="COVERAGE IS SKEWED - see survivorship measurement; "
                              "delisted names are retained where the store has "
                              "them, but the store has proportionally fewer",
            missingness="a name without a resolvable CIK or without a filed "
                        "annual record carries NaN and is excluded from the "
                        "fundamental-matched sample",
            eligible_transformations=("ratio to assets", "ratio to equity",
                                      "year-over-year growth",
                                      "per-date cross-sectional rank")),
    ]

    body = {
        "contract": MANIFEST_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "snapshot_cache": r31.file_fingerprint(snap.path),
        "feature_order": list(snap.feature_names),
        "price_feature_names": list(PRICE_FEATURES),
        "fundamental_feature_names": list(FUNDAMENTAL_FEATURES),
        "horizons_sessions": list(_contract.HORIZONS),
        "step_sessions": _contract.STEP_SESSIONS,
        "cross_sections_total": snap.n_sections,
        "samples": {
            _contract.SAMPLE_PRICE_FULL: {
                "cross_sections": len(price_sections),
                "first_date": snap.dates[price_sections[0]] if price_sections else None,
                "last_date": snap.dates[price_sections[-1]] if price_sections else None,
                "features": list(PRICE_FEATURES),
                "survivorship": "FREE",
                "may_carry_verdict": True,
            },
            _contract.SAMPLE_FUND_MATCHED: {
                "cross_sections": len(fund_sections),
                "first_date": snap.dates[fund_sections[0]] if fund_sections else None,
                "last_date": snap.dates[fund_sections[-1]] if fund_sections else None,
                "features": list(ALL_FEATURES),
                "survivorship": "LIMITED",
                "may_carry_verdict": False,
            },
        },
        "data_families": families,
        "survivorship_measurement": surv,
        "point_in_time_controls": [
            "no future row is readable at any decision index",
            "no cross-sectional statistic spans a date after the decision date",
            "normalisation is per-date and uses that date's cross-section only",
            "no random train/test split anywhere in the campaign",
            "no current metadata is substituted into a historical row",
            "no historical analyst revision is fabricated or back-filled",
        ],
        "inadmissible_information": dict(_contract.INADMISSIBLE_INFORMATION),
    }
    body["manifest_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r31.campaign_dir(campaign_id) / MANIFEST_NAME


def freeze(manifest: dict) -> Path:
    return r31.write_json(path_for(manifest["campaign_id"]), manifest)
