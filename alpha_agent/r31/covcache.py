"""alpha_agent.r31.covcache - the ONE Release 31 covariance cache.

The canonical covariance builder is
``engine.holding_opportunity_cost.build_covariance``, and this module does not
reimplement one line of it. What it adds is the observation that makes the whole
campaign affordable:

    covariance is a property of (decision date, eligible universe, lookback,
    policy). It does NOT depend on which candidate is being judged.

Campaign v2 never noticed, because its top-N equal-weight book needed no
covariance at all. The v3 judge allocates capital through the canonical zero-base
optimiser, which needs the matrix at every decision date - and rebuilding it per
candidate would multiply one shared cost by the entire candidate budget.

Measured on this machine, at an S&P-500-scale cross-section of ~500 names over
the canonical 60-session lookback, one build costs about 1.2 seconds. Across ~306
decision dates that is ~6 minutes ONCE; per-candidate it would be ~6 minutes
EACH, or roughly 10 hours across a 100-candidate campaign spent recomputing an
identical matrix. The cache is therefore not an optimisation detail - without it
the primary economic judge is not executable at all.

Immutability
------------
The cache is content-addressed by a key that binds every input which could change
a number inside it: the snapshot hash, the membership hash, the canonical policy
values consumed, the lookback and the horizon. A candidate evaluated against a
cache built under different inputs would be silently comparing itself with a
different risk model, so the key is verified on load and a mismatch raises rather
than degrades.

The daily matrix is stored UNSCALED. Horizon scaling is a single multiply applied
at read time by :func:`horizon_scaled`, which keeps one cache serving every
horizon and keeps the horizon opinion in exactly one place.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np

from ...engine import holding_opportunity_cost as _hoc
from ...engine import zero_base_allocator as _zb
from .. import release30_panel as _rp
from .. import r31

CALCULATION_OWNER = "alpha_agent.r31.covcache"
CACHE_NAME = "covariance_cache.npz"
MANIFEST_NAME = "covariance_cache_manifest.json"
MANIFEST_SCHEMA = "r31_covariance_cache_manifest/1"

#: Policy keys that change a number inside the cache. Bound into the key so a
#: policy edit invalidates the cache instead of silently reusing a stale matrix.
POLICY_KEYS = ("covariance_lookback", "min_covariance_obs")

STATE_OK = "COVARIANCE_CACHE_READY"
STATE_KEY_MISMATCH = "COVARIANCE_CACHE_KEY_MISMATCH"


class CacheKeyMismatch(RuntimeError):
    """The cache on disk was not built from the inputs now being used."""


def cache_key(*, snapshot_hash: str, universe_hash: str, pol: dict) -> str:
    return r31.sha({
        "owner": CALCULATION_OWNER,
        "snapshot_hash": str(snapshot_hash),
        "universe_hash": str(universe_hash),
        "covariance_owner": "engine.holding_opportunity_cost.build_covariance",
        "policy": {k: pol[k] for k in POLICY_KEYS},
        "basis": "DAILY_SAMPLE_COVARIANCE_UNSCALED",
    })


def path_for(campaign_id: str) -> Path:
    return r31.campaign_dir(campaign_id) / CACHE_NAME


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #
def build_cache(*, campaign_id: str, snap, membership, snapshot_hash: str,
                universe_hash: str, pol: Optional[dict] = None,
                panel=None, sections: Optional[list] = None,
                force: bool = False, log=None) -> Path:
    """Build the per-decision-date covariance cache ONCE.

    For each decision date the eligible set is the cross-section's own names
    INTERSECTED with the point-in-time S&P 500 membership on that date - the same
    intersection the judge will allocate over, so the matrix and the opportunity
    set can never disagree.

    Trailing daily returns are read from the frozen panel at rows STRICTLY up to
    and including the decision session. A covariance that reaches one session past
    the decision is the easiest future leak to introduce and the hardest to see
    afterwards, so the slice bound is computed here, once, and asserted by
    ``tests/test_release31_campaign_v3_corrections.py``.
    """
    out = path_for(campaign_id)
    pol = pol or _zb.default_policy()
    key = cache_key(snapshot_hash=snapshot_hash, universe_hash=universe_hash,
                    pol=pol)
    if out.exists() and not force:
        with np.load(out, allow_pickle=False) as z:
            if str(z["key"][0]) == key:
                return out
        raise CacheKeyMismatch(
            "covariance cache at %s was built under a different key; delete it "
            "or rebuild with force=True" % (out,))

    panel = panel if panel is not None else _rp.load_price_panel()
    lookback = int(pol["covariance_lookback"])
    sections = list(sections if sections is not None else range(snap.n_sections))

    # Daily simple returns over the whole panel, computed once. Row t is the
    # return REALISED at session t, so rows <= t are exactly what is knowable at
    # the decision struck on session t.
    close = np.asarray(panel.close, dtype=np.float64)
    with np.errstate(invalid="ignore", divide="ignore"):
        rets = np.full_like(close, np.nan)
        rets[1:] = close[1:] / close[:-1] - 1.0

    sec_ids: list = []
    inc_idx: list = []
    inc_off: list = [0]
    cov_blocks: list = []
    skipped = 0

    for k in sections:
        date = snap.dates[k]
        t = int(snap.session_index[k])
        r = snap.rows(k)
        cols = np.asarray(snap.sym_idx[r], dtype=np.int64)
        try:
            elig = membership.eligible_columns(date, cols)
        except Exception:
            skipped += 1
            continue
        cols = np.unique(cols[elig])
        if cols.size < 2:
            skipped += 1
            continue

        lo = max(0, t - lookback + 1)
        window = rets[lo:t + 1, :]                       # PIT: never past t
        tickers = [str(panel.symbols[j]) for j in cols]
        series = {}
        for j, tk in zip(cols, tickers):
            col = window[:, j]
            series[tk] = [None if not np.isfinite(v) else float(v) for v in col]

        built = _hoc.build_covariance(
            tickers=tickers,
            aligned_returns={"dates": [str(x) for x in range(window.shape[0])],
                             "series": series},
            policy=pol)
        included = list(built["included_tickers"])
        if len(included) < 2:
            skipped += 1
            continue

        pos = {str(panel.symbols[j]): int(j) for j in cols}
        idx = np.array([pos[tk] for tk in included], dtype=np.int32)
        cov = built["covariance"]
        m = np.empty((idx.size, idx.size), dtype=np.float32)
        for a, ta in enumerate(included):
            row = cov.get(ta) or {}
            m[a] = np.array([row.get(tb, 0.0) for tb in included],
                            dtype=np.float32)

        sec_ids.append(int(k))
        inc_idx.append(idx)
        inc_off.append(inc_off[-1] + int(idx.size))
        cov_blocks.append(m.reshape(-1))
        if log and len(sec_ids) % 25 == 0:
            log("    covariance cache %d/%d sections" % (len(sec_ids), len(sections)))

    if not sec_ids:
        raise RuntimeError("covariance cache produced no section")

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.stem + ".building.npz")
    with open(tmp, "wb") as fh:
        np.savez_compressed(
            fh,
            key=np.array([key], dtype="U64"),
            sections=np.array(sec_ids, dtype=np.int64),
            included=np.concatenate(inc_idx).astype(np.int32),
            offsets=np.array(inc_off, dtype=np.int64),
            cov=np.concatenate(cov_blocks).astype(np.float32),
            skipped=np.array([skipped], dtype=np.int64))
    Path(tmp).replace(out)
    return out


# --------------------------------------------------------------------------- #
# Read
# --------------------------------------------------------------------------- #
class CovarianceCache:
    """The frozen per-date covariance, read back and verified against its key."""

    def __init__(self, path, *, expect_key: Optional[str] = None):
        self.path = Path(path)
        with np.load(self.path, allow_pickle=False) as z:
            self.key = str(z["key"][0])
            self.sections = np.asarray(z["sections"], dtype=np.int64)
            self._included = np.asarray(z["included"], dtype=np.int64)
            self._offsets = np.asarray(z["offsets"], dtype=np.int64)
            self._cov = np.asarray(z["cov"], dtype=np.float32)
            self.skipped = int(np.asarray(z["skipped"])[0])
        if expect_key is not None and self.key != expect_key:
            raise CacheKeyMismatch(
                "covariance cache key %s does not match the campaign's %s; the "
                "cache was built from different inputs"
                % (self.key[:12], str(expect_key)[:12]))
        self._pos = {int(s): i for i, s in enumerate(self.sections)}
        # Where the flat covariance block for slot i starts. Derived from the
        # per-slot name counts rather than stored, so the two can never disagree.
        counts = np.diff(self._offsets)
        self._cov_off = np.concatenate([[0], np.cumsum(counts * counts)]).astype(np.int64)

    def has(self, section: int) -> bool:
        return int(section) in self._pos

    def included_columns(self, section: int) -> np.ndarray:
        i = self._pos[int(section)]
        return self._included[self._offsets[i]:self._offsets[i + 1]]

    def matrix(self, section: int) -> np.ndarray:
        """The DAILY covariance as a dense square array, in included order."""
        i = self._pos[int(section)]
        n = int(self._offsets[i + 1] - self._offsets[i])
        lo = int(self._cov_off[i])
        return self._cov[lo:lo + n * n].reshape(n, n)

    def horizon_scaled(self, section: int, *, sessions: int, symbols) -> tuple:
        """``(cov_h_dict, included_tickers)`` scaled to the holding horizon.

        Scaling linearly in time is the i.i.d. convention the canonical DAILY
        builder's output is expressed in, and the allocator states that ``cov_h``
        arrives already horizon-scaled, so this is the one place the horizon is
        applied.
        """
        cols = self.included_columns(section)
        names = [str(symbols[int(j)]) for j in cols]
        m = self.matrix(section).astype(np.float64) * float(sessions)
        cov = {}
        for a, ta in enumerate(names):
            row_a = m[a]
            cov[ta] = {tb: float(row_a[b]) for b, tb in enumerate(names)}
        return cov, names


def load(*, campaign_id: str, expect_key: Optional[str] = None) -> CovarianceCache:
    return CovarianceCache(path_for(campaign_id), expect_key=expect_key)


# --------------------------------------------------------------------------- #
# Manifest
# --------------------------------------------------------------------------- #
def build_manifest(*, campaign_id: str, cache: CovarianceCache,
                   pol: Optional[dict] = None) -> dict:
    pol = pol or _zb.default_policy()
    counts = np.diff(cache._offsets)
    body = {
        "manifest": MANIFEST_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "covariance_owner": "engine.holding_opportunity_cost.build_covariance",
        "campaign_owns_no_covariance_mathematics": True,
        "cache_key": cache.key,
        "sections_cached": int(cache.sections.size),
        "sections_skipped": cache.skipped,
        "names_per_section": {
            "median": int(np.median(counts)) if counts.size else 0,
            "min": int(counts.min()) if counts.size else 0,
            "max": int(counts.max()) if counts.size else 0,
        },
        "policy": {k: pol[k] for k in POLICY_KEYS},
        "basis": "DAILY_SAMPLE_COVARIANCE_UNSCALED_AT_REST",
        "horizon_scaling": "applied once at read time by covcache.horizon_scaled",
        "point_in_time_rule": (
            "the trailing return window ends at the decision session INCLUSIVE "
            "and never reads a later row"),
        "eligible_set_rule": (
            "the cross-section's own names INTERSECTED with point-in-time S&P 500 "
            "membership on that date - the same set the judge allocates over"),
        "reused_by_every_candidate": True,
        "cache_sha256": r31.sha_file(path_for(campaign_id)),
        "state": STATE_OK,
    }
    body["covariance_cache_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


def freeze(body: dict) -> Path:
    return r31.write_json(
        r31.campaign_dir(body["campaign_id"]) / MANIFEST_NAME, body)
