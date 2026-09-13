"""R63 canonical scorer: the sparse-event winsor repair.

``alpha_agent.r63.sensitivity._fit_scaler`` winsorises every feature at its 1st
and 99th TRAINING percentiles. An event feature is 0 on rows without an event,
so when events are rarer than the winsor fraction both percentiles land on the
zero mass and the clip turns the event into a constant column. The augmented
arm then cannot respond and ``run_cell`` returns NO_RESPONSE. The 8-K Item-code
family (36d7d47) measured it at 0.15-0.52% incidence.

The repair lives INSIDE the one scorer, not beside it. These tests make each of
its claims checkable rather than asserted:

1. a dense feature scales bit-identically to the released scaler - the released
   function is read back from git at 36d7d47, not retyped from memory - and
   ``run_cell``'s whole output on a dense dataset is byte-identical
2. a sparse binary event below 1% incidence is no longer flattened, and
   ``run_cell`` no longer returns NO_RESPONSE for it
3. the zero mass stays exactly zero before standardisation
4. an extreme event is still clipped, at the winsor percentile of the events
5. no future information: data after a date cannot move a prediction before it
6. nothing else in the scorer changed (AST-identical to 36d7d47)
7. the closed 13F / 13D/G / 8-K records and verdict code are the released ones
8. no threshold, floor or gate moved

Pure and local: no network, no store, no live path.
"""
from __future__ import annotations

import ast
import json
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent import r63 as R
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S

pytestmark = pytest.mark.filterwarnings("ignore::RuntimeWarning")

REPO = Path(__file__).resolve().parents[1]
RELEASED_COMMIT = "36d7d47"
SCORER = "alpha_agent/r63/sensitivity.py"
REPAIRED = ("_zero_mass_bounds", "_fit_scaler")
BASE = ("PRICE_RETURN_STATE", "TREND")

#: alpha_agent/r63/sensitivity.py::_fit_scaler exactly as released at 36d7d47.
#: test_the_released_scaler_copy_is_the_committed_one proves it against git.
RELEASED_FIT_SCALER_SRC = '''def _fit_scaler(X: np.ndarray) -> dict:
    lo = np.nanpercentile(X, WINSOR[0] * 100, axis=0)
    hi = np.nanpercentile(X, WINSOR[1] * 100, axis=0)
    Xc = np.clip(X, lo, hi)
    mu = np.nanmean(Xc, axis=0)
    sd = np.nanstd(Xc, axis=0)
    sd = np.where(np.isfinite(sd) & (sd > 1e-12), sd, 1.0)
    return {"lo": lo, "hi": hi, "mu": mu, "sd": sd}
'''


def _released_fit_scaler():
    ns = {"np": np, "WINSOR": (0.01, 0.99)}
    exec(RELEASED_FIT_SCALER_SRC, ns)  # noqa: S102 - a pinned, git-verified literal
    return ns["_fit_scaler"]


def _git_show(commit: str, path: str) -> str:
    out = subprocess.run(["git", "-C", str(REPO), "show", "%s:%s" % (commit, path)],
                         capture_output=True)
    if out.returncode != 0:
        pytest.skip("git object %s:%s not available in this checkout" % (commit, path))
    return out.stdout.decode("utf-8").replace("\r\n", "\n")


def _bytes(sc: dict) -> dict:
    return {k: np.asarray(sc[k]).tobytes() for k in ("lo", "hi", "mu", "sd")}


# --------------------------------------------------------------------------- #
# 6 / 8 - only the scaler changed, and nothing was relaxed
# --------------------------------------------------------------------------- #
def test_the_released_scaler_copy_is_the_committed_one():
    src = _git_show(RELEASED_COMMIT, SCORER)
    node = next(n for n in ast.parse(src).body
                if isinstance(n, ast.FunctionDef) and n.name == "_fit_scaler")
    assert ast.get_source_segment(src, node) + "\n" == RELEASED_FIT_SCALER_SRC


def test_only_the_scaler_changed_in_the_canonical_scorer():
    old = ast.parse(_git_show(RELEASED_COMMIT, SCORER))
    new = ast.parse((REPO / SCORER).read_text(encoding="utf-8"))

    def untouched(tree):
        return [ast.dump(n) for n in tree.body if getattr(n, "name", None) not in REPAIRED]

    # every constant, statistic, book, fold, gate and the verdict ladder
    assert untouched(new) == untouched(old)

    def fn(tree, name):
        return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)

    # _fit_scaler gained exactly one statement: the zero-mass bound correction
    new_body = [ast.dump(s) for s in fn(new, "_fit_scaler").body]
    old_body = [ast.dump(s) for s in fn(old, "_fit_scaler").body]
    added = [s for s in new_body if "_zero_mass_bounds" in s]
    assert len(added) == 1
    assert [s for s in new_body if s not in added] == old_body


def test_no_threshold_floor_or_gate_moved():
    assert S.WINSOR == (0.01, 0.99)
    assert (S.MIN_XS_NAMES, S.MIN_ROWS, S.COVERAGE_FLOOR) == (5, 200, 0.60)
    assert (S.MAX_NAME_WEIGHT, S.DEGENERATE_DD) == (0.25, -0.90)
    assert R.MIN_EFFECTIVE_PERIODS == 36
    assert (R.CONDITIONAL_T_FLOOR, R.STANDALONE_T_FLOOR) == (2.0, 2.0)
    assert (R.MATERIALITY_ANN_NET, R.BH_Q) == (0.015, 0.10)
    assert (R.REDUNDANT_RESIDUAL_SHARE_MAX, R.PARTIAL_RESIDUAL_SHARE_MAX) == (0.10, 0.35)
    assert R.EQ_COST_RATE_PER_SIDE == 0.00125
    assert (R.RIDGE_ALPHAS, R.INNER_CV_FOLDS) == ((0.1, 1.0, 10.0, 100.0), 3)
    assert (R.MIN_TRAIN_SESSIONS, R.LOCKBOX_START) == (1260, "2023-01-01")
    pkg = "alpha_agent/r63/__init__.py"
    assert (REPO / pkg).read_text(encoding="utf-8") == _git_show(RELEASED_COMMIT, pkg)


# --------------------------------------------------------------------------- #
# 1 - dense features are bit-identical
# --------------------------------------------------------------------------- #
def _dense_battery(n: int, seed: int) -> np.ndarray:
    """Every dense shape the estate scores, including discrete ones with an
    exact-zero mass that must NOT be mistaken for a sparse event."""
    rng = np.random.default_rng(seed)
    cols = [
        rng.normal(size=n),                                             # gaussian
        rng.standard_t(2, size=n) * 5.0,                                # heavy tails
        rng.lognormal(size=n),                                          # positive skew
        np.where(rng.random(n) < 0.30, 0.0, rng.lognormal(size=n)),     # 30% exact zeros
        -np.where(rng.random(n) < 0.10, 0.0, rng.exponential(size=n)),  # <= 0, zero at the high
        rng.poisson(2.0, size=n).astype(float),                         # counts with a zero mass
        (rng.random(n) < 0.015).astype(float),                          # 1.5% event
        (rng.random(n) < 0.05).astype(float),                           # 5% event (13D/G density)
        (rng.random(n) < 0.50).astype(float),                           # balanced binary
        (pd.Series(rng.normal(size=n)).rank().to_numpy() - 1.0) / (n - 1.0),  # rank in [0,1] (13F)
        np.full(n, 3.0),                                                # constant
        np.round(rng.normal(size=n), 1),                                # ties, incl. exact zeros
    ]
    X = np.column_stack(cols)
    X[rng.random(X.shape) < 0.02] = np.nan                              # scattered missing
    return X


@pytest.mark.parametrize("seed", [63, 64, 65])
def test_dense_features_scale_bit_identically(seed):
    released = _released_fit_scaler()
    X_tr, X_te = _dense_battery(6000, seed), _dense_battery(2000, seed + 100)
    new, old = S._fit_scaler(X_tr), released(X_tr)
    assert _bytes(new) == _bytes(old)
    assert S._apply_scaler(X_te, new).tobytes() == S._apply_scaler(X_te, old).tobytes()
    # structurally: no dense bound is even re-assigned
    lo = np.nanpercentile(X_tr, 1, axis=0)
    hi = np.nanpercentile(X_tr, 99, axis=0)
    lo2, hi2 = S._zero_mass_bounds(X_tr, lo, hi)
    assert lo2 is lo and hi2 is hi


# --------------------------------------------------------------------------- #
# 2 / 3 / 4 - sparse events are preserved, zero stays zero, extremes are bounded
# --------------------------------------------------------------------------- #
def test_the_released_scaler_flattened_a_sparse_event_and_the_repair_does_not():
    rng = np.random.default_rng(8)
    n = 20000
    e = (rng.random(n) < 0.004).astype(float)
    X = np.column_stack([rng.normal(size=n), e])
    old = _released_fit_scaler()(X)
    assert np.ptp(S._apply_scaler(X, old)[:, 1]) == 0.0      # the defect, measured
    new = S._fit_scaler(X)
    Z = S._apply_scaler(X, new)[:, 1]
    assert np.unique(Z).size == 2
    assert Z[e == 1].min() > Z[e == 0].max()
    assert (new["lo"][1], new["hi"][1]) == (0.0, 1.0)
    # the dense neighbour in the same matrix is untouched, bit for bit
    for k in ("lo", "hi", "mu", "sd"):
        assert np.asarray(new[k])[0].tobytes() == np.asarray(old[k])[0].tobytes()


def test_a_single_training_event_is_enough_to_stay_distinguishable():
    x = np.zeros(5000)
    x[1234] = 1.0
    Z = S._apply_scaler(x[:, None], S._fit_scaler(x[:, None]))[:, 0]
    assert np.unique(Z).size == 2 and Z[1234] > Z[0]


def test_a_signed_sparse_event_keeps_three_ordered_levels():
    rng = np.random.default_rng(9)
    n = 30000
    u = rng.random(n)
    x = np.where(u < 0.003, 1.0, np.where(u > 0.997, -1.0, 0.0))
    sc = S._fit_scaler(x[:, None])
    assert (sc["lo"][0], sc["hi"][0]) == (-1.0, 1.0)
    Z = S._apply_scaler(x[:, None], sc)[:, 0]
    assert Z[x == -1].max() < Z[x == 0].min() and Z[x == 0].max() < Z[x == 1].min()


def test_the_zero_mass_stays_zero_before_standardisation():
    rng = np.random.default_rng(11)
    n = 30000
    x = np.zeros(n)
    pos = rng.random(n) < 0.004
    neg = (rng.random(n) < 0.003) & ~pos
    x[pos] = rng.lognormal(size=int(pos.sum()))
    x[neg] = -rng.lognormal(size=int(neg.sum()))
    x[rng.random(n) < 0.01] = np.nan
    pos, neg = x > 0.0, x < 0.0      # a missing row is neither event nor no-event
    X = x[:, None]
    sc = S._fit_scaler(X)
    assert sc["lo"][0] < 0.0 < sc["hi"][0]
    zero = x == 0.0
    assert np.all(np.clip(X, sc["lo"], sc["hi"])[zero, 0] == 0.0)
    Z = S._apply_scaler(X, sc)[:, 0]
    z0 = np.unique(Z[zero])
    assert z0.size == 1
    assert (Z[pos] > z0[0]).all() and (Z[neg] < z0[0]).all()


def test_an_extreme_event_is_still_bounded_on_both_sides():
    rng = np.random.default_rng(12)
    n = 40000
    x = np.zeros(n)
    idx = rng.choice(n, 300, replace=False)
    x[idx[:150]] = rng.lognormal(size=150)
    x[idx[150:]] = -rng.lognormal(size=150)
    x[idx[0]], x[idx[150]] = 1e6, -1e6
    sc = S._fit_scaler(x[:, None])
    assert sc["hi"][0] == np.percentile(x[x > 0], 99) and sc["hi"][0] < 1e6
    assert sc["lo"][0] == np.percentile(x[x < 0], 1) and sc["lo"][0] > -1e6
    # an out-of-sample event beyond the training range is clipped to the bound
    probe = np.array([[1e9], [sc["hi"][0]], [-1e9], [sc["lo"][0]]])
    Z = S._apply_scaler(probe, sc)[:, 0]
    assert Z[0] == Z[1] and Z[2] == Z[3]


def test_a_column_that_keeps_variation_is_left_to_the_conventional_winsor():
    """Only a column the clip would FLATTEN is re-bound. Two cases that keep
    variation stay bit-identical to the release:

    * a signed event whose rarer side alone lands on the zero mass (the
      documented limitation: that side keeps the conventional clip)
    * a dense spread whose 1st percentile sits on an exact zero - the shape
      the R63 funding block has in one early fold (effr - cmt3m)
    """
    rng = np.random.default_rng(13)
    n = 40000
    u = rng.random(n)
    event = np.zeros(n)
    pos, neg = u < 0.045, (u >= 0.045) & (u < 0.050)
    event[pos] = rng.lognormal(size=int(pos.sum()))
    event[neg] = -rng.lognormal(size=int(neg.sum()))
    spread = np.round(rng.lognormal(mean=-1.0, size=n), 2)
    spread[u > 0.975] = 0.0
    spread[u > 0.995] = -np.round(rng.lognormal(mean=-1.0, size=int((u > 0.995).sum())), 2) - 0.01
    X = np.column_stack([event, spread])
    old, new = _released_fit_scaler()(X), S._fit_scaler(X)
    assert old["lo"][0] == 0.0 and old["lo"][1] == 0.0   # both lower bounds sit on zero
    assert old["hi"][0] > 0.0 and old["hi"][1] > 0.0     # and neither column is flattened
    assert _bytes(new) == _bytes(old)


# --------------------------------------------------------------------------- #
# The scorer itself
# --------------------------------------------------------------------------- #
def _dataset(*, event_rate: float | None = None, effect: float = 0.0,
             n_inst: int = 12, n_dates: int = 252 * 14, seed: int = 7) -> dict:
    """The R63 synthetic dataset shape; TESTDIM is dense or a 0/1 event."""
    rng = np.random.default_rng(seed)
    cal = np.array(pd.bdate_range("2010-01-01", periods=n_dates).strftime("%Y-%m-%d"))
    b = rng.normal(size=(n_inst, n_dates))
    if event_rate is None:
        d = rng.normal(size=(n_inst, n_dates))
        y = 0.003 * b + 0.02 * 0.15 * d
    else:
        d = (rng.random((n_inst, n_dates)) < event_rate).astype(float)
        y = 0.003 * b + effect * d
    y = y + rng.normal(size=(n_inst, n_dates)) * 0.02
    ret = np.zeros((n_inst, n_dates))
    ret[:, 2:] = y[:, :-2]              # NEXT_CLOSE: forward_compound(t) == y(t)
    h = cad = 1
    fwd = pit.forward_compound(ret, h)
    vol = np.full((n_inst, n_dates), 0.3)
    blocks = {"PRICE_RETURN_STATE": b[:, :, None],
              "TREND": rng.normal(size=(n_inst, n_dates))[:, :, None],
              "TESTDIM": d[:, :, None]}
    return {"scope": "SYNTH_SPARSE", "mode": "XS", "horizon": h, "cadence": cad,
            "dates": cal, "dec": pit.decision_indices(cal, "2010-01-01", cad, h),
            "inst": list(range(n_inst)), "y": fwd,
            "y_scaled": fwd / (vol * np.sqrt(h / 252.0)),
            "elig": np.ones((n_inst, n_dates), bool), "vol": vol,
            "cost": np.full(n_inst, 0.0002), "blocks": blocks, "market_level": set(),
            "regime_vix": None, "regime_trend": None, "book": "XS_LONG_SHORT"}


def _cell_text(cell: dict) -> str:
    return json.dumps(cell, sort_keys=True, default=str)


@pytest.mark.parametrize("event_rate", [None, 0.05])
def test_run_cell_on_a_dense_dataset_is_byte_identical(monkeypatch, event_rate):
    ds = _dataset(event_rate=event_rate, effect=0.01, seed=21)
    monkeypatch.setattr(S, "_ALPHA_CACHE", {})
    repaired = S.run_cell(ds, BASE, "TESTDIM")
    with monkeypatch.context() as m:
        m.setattr(S, "_ALPHA_CACHE", {})
        m.setattr(S, "_fit_scaler", _released_fit_scaler())
        released = S.run_cell(ds, BASE, "TESTDIM")
    assert "conditional" in repaired
    assert _cell_text(repaired) == _cell_text(released)


def test_run_cell_scores_a_sparse_event_the_released_scaler_could_not(monkeypatch):
    ds = _dataset(event_rate=0.004, effect=0.02, seed=31)
    with monkeypatch.context() as m:
        m.setattr(S, "_ALPHA_CACHE", {})
        m.setattr(S, "_fit_scaler", _released_fit_scaler())
        released = S.run_cell(ds, BASE, "TESTDIM")
    assert released["verdict"] == S.V_NO_RESPONSE
    monkeypatch.setattr(S, "_ALPHA_CACHE", {})
    cell = S.run_cell(ds, BASE, "TESTDIM")
    assert cell["verdict"] != S.V_NO_RESPONSE
    assert "conditional" in cell and cell["conditional"]["increment"] > 0
    # the verdict still comes from the unchanged ladder
    assert cell["verdict"] == S.verdict(cell)


# --------------------------------------------------------------------------- #
# 5 - no future information
# --------------------------------------------------------------------------- #
def test_a_later_event_cannot_move_an_earlier_prediction(monkeypatch):
    ds = _dataset(event_rate=0.004, effect=0.02, seed=41)
    monkeypatch.setattr(S, "_ALPHA_CACHE", {})
    a = S.run_cell(ds, BASE, "TESTDIM", keep_predictions=True)
    cut = "2020-07-01"
    late = np.asarray(ds["dates"]) >= cut
    D2 = ds["blocks"]["TESTDIM"].copy()
    shock = np.random.default_rng(99).random(D2[:, late, 0].shape) < 0.02
    D2[:, late, 0] = np.where(shock, 1e6, D2[:, late, 0])   # extreme future events only
    ds2 = dict(ds)
    ds2["blocks"] = dict(ds["blocks"])
    ds2["blocks"]["TESTDIM"] = D2
    monkeypatch.setattr(S, "_ALPHA_CACHE", {})
    b = S.run_cell(ds2, BASE, "TESTDIM", keep_predictions=True)
    pa, pb = a["_predictions"], b["_predictions"]
    assert np.array_equal(pa["gid"], pb["gid"])
    slot = np.asarray(pa["slot_dates"])[pa["gid"]]
    before = slot < cut
    scored = np.isfinite(pa["pred_BD"])
    assert (before & scored).sum() > 5000 and (~before & scored).sum() > 5000
    for k in ("pred_B", "pred_BD", "pred_D"):
        assert pa[k][before].tobytes() == pb[k][before].tobytes(), k
    # the perturbation is real: it does reach predictions after the cut
    assert not np.array_equal(pa["pred_BD"][~before & scored], pb["pred_BD"][~before & scored])


# --------------------------------------------------------------------------- #
# 7 - the closed families are not rewritten by the repair
# --------------------------------------------------------------------------- #
CLOSED_FAMILY_FILES = (
    "research/preregistration/INSTITUTIONAL_OWNERSHIP_BREADTH_PREREGISTRATION.md",
    "research/preregistration/CONTROL_BLOCK_13DG_PREREGISTRATION.md",
    "research/preregistration/CONTROL_BLOCK_13DG_RESULT.md",
    "research/preregistration/EVENT_8K_ITEM_PREREGISTRATION.md",
    "research/preregistration/EVENT_8K_ITEM_RESULT.md",
    "alpha_agent/alpha_recovery/ownership_breadth.py",
    "alpha_agent/alpha_recovery/control_block_alpha.py",
    "alpha_agent/alpha_recovery/control_block_events.py",
    "alpha_agent/alpha_recovery/event_8k_alpha.py",
    "alpha_agent/alpha_recovery/event_8k_events.py",
    "alpha_agent/alpha_recovery/event_8k_data.py",
)


@pytest.mark.parametrize("path", CLOSED_FAMILY_FILES)
def test_closed_family_records_and_verdict_code_are_the_released_ones(path):
    """A scorer repair is not a rescue: the 13F, 13D/G and 8-K records and the
    code that reached their verdicts stay exactly as released."""
    assert (REPO / path).read_text(encoding="utf-8") == _git_show(RELEASED_COMMIT, path)
