r"""alpha_agent.r61.power - THE power calibration harness (Workstream B).

RESEARCH INFRASTRUCTURE ONLY. THIS IS NOT AN ALPHA SEARCH.

It registers no hypothesis, charges no research-family burden, consumes no
lockbox budget, creates no forward candidate, promotes nothing and alters no
historical research outcome. The signal it measures is SYNTHETIC and known to
be synthetic; there is no verb here that could file it as a finding.

THE QUESTION
------------
The estate has settled ~8,453 hypotheses and qualified none, and has never
checked whether its pipeline could see alpha that was there. Until that is
known, every NO_ALPHA_EVIDENCE verdict is ambiguous between "there is nothing
there" and "we could not have seen it".

So: inject a signal of KNOWN strength into the REAL panels, under the REAL
cost models, the REAL universes and missingness, the REAL decision dates, the
REAL D/V/L partition, the REAL stage-advance rule and the REAL qualification
gate - and count how often the apparatus catches it.

THE INJECTION MECHANISM, AND WHY THIS ONE
-----------------------------------------
ONE canonical mechanism: a pre-declared cross-sectional INFORMATION
COEFFICIENT. At every decision date, over that date's own eligible
cross-section::

    z_i     = normal scores of the REALISED forward return  (rank -> N(0,1))
    eps_i   = N(0,1), drawn from an RNG keyed by (seed, panel, DECISION DATE)
    score_i = rho * z_i + sqrt(1 - rho^2) * eps_i

Both components are standardised, so the ex-ante cross-sectional correlation
between the score and the normal-scored forward rank is exactly ``rho``, and
the realised Spearman IC is approximately ``rho``. It is MEASURED and reported
per cell rather than asserted.

Using the realised forward return is hindsight BY CONSTRUCTION, and that is
the point: this is a stand-in for a real predictor whose information
coefficient is ``rho``. Everything else the cell touches is untouched real
data. The panel is not replaced with Gaussian toy data; universe membership,
missingness, the calendar, the cost vector, the eligibility rule, the roll
schedule and the delisting path are all the estate's own.

WHAT ``rho`` BUYS, MEASURED NOT ASSERTED
----------------------------------------
The mapping the brief asks for::

    INJECTED_EFFECT (rho)  ->  EXPECTED_GROSS_RETURN  ->  EXPECTED_NET_RETURN

is not a closed form here and is not pretended to be. It is MEASURED: every
cell reports the realised rank IC, the realised ``ann_gross_excess`` and the
realised ``ann_net_excess`` of every layer. The MDE is then reported in BOTH
units - the rho at which detection crosses the threshold, and the median net
annual excess that rho actually delivered on that panel.

TURNOVER IS NOT INFLATED BY THE INJECTION, AND THIS WAS CHECKED
---------------------------------------------------------------
A signal redrawn from independent noise every decision churns, and a churning
signal pays more cost than a persistent one - which would understate the
apparatus's power. On these panels at a 21-session cadence it does not bite:
a fully redrawn random rank book measures ~0.65 one-way on commodity futures
and ~0.79-0.90 on equity top-N, against R60's REAL cells at 0.598, 0.612 and
0.870. The synthetic signal trades like the real ones did, so no persistence
parameter is introduced; every cell reports its own turnover so a reader can
check this rather than take it.

WHAT COUNTS AS A DETECTION
--------------------------
The full governed path, and nothing less:

    D advances (engines.stage_advance)  ->  V advances  ->  L measured  ->
    engines.gate(...) returns qualified

Discovery-only success is NOT a detection. A halt at D or V is a miss, exactly
as it would be in a real campaign. The gate is reached through the same
functions ``pipeline.skeptic_review`` reaches it through; only the memory
write is absent, because no hypothesis exists to write.

DETERMINISM
-----------
The noise for a decision date is keyed by that DATE, so the draw is identical
whether a decision is measured inside the D-stage prefix or inside the
L-stage's full run. Without that, a stage prefix would score a different book
from the one the full run scores and the partition would leak noise.
"""
from __future__ import annotations

import math
from typing import Callable, Optional

import numpy as np

from .. import r59
from ..agents_v2 import books as B
from ..r57 import engine as K
from ..r59 import engines as E
from ..r59 import native
from . import stable_hash
from . import calibration_panels as CP

POWER_OWNER = "alpha_agent.r61.power"
POWER_CALIBRATION_VERSION = "R61_SYNTHETIC_IC_POWER_CALIBRATION_V1"

# --------------------------------------------------------------------------- #
# THE PRE-REGISTRATION. Frozen BEFORE any detection rate was observed.
# --------------------------------------------------------------------------- #
#: The effect grid, in cross-sectional rank-IC units. Chosen on EX-ANTE
#: grounds and never moved afterwards: published cross-sectional rank ICs for
#: equity factors sit around 0.01-0.05 at monthly horizons, and commodity and
#: futures carry/momentum ICs are reported up to ~0.10-0.15 on small panels.
#: 0.00 is the null and is not decoration - it measures the gate's own
#: false-positive rate, which is the other half of the answer. 0.20 is far
#: above anything published, so the grid brackets the whole transition on
#: every panel without any point being chosen to make a curve look better.
RHO_GRID = (0.00, 0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.10, 0.15, 0.20)

#: Monte-Carlo seeds per (panel, effect). 120 gives a standard error of about
#: 3.7 percentage points at a detection rate of 0.80, and every point reports
#: its own Wilson interval rather than a bare rate.
#:
#: This number was raised from 40 after a TIMING probe showed a full cell costs
#: well under a second once the panel's eligibility and forward returns are
#: precomputed. That is disclosed rather than buried, and it is not grid
#: tuning: the seed count is pure Monte-Carlo precision. It cannot move the
#: quantity being estimated, only the width of the interval around it. The
#: EFFECT GRID - the thing that could bias an answer - was frozen on ex-ante
#: grounds and has not moved.
SEEDS_PER_POINT = 120

#: The PRIMARY burden denominator, declared here. 1 is the most FAVOURABLE
#: setting the gate admits - it is what R60's own lockbox was graded at - so a
#: detection floor measured here is a LOWER BOUND on the apparatus's floor.
#: If it cannot see an effect at a denominator of 1, no burden setting saves
#: it. The other points are reported alongside because burden is a pure
#: post-hoc function of the measured p, so they cost no compute and they are
#: what answer "what does this imply for the 8,453 historical nulls".
PRIMARY_BURDEN_DENOMINATOR = 1
BURDEN_DENOMINATOR_GRID = (1, 10, 100, 1000)

#: Detection = the full governed path. Declared, so it cannot be softened
#: after a curve is seen.
DETECTION_RULE = ("D advances -> V advances -> L measured -> "
                  "engines.gate(qualified). A halt at D or V is a MISS. "
                  "Discovery-only success is NOT a detection.")

#: A second, STRICTER reading reported alongside at no extra compute: the gate
#: qualified AND both lockbox halves are positive (the deterministic
#: subperiod attack, which is free because the halves are in the layer).
STRICT_DETECTION_RULE = ("gate qualified AND both lockbox halves > 0 "
                         "(subperiod_stable, read from the measured layer)")

#: The power levels the MDE is reported at.
POWER_LEVELS = (0.50, 0.80, 0.90)


def pre_registration() -> dict:
    """The frozen calibration design, content-hashed. Written BEFORE the run."""
    body = {
        "owner": POWER_OWNER,
        "version": POWER_CALIBRATION_VERSION,
        "is_alpha_experiment": False,
        "registers_hypothesis": False,
        "charges_search_burden": False,
        "consumes_lockbox_budget": False,
        "creates_forward_request": False,
        "panels": list(CP.PANEL_IDS),
        "injection_mechanism": "CROSS_SECTIONAL_INFORMATION_COEFFICIENT",
        "injection_formula":
            "score_i = rho * z_i + sqrt(1 - rho^2) * eps_i, where z is the "
            "normal-score transform of the REALISED forward return over the "
            "eligible cross-section at that decision, and eps ~ N(0,1) is "
            "keyed by (seed, panel_id, decision DATE)",
        "effect_grid_units": "cross_sectional_rank_information_coefficient",
        "effect_grid": list(RHO_GRID),
        "effect_grid_frozen_before_results": True,
        "seeds_per_point": SEEDS_PER_POINT,
        "primary_burden_denominator": PRIMARY_BURDEN_DENOMINATOR,
        "burden_denominator_grid": list(BURDEN_DENOMINATOR_GRID),
        "detection_rule": DETECTION_RULE,
        "strict_detection_rule": STRICT_DETECTION_RULE,
        "power_levels": list(POWER_LEVELS),
        "gate_owner": "alpha_agent.r59.engines.gate",
        "stage_advance_owner": "alpha_agent.r59.engines.stage_advance",
        "book_owner": "alpha_agent.agents_v2.books.run_stage",
        "inherited_conventions": {
            "discovery_start": r59.DISCOVERY_START,
            "validation_start": r59.VALIDATION_START,
            "lockbox_start": r59.LOCKBOX_START,
            "cadence": r59.CADENCE, "horizon": r59.HORIZON,
            "bh_q": r59.BH_Q, "obs_floor": r59.OBS_FLOOR,
            "materiality_floors": dict(r59.GATE_MATERIALITY_FLOORS),
            "validation_materiality_fraction":
                r59.VALIDATION_MATERIALITY_FRACTION,
            "stage_advance_fraction": r59.STAGE_ADVANCE_FRACTION,
        },
    }
    body["pre_registration_hash"] = stable_hash(body)
    return body


# --------------------------------------------------------------------------- #
# Forward returns - the quantity each book actually scores against
# --------------------------------------------------------------------------- #
def equity_forward_return(panel: dict, t: int, horizon: int) -> np.ndarray:
    """Forward total return per name over [t+1, t+1+horizon], NaN if absent.

    Identical to ``r57.engine.forward_return`` in entry, exit and delisting
    treatment, with ONE deliberate difference: a name with no usable forward
    window is NaN here and 0.0 there. The book is right to earn zero on it;
    the injection must not treat "no data" as "a return of exactly zero" and
    rank it in the middle of the cross-section.
    """
    tr = panel["tr"]
    entry = tr[:, t + 1]
    window = tr[:, t + 1:t + 1 + horizon + 1]
    if window.shape[1] == 0:
        return np.full(tr.shape[0], np.nan)
    fin = np.isfinite(window)
    idx = np.where(fin, np.arange(window.shape[1])[None, :], -1)
    last_ix = idx.max(axis=1)
    exitp = window[np.arange(window.shape[0]), np.clip(last_ix, 0, None)]
    ok = (last_ix > 0) & np.isfinite(entry) & (entry > 0)
    with np.errstate(invalid="ignore", divide="ignore"):
        r = np.where(ok, exitp / entry - 1.0, np.nan)
    return np.where(np.isfinite(r), r, np.nan)


def futures_forward_return(layer: dict, t: int, horizon: int) -> np.ndarray:
    """Cumulative front-contract return over the book's own forward window.

    The book computes ``cumprod(1 + _window_returns) - 1`` and multiplies it
    by the target weights, so that is exactly what the injected signal must
    correlate with. A market with no finite session in the window has no
    forward return and is NaN, not zero.
    """
    ret = layer["ret"]
    w = ret[:, t + 1:t + 1 + horizon]
    if w.shape[1] == 0:
        return np.full(ret.shape[0], np.nan)
    fin = np.isfinite(w)
    r0 = np.where(fin, w, 0.0)
    f = np.cumprod(1.0 + r0, axis=1)[:, -1] - 1.0
    return np.where(fin.any(axis=1), f, np.nan)


def forward_return(spec: dict, t: int, horizon: int = r59.HORIZON
                   ) -> np.ndarray:
    if spec["book"] == B.BOOK_FUTURES:
        return futures_forward_return(spec["panel"], int(t), int(horizon))
    return equity_forward_return(spec["panel"], int(t), int(horizon))


# --------------------------------------------------------------------------- #
# Normal scores
# --------------------------------------------------------------------------- #
def normal_scores(values: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Van der Waerden scores of ``values`` over ``mask``, NaN elsewhere.

    The rank transform is what makes the injected correlation exactly ``rho``
    regardless of how fat the return distribution's tails are on that panel,
    and it stops one extreme delisting return from becoming the entire signal.
    """
    from scipy.stats import norm                             # noqa: PLC0415

    out = np.full(np.shape(values), np.nan, dtype=np.float64)
    sel = np.asarray(mask, bool) & np.isfinite(values)
    n = int(sel.sum())
    if n < 2:
        return out
    v = np.asarray(values, dtype=np.float64)[sel]
    order = np.argsort(np.argsort(v))                # 0..n-1, ties broken by
    q = (order + 0.5) / float(n)                     # stable sort order
    z = norm.ppf(q)
    z = (z - z.mean()) / (z.std() or 1.0)
    out[np.where(sel)[0]] = z
    return out


# --------------------------------------------------------------------------- #
# The decision grid, and the precomputation every cell reuses
# --------------------------------------------------------------------------- #
def decision_indices(spec: dict, *, cadence: int = r59.CADENCE,
                     horizon: int = r59.HORIZON) -> np.ndarray:
    """The FULL decision grid of the panel, as its own book computes it."""
    dates = np.asarray(spec["panel"]["dates"])
    if spec["book"] == B.BOOK_FUTURES:
        return native.decision_indices(dates, cadence, horizon)
    return K.decision_indices(dates, cadence, r59.DISCOVERY_START, horizon)


def precompute(spec: dict, *, cadence: int = r59.CADENCE,
               horizon: int = r59.HORIZON, verbose: bool = False) -> dict:
    """Everything that does NOT depend on rho or the seed, computed once.

    The eligibility mask, the forward returns and their normal scores are
    identical for every effect size and every seed, and they are what costs
    real time on a 2,947-name panel. Computing them once per panel turns a
    two-day sweep into a two-hour one and changes no number.
    """
    panel = spec["panel"]
    idx = decision_indices(spec, cadence=cadence, horizon=horizon)
    key = "tr" if spec["book"] != B.BOOK_FUTURES else "ret"
    n_inst = int(np.shape(panel[key])[0])
    n_sess = int(np.shape(panel[key])[1])
    z = np.full((n_inst, len(idx)), np.nan)
    elig = np.zeros((n_inst, len(idx)), dtype=bool)
    n_elig = np.zeros(len(idx), dtype=int)
    fwd = np.full((n_inst, len(idx)), np.nan)
    # THE BOOK'S OWN ELIGIBILITY, MATERIALISED. The two equity eligibility
    # rules read only data at or before t (plus tr[:, t+1], the entry, which
    # every decision has because the grid stops at len - horizon - 2), so a
    # column computed from the FULL panel is byte-identical to the one the
    # rule returns when called on a truncated stage prefix. Handing the book
    # the matrix instead of the callable therefore changes no number and stops
    # a 2,947-name rolling-median being recomputed on every one of 400 runs.
    elig_full = (np.zeros((n_inst, n_sess), dtype=bool)
                 if spec["book"] != B.BOOK_FUTURES else None)
    for j, t in enumerate(idx):
        t = int(t)
        e = CP.eligible_at(spec, t)
        f = forward_return(spec, t, horizon)
        elig[:, j] = e
        fwd[:, j] = f
        z[:, j] = normal_scores(f, e)
        n_elig[j] = int((e & np.isfinite(f)).sum())
        if elig_full is not None:
            elig_full[:, t] = e
        if verbose and j % 50 == 0:
            print("   precompute %s %d/%d" % (spec["panel_id"], j, len(idx)),
                  flush=True)
    return {"panel_id": spec["panel_id"], "decision_idx": np.asarray(idx),
            "col_of": {int(t): j for j, t in enumerate(idx)},
            "eligible": elig, "forward": fwd, "z": z,
            "eligible_full": elig_full,
            "n_eligible": n_elig,
            "n_decisions": int(len(idx)),
            "median_eligible": float(np.median(n_elig)) if len(idx) else 0.0}


def noise_matrix(spec: dict, pre: dict, seed: int) -> np.ndarray:
    """``eps`` for every instrument at every decision, keyed by DATE.

    Keying by the decision's own date - not by its position in whatever grid
    a stage prefix happens to produce - is what makes a D-stage run score the
    same book the L-stage run scores over the same dates.
    """
    dates = np.asarray(spec["panel"]["dates"])
    n_inst = pre["z"].shape[0]
    eps = np.empty((n_inst, pre["n_decisions"]), dtype=np.float64)
    for j, t in enumerate(pre["decision_idx"]):
        key = "%s|%s|%s" % (int(seed), spec["panel_id"], str(dates[int(t)]))
        rng = np.random.default_rng(
            np.frombuffer(bytes.fromhex(stable_hash(key)[:16]),
                          dtype=np.uint64))
        eps[:, j] = rng.standard_normal(n_inst)
    return eps


# --------------------------------------------------------------------------- #
# The injected signal
# --------------------------------------------------------------------------- #
def make_score_fn(spec: dict, pre: dict, eps: np.ndarray, rho: float
                  ) -> Callable:
    """The callable the book is handed, in that book's own contract."""
    rho = float(rho)
    w_signal = rho
    w_noise = math.sqrt(max(0.0, 1.0 - rho * rho))
    col_of = pre["col_of"]
    z, elig = pre["z"], pre["eligible"]
    n_inst = z.shape[0]

    def _score(t: int) -> np.ndarray:
        j = col_of.get(int(t))
        if j is None:
            return np.full(n_inst, np.nan)
        zz = z[:, j]
        s = w_signal * np.where(np.isfinite(zz), zz, 0.0) + w_noise * eps[:, j]
        return np.where(elig[:, j], s, np.nan)

    if spec["book"] == B.BOOK_FUTURES:
        rows = spec["rows"]
        min_markets = int(spec["min_markets"])

        def _weights(t, live):
            s = _score(int(t))
            scored = (B.live_markets(spec["panel"], int(t))
                      if live is None else np.asarray(live, bool))
            scored = (scored & CP.base_live(spec["panel"], int(t), scored)
                      & rows & np.isfinite(s))
            if int(scored.sum()) < min_markets:
                return np.zeros(n_inst)
            return B.rank_weights(s, scored, min_markets=min_markets)

        return _weights

    def _equity_score(panel, t):
        return _score(int(t))

    return _equity_score


def realised_ic(spec: dict, pre: dict, eps: np.ndarray, rho: float,
                stage: Optional[str] = None) -> dict:
    """The Spearman IC the injection actually achieved, per decision.

    Reported so the claim "an effect of rho was injected" is MEASURED. A
    calibration that asserts its own treatment is not a calibration.
    """
    import warnings                                          # noqa: PLC0415

    from scipy.stats import ConstantInputWarning, spearmanr   # noqa: PLC0415

    fn = make_score_fn(spec, pre, eps, rho)
    dates = np.asarray(spec["panel"]["dates"])
    ics = []
    for j, t in enumerate(pre["decision_idx"]):
        t = int(t)
        if stage is not None:
            lab = _layer_of_date(spec, dates[t])
            if lab != stage:
                continue
        s = (fn(t, None) if spec["book"] == B.BOOK_FUTURES
             else fn(spec["panel"], t))
        m = pre["eligible"][:, j] & np.isfinite(s) & np.isfinite(
            pre["forward"][:, j])
        if int(m.sum()) < 5:
            continue
        # A decision where every eligible instrument carries the SAME forward
        # return has no defined rank correlation. That is a property of the
        # panel on that date, not an error, so it is skipped rather than
        # allowed to print over the run log.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ConstantInputWarning)
            rho_hat = spearmanr(np.asarray(s)[m],
                                pre["forward"][:, j][m]).statistic
        if rho_hat == rho_hat:
            ics.append(float(rho_hat))
    if not ics:
        return {"mean_rank_ic": None, "n_decisions_scored": 0}
    a = np.asarray(ics)
    return {"mean_rank_ic": float(a.mean()),
            "median_rank_ic": float(np.median(a)),
            "ic_std": float(a.std()),
            "n_decisions_scored": int(a.size)}


def _layer_of_date(spec: dict, date) -> Optional[str]:
    d = str(date)
    if d < r59.DISCOVERY_START:
        return None
    if d < r59.VALIDATION_START:
        return "D"
    if d < r59.LOCKBOX_START:
        return "V"
    return "L"


# --------------------------------------------------------------------------- #
# Detection - the governed path, verbatim
# --------------------------------------------------------------------------- #
def detect(layers: dict, *, burden_denominator: int) -> dict:
    """Would this measured set of layers have QUALIFIED?

    ``burden_denominator`` is the total the gate divides by; the gate takes
    ``prior_burden + family_tests``, so ``prior_burden = denominator - 1``.
    """
    g = E.gate({"layers": layers}, prior_burden=max(0, int(burden_denominator) - 1),
               family_tests=1)
    halves = (layers.get("L") or {}).get("halves_ann_net_excess") or []
    strict = bool(g["qualified"] and halves
                  and all(h is not None and h > 0 for h in halves))
    return {"qualified": bool(g["qualified"]), "strict": strict,
            "failed_gates": g["failed_gates"],
            "burden_denominator": g["burden_denominator"],
            "burden_corrected_p": g["burden_corrected_p"],
            "lockbox_t": g["lockbox_t"], "lockbox_p_one_sided":
                g["lockbox_p_one_sided"],
            "lockbox_materiality": g["lockbox_materiality"],
            "validation_materiality": g["validation_materiality"]}


def run_cell(spec: dict, pre: dict, *, rho: float, seed: int,
             cadence: int = r59.CADENCE, horizon: int = r59.HORIZON) -> dict:
    """ONE (panel, effect, seed): measure the governed path and judge it.

    The layers are revealed in order and the run STOPS the moment one does not
    earn the next, exactly as ``runner.run_experiment`` stops. That is not an
    optimisation: a cell that halts at D never computes a lockbox in a real
    campaign, so a calibration that computed one anyway would be measuring a
    pipeline the estate does not run.
    """
    eps = noise_matrix(spec, pre, seed)
    fn = make_score_fn(spec, pre, eps, rho)
    kwargs = dict(spec.get("book_kwargs") or {})
    if pre.get("eligible_full") is not None:
        kwargs["elig"] = pre["eligible_full"]
    layers: dict = {}
    halted_at = None
    halt_reasons: list = []
    for stage in r59.STAGES:
        try:
            measured = B.run_stage(book=spec["book"], stage=stage,
                                   panel=spec["panel"], score_fn=fn,
                                   cadence=cadence, horizon=horizon,
                                   label="r61:%s:%s" % (spec["panel_id"],
                                                        stage),
                                   **kwargs)
        except B.BookRefusal as exc:
            return {"panel_id": spec["panel_id"], "rho": float(rho),
                    "seed": int(seed), "state": "BOOK_REFUSED",
                    "reason": str(exc)[:300], "layers": layers,
                    "detected": False}
        layers[stage] = measured["stats"]
        adv = E.stage_advance(stage, measured["stats"], expected_sign=1)
        if not adv["advance"] and stage != "L":
            halted_at, halt_reasons = stage, adv["halt_reasons"]
            break
    out = {
        "panel_id": spec["panel_id"], "rho": float(rho), "seed": int(seed),
        "state": "HALTED" if halted_at else "MEASURED",
        "halted_at": halted_at, "halt_reasons": halt_reasons,
        "layers": {s: _compact(v) for s, v in layers.items()},
        "lockbox_computed": "L" in layers,
    }
    for den in BURDEN_DENOMINATOR_GRID:
        d = ({"qualified": False, "strict": False,
              "failed_gates": ["HALTED_AT_%s" % halted_at],
              "burden_denominator": den}
             if halted_at else detect(layers, burden_denominator=den))
        out["detection_burden_%d" % den] = d
    prim = out["detection_burden_%d" % PRIMARY_BURDEN_DENOMINATOR]
    out["detected"] = bool(prim["qualified"])
    out["detected_strict"] = bool(prim.get("strict"))
    return out


_KEEP = ("ann_net_excess", "ann_gross_excess", "ann_cost_drag",
         "mean_oneway_turnover_per_period", "t_net_excess", "t_net",
         "p_one_sided", "periods", "effective_observations",
         "overlap_factor", "halves_ann_net_excess", "hit_rate",
         "strategy_max_drawdown", "excess_max_drawdown",
         "median_universe", "median_live_markets", "median_positions")


def _compact(stats: dict) -> dict:
    out = {}
    for k in _KEEP:
        if k in (stats or {}):
            v = stats[k]
            out[k] = (float(v) if isinstance(v, (int, float, np.generic))
                      and not isinstance(v, bool) else v)
    return out


# --------------------------------------------------------------------------- #
# Aggregation: detection rate, uncertainty, MDE
# --------------------------------------------------------------------------- #
def wilson_interval(successes: int, n: int, z: float = 1.96) -> dict:
    """Wilson score interval - honest at 0 and at 1, where normal is not."""
    if n <= 0:
        return {"lo": None, "hi": None, "point": None, "n": 0}
    p = successes / float(n)
    d = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = (z / d) * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return {"point": p, "lo": max(0.0, centre - half),
            "hi": min(1.0, centre + half), "n": int(n)}


def detection_curve(cells: list, *, burden_denominator: int
                    = PRIMARY_BURDEN_DENOMINATOR, strict: bool = False
                    ) -> list:
    """Detection rate per effect size, with its Monte-Carlo uncertainty."""
    key = "detection_burden_%d" % int(burden_denominator)
    field = "strict" if strict else "qualified"
    by_rho: dict = {}
    for c in cells:
        by_rho.setdefault(float(c["rho"]), []).append(c)
    rows = []
    for rho in sorted(by_rho):
        group = by_rho[rho]
        hits = sum(1 for c in group if (c.get(key) or {}).get(field))
        ci = wilson_interval(hits, len(group))
        nets = [(c.get("layers", {}).get("L") or {}).get("ann_net_excess")
                for c in group]
        nets = [v for v in nets if isinstance(v, (int, float))]
        gross = [(c.get("layers", {}).get("L") or {}).get("ann_gross_excess")
                 for c in group]
        gross = [v for v in gross if isinstance(v, (int, float))]
        turns = [(c.get("layers", {}).get("L") or {})
                 .get("mean_oneway_turnover_per_period") for c in group]
        turns = [v for v in turns if isinstance(v, (int, float))]
        rows.append({
            "rho": rho, "seeds": len(group), "detections": hits,
            "detection_rate": ci["point"],
            "ci95_lo": ci["lo"], "ci95_hi": ci["hi"],
            "lockboxes_computed": sum(1 for c in group
                                      if c.get("lockbox_computed")),
            "halted_at_D": sum(1 for c in group if c.get("halted_at") == "D"),
            "halted_at_V": sum(1 for c in group if c.get("halted_at") == "V"),
            "median_lockbox_ann_net_excess":
                (float(np.median(nets)) if nets else None),
            "median_lockbox_ann_gross_excess":
                (float(np.median(gross)) if gross else None),
            "median_oneway_turnover":
                (float(np.median(turns)) if turns else None),
        })
    return rows


def _interp_x_at_y(xs: list, ys: list, target: float) -> Optional[float]:
    """First x where a monotone-ish y crosses ``target``, linearly.

    Returns None when the curve never reaches the target on the measured grid
    - which is a RESULT ("not detectable at any effect size we swept"), not a
    failure, and is reported as such rather than extrapolated into a number.
    """
    for i in range(1, len(xs)):
        y0, y1 = ys[i - 1], ys[i]
        if y0 is None or y1 is None:
            continue
        if y0 < target <= y1:
            if y1 == y0:
                return float(xs[i])
            w = (target - y0) / (y1 - y0)
            return float(xs[i - 1] + w * (xs[i] - xs[i - 1]))
        if y0 >= target and i == 1:
            return float(xs[0])
    return None


def mde(curve: list, *, power: float) -> dict:
    """Minimum detectable effect at ``power``, in BOTH units.

    ``rho`` is the injected information coefficient; ``ann_net_excess`` is
    what that rho actually delivered net of the panel's own costs, which is
    the unit the interpretation rule is written in.

    A CAVEAT THAT BELONGS NEXT TO THE NUMBER, NOT IN A FOOTNOTE. A run that
    halts at D or V never computes a lockbox, so the median net excess at a
    given rho is conditioned on the runs that REACHED the lockbox - the
    luckier draws - and is mildly optimistic wherever coverage is partial.
    Near the 80% point coverage is close to complete on every panel, so the
    MDE_80 return figure is essentially unconditioned; the MDE_50 figure sits
    lower on the curve and should be read as a mild UNDERstatement of the
    floor. ``mde_rho`` is unaffected either way, because the detection rate is
    counted over every seed whether it halted or not.
    """
    xs = [r["rho"] for r in curve]
    ys = [r["detection_rate"] for r in curve]
    nets = [r["median_lockbox_ann_net_excess"] for r in curve]
    gross = [r["median_lockbox_ann_gross_excess"] for r in curve]
    rho_star = _interp_x_at_y(xs, ys, float(power))
    out = {"power": float(power), "mde_rho": rho_star,
           "mde_ann_net_excess": None, "mde_ann_gross_excess": None,
           "reached": rho_star is not None}
    if rho_star is None:
        out["note"] = ("detection never reached %.0f%% anywhere on the frozen "
                       "effect grid (max rho %.3f); the MDE is ABOVE the grid "
                       "and is not extrapolated"
                       % (100 * power, max(xs) if xs else float("nan")))
        return out
    for name, series in (("mde_ann_net_excess", nets),
                         ("mde_ann_gross_excess", gross)):
        pairs = [(x, v) for x, v in zip(xs, series) if v is not None]
        if len(pairs) >= 2:
            px = [p[0] for p in pairs]
            pv = [p[1] for p in pairs]
            out[name] = float(np.interp(rho_star, px, pv))
    return out


def panel_summary(panel_id: str, cells: list, pre_meta: dict,
                  spec_meta: dict) -> dict:
    """The per-panel row of the final table."""
    curve = detection_curve(cells)
    strict_curve = detection_curve(cells, strict=True)
    out = {
        "panel": panel_id,
        "instrument_count": spec_meta.get("instrument_count"),
        "decision_count": pre_meta.get("n_decisions"),
        "median_eligible_per_decision": pre_meta.get("median_eligible"),
        "cost_model": spec_meta.get("cost_model"),
        "cost_per_side": spec_meta.get("cost_per_side"),
        "burden_used": PRIMARY_BURDEN_DENOMINATOR,
        "burden_grid": list(BURDEN_DENOMINATOR_GRID),
        "seeds_per_point": SEEDS_PER_POINT,
        "curve": curve,
        "strict_curve": strict_curve,
        "mde": {("MDE_%d" % int(100 * p)): mde(curve, power=p)
                for p in POWER_LEVELS},
        "mde_strict": {("MDE_%d" % int(100 * p)): mde(strict_curve, power=p)
                       for p in POWER_LEVELS},
    }
    for den in BURDEN_DENOMINATOR_GRID:
        c = detection_curve(cells, burden_denominator=den)
        out["mde_at_burden_%d" % den] = {
            ("MDE_%d" % int(100 * p)): mde(c, power=p) for p in POWER_LEVELS}
    null = [r for r in curve if r["rho"] == 0.0]
    out["false_positive_rate_at_rho_zero"] = (null[0]["detection_rate"]
                                              if null else None)
    out["effective_observations_lockbox"] = _modal(
        cells, "effective_observations")
    out["lockbox_periods"] = _modal(cells, "periods")
    return out


def _modal(cells: list, key: str):
    vals = [(c.get("layers", {}).get("L") or {}).get(key) for c in cells]
    vals = [v for v in vals if isinstance(v, (int, float))]
    if not vals:
        return None
    return float(np.median(vals))
