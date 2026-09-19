r"""campaign_r56_v2.features_eq_ext - the leak-safe equity EXTENSION price feature set (work order FS5).

Feature set : ``FS_EQ_EXT_PRICE_V1``        (PAPER_TRADER_MULTI_AGENT_ALPHA_CAMPAIGN_R56_V2)
Universe    : ``EQ_EXT_SMALLMID_PIT_V1``     (universe_eq_ext.py, work order U5)
Dataset     : ``eq_ext_smallmid_pit_panel_v1`` (data_eq_ext.py, certified PIT_SAFE, DF2)
Owner       : feature-library-agent.  RESEARCH ONLY - no order, no fill, no broker, no promotion.
Used by     : P6 (amihud liquidity premium, H21), P7 (residual momentum, H21), P8 (H5 reversal, next OPEN).

WHAT THIS MODULE IS
-------------------
It OWNS NO FORMULA. The three signals are the canonical owner's closures, imported and called UNCHANGED:

    amihud_252        = alpha_agent.r57.families.amihud(252)                    (families.py lines 141-150)
    resid_mom_252_21  = alpha_agent.r57.families.residual_mom(252, skip=21)     (families.py lines  70-80)
    -ret_5            = alpha_agent.r57.families.reversal(5)                    (families.py lines  62-66,
                        i.e. -(tr[:, t] / tr[:, t-5] - 1) through _trailing_return, lines 16-22)

What it ADDS is the lineage (lag, source, availability instant of every feature), and ONE index guard
(``_scorable_index``) that makes the owner's small-``t`` behaviour explicit instead of silent. No lookback,
filter, winsorisation, neutralisation or minimum-observation rule is added, removed or tuned here: the
definitions are the director's (agenda.json P6 / P7 / P8, phase1b_rulings.json R6 / R7, FS5) and are FROZEN.

THE SCORE CONTRACT (what research/agents/campaign_r56_v2/evaluator.py consumes)
------------------------------------------------------------------------------
    score_fn(panel, t) -> float64[n_securities]      higher = SELECTED, NaN = unscorable
    panel = data_eq_ext.load_eq_ext_panel()          (float64, orientation [security, session])
    t     = session index of the DECISION CLOSE      (data with session index <= t only)

    P6  evaluator.run_equity_topn(..., score_fn=score_amihud_252, ...)                 entry = close t+1
    P7  evaluator.run_equity_topn(..., score_fn=score_resid_mom_252_21, ...)           entry = close t+1
    P8  evaluator.run_equity_daily_tranche_book(..., score_fn=score_neg_ret_5, ...)    entry = OPEN  t+1

SIGN OF EVERY SCORE (read from the owner's code, not assumed)
------------------------------------------------------------
* ``amihud``: families.py line 146 ``illiq = np.abs(r) / dvol`` (absolute daily return per dollar traded),
  line 147 its NaN-mean over the window, line 148 ``np.log(m)``. A HIGHER returned value is a LARGER price
  move per dollar traded = MORE ILLIQUID. The owner does NOT negate it (contrast ``low_vol`` line 104 and
  ``low_beta`` line 112, which return ``-sd`` / ``-beta``). ``run_equity_topn`` holds the HIGHEST scores, so
  the owner's output handed over UNNEGATED selects the 100 MOST ILLIQUID names = P6, expected sign +1.
* ``residual_mom``: line 77 ``z = cum / (sd * sqrt(lookback))`` - higher = stronger residual momentum. P7, +1.
* ``reversal(5)``: line 64 ``return -_trailing_return(panel, t, lookback, 0)`` = -ret_5. P8 declares the
  relation of ret_5 to the forward return as NEGATIVE (expected_sign -1) and holds the LOWEST ret_5; the score
  handed to the evaluator is therefore -ret_5 (highest score = lowest trailing 5-session return).
  ``feature_ret_5`` is the un-negated feature value, for lineage and diagnostics only.

TIMING (state it once, exactly)
-------------------------------
lag = 0 sessions versus the decision close t: the NEWEST input of every signal is the close of session t
(``tr[:, t]``, ``un[:, t] * vol[:, t]``, ``spy_tr[t]``). Availability instant = after the close of session t
(DF2 certification: "daily OHLCV for session s observable after the close of s"). Nothing with a session
index > t is read - proved by physical truncation and by poisoning (fs5_report.json). The first executable
instant is session t+1: the close of t+1 for P6 / P7, the OPEN of t+1 for P8. lag versus the entry session = 1.

``op_tr`` (total-return-adjusted open) is an EXECUTION PRICE ONLY: observable at the open of its own session,
read by the evaluator AFTER the selection is fixed, to fill at t+1 and to liquidate at t+1+hold. It is never a
signal, selection or universe input and is deliberately absent from ``SCORE_FNS``. No function in this module
reads ``panel['op_tr']``, ``panel['dv']``, ``panel['mem']``, ``panel['sp500_mem']`` or ``panel['sectors']``.

THE NEGATIVE-INDEX TRAP (why the guard exists)
----------------------------------------------
``families._trailing_return`` indexes ``tr[:, t - skip - lookback]`` with a plain INTEGER. For
``t < skip + lookback`` that integer is negative and numpy silently WRAPS it to the END of the array: the
"trailing" return would be measured against a FUTURE price. For ``reversal(5)`` that is t in {0,1,2,3,4}.
``amihud`` and ``residual_mom`` use SLICES (``tr[:, t - lookback:t + 1]``); a negative slice start on the full
panel gives an EMPTY window (all-NaN scores), not a future read, but on a short panel it gives a window of the
wrong length. ``MIN_SAFE_T`` is the first t at which the owner's window is complete and entirely >= 0:

    ret_5 : 5        amihud_252 : 252        resid_mom_252_21 : 273

Below it every exposed function returns all-NaN (unscorable - the history the definition needs does not
exist) and never calls the owner. The campaign's first decision is 2011-07-01 = panel column 629 > 273.

Traps the signal agents must respect
------------------------------------
1. Hand the score functions to the evaluator AS THEY ARE. Do not wrap, shift, rank-transform, winsorise,
   neutralise or combine them: that is a new feature and a new experiment.
2. ``score_neg_ret_5`` is ALREADY negated. Do not negate it again; do not pass ``feature_ret_5`` to the book.
3. Eligibility is NOT applied here. The evaluator ANDs the U5 mask (``universe_eq_ext.load_u5_mask``); a
   finite score on a non-eligible name means nothing.
4. The panel must be ``load_eq_ext_panel()`` at its default float64 (the U5 mask is defined on it). A float32
   panel is refused.
5. ``amihud`` allocates the full ``un * vol`` product on every call (about 204 MB on this panel): fine on the
   H21 grid, never call it in a per-session loop.
6. ``amihud`` has NO minimum-observation rule (owner code: one finite daily ratio is enough) and drops
   zero-volume sessions from the mean; ``residual_mom`` needs >= 219 finite daily returns of 273
   (``cnt >= lookback * 0.8``, line 49). Neither is changed here; U5's history rule is the only other gate.
7. ``tr`` / ``op_tr`` LEVELS are back-adjusted: every signal here uses RATIOS of ``tr`` only. ``un * vol`` is
   the point-in-time dollar volume; ``dv`` is never read.

Usage (PowerShell):
    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\research\agents\campaign_r56_v2\features_eq_ext.py catalog
    ... features_eq_ext.py selftest     # truncation + poison on a few sessions; exit 0 / 2

Import:
    import sys
    sys.path.insert(0, r"C:\Users\binis\paper_trader")
    sys.path.insert(0, r"C:\Users\binis\paper_trader\research\agents\campaign_r56_v2")
    from data_eq_ext import load_eq_ext_panel
    from universe_eq_ext import load_u5_mask
    from features_eq_ext import score_amihud_252, score_resid_mom_252_21, score_neg_ret_5
    import evaluator
    panel = load_eq_ext_panel(); u5 = load_u5_mask(panel=panel)
    res = evaluator.run_equity_topn(panel, score_amihud_252, u5["elig"], label="P6", cadence=21, horizon=21,
                                    top_n=100, cost_rate=0.0025, cost_mult=1.0, first_date="2011-07-01")
"""
from __future__ import annotations

import hashlib
import json
import sys
import warnings
from pathlib import Path

import numpy as np

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
for _p in (str(_REPO), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from alpha_agent.r57 import families as _families          # noqa: E402  (the canonical signal owner)

FEATURE_SET_ID = "FS_EQ_EXT_PRICE_V1"
FEATURE_SET_VERSION = 1
UNIVERSE_ID = "EQ_EXT_SMALLMID_PIT_V1"
DATASET_ID = "eq_ext_smallmid_pit_panel_v1"
CAMPAIGN_FIRST_DECISION_DATE = "2011-07-01"

# Frozen by the director (agenda.json P6 / P7 / P8; foundation_requests.json FS5). NOT tunable.
AMIHUD_LOOKBACK = 252
RESID_LOOKBACK = 252
RESID_SKIP = 21
RET_LOOKBACK = 5

# The canonical owner's closures, built ONCE, called UNCHANGED.
_OWNER_AMIHUD_252 = _families.amihud(AMIHUD_LOOKBACK)
_OWNER_RESID_MOM_252_21 = _families.residual_mom(RESID_LOOKBACK, skip=RESID_SKIP)
_OWNER_REVERSAL_5 = _families.reversal(RET_LOOKBACK)        # == -(tr[:, t] / tr[:, t-5] - 1)

#: First session index at which the owner's window is complete and every index it forms is >= 0.
MIN_SAFE_T = {
    "amihud_252": AMIHUD_LOOKBACK,                          # tr[:, t-252 : t+1]
    "resid_mom_252_21": RESID_LOOKBACK + RESID_SKIP,        # tr[:, t-273 : t+1], spy_tr[t-273 : t+1]
    "ret_5": RET_LOOKBACK,                                  # tr[:, t-5]  (INTEGER index: wraps when negative)
}

#: Earliest / latest session index read at decision t, per panel key (offsets relative to t). Proved, not
#: asserted: fs5_report.json "window_proof" destroys every column outside these ranges.
INPUT_WINDOWS = {
    "amihud_252": {"tr": (-AMIHUD_LOOKBACK, 0), "un": (-AMIHUD_LOOKBACK + 1, 0),
                   "vol": (-AMIHUD_LOOKBACK + 1, 0)},
    "resid_mom_252_21": {"tr": (-(RESID_LOOKBACK + RESID_SKIP), 0),
                         "spy_tr": (-(RESID_LOOKBACK + RESID_SKIP), 0)},
    "ret_5": {"tr": (-RET_LOOKBACK, 0)},                   # exactly the two columns t-5 and t
}

#: Panel keys that may NEVER be read by a signal of this feature set.
NEVER_A_SIGNAL_INPUT = ("op_tr", "dv", "mem", "sp500_mem", "sectors")
EXECUTION_ONLY = ("op_tr",)

PANEL_KEYS_2D = ("tr", "un", "vol", "op_tr", "dv", "mem", "sp500_mem")
PANEL_KEYS_1D = ("spy_tr", "dates")

TIMING_STATEMENT = ("score_fn(panel, t) reads data with session index <= t only (newest input = the close of "
                    "session t, lag 0); it is observable after the close of t and actionable no earlier than "
                    "session t+1 (close t+1 for P6/P7, OPEN t+1 for P8; lag versus the entry session = 1)")


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def module_sha256() -> str:
    """sha256 of THIS file - bound into every evidence file and into the pre-registration parameters."""
    return file_sha256(Path(__file__).resolve())


def owner_sha256() -> str:
    """sha256 of alpha_agent/r57/families.py, the file that owns the three formulas."""
    return file_sha256(Path(_families.__file__).resolve())


# --------------------------------------------------------------------------- #
# The one guard
# --------------------------------------------------------------------------- #
def _scorable_index(panel: dict, t, name: str, float_keys: tuple) -> bool:
    """True when the owner may be called at ``t``. Refuses a non-integer / out-of-range index and a
    non-float64 panel; returns False (-> all-NaN, unscorable) below ``MIN_SAFE_T[name]``."""
    if isinstance(t, (bool, np.bool_)) or not isinstance(t, (int, np.integer)):
        raise TypeError("decision index t must be an integer session index, got %r" % (type(t),))
    n_dates = panel["tr"].shape[1]
    if t < 0 or t >= n_dates:
        raise IndexError("decision index %d outside the panel [0, %d): a negative index would WRAP to the "
                         "end of the array (future data)" % (int(t), n_dates))
    for k in float_keys:
        if panel[k].dtype != np.float64:
            raise ValueError("%s is defined on load_eq_ext_panel(dtype=float64); panel[%r] is %s"
                             % (FEATURE_SET_ID, k, panel[k].dtype))
    return int(t) >= MIN_SAFE_T[name]


def _unscorable(panel: dict) -> np.ndarray:
    return np.full(panel["tr"].shape[0], np.nan, dtype=np.float64)


# --------------------------------------------------------------------------- #
# The three scores  (score_fn(panel, t) -> float64[n_securities]; higher = selected; NaN = unscorable)
# --------------------------------------------------------------------------- #
def score_amihud_252(panel: dict, t: int) -> np.ndarray:
    """P6 score = feature ``amihud_252`` = ``alpha_agent.r57.families.amihud(252)`` UNCHANGED, sign +1.

    value        log( NaN-mean over sessions s in [t-251, t] of |tr[s]/tr[s-1] - 1| / (un[s] * vol[s]) );
                 non-finite daily ratios (zero / missing dollar volume, missing return) are dropped from the
                 mean; NaN when no finite ratio exists or the mean is not > 0. HIGHER = MORE ILLIQUID.
    lag          0 sessions versus the decision close t (newest input: session t).
    source       eq_ext_smallmid_pit_panel_v1: tr[:, t-252 .. t] (ratios only), un[:, t-251 .. t],
                 vol[:, t-251 .. t]. Never dv, op_tr, mem, sp500_mem, sectors.
    availability after the close of session t; position entered at the close of t+1 (run_equity_topn).
    unscorable   all-NaN for t < 252 (window incomplete; the owner is not called).
    """
    if not _scorable_index(panel, t, "amihud_252", ("tr", "un", "vol")):
        return _unscorable(panel)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)      # "Mean of empty slice" on never-priced rows
        return _OWNER_AMIHUD_252(panel, int(t))


def score_resid_mom_252_21(panel: dict, t: int) -> np.ndarray:
    """P7 score = feature ``resid_mom_252_21`` = ``alpha_agent.r57.families.residual_mom(252, skip=21)``
    UNCHANGED, sign +1.

    value        beta of the name's daily tr-returns versus the panel's spy_tr daily returns over the 273
                 sessions [t-272, t]; residual = r - beta * m; score = NaN-sum of the residuals of the FIRST
                 252 of those sessions ([t-272, t-21]) / (their NaN-std * sqrt(252)); NaN unless >= 219 of
                 the 273 daily returns are finite (owner: cnt >= lookback * 0.8) and the std is > 0.
                 HIGHER = STRONGER RESIDUAL MOMENTUM.
    lag          0 sessions versus the decision close t (the beta window ends at session t; the summed
                 residual window ends at t-21 by the owner's skip).
    source       eq_ext_smallmid_pit_panel_v1: tr[:, t-273 .. t] (ratios only), spy_tr[t-273 .. t].
    availability after the close of session t; position entered at the close of t+1 (run_equity_topn).
    unscorable   all-NaN for t < 273 (window incomplete; the owner is not called).
    """
    if not _scorable_index(panel, t, "resid_mom_252_21", ("tr", "spy_tr")):
        return _unscorable(panel)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)      # nanstd / mean on never-priced rows
        return _OWNER_RESID_MOM_252_21(panel, int(t))


def score_neg_ret_5(panel: dict, t: int) -> np.ndarray:
    """P8 score = MINUS feature ``ret_5`` = ``alpha_agent.r57.families.reversal(5)`` UNCHANGED.

    value        -(tr[:, t] / tr[:, t-5] - 1); NaN when tr is non-finite at t or at t-5 (or the ratio is not
                 finite). P8 holds the LOWEST ret_5 (declared sign -1), so the HIGHEST score is selected.
                 ALREADY NEGATED - do not negate again.
    lag          0 sessions versus the decision close t (newest input: session t).
    source       eq_ext_smallmid_pit_panel_v1: tr[:, t] and tr[:, t-5] only (a ratio).
    availability after the close of session t; tranche entered at the OPEN of t+1
                 (run_equity_daily_tranche_book). A close-t fill is FORBIDDEN by the agenda.
    unscorable   all-NaN for t < 5. THE TRAP: the owner indexes tr[:, t-5] with an integer; for t < 5 that
                 index is negative and numpy wraps it to the END of the array (a FUTURE price). The owner is
                 never called there.
    """
    if not _scorable_index(panel, t, "ret_5", ("tr",)):
        return _unscorable(panel)
    return _OWNER_REVERSAL_5(panel, int(t))


def feature_ret_5(panel: dict, t: int) -> np.ndarray:
    """The un-negated feature value ``ret_5 = tr[:, t] / tr[:, t-5] - 1`` (lineage / diagnostics ONLY).

    NOT a score: handing it to a book would buy the HIGHEST trailing return, the opposite of P8."""
    return -score_neg_ret_5(panel, t)


#: feature name -> the score function handed to the evaluator (higher = selected).
SCORE_FNS = {
    "amihud_252": score_amihud_252,
    "resid_mom_252_21": score_resid_mom_252_21,
    "ret_5": score_neg_ret_5,
}

#: feature name -> the feature VALUE (before the declared sign is applied).
FEATURE_VALUE_FNS = {
    "amihud_252": score_amihud_252,
    "resid_mom_252_21": score_resid_mom_252_21,
    "ret_5": feature_ret_5,
}

#: score = SCORE_SIGN * feature value.
SCORE_SIGN = {"amihud_252": 1, "resid_mom_252_21": 1, "ret_5": -1}

#: feature name -> the raw owner closure (for the wrapper == owner proof).
OWNER_FNS = {
    "amihud_252": _OWNER_AMIHUD_252,
    "resid_mom_252_21": _OWNER_RESID_MOM_252_21,
    "ret_5": _OWNER_REVERSAL_5,
}


def get_score_fn(name: str):
    """The score function of a published SIGNAL feature. ``op_tr`` is refused: it is an execution price."""
    if name in EXECUTION_ONLY:
        raise KeyError("%s is an EXECUTION PRICE ONLY - never a signal, selection or universe input" % name)
    return SCORE_FNS[name]


# --------------------------------------------------------------------------- #
# Lineage  (the publish_features payload and the catalog are assembled from THIS, never hand-typed)
# --------------------------------------------------------------------------- #
def catalog() -> list:
    """Per-feature lineage: name, lag, source (the three fields the pipeline refuses to go without), plus the
    definition, the availability instant, the role, the owner and the score handed to the evaluator."""
    common = {"lag_unit": "sessions versus the decision close t (0 = the newest input is session t itself)",
              "lag_versus_entry_session": 1, "dataset_id": DATASET_ID, "universe_id": UNIVERSE_ID,
              "feature_set_id": FEATURE_SET_ID, "feature_set_version": FEATURE_SET_VERSION,
              "module": "research/agents/campaign_r56_v2/features_eq_ext.py"}
    rows = [
        {"name": "amihud_252", "lag": 0,
         "source": "eq_ext_smallmid_pit_panel_v1: tr (ratios, sessions t-252..t), un, vol (sessions t-251..t)",
         "definition": "alpha_agent.r57.families.amihud(252) UNCHANGED: log of the NaN-mean over sessions "
                       "[t-251, t] of |tr[s]/tr[s-1]-1| / (un[s]*vol[s]); non-finite daily ratios dropped; "
                       "NaN if no finite ratio or mean <= 0",
         "owner": "alpha_agent/r57/families.py::amihud (lines 141-150)",
         "availability": "after the close of session t; position entered at the close of t+1",
         "role": "SIGNAL P6", "score_fn": "score_amihud_252", "score_sign_versus_feature": 1,
         "orientation": "HIGHER = MORE ILLIQUID = SELECTED (owner returns log(mean |r|/dollar volume), "
                        "un-negated; families.py lines 146-148)",
         "expected_sign": 1, "min_safe_t": MIN_SAFE_T["amihud_252"],
         "inputs": ["tr", "un", "vol"]},
        {"name": "resid_mom_252_21", "lag": 0,
         "source": "eq_ext_smallmid_pit_panel_v1: tr (ratios, sessions t-273..t), spy_tr (sessions t-273..t)",
         "definition": "alpha_agent.r57.families.residual_mom(252, skip=21) UNCHANGED: beta versus the "
                       "panel's spy_tr over the 273 sessions [t-272, t]; NaN-sum of the residuals of the first "
                       "252 sessions / (their NaN-std * sqrt(252)); NaN unless >= 219 of 273 daily returns "
                       "are finite and std > 0",
         "owner": "alpha_agent/r57/families.py::residual_mom (lines 70-80) + _beta_resid (lines 37-50)",
         "availability": "after the close of session t; position entered at the close of t+1",
         "role": "SIGNAL P7", "score_fn": "score_resid_mom_252_21", "score_sign_versus_feature": 1,
         "orientation": "HIGHER = STRONGER RESIDUAL MOMENTUM = SELECTED",
         "expected_sign": 1, "min_safe_t": MIN_SAFE_T["resid_mom_252_21"],
         "inputs": ["tr", "spy_tr"]},
        {"name": "ret_5", "lag": 0,
         "source": "eq_ext_smallmid_pit_panel_v1: tr (ratio of session t to session t-5)",
         "definition": "tr[t]/tr[t-5] - 1; NaN when tr is non-finite at t or t-5. Computed as MINUS "
                       "alpha_agent.r57.families.reversal(5), which is -(tr[t]/tr[t-5]-1) UNCHANGED",
         "owner": "alpha_agent/r57/families.py::reversal (lines 62-66) + _trailing_return (lines 16-22)",
         "availability": "after the close of session t; tranche entered at the OPEN of t+1",
         "role": "SIGNAL P8", "score_fn": "score_neg_ret_5", "score_sign_versus_feature": -1,
         "orientation": "P8 holds the LOWEST ret_5 (declared sign -1): the score handed to the evaluator is "
                        "-ret_5, HIGHER score = LOWER trailing 5-session return = SELECTED",
         "expected_sign": -1, "min_safe_t": MIN_SAFE_T["ret_5"],
         "inputs": ["tr"]},
        {"name": "op_tr", "lag": 0,
         "source": "eq_ext_smallmid_pit_panel_v1: op_tr (total-return-adjusted OPEN, NaN on a session "
                   "without an open print)",
         "definition": "total-return-adjusted open of each session, published AS STORED in the certified "
                       "panel; nothing is computed from it here",
         "owner": "research/agents/campaign_r56_v2/data_eq_ext.py (DF2)",
         "availability": "at the open of its own session s; read by the evaluator only AFTER the selection "
                         "of decision t is fixed, to fill at s = t+1 and to liquidate at t+1+hold",
         "role": "EXECUTION PRICE ONLY P8 - NEVER a signal, selection or universe input",
         "score_fn": None, "score_sign_versus_feature": None,
         "orientation": "not a score", "expected_sign": None, "min_safe_t": None, "inputs": ["op_tr"]},
    ]
    return [dict(common, **r) for r in rows]


# --------------------------------------------------------------------------- #
# Leak-test helpers  (used by the FS5 evidence scripts; reusable by the validation-skeptic-agent)
# --------------------------------------------------------------------------- #
def truncate_panel(panel: dict, t: int) -> dict:
    """A PHYSICAL copy of the panel holding sessions 0..t ONLY. Every array is a fresh buffer of width t+1
    (``.copy()`` of the slice), so no later session exists anywhere in the returned object."""
    t = int(t)
    out = {}
    for k in PANEL_KEYS_2D:
        if k in panel:
            out[k] = panel[k][:, :t + 1].copy()
    for k in PANEL_KEYS_1D:
        if k in panel:
            out[k] = panel[k][:t + 1].copy()
    for k in ("symbols", "sectors", "assetid", "delisted"):
        if k in panel:
            out[k] = panel[k]
    return out


def compare_values(a: np.ndarray, b: np.ndarray) -> dict:
    """Exact comparison of two score vectors: NaN pattern and BITWISE equality of the finite cells."""
    a = np.asarray(a, dtype=np.float64)
    b = np.asarray(b, dtype=np.float64)
    fa, fb = np.isfinite(a), np.isfinite(b)
    both = fa & fb
    return {"cells": int(a.size), "finite_a": int(fa.sum()), "finite_b": int(fb.sum()),
            "nan_pattern_mismatch": int((np.isnan(a) != np.isnan(b)).sum()),
            "finite_pattern_mismatch": int((fa != fb).sum()),
            "differing_finite_cells": int((a[both] != b[both]).sum())}


class RecordingPanel(dict):
    """A panel that records which keys a function reads (``panel[key]``)."""

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.read = []

    def __getitem__(self, key):
        self.read.append(key)
        return super().__getitem__(key)


def main(argv) -> int:
    cmd = argv[1] if len(argv) > 1 else "catalog"
    if cmd == "catalog":
        print(json.dumps({"feature_set_id": FEATURE_SET_ID, "module_sha256": module_sha256(),
                          "owner_sha256": owner_sha256(), "min_safe_t": MIN_SAFE_T,
                          "timing": TIMING_STATEMENT, "features": catalog()}, indent=1))
        return 0
    from data_eq_ext import load_eq_ext_panel                # type: ignore
    panel = load_eq_ext_panel(verify_hash=True)
    n_dates = panel["tr"].shape[1]
    first = int(np.searchsorted(panel["dates"], CAMPAIGN_FIRST_DECISION_DATE))
    bad = 0
    rows = []
    for t in (first, (first + n_dates) // 2, n_dates - 2):
        cut = truncate_panel(panel, t)
        for name, fn in SCORE_FNS.items():
            c = compare_values(fn(panel, t), fn(cut, t))
            c.update({"feature": name, "t": int(t), "date": str(panel["dates"][t])})
            bad += c["nan_pattern_mismatch"] + c["differing_finite_cells"]
            rows.append(c)
    print(json.dumps({"feature_set_id": FEATURE_SET_ID, "module_sha256": module_sha256(),
                      "selftest_rows": rows, "pass": bad == 0}, indent=1))
    return 0 if bad == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
