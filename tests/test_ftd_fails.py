"""Regressions for the preregistered SEC fails-to-deliver relative-to-volume axis.

These tests make the honesty claims CHECKABLE: that the fails stream and the
signal never read a return, that a fails file is never used before its declared
usable day, that an absent balance is an observed zero while an unidentified
security is unassessable, that the frozen constants are the frozen constants, that
no second scorer was written, and that the inherited burden only raises the bar.

Everything here is pure and local. No network, no store, no live path.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent.alpha_recovery import control_block_alpha as CBA
from alpha_agent.alpha_recovery import ftd_fails_alpha as A
from alpha_agent.alpha_recovery import ftd_fails_data as D
from alpha_agent.alpha_recovery import ftd_fails_signal as FS

REPO = Path(__file__).resolve().parents[1]
PREREG = REPO / "research" / "preregistration" / "FTD_FAILS_PREREGISTRATION.md"

#: sha256 of the preregistration BLOB as committed, before any real forward
#: return for this family existed. Read from git, so a later edit cannot pass.
PREREG_SHA256 = "7626c3695829cc529cd023118cb7a588ec008045541277df82749a4ca6a4c0de"
PREREG_COMMIT = "bc7da89"


# --------------------------------------------------------------------------- #
# The preregistration was not moved after the results
# --------------------------------------------------------------------------- #
def test_preregistration_blob_is_unchanged_since_it_was_frozen():
    import hashlib
    import subprocess
    out = subprocess.run(
        ["git", "-C", str(REPO), "cat-file", "blob",
         "%s:research/preregistration/FTD_FAILS_PREREGISTRATION.md" % PREREG_COMMIT],
        capture_output=True)
    if out.returncode != 0:
        pytest.skip("git object not available in this checkout")
    assert hashlib.sha256(out.stdout).hexdigest() == PREREG_SHA256
    wt = PREREG.read_bytes().replace(b"\r\n", b"\n")
    assert hashlib.sha256(wt).hexdigest() == PREREG_SHA256


def test_runner_points_at_the_frozen_commit():
    assert A.PREREGISTRATION_COMMIT == PREREG_COMMIT
    assert A.PREREGISTRATION.endswith("FTD_FAILS_PREREGISTRATION.md")


def test_frozen_family_shape():
    assert FS.CELLS == ("FAILS_TO_VOLUME",)
    assert FS.CELL_SPECS["FAILS_TO_VOLUME"]["sign"] == FS.SIGN_NEGATIVE == -1
    assert A.HORIZONS_FROZEN == (21, 63)
    assert A.declared_m() == 2
    assert len(A.PRIOR_SHORT_POSITIONING_TESTS) == 2 and A.inherited_m() == 4
    assert D.FIRST_HALF_USABLE_AFTER_DAY == 10 and D.SECOND_HALF_USABLE_AFTER_DAY == 25
    assert D.ALL_NONZERO_BALANCES_FROM == "2008-09-16"
    assert FS.MIN_NAMES == 50


def test_the_family_fits_the_existing_primary_budget():
    from alpha_agent import alpha_recovery as AR
    assert A.declared_m() <= AR.FAMILY_PRIMARY_MAX


# --------------------------------------------------------------------------- #
# No return before the preregistration; no second scorer
# --------------------------------------------------------------------------- #
def test_the_stream_and_the_signal_never_read_a_return():
    for mod in (D, FS):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        assert "forward_compound" not in src and '["tr"]' not in src and "spy_tr" not in src, \
            "%s must not touch returns" % mod.__name__
    assert '["price"]' not in Path(D.__file__).read_text(encoding="utf-8"), \
        "the fails stream must not read prices or volume at all"


def test_no_second_scorer_was_written():
    for mod in (A, D, FS):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        assert "def run_cell" not in src and "def _book_returns" not in src
        assert "def nw_tstat" not in src and "def spearman" not in src


def test_the_frozen_13dg_owners_are_called_not_copied():
    src = Path(A.__file__).read_text(encoding="utf-8")
    assert "CBA.raw_and_orthogonal(" in src and "CBA.incremental_vs_incumbent(" in src
    assert "CBA.missingness_bias(" in src and "def raw_and_orthogonal" not in src


# --------------------------------------------------------------------------- #
# Publication: never before the declared usable day
# --------------------------------------------------------------------------- #
def test_half_month_windows_follow_the_calendar():
    assert D.half_month_window(2024, 2, "a") == (pd.Timestamp("2024-02-01").date(),
                                                 pd.Timestamp("2024-02-15").date())
    assert str(D.half_month_window(2024, 2, "b")[1]) == "2024-02-29"
    assert str(D.half_month_window(2023, 2, "b")[1]) == "2023-02-28"


def test_usable_days_carry_slack_over_every_measured_posting():
    assert str(D.usable_after(2026, 1, "a")) == "2026-02-10"      # measured posting 02-05
    assert str(D.usable_after(2026, 1, "b")) == "2026-02-25"      # measured posting 02-17
    assert str(D.usable_after(2020, 12, "b")) == "2021-01-25"


def test_usable_days_are_monotone_in_the_period():
    periods = [(y, m, h) for y in (2019, 2020) for m in range(1, 13) for h in ("a", "b")]
    ua = [D.usable_after(*p) for p in periods]
    assert ua == sorted(ua)


def _files():
    return [{"name": "a", "period": "2020-01a", "first_day": "2020-01-01", "last_day": "2020-01-15",
             "usable_after": "2020-02-10"},
            {"name": "b", "period": "2020-01b", "first_day": "2020-01-16", "last_day": "2020-01-31",
             "usable_after": "2020-02-25"},
            {"name": "c", "period": "2020-02a", "first_day": "2020-02-01", "last_day": "2020-02-15",
             "usable_after": "2020-03-10"}]


def test_a_file_is_visible_only_strictly_after_its_usable_day():
    dates = pd.bdate_range("2020-01-01", "2020-03-31").strftime("%Y-%m-%d").values.astype("<U10")
    vis = D.visible_file_index(dates, _files())
    at = {d: int(vis[list(dates).index(d)]) for d in
          ("2020-02-10", "2020-02-11", "2020-02-25", "2020-02-26", "2020-03-10", "2020-03-11")}
    assert at == {"2020-02-10": -1, "2020-02-11": 0, "2020-02-25": 0, "2020-02-26": 1,
                  "2020-03-10": 1, "2020-03-11": 2}
    assert D.visible_file_index(dates, []).max() == -1


# --------------------------------------------------------------------------- #
# The balance: absent = observed zero; unidentified = unassessable
# --------------------------------------------------------------------------- #
def _write_ftd(dirpath: Path, name: str, rows: list):
    lines = ["SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE"] + rows
    with zipfile.ZipFile(dirpath / name, "w") as zf:
        zf.writestr(name.replace(".zip", ".txt"), "\n".join(lines) + "\n")


def test_absent_is_zero_unidentified_is_nan_and_lines_of_one_security_sum(tmp_path, monkeypatch):
    _write_ftd(tmp_path, "cnsfails202001a.zip", [
        "20200102|AAAAAAAA1|AAA|100|A CO|10.00",
        "20200103|CCCCCCCC1|AAA.B|50|A CO CL B|10.00",
        "20200103|DDDDDDDD1|ZZZ|999|NOT IN PANEL|1.00",
        "20200103|EEEEEEEE1|EEE|0|ZERO ROW|1.00",
    ])
    monkeypatch.setattr(D.OD, "ftd_dir", lambda: tmp_path)
    c2r = {"AAAAAAAA": 0, "CCCCCCCC": 0, "BBBBBBBB": 1, "EEEEEEEE": 1}
    bal = D.half_month_balances(c2r, 3, verbose=False)
    assert bal["settlement_days"].tolist() == [2]
    assert bal["mean_balance"][0, 0] == pytest.approx(75.0)          # (100 + 50) / 2 dates
    assert bal["mean_balance"][0, 1] == 0.0                         # traded, nothing failed
    assert np.isnan(bal["mean_balance"][0, 2])                      # no bridged CUSIP
    assert bal["nonzero_days"][0, 0] == 2 and bal["nonzero_days"][0, 1] == 0
    assert bal["identified"].tolist() == [True, True, False]
    assert bal["files"][0]["usable_after"] == "2020-02-10"


def test_a_stale_cache_is_refused_when_identity_changes(tmp_path, monkeypatch):
    _write_ftd(tmp_path, "cnsfails202001a.zip", ["20200102|AAAAAAAA1|AAA|100|A CO|10.00"])
    monkeypatch.setattr(D.OD, "ftd_dir", lambda: tmp_path)
    monkeypatch.setattr(D, "derived_path", lambda: tmp_path / "cache.npz")
    first = D.load_or_build_balances({"AAAAAAAA": 0}, 2, verbose=False)
    again = D.load_or_build_balances({"AAAAAAAA": 0}, 2, verbose=False)
    moved = D.load_or_build_balances({"AAAAAAAA": 1}, 2, verbose=False)
    assert first["from_cache"] is False and again["from_cache"] is True
    assert moved["from_cache"] is False and moved["identified"].tolist() == [False, True]


# --------------------------------------------------------------------------- #
# The ratio, the projection and the rank
# --------------------------------------------------------------------------- #
def test_ratio_is_nan_without_volume_and_projection_is_strict():
    files = _files()
    dates = pd.bdate_range("2020-01-01", "2020-03-31").strftime("%Y-%m-%d").values.astype("<U10")
    vol = np.full((3, len(dates)), 1000.0)
    vol[1, :] = 0.0
    vol[2, :] = np.nan
    E = {"dates": dates, "symbols": np.array(["A", "B", "C"]), "price": {"vol": vol}}
    bal = {"files": files, "mean_balance": np.array([[10.0, 5.0, 5.0]] * 3),
           "settlement_days": np.array([10, 11, 10])}
    per = FS.fails_to_volume_by_file(E, bal)
    assert per[0, 0] == pytest.approx(0.01)
    assert np.isnan(per[0, 1]) and np.isnan(per[0, 2])
    M, _vis = FS.session_matrix(E, per, files)
    d = list(dates)
    assert np.isnan(M[0, d.index("2020-02-10")]) and M[0, d.index("2020-02-11")] == pytest.approx(0.01)


def test_rank_signal_applies_the_frozen_sign_and_leaves_gaps_unassessable():
    n, T = 60, 5
    M = np.tile(np.arange(n, dtype=float)[:, None], (1, T))
    M[3, :] = np.nan
    elig = np.ones((n, T), dtype=bool)
    elig[4, :] = False
    sig, per = FS.rank_signal(M, elig, np.array([2]), FS.SIGN_NEGATIVE)
    col = sig[:, 2]
    assert np.isnan(col[3]) and np.isnan(col[4])
    assert col[n - 1] == pytest.approx(-1.0)                        # most fails -> most negative
    assert np.nanmax(col) < 0 and per[0]["ranked"] == n - 2
    assert np.isnan(sig[:, 1]).all()                                # not a decision session
    thin = np.zeros((n, T), dtype=bool)
    thin[:10, :] = True
    s2, p2 = FS.rank_signal(M, thin, np.array([2]), FS.SIGN_NEGATIVE)
    assert np.isnan(s2).all() and p2 == []


def test_coverage_report_counts_sessions_under_the_floor():
    elig = np.ones((100, 3), dtype=bool)
    identified = np.ones(100, dtype=bool)
    identified[:6] = False                                          # 94% identified
    rep = A.coverage_report(elig, identified, np.array([0, 1, 2]))
    assert rep["uncovered_share"] == 1.0 and rep["coverage_mean"] == pytest.approx(0.94)


# --------------------------------------------------------------------------- #
# The inherited burden can only raise the bar
# --------------------------------------------------------------------------- #
def test_prior_tests_enter_at_p_one_and_raise_the_bar():
    _r, own = A._bh([0.04, 0.9])
    assert own[0] is True
    res, inh = A._bh([0.04, 0.9], extra_nulls=len(A.PRIOR_SHORT_POSITIONING_TESTS))
    assert inh[0] is False and res["m"] == 4
    _r, none_p = A._bh([None, 0.9])
    assert _r["m"] == 2


# --------------------------------------------------------------------------- #
# Verdict order
# --------------------------------------------------------------------------- #
_GOOD_CELL = {"effective_periods": 115, "conditional": {"t": 3.0},
              "redundancy": {"redundancy": "DISTINCT"}, "economics": {"ann_net_increment": 0.05}}
_GOOD_DESC = {"rank_ic_raw": {"mean": 0.01, "t": 3.0}, "rank_ic_orthogonalised": {"t": 3.0}}
_GOOD_INC = {"conditional": {"t": 3.0}}


def _v(**kw):
    args = dict(cell=_GOOD_CELL, inc_cell=_GOOD_INC, desc=_GOOD_DESC,
                coverage={"uncovered_share": 0.0}, bias={"biased": False})
    opts = dict(fdr_pass=True, inherited_fdr_pass=True)
    for k in list(kw):
        if k in args:
            args[k] = kw.pop(k)
    opts.update(kw)
    return A.verdict_for(args["cell"], args["inc_cell"], args["desc"], args["coverage"],
                         args["bias"], **opts)


def test_every_gate_passing_is_the_only_route_to_qualified():
    assert _v()["verdict"] == A.V_QUALIFIED


def test_gate_order():
    assert _v(coverage={"uncovered_share": 0.3})["gate"] == "COVERAGE"
    assert _v(bias={"biased": True, "ann_diff": 0.04})["gate"] == "MISSINGNESS_BIAS"
    assert _v(cell=dict(_GOOD_CELL, effective_periods=20))["gate"] == "EFFECTIVE_PERIODS"
    assert _v(cell={"verdict": "DATA_HOLD", "why": "x"})["gate"] == "SCORER_FLOOR"
    assert _v(desc=dict(_GOOD_DESC, rank_ic_raw={"mean": -0.01, "t": -4.0}))["gate"] == "FROZEN_SIGN"
    assert _v(desc=dict(_GOOD_DESC, rank_ic_raw={"mean": 0.01, "t": 1.5}))["gate"] == "STANDALONE_T"
    assert _v(cell=dict(_GOOD_CELL, conditional={"t": -3.0}))["gate"] == "CONDITIONAL_T"
    assert _v(fdr_pass=False)["gate"] == "MULTIPLICITY"
    assert _v(inherited_fdr_pass=False)["gate"] == "INHERITED_MULTIPLICITY"
    assert _v(desc=dict(_GOOD_DESC, rank_ic_orthogonalised={"t": 1.0}))["verdict"] == A.V_NO_INCREMENTAL
    assert _v(cell=dict(_GOOD_CELL, economics={"ann_net_increment": 0.001}))["gate"] == "MATERIALITY"


def test_verdicts_are_the_existing_five_and_capital_is_never_eligible():
    assert set(A.VERDICTS) == {CBA.V_QUALIFIED, CBA.V_NO_EDGE, CBA.V_NO_INCREMENTAL,
                               CBA.V_DATA_HOLD, CBA.V_NEED_MORE}
    src = Path(A.__file__).read_text(encoding="utf-8")
    assert '"capital_eligible": False' in src and "capital_eligible\": True" not in src


def test_the_campaign_runner_registers_the_stage():
    src = (REPO / "scripts" / "run_alpha_recovery_offensive.py").read_text(encoding="utf-8")
    assert '"ftd_fails"' in src and "_stage_ftd_fails" in src
