r"""R94 director defects D2 / D5 - prototype mechanics for the amended empirical-null rule (NOT applied to R94)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from alpha_agent.r57 import empirical_null as EN


def _frame(rng, n, seed_col=None, seeds=(1,)):
    rows = []
    for s in seeds:
        t = rng.standard_normal(n)
        d = pd.DataFrame({"engine": "X", "panel": "F", "book": "B", "horizon": 5, "t_net_nw": t, "net_ann": t * 0.01, "cost_ann": 0.001, "n_decisions": 100,
                          "relationship_id": ["r%d_%d" % (s, i) for i in range(n)]})
        if seed_col:
            d[seed_col] = s
        rows.append(d)
    return pd.concat(rows, ignore_index=True)


def test_D2_dedupe_marks_rank_identical_books():
    rng = np.random.default_rng(0)
    real = _frame(rng, 50)
    dup = real.iloc[:5].copy()
    dup["relationship_id"] = ["dup%d" % i for i in range(5)]
    both = pd.concat([real, dup], ignore_index=True)
    out = EN.dedupe_identical_statistics(both, strata=["engine", "panel", "book", "horizon"], stat_col="t_net_nw")
    assert int(out["is_duplicate_book"].sum()) == 5
    assert set(out[out["is_duplicate_book"]]["relationship_id"]) == {"dup%d" % i for i in range(5)}


def test_D5_two_nulls_promote_only_when_both_pass():
    rng = np.random.default_rng(1)
    real = _frame(rng, 400)
    real.loc[:19, "t_net_nw"] = 7.0 + rng.standard_normal(20) * 0.1          # a real cluster
    null_a = _frame(rng, 400, "null_seed", seeds=(11, 12, 13, 14, 15))
    null_b = _frame(rng, 400, "null_seed", seeds=(21, 22, 23, 24, 25))
    null_b.loc[null_b.index[:40], "t_net_nw"] = 7.5                           # a HARDER null: the cluster is ordinary here
    strata = ["engine", "panel", "book", "horizon"]
    thr_a = EN.freeze_thresholds(null_a, strata=strata, stat_col="t_net_nw", seed_col="null_seed", sided=EN.SIDED_TWO, fdr_promote=0.05, p_shortlist=0.01, rule_text="a")
    thr_b = EN.freeze_thresholds(null_b, strata=strata, stat_col="t_net_nw", seed_col="null_seed", sided=EN.SIDED_TWO, fdr_promote=0.05, p_shortlist=0.01, rule_text="b")
    a = EN.calibrate_frame(real, null_a, thr_a)
    assert int(a["promote_null"].sum()) >= 15, "under the easy null the cluster is promoted"
    two = EN.calibrate_two_nulls(real, null_a, thr_a, null_b, thr_b, dedupe=True)
    assert int(two["promote_null"].sum()) == 0, "under BOTH nulls it is not"
    assert (two["empirical_fdr_q"] >= two["empirical_fdr_q_null_a"]).all()
    assert two["two_null_rule"].all()
