"""R96 — the paid-sample gates never pass on a missing input and detect a rewritten history."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "research" / "agents"))

from campaign_r96_data_activation_alpha_offensive import r96_paid_sample_gates as G  # noqa: E402


def test_no_sample_is_awaiting_not_pass(tmp_path):
    for gate in G.GATES:
        assert G.run_gate(gate, None, None)["verdict"] == "AWAITING_SAMPLE"


def test_missing_columns_fail(tmp_path):
    d = tmp_path / "s"
    d.mkdir()
    pd.DataFrame({"ticker": ["A"]}).to_csv(d / "x.csv", index=False)
    r = G.run_gate("INTRINIO_ESTIMATE_REVISION", d, None)
    assert r["verdict"] == "FAIL" and r["checks"][0]["check"] == "REQUIRED_COLUMNS_PRESENT"


def test_rewritten_history_between_deliveries_fails(tmp_path):
    cols = G.GATES["MACRO_CONSENSUS"]["required_columns"]
    row = {"country": "US", "indicator": "CPI", "release_ts": "2010-01-15T13:30:00Z", "consensus": 0.2,
           "actual": 0.3, "previous": 0.1, "consensus_as_of_ts": "2010-01-14T00:00:00Z"}
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir()
    b.mkdir()
    pd.DataFrame([row])[cols].to_csv(a / "x.csv", index=False)
    pd.DataFrame([dict(row, consensus=0.25)])[cols].to_csv(b / "x.csv", index=False)
    r = G.run_gate("MACRO_CONSENSUS", a, b)
    chk = {c["check"]: c for c in r["checks"]}
    assert chk["PIT_NO_RESTATEMENT_BETWEEN_DELIVERIES"]["passed"] is False
    assert r["verdict"] == "FAIL"


def test_licence_is_never_inferred(tmp_path):
    cols = G.GATES["OPTIONS_CHAIN"]["required_columns"]
    d = tmp_path / "o"
    d.mkdir()
    pd.DataFrame([{c: 1 for c in cols}]).to_csv(d / "x.csv", index=False)
    r = G.run_gate("OPTIONS_CHAIN", d, d)
    chk = {c["check"]: c for c in r["checks"]}
    assert chk["LICENSING_RESEARCH_USE_CONFIRMED_IN_WRITING"]["passed"] is False
