"""alpha_agent.r64.report - the human-readable R64 result, rendered from the
artifacts and nothing else."""
from __future__ import annotations

from . import read_artifact, research_root
from . import challenger as CH
from . import experiments as X
from . import family as FAM
from . import frontier as FR
from . import handoff_validation as HV
from . import carry as C

REPORT_NAME = "R64_REPORT.md"


def _f(x, nd=4):
    if x is None:
        return "-"
    try:
        return ("%%.%df" % nd) % float(x)
    except (TypeError, ValueError):
        return str(x)


def render() -> str:
    hv = read_artifact(HV.ARTIFACT_NAME) or {}
    dc = read_artifact(C.ARTIFACT_NAME) or {}
    mx = read_artifact(X.MATRIX_ARTIFACT) or {}
    fam = read_artifact(FAM.ARTIFACT_NAME) or {}
    ch = read_artifact(CH.ARTIFACT_NAME) or {}
    ov = read_artifact(FR.ARTIFACT_NAME) or {}
    out = ["# R64 - Information-directed alpha: measured results", ""]
    out += ["## 1. R63 handoff", "",
            "verdict: **%s** (%d checks, failed: %s)" % (hv.get("verdict"), hv.get("n_checks") or 0,
                                                        hv.get("failed_checks") or []), ""]
    rep = hv.get("reproduction") or {}
    if rep:
        out += ["fresh run reproduces the persisted FX carry cell: **%s**" % rep.get("matches"), ""]
    out += ["## 2. Distinct contracts", "",
            "verdict: **%s**; markets %s; refused %s; max share c1==c2 %s" % (
                dc.get("verdict"), dc.get("n_markets"), dc.get("refused"),
                _f(dc.get("max_share_c1_equals_c2"))), ""]
    s = mx.get("summary") or {}
    out += ["## 3. Cells", "",
            "cells %s; by verdict %s; reproduced %s of which matched %s" % (
                s.get("n_cells"), s.get("by_verdict"), s.get("n_reproduced"),
                s.get("n_reproduction_matches")), "",
            "| cell | tag | R63 verdict | R64 verdict | cond t | FDR | R63 net inc | R63 dd | R64 net inc | 2x | Sharpe inc | R64 t | aug vol | aug dd | turnover | lev | at cap |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for b in mx.get("brief") or []:
        out.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            b["cell_id"], b["tag"], b["r63_verdict"], b["r64_verdict"], _f(b["conditional_t"], 2),
            b["fdr_pass_conditional"], _f(b["r63_ann_net_increment"]), _f(b["r63_max_dd_augmented"], 2),
            _f(b["r64_ann_net_increment"]), _f(b["r64_ann_net_increment_at_2x_cost"]),
            _f(b["r64_sharpe_increment"], 3), _f(b["r64_t_increment"], 2),
            _f(b["r64_augmented_ann_vol"], 3), _f(b["r64_augmented_max_dd"], 2),
            _f(b["r64_augmented_turnover"], 3), _f(b["r64_augmented_mean_gross_leverage"], 2),
            _f(b["r64_augmented_at_cap_share"], 2)))
    out += ["", "## 4. FX carry family (one economic family)", "",
            "verdict **%s**; family Holm p conditional %s, economic %s; cells %s of %s" % (
                fam.get("verdict"), _f(fam.get("family_p_conditional"), 6),
                _f(fam.get("family_p_economic"), 6), fam.get("n_cells"), fam.get("n_expected")), "",
            "| cell | cond t | cond p Holm | econ t | econ p Holm | net inc | 2x | Sharpe inc | R64 verdict |",
            "|---|---|---|---|---|---|---|---|---|"]
    for r in fam.get("rows") or []:
        out.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            r["cell_id"], _f(r["conditional_t"], 2), _f(r["conditional_p_holm"], 5),
            _f(r["economic_t"], 2), _f(r["economic_p_holm"], 5), _f(r["ann_net_increment"]),
            _f(r["ann_net_increment_at_2x_cost"]), _f(r["sharpe_increment"], 3), r["r64_verdict"]))
    out += ["", "## 5. Challengers", "", "counts %s" % (ch.get("counts"),), ""]
    for rec in (ch.get("ready_for_forward_qualification") or []) + (ch.get("more_research_required") or []):
        out.append("- **%s** %s: %s (freeze_record_hash %s)" % (
            rec["classification"], rec["challenger_id"], rec["why"], rec["freeze_record_hash"][:16]))
    for rec in ch.get("rejected") or []:
        out.append("- REJECTED %s: %s" % (rec["cell_id"], rec["binding_failure"]))
    out += ["", "## 6. Information-need overlay", "",
            "%s updates over frontier %s" % (ov.get("n_updates"), ov.get("source_frontier_artifact_hash"))]
    for u in ov.get("updates") or []:
        out.append("- %s -> %s (x%.2f) via %s" % (u["cell_key"], u["r64_verdict"],
                                                 u["remaining_research_value_multiplier"], u["cell_id"]))
    out += ["", "Research only. Nothing promoted, registered, adopted, approved, ordered or purchased.", ""]
    return "\n".join(out)


def write():
    p = research_root() / "results" / REPORT_NAME
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(render(), encoding="utf-8")
    return p
