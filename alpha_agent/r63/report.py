"""alpha_agent.r63.report - the human-readable R63 result, rendered from the
machine-readable artifacts (never from memory of what a run printed).
"""
from __future__ import annotations

from . import (CH_MORE, CH_READY, DISPOSITIONS, HORIZONS, OBS_BLOCKED, OBS_NOT,
               OBS_PARTIAL, OBS_WELL, now_iso, read_artifact, research_root)
from . import asset_horizon as AH
from . import challengers as CH
from . import experiments as X
from . import gaps as G
from . import handoff as HO
from . import inventory as INV
from . import ontology as ONT
from . import sensitivity as S
from . import sourcing as SO

REPORT_NAME = "R63_REPORT.md"


def _f(x, nd=3):
    if x is None:
        return "n/a"
    try:
        return ("%%.%df" % nd) % float(x)
    except (TypeError, ValueError):
        return str(x)


def _cells():
    return (read_artifact(X.MATRIX_ARTIFACT) or {}).get("cells") or []


def render() -> str:
    cells = _cells()
    res = read_artifact(X.RESULTS_ARTIFACT) or {}
    inv = read_artifact(INV.ARTIFACT_NAME) or {}
    mat = read_artifact(AH.ARTIFACT_NAME) or {}
    gaps = read_artifact(G.ARTIFACT_NAME) or {}
    src = read_artifact(SO.ARTIFACT_NAME) or {}
    ch = read_artifact(CH.ARTIFACT_NAME) or {}
    ho = read_artifact(HO.ARTIFACT_NAME) or {}
    ortho = read_artifact(X.ORTHO_ARTIFACT) or {}
    scored = [c for c in cells if c.get("conditional")]
    L = []
    L.append("# R63 - Information Sensitivity, Orthogonal Information Discovery and the Alpha Offensive")
    L.append("")
    L.append("Generated %s from the machine-readable artifacts under `%s`." % (now_iso(), research_root()))
    L.append("")
    L.append("## 1. Certification (Stage 1)")
    L.append("")
    L.append("| disposition | fields |")
    L.append("|---|---|")
    for d in DISPOSITIONS:
        L.append("| %s | %d |" % (d, (inv.get("counts") or {}).get(d, 0)))
    L.append("")
    L.append("Providers certified: %d (%s). Fields: %d. UNKNOWN fields: %d. Research memory: %s, %s hypotheses."
             % (inv.get("n_providers", 0), ", ".join(inv.get("providers") or []), inv.get("n_fields", 0),
                len(inv.get("unknown_fields") or []), (inv.get("research_memory") or {}).get("state"),
                (inv.get("research_memory") or {}).get("n_hypotheses")))
    L.append("")
    L.append("## 2. Ontology and the asset x horizon map (Stages 2-3)")
    L.append("")
    L.append("%d information dimensions in %d classes (hash `%s`)." % (len(ONT.DIMENSIONS), len(ONT.INFORMATION_CLASSES), ONT.ontology_hash()[:16]))
    cnt = mat.get("counts") or {}
    L.append("Cells: %s." % ", ".join("%s %d" % (k, cnt.get(k, 0)) for k in (OBS_WELL, OBS_PARTIAL, OBS_NOT, OBS_BLOCKED)))
    L.append("")
    L.append("Most under-informed (asset class x horizon, share of blind dimensions):")
    for r in (mat.get("most_under_informed") or [])[:8]:
        L.append("- %s @ %d sessions: blind %d of %d (%.0f%%), partial %d" % (
            r["asset_class"], r["horizon"], r["blind"], r["dimensions"], 100 * (r["blind_share"] or 0), r["partial"]))
    L.append("")
    L.append("## 3. Sensitivity engine results (Stages 4-6)")
    L.append("")
    L.append("Cells run: %d; scored: %d; by verdict: %s." % (len(cells), len(scored),
             ", ".join("%s %d" % kv for kv in sorted((res.get("by_verdict") or {}).items()))))
    fdr = (res.get("fdr") or {}).get("conditional") or {}
    L.append("Multiple testing (conditional increments): m=%s, raw one-sided p<0.05: %s, BH q=0.10 survivors: %s."
             % (fdr.get("m"), fdr.get("raw_p_below_0_05"), fdr.get("n_rejected")))
    fdre = (res.get("fdr") or {}).get("economic") or {}
    L.append("Multiple testing (economic increments): m=%s, raw p<0.05: %s, BH survivors: %s." % (
        fdre.get("m"), fdre.get("raw_p_below_0_05"), fdre.get("n_rejected")))
    L.append("")
    L.append("Strongest conditional cell per scope:")
    L.append("")
    L.append("| scope | cell | kind | increment | t | lockbox t | residual share | net inc/yr | Sharpe inc | verdict |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for k, b in sorted((res.get("strongest_by_scope") or {}).items()):
        L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            k, b["cell_id"], b.get("kind"), _f(b.get("increment"), 4), _f(b.get("t"), 2), _f(b.get("t_lockbox"), 2),
            _f(b.get("residual_share"), 2), _f(b.get("ann_net_increment"), 4), _f(b.get("sharpe_increment"), 3), b.get("verdict")))
    L.append("")
    L.append("Top 25 conditional cells across the campaign:")
    L.append("")
    L.append("| cell | kind | t | p | econ inc/yr | t econ | blocks+ | residual | FDR | verdict |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for b in (res.get("top_conditional_cells") or [])[:25]:
        L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            b["cell_id"], b.get("kind"), _f(b.get("t"), 2), _f(b.get("p"), 4), _f(b.get("ann_net_increment"), 4),
            _f(b.get("t_econ"), 2), _f(b.get("share_blocks_positive"), 2), _f(b.get("residual_share"), 2),
            b.get("fdr_pass_conditional"), b.get("verdict")))
    L.append("")
    # what drives current value: ablations with positive marginal value
    abl = [c for c in scored if c.get("kind") == "ABLATION"]
    L.append("## 4. What drives current predictive value (baseline ablations)")
    L.append("")
    by_dim: dict = {}
    for c in abl:
        by_dim.setdefault(c["dimension"], []).append(c)
    L.append("| baseline dimension | cells | mean marginal t | cells with t>=2 | mean net inc/yr |")
    L.append("|---|---|---|---|---|")
    for d, cs in sorted(by_dim.items()):
        ts = [c["conditional"]["t"] for c in cs if c["conditional"].get("t") is not None]
        ec = [c["economics"]["ann_net_increment"] for c in cs if (c.get("economics") or {}).get("ann_net_increment") is not None]
        L.append("| %s | %d | %s | %d | %s |" % (d, len(cs), _f(sum(ts) / len(ts) if ts else None, 2),
                                                sum(1 for t in ts if t >= 2), _f(sum(ec) / len(ec) if ec else None, 4)))
    L.append("")
    L.append("## 5. Redundancy (orthogonality matrix)")
    L.append("")
    for sc, body in sorted((ortho.get("scopes") or {}).items()):
        if body.get("state") != "OK":
            continue
        rs = body.get("residual_share_vs_baseline") or {}
        red = sorted(((d, v.get("residual_share")) for d, v in rs.items() if v.get("residual_share") is not None), key=lambda kv: kv[1])
        L.append("- %s: most redundant %s; most distinct %s" % (
            sc, ", ".join("%s %.2f" % kv for kv in red[:3]), ", ".join("%s %.2f" % kv for kv in red[-3:])))
    L.append("")
    L.append("## 6. Information gap frontier (Stage 7)")
    L.append("")
    L.append("| rank | need | state | remaining value | next action | sourcing |")
    L.append("|---|---|---|---|---|---|")
    for r in (gaps.get("top_gaps") or [])[:20]:
        L.append("| %d | %s | %s | %s | %s | %s |" % (r["rank"], r["cell_key"], r["observation_state"],
                                                    _f(r["remaining_research_value"], 5), r["next_action"], r["sourcing_step"]))
    L.append("")
    L.append("By dimension (remaining research value):")
    for r in (gaps.get("by_dimension") or [])[:12]:
        L.append("- %s: %s (blind cells %d of %d)" % (r["dimension"], _f(r["remaining_total"], 4), r["blind_cells"], r["cells"]))
    L.append("")
    L.append("## 7. Sourcing and the paid-data gate (Stages 8-10)")
    L.append("")
    nav = src.get("authoritative_nav") or {}
    L.append("Authoritative paper NAV (read-only): %s on %s. Purchases: %s. Money spent: %s." % (
        nav.get("nav"), nav.get("date"), src.get("purchases"), src.get("money_spent_usd")))
    for g in (src.get("paid_data_gate") or [])[:12]:
        be = g.get("break_even") or {}
        L.append("- %s (%s): proxy best t %s in %s; fee %s; break-even alpha %s; gate-1 %s" % (
            g["dimension"], g["asset_class"], _f(g.get("proxy_best_t"), 2), g.get("proxy_best_cell"),
            be.get("fee_usd"), _f(be.get("break_even_alpha_annual"), 4), g.get("gate_1_verdict")))
    L.append("")
    L.append("## 8. Challengers (Stage 11)")
    L.append("")
    L.append("Counts: %s." % (ch.get("counts") or {}))
    for rec in (ch.get("ready_for_forward_qualification") or []):
        L.append("- READY: %s - %s" % (rec["challenger_id"], rec["why"]))
    for rec in (ch.get("more_research_required") or [])[:15]:
        L.append("- MORE_RESEARCH: %s - %s; failure mode %s" % (rec["challenger_id"], rec["why"], rec["strongest_failure_mode"]))
    L.append("")
    L.append("## 9. AlphaAgent handoff")
    L.append("")
    for r in (ho.get("ranked") or [])[:10]:
        L.append("- %s: %s (%s)" % (r["cell_key"], r["next_action"], _f(r["remaining_research_value"], 5)))
    st = ho.get("stop_transforming") or {}
    L.append("")
    L.append("Stop-transforming flags: %s" % {k: v.get("flag") for k, v in st.items()})
    L.append("")
    return "\n".join(L)


def write():
    p = research_root() / "results" / REPORT_NAME
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(render(), encoding="utf-8")
    return p
