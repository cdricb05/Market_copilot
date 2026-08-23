"""alpha_agent.r40.shadow_registry - SHADOW_REGISTRY_V2 +
SHADOW_SPECIFICATION_HASHES (Track D).

The prospective family stays intentionally SMALL: at most FIVE research
shadows after Release 40. The three Release-39 shadows are IMMUTABLE and
enter this registry by reference (id, spec hash, coefficient hash, freeze
timestamp, ledger paths under the R39 root); their ledgers keep being
written by the R39 capture owner. Two slots are open:

* SLOT 4 - the international-rates carry / relative-value rule
  (``c39_1a0105dd2f0c``): economically distinct, from the newly unlocked
  Norgate international bond-futures universe, selected on Zone-B evidence
  before any R40 forward outcome. A rule has no parameters; its
  specification hash IS its model.
* SLOT 5 - chosen by the rule frozen in ``contract.SLOT_5_SELECTION_RULE``
  (hashed into the closeout-import artifact before any R40 evaluation):
  the highest Zone-B after-cost t among eligible candidates drawn from
  (A) the R39 TCN, (B) the corrected WIDE successor, (C) the best new R40
  branch candidate; eligibility = t >= 1.5, same-sign halves, positive at
  2x costs, |corr| < 0.90 with every existing shadow's Zone-B stream, not
  an identical family+expression. NULL is a valid outcome.

A learned winner is frozen as BYTES (ridge coefficients, or a torch
state_dict + standardiser statistics written under the campaign directory
and hashed); capture never refits. Every new shadow carries
RESEARCH_SHADOW_ONLY / HISTORICAL_QUALIFICATION = FAIL /
PROMOTION_ALLOWED = False, its own freeze timestamp, and its own
chain-hashed ledgers (R39 desk primitives) under the R40 root.
"""
from __future__ import annotations

import io
import time

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r39 import research_shadow as RS
from ..r39 import trade_space_ext as TX
from ..r39.continuation_director import new_cand
from ..r39.wide_prosecution import WIDE_ID, wide_candidate
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import contract as C
from . import director as D

CALCULATION_OWNER = "alpha_agent.r40.shadow_registry"
REGISTRY_NAME = "shadow_registry_v2.json"
HASHES_NAME = "shadow_specification_hashes.json"
MODELS_DIRNAME = "shadow_models"
SHADOW_DIRNAME = "research_shadow_forward"
SNAPSHOT_LEDGER = RS.SNAPSHOT_LEDGER
OUTCOME_LEDGER = RS.OUTCOME_LEDGER
STAGE = "R40_SLOT_RESOLUTION"

RESEARCH_SHADOW_ONLY = True
PROMOTION_ALLOWED = False
HISTORICAL_QUALIFICATION = "FAIL"


def shadow_dir(campaign_id: str = CAMPAIGN_ID):
    d = campaign_dir(campaign_id) / SHADOW_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def models_dir(campaign_id: str = CAMPAIGN_ID):
    d = campaign_dir(campaign_id) / MODELS_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def load(campaign_id: str = CAMPAIGN_ID):
    return _r39.read_json(campaign_dir(campaign_id) / REGISTRY_NAME)


class FamilyCapExceeded(RuntimeError):
    """More research shadows than the frozen family cap allows."""


def enforce_cap(rows: list) -> None:
    if len(rows) > C.MAX_RESEARCH_SHADOW_FAMILY:
        raise FamilyCapExceeded(
            "%d research shadows exceed the frozen family cap of %d"
            % (len(rows), C.MAX_RESEARCH_SHADOW_FAMILY))
    ids = [r["shadow_id"] for r in rows]
    if len(set(ids)) != len(ids):
        raise FamilyCapExceeded("duplicate shadow id in the family")


# --------------------------------------------------------------------------- #
# Candidate specs
# --------------------------------------------------------------------------- #
def slot4_candidate() -> dict:
    return new_cand("FUT", "INTL_RATES", "FUT_CLASSICAL", "FUT:CELL_RV",
                    "rule:carry_slope_ann", "GROUP_RV")


def tcn_candidate() -> dict:
    return new_cand("FUT", "ALL_FUT", "SEQ_CLS", "FUT:MODEL_DEEP", "tcn_seq",
                    "XS_LONG_SHORT")


def carry_rule_candidate() -> dict:
    return {"lane": "FUT", "scope": "ALL_FUT", "target": "tgt_excess_21",
            "horizon": 21, "expression": "XS_LONG_SHORT",
            "model": "rule:carry_slope_ann", "bundle": "FUT_CLASSICAL",
            "family": "FUT:HAND_RULE", "hyper": "none",
            "candidate_id": "c39_8278ddd2d3b9"}


def _stream_stats(rep: dict) -> dict:
    s = D.stream(rep)
    if s.empty:
        return {}
    return {"sigma_per_period": float(s.std(ddof=1)),
            "mu_per_period": float(s.mean()),
            "periods": int(len(s)),
            "first": str(s.index.min().date()),
            "last": str(s.index.max().date())}


# --------------------------------------------------------------------------- #
# Frozen learned models
# --------------------------------------------------------------------------- #
def _freeze_torch_seq(d3, cand: dict, campaign_id: str) -> dict:
    """Fit the sequence net ONCE on Zone A+B and freeze its bytes."""
    p = d3._panel(cand)
    fit_rows = p[p["zone"].isin(("ZONE_A", "ZONE_B"))]
    X, y, cols = d3._matrices(cand, fit_rows)
    ok = np.isfinite(y)
    model = d3._make_model(cand)
    model.fit(X[ok], y[ok])
    torch = __import__("torch")
    buf = io.BytesIO()
    torch.save(model.net.state_dict(), buf)
    raw = buf.getvalue()
    path = models_dir(campaign_id) / ("%s.pt" % cand["candidate_id"])
    path.write_bytes(raw)
    frozen = {
        "kind": "TORCH_SEQ", "model": cand["model"],
        "features": cols, "n_feats": int(model.n_feats),
        "n_steps": int(model.n_steps),
        "impute_median": [float(v) for v in model.prep.med],
        "standardise_mu": [float(v) for v in model.prep.mu],
        "standardise_sd": [float(v) for v in model.prep.sd],
        "state_dict_path": str(path),
        "state_dict_sha256": _r39.sha_file(path),
        "fit_zones": ["ZONE_A", "ZONE_B"], "n_fit_rows": int(ok.sum()),
        "seed": 3903, "torch_version": torch.__version__,
    }
    frozen["coefficient_hash"] = _r39.sha(frozen)
    return frozen


def _freeze_ridge(d3, cand: dict) -> dict:
    p = d3._panel(cand)
    fit_rows = p[p["zone"].isin(("ZONE_A", "ZONE_B"))]
    X, y, cols = d3._matrices(cand, fit_rows)
    ok = np.isfinite(y)
    model = d3._make_model(cand)
    model.fit(X[ok], y[ok])
    inner = model._model
    frozen = {"kind": "RIDGE", "features": cols,
              "impute_median": [float(v) for v in model._med],
              "standardise_mu": [float(v) for v in model._mu],
              "standardise_sd": [float(v) for v in model._sd],
              "coef": [float(v) for v in inner.coef_],
              "intercept": float(inner.intercept_), "ridge_alpha": 10.0,
              "fit_zones": ["ZONE_A", "ZONE_B"], "n_fit_rows": int(ok.sum())}
    frozen["coefficient_hash"] = _r39.sha(frozen)
    return frozen


# --------------------------------------------------------------------------- #
# Scoring at ONE decision date (used by the research cycle)
# --------------------------------------------------------------------------- #
def _predict_frozen(frozen: dict, rows: pd.DataFrame):
    cols = frozen["features"]
    if any(c not in rows.columns for c in cols):
        return None
    X = rows[cols].to_numpy(dtype=float)
    if frozen["kind"] == "RIDGE":
        return RS.apply_frozen_wide(frozen, X)
    if frozen["kind"] == "TORCH_SEQ":
        from .model_challenge import SSMLiteSeq, PatchTSTLiteSeq
        from ..r39.models_ext import SeqNetAdapter
        import torch
        med = np.asarray(frozen["impute_median"], dtype=float)
        mu = np.asarray(frozen["standardise_mu"], dtype=float)
        sd = np.asarray(frozen["standardise_sd"], dtype=float)
        Z = (np.where(np.isfinite(X), X, med) - mu) / sd
        kind = frozen["model"]
        if kind in ("tcn_seq", "gru_seq"):
            ad = SeqNetAdapter(kind, frozen["n_feats"])
            ad.net = ad._build(torch)
            seq = Z.reshape(Z.shape[0], frozen["n_feats"], frozen["n_steps"])
            if kind == "gru_seq":
                seq = np.transpose(seq, (0, 2, 1))
        else:
            cls = SSMLiteSeq if kind == "ssm_lite_seq" else PatchTSTLiteSeq
            ad = cls(frozen["n_feats"], frozen.get("hyper") or "default")
            ad.net = ad._build(torch)
            seq = np.transpose(Z.reshape(Z.shape[0], frozen["n_feats"],
                                         frozen["n_steps"]), (0, 2, 1))
        raw = open(frozen["state_dict_path"], "rb").read()
        import hashlib
        if hashlib.sha256(raw).hexdigest() != frozen["state_dict_sha256"]:
            raise RuntimeError("frozen state_dict hash mismatch")
        ad.net.load_state_dict(torch.load(io.BytesIO(raw)))
        ad.net.eval()
        with torch.no_grad():
            return ad.net(torch.tensor(seq, dtype=torch.float32)).numpy() \
                .ravel()
    if frozen["kind"] == "TABPFN_CONTEXT":
        from . import open_models as OM
        ctx = np.load(frozen["context_path"])
        import hashlib
        if hashlib.sha256(open(frozen["context_path"], "rb").read()
                          ).hexdigest() != frozen["context_sha256"]:
            raise RuntimeError("frozen context hash mismatch")
        ad = OM.TabPFNAdapter(seed=frozen["seed"],
                              context_rows=int(ctx["X"].shape[0]))
        ad.fit(ctx["X"], ctx["y"])
        return ad.predict(X)
    return None


def _freeze_tabpfn(d3, cand: dict, campaign_id: str) -> dict:
    """The 'model' of an in-context learner is its context: freeze the
    seeded Zone A+B context rows as bytes plus the checkpoint hash."""
    from . import open_models as OM
    p = d3._panel(cand)
    fit_rows = p[p["zone"].isin(("ZONE_A", "ZONE_B"))]
    X, y, cols = d3._matrices(cand, fit_rows)
    ok = np.isfinite(y)
    ad = OM.TabPFNAdapter(seed=3903)
    ad.fit(X[ok], y[ok])
    rng = np.random.default_rng(3903)
    n = int(ok.sum())
    idx = np.arange(n)
    if n > ad.context_rows:
        idx = np.sort(rng.choice(n, ad.context_rows, replace=False))
    path = models_dir(campaign_id) / ("%s_context.npz" % cand["candidate_id"])
    np.savez(path, X=X[ok][idx], y=y[ok][idx])
    prov = _r39.read_json(campaign_dir(campaign_id) / OM.PROVENANCE_NAME) \
        or {}
    ck = ((prov.get("weights") or {}).get(
        "TABULAR_FOUNDATION::TabPFN-v2-reg") or {}).get("files") or []
    frozen = {"kind": "TABPFN_CONTEXT", "model": "tabpfn_v2",
              "features": cols, "context_path": str(path),
              "context_sha256": _r39.sha_file(path),
              "n_context_rows": int(idx.size), "seed": 3903,
              "checkpoint_sha256": ck[0]["sha256"] if ck else None,
              "fit_zones": ["ZONE_A", "ZONE_B"], "n_fit_rows": n}
    frozen["coefficient_hash"] = _r39.sha(frozen)
    return frozen


def score_at(sh: dict, rows_d: pd.DataFrame):
    """Weights dict for one shadow at one decision date, or None."""
    if rows_d.empty:
        return None
    model = sh["model"]
    if model.startswith("rule:"):
        feat = model.split(":", 1)[1]
        if feat not in rows_d.columns:
            return None
        pred = rows_d[feat].to_numpy(dtype=float)
    else:
        pred = _predict_frozen(sh["frozen_model"], rows_d)
        if pred is None:
            return None
    ids = rows_d["market_id"].tolist()
    s = pd.Series(pred, index=ids).dropna()
    expr = sh["expression"]
    if expr == "XS_LONG_SHORT":
        if len(s) < 6:
            return None
        ranks = s.rank(pct=True)
        long = ranks[ranks >= 2.0 / 3.0].index.tolist()
        short = ranks[ranks <= 1.0 / 3.0].index.tolist()
        w = {m: 1.0 / (2 * len(long)) for m in long}
        w.update({m: -1.0 / (2 * len(short)) for m in short})
        return w
    if expr == "GROUP_RV":
        if len(s) < 2:
            return None
        vol = rows_d.set_index("market_id")["vol_63"].reindex(s.index)
        pred_m = pd.DataFrame([s.to_dict()])
        fwd_m = pd.DataFrame([{m: 0.0 for m in s.index}])
        vol_m = pd.DataFrame([vol.to_dict()])
        groups = {"ALL": list(s.index)} if sh.get("hyper") == "one_group" \
            else {}
        if not groups:
            for m, g in rows_d.set_index("market_id")["economic_group"] \
                    .reindex(s.index).items():
                groups.setdefault(str(g), []).append(m)
        book = TX.vol_scaled_group_rv(pred_m, fwd_m, pd.Series(0.0,
                                                               index=s.index),
                                      groups, vol_m)
        W = book["weights"].iloc[0]
        return {m: float(v) for m, v in W.items() if v != 0.0}
    if expr == "TS_OUTRIGHT":
        return {m: float(np.sign(v)) / len(s) for m, v in s.items()}
    return None


# --------------------------------------------------------------------------- #
# Freeze
# --------------------------------------------------------------------------- #
def _r39_rows() -> list:
    reg = RS.load_registry(C.R39_CONTINUATION_CAMPAIGN_ID) or {}
    hashes = _r39.read_json(
        _r39.campaign_dir(C.R39_CONTINUATION_CAMPAIGN_ID)
        / RS.SPEC_HASHES_NAME) or {}
    rows = []
    for sh in reg.get("shadows", []):
        h = (hashes.get("hashes") or {}).get(sh["shadow_id"], {})
        rows.append({
            "shadow_id": sh["shadow_id"], "candidate_id": sh["candidate_id"],
            "origin_release": "release39", "slot": len(rows) + 1,
            "family": sh["family"], "lane": sh["lane"], "model": sh["model"],
            "expression": sh["expression"], "scope": sh["scope"],
            "cadence": sh["cadence"], "horizon_sessions":
                sh["horizon_sessions"], "control": sh["control"],
            "economic_hypothesis": sh["economic_hypothesis"],
            "frozen_at": sh["frozen_at"],
            "spec_hash": h.get("spec_hash"),
            "coefficient_hash": h.get("coefficient_hash"),
            "immutable": True,
            "ledger_root": "release39 campaign (written only by "
                           "alpha_agent.r39.research_shadow)",
            "snapshot_ledger": reg.get("snapshot_ledger"),
            "outcome_ledger": reg.get("outcome_ledger"),
            "research_shadow_only": True, "promotion_allowed": False,
            "historical_qualification": "FAIL",
        })
    return rows, reg.get("shadow_registry_hash")


def _candidate_row(d3, cand: dict, label: str) -> dict:
    rep = D.zone_b(cand, stage=STAGE, d2=d3)
    row = {"label": label, "candidate_id": cand["candidate_id"],
           "spec_hash": d3.spec_hash(cand), "spec": {
               k: cand[k] for k in ("lane", "scope", "target", "horizon",
                                    "expression", "model", "bundle",
                                    "family", "hyper")},
           "zone_b": D.summarise(rep)}
    if rep.get("state") == "OK":
        row["halves"] = D.halves_same_sign(rep)
        row["cost_2x"] = D.cost_stress(cand, d2=d3)
        row["stream_stats"] = _stream_stats(rep)
        row["_stream"] = D.stream(rep)
    return row


def resolve_slot5(d3, existing_streams: dict, branch_candidates: list,
                  branch_burden: dict) -> dict:
    """Apply the frozen Slot-5 rule. ``branch_candidates`` are (label, cand)
    pairs for options A/B/C; ``branch_burden`` = distinct R40 evaluations
    consumed by each label's branch."""
    rows = []
    for label, cand in branch_candidates:
        row = _candidate_row(d3, cand, label)
        zb = row["zone_b"]
        t = zb.get("after_cost_excess_t_stat")
        reasons = []
        if zb.get("state") != "OK" or t is None:
            reasons.append("NO_ZONE_B_RESULT")
        else:
            if t < C.SLOT_5_MIN_ZONE_B_T:
                reasons.append("ZONE_B_T_BELOW_%.1f" % C.SLOT_5_MIN_ZONE_B_T)
            if not (row.get("halves") or {}).get("halves_same_sign"):
                reasons.append("HALVES_NOT_SAME_SIGN")
            if (row.get("cost_2x") or {}).get(
                    "after_cost_excess_annualised", -1) <= 0:
                reasons.append("NEGATIVE_AT_2X_COST")
            corr = {}
            for sid, s in existing_streams.items():
                corr[sid] = D.correlation(row["_stream"], s)
            row["correlation_with_existing"] = corr
            worst = max((abs(v) for v in corr.values()
                         if v is not None and np.isfinite(v)), default=0.0)
            if worst >= C.SLOT_5_DUPLICATE_CORRELATION:
                reasons.append("NEAR_DUPLICATE_|corr|>=%.2f"
                               % C.SLOT_5_DUPLICATE_CORRELATION)
            row["partially_redundant"] = bool(
                C.SLOT_5_PARTIAL_REDUNDANCY_CORRELATION <= worst
                < C.SLOT_5_DUPLICATE_CORRELATION)
            row["max_abs_correlation_with_existing"] = worst
        row["eligible"] = not reasons
        row["ineligibility_reasons"] = reasons
        row["branch_distinct_evaluations"] = branch_burden.get(label)
        rows.append(row)
    eligible = [r for r in rows if r["eligible"]]
    winner = None
    if eligible:
        eligible.sort(key=lambda r: (
            -(r["zone_b"]["after_cost_excess_t_stat"]),
            r.get("branch_distinct_evaluations") or 0))
        top = eligible[0]
        ties = [r for r in eligible
                if abs(r["zone_b"]["after_cost_excess_t_stat"]
                       - top["zone_b"]["after_cost_excess_t_stat"]) < 0.05]
        ties.sort(key=lambda r: r.get("branch_distinct_evaluations") or 0)
        winner = ties[0]
    return {"rule": C.SLOT_5_SELECTION_RULE,
            "candidates": [{k: v for k, v in r.items() if k != "_stream"}
                           for r in rows],
            "winner": None if winner is None else {
                "label": winner["label"],
                "candidate_id": winner["candidate_id"],
                "zone_b_t": winner["zone_b"]["after_cost_excess_t_stat"],
                "partially_redundant": winner.get("partially_redundant")},
            "_winner_row": winner,
            "null_outcome": winner is None}


def freeze(d2=None, campaign_id: str = CAMPAIGN_ID,
           branch_candidates: list = None, branch_burden: dict = None) -> dict:
    """Freeze SHADOW_REGISTRY_V2. Immutable once written."""
    existing = load(campaign_id)
    if existing:
        return existing
    from .model_challenge import _upgrade
    from . import burden_ledger as BL
    d3 = _upgrade(d2 or D.session())
    r39_rows, r39_hash = _r39_rows()
    frozen_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Zone-B streams of the EXISTING members (re-scores under their ids)
    existing_streams = {}
    for label, cand in (("shadow_wide_xs", wide_candidate()),
                        ("shadow_carry_rule_xs", carry_rule_candidate())):
        rep = D.zone_b(cand, stage=STAGE, d2=d3)
        existing_streams[label] = D.stream(rep)
    # VX shadow is a different lane (weekly); its correlation is measured
    # on monthly-aggregated dates inside the research-portfolio owner.

    # ---- Slot 4 ---------------------------------------------------------- #
    s4 = _candidate_row(d3, slot4_candidate(), "SLOT4_INTL_RATES_CARRY_RV")
    assert s4["candidate_id"] == C.SLOT_4_CANDIDATE_ID, s4["candidate_id"]
    existing_streams["shadow_intl_rates_carry_rv"] = s4.get("_stream",
                                                            pd.Series())
    intl = d3.state["fut_intl_rates"]
    costs = intl.groupby("market_id")["cost_bps_per_side"].median()
    rows = list(r39_rows)
    rows.append({
        "shadow_id": "shadow_intl_rates_carry_rv",
        "candidate_id": s4["candidate_id"], "origin_release": "release40",
        "slot": 4, "family": "FUT:CELL_RV", "lane": "FUT_INTL_RATES",
        "model": "rule:carry_slope_ann (no parameters)",
        "expression": "GROUP_RV", "scope": "INTL_RATES", "hyper": "default",
        "cadence": "each market's last session per calendar month",
        "horizon_sessions": 21, "control": "RISK_MATCHED_CASH",
        "economic_hypothesis": "carry relative value across the 11 "
                               "international bond futures inside ONE "
                               "declared group, vol-scaled, self-financed",
        "markets": sorted(intl["market_id"].unique().tolist()),
        "cost_model": {"base": "TRADED_NOTIONAL",
                       "state": "MODELLED_NOT_OBSERVED",
                       "bps_per_side": {k: float(v) for k, v in
                                        costs.items()}},
        "position_sizing": "demeaned carry rank x inverse trailing vol, "
                           "re-centred, gross = 1 per live group",
        "spec_hash": s4["spec_hash"], "coefficient_hash": None,
        "frozen_at": frozen_at,
        "selection_evidence": {k: v for k, v in s4.items()
                               if k in ("zone_b", "halves", "cost_2x")},
        "zone_b_stream_stats": s4.get("stream_stats"),
        "selection_reason": C.SLOT_4_REASON,
        "first_eligible_forward_decision": "the first per-market month-end "
                                           "session strictly AFTER "
                                           "frozen_at",
        "research_shadow_only": True, "promotion_allowed": False,
        "historical_qualification": "FAIL", "immutable": True,
        "ledger_root": str(shadow_dir(campaign_id)),
    })

    # ---- Slot 5 ---------------------------------------------------------- #
    if branch_candidates is None:
        branch_candidates = default_branch_candidates(d3, campaign_id)
    if branch_burden is None:
        branch_burden = {}
    res = resolve_slot5(d3, existing_streams, branch_candidates,
                        branch_burden)
    winner = res.get("_winner_row")
    if winner is not None:
        cand = dict(winner["spec"])
        cand["candidate_id"] = winner["candidate_id"]
        frozen_model = None
        if not cand["model"].startswith("rule:"):
            if cand["model"] in ("tcn_seq", "gru_seq", "ssm_lite_seq",
                                 "patchtst_lite_seq"):
                frozen_model = _freeze_torch_seq(d3, cand, campaign_id)
                frozen_model["hyper"] = cand.get("hyper")
            elif cand["model"] == "ridge":
                frozen_model = _freeze_ridge(d3, cand)
            elif cand["model"] == "tabpfn_v2":
                frozen_model = _freeze_tabpfn(d3, cand, campaign_id)
            else:
                raise RuntimeError("no freezer for %s" % cand["model"])
        fut = d3.state["fut"]
        costs5 = fut.groupby("market_id")["cost_bps_per_side"].median()
        rows.append({
            "shadow_id": "shadow_slot5_" + winner["candidate_id"],
            "candidate_id": winner["candidate_id"],
            "origin_release": "release40", "slot": 5,
            "family": cand["family"], "lane": cand["lane"],
            "model": cand["model"], "bundle": cand["bundle"],
            "hyper": cand.get("hyper"),
            "expression": cand["expression"], "scope": cand["scope"],
            "cadence": "each market's last session per calendar month",
            "horizon_sessions": int(cand["horizon"]),
            "control": "RISK_MATCHED_CASH"
            if cand["expression"] != "TS_OUTRIGHT"
            else "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE",
            "economic_hypothesis": "Slot-5 winner under the frozen rule: "
                                   "%s" % winner["label"],
            "cost_model": {"base": "TRADED_NOTIONAL",
                           "state": "MODELLED_NOT_OBSERVED",
                           "bps_per_side": {k: float(v) for k, v in
                                            costs5.items()}},
            "position_sizing": "equal weight within expression legs; "
                               "gross exposure <= 1; no leverage",
            "spec_hash": winner["spec_hash"],
            "frozen_model": frozen_model,
            "coefficient_hash": (frozen_model or {}).get("coefficient_hash"),
            "frozen_at": frozen_at,
            "selection_evidence": {k: v for k, v in winner.items()
                                   if k in ("zone_b", "halves", "cost_2x",
                                            "correlation_with_existing",
                                            "partially_redundant")},
            "zone_b_stream_stats": winner.get("stream_stats"),
            "distinctness": {
                "max_abs_correlation_with_existing":
                    winner.get("max_abs_correlation_with_existing"),
                "partially_redundant": winner.get("partially_redundant"),
                "family_expression_unique": True},
            "first_eligible_forward_decision": "the first per-market "
                                               "month-end session strictly "
                                               "AFTER frozen_at",
            "research_shadow_only": True, "promotion_allowed": False,
            "historical_qualification": "FAIL", "immutable": True,
            "ledger_root": str(shadow_dir(campaign_id)),
        })
    enforce_cap(rows)

    body = artifact_body("r40_shadow_registry_v2/1", {
        "calculation_owner": CALCULATION_OWNER,
        "frozen_at": frozen_at,
        "family_cap": C.MAX_RESEARCH_SHADOW_FAMILY,
        "r39_registry_hash": r39_hash,
        "r39_shadows_remain_immutable": C.R39_SHADOWS_REMAIN_IMMUTABLE,
        "shadows": rows, "n_shadows": len(rows),
        "slot_4": {"candidate_id": C.SLOT_4_CANDIDATE_ID,
                   "reason": C.SLOT_4_REASON},
        "slot_5_resolution": {k: v for k, v in res.items()
                              if k != "_winner_row"},
        "ledger_primitives": "api.paper_trading_desk chain-hash ledgers "
                             "(canonical) under the R40 research root for "
                             "R40 members; R39 members keep their R39 "
                             "ledgers",
        "snapshot_ledger": str(shadow_dir(campaign_id) / SNAPSHOT_LEDGER),
        "outcome_ledger": str(shadow_dir(campaign_id) / OUTCOME_LEDGER),
        "historical_observations_can_never_enter": True,
        "zone_c_read_for_selection": False,
        "true_forward_read_for_selection": False,
        "burden_at_freeze": BL.summary(campaign_id),
    })
    body["shadow_registry_v2_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / REGISTRY_NAME, body)
    hashes = artifact_body("r40_shadow_specification_hashes/1", {
        "calculation_owner": CALCULATION_OWNER, "frozen_at": frozen_at,
        "hashes": {r["shadow_id"]: {"candidate_id": r["candidate_id"],
                                    "spec_hash": r.get("spec_hash"),
                                    "coefficient_hash":
                                        r.get("coefficient_hash"),
                                    "origin_release": r["origin_release"],
                                    "frozen_at": r["frozen_at"]}
                   for r in rows},
        "registry_hash": body["shadow_registry_v2_hash"]})
    hashes["shadow_specification_hashes_hash"] = _r39.sha(hashes)
    _r39.write_json(campaign_dir(campaign_id) / HASHES_NAME, hashes)
    return body


def default_branch_candidates(d3, campaign_id: str) -> list:
    """Options A/B/C read from the R40 artifacts (Zone-B evidence only)."""
    from . import model_challenge as MC
    from . import wide_successor as WS
    out = [("A_R39_TCN", tcn_candidate())]
    succ = _r39.read_json(campaign_dir(campaign_id) / WS.ARTIFACT_NAME) or {}
    best = succ.get("best_successor")
    if best:
        row = succ["results"][best["bundle"]]
        if best["bundle"] not in d3.bundles:
            d3.bundles[best["bundle"]] = list(row["features"])
        out.append(("B_CORRECTED_WIDE_SUCCESSOR",
                    new_cand("FUT", "ALL_FUT", best["bundle"],
                             "FUT:WIDE_SUCCESSOR", "ridge", "XS_LONG_SHORT")))
    models = _r39.read_json(campaign_dir(campaign_id) / MC.ARTIFACT_NAME) or {}
    table = [r for r in (models.get("comparison") or [])
             if not r["key"].startswith("baseline_")
             and r.get("evidence_role") != "REPRESENTATION_RESEARCH"
             and r.get("zone_b_t") is not None]
    if table:
        best_key = table[0]["key"]
        r = models["results"][best_key]
        spec_model, bundle, expr, hyper = r["model"], r["bundle"], \
            r["expression"], r.get("hyper") or "default"
        if bundle == "GRAPH_AGG" and bundle not in d3.bundles:
            from ..r39.representation_factory import CLASSICAL_FUT
            fut2, _n, _i = MC.add_graph_aggregates(d3.state["fut"])
            d3.state["fut"] = fut2
            d3.bundles["GRAPH_AGG"] = list(CLASSICAL_FUT) + \
                list(MC.NBR_FEATURES)
        if bundle == "CLS_ADMISSIBLE" and bundle not in d3.bundles:
            from ..r39.representation_factory import CLASSICAL_FUT
            d3.bundles["CLS_ADMISSIBLE"] = [c for c in CLASSICAL_FUT
                                            if c != "cot_commercial_z"]
        out.append(("C_R40_BRANCH_" + best_key,
                    new_cand("FUT", "ALL_FUT", bundle, "FUT:MODEL_R40",
                             spec_model, expr, hyper=hyper)))
    return out
