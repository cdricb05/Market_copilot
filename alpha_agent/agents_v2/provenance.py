r"""alpha_agent.agents_v2.provenance - what a measurement is BOUND to (post-R99 repair).

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION.

R99 exposed three ways a measured number could drift from what its pre-registration
named. Each has ONE owner here, and the canonical runner (``agents_v2.runner``)
enforces all three BEFORE the first layer is read:

1. FROZEN SOURCE (R99 PF4). R99 recorded three different hashes of ``r99_cells.py``
   across waves and none equals the final file; R15's frozen event-rules file
   drifted the same way. A hash that is recorded but never checked binds nothing,
   and a file that is edited in place cannot be reconstructed. A pre-registration
   may now carry ``parameters.frozen_source``: a manifest of
   ``{repo-relative path: sha256}`` whose bytes were copied into a write-once,
   CONTENT-ADDRESSED store (``<store>/<sha256>``). The manifest sits inside the
   frozen spec, so it is covered by the spec hash and immutable in research
   memory. Before measurement every live file must hash to its manifest entry
   and every blob must be present in the store; any mismatch FAILS CLOSED. The
   exact source is reconstructable with :func:`materialize`.

2. ONE HORIZON, ONE CADENCE (R99 PF2). R99's cell table exposed ``h=21`` while an
   import-time override executed ``h=1``. The executed values are what the book
   reads from ``plan["book_kwargs"]`` (else the book's own defaults); they must
   equal the pre-registration's ``horizon_sessions`` / ``cadence_sessions``.
   A divergence - or a frozen spec that states neither - is refused for the
   dated-contract and equity books. The event book keeps its own R91 check.

3. ADMISSION (R99 C04). C04 was measured at 15:46:12Z under the W1 admission;
   the W1A amendment withdrawing it was written at 15:46:34Z. The latest
   authoritative director ruling must be ``G7 = PASS`` immediately before the
   first layer is read, and a withdrawal that lands AFTER measurement is reported
   as a GOVERNANCE EXCEPTION, never silently counted as a clean admissible
   experiment.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Iterable, Optional

#: R100 repair - the provenance contract a new campaign MUST declare. Under it every
#: pre-registration carries a frozen source snapshot AND a frozen recipe, and the
#: runner refuses any measurement whose executed recipe differs from it.
PROVENANCE_CONTRACT_FULL = "FULL_RECIPE_V1"
RECIPE_KEY = "recipe"
#: The eight parts of a recipe. Nothing else about an experiment may vary after
#: pre-registration without a new pre-registration.
RECIPE_KEYS = ("data", "signal", "instruments", "direction", "holding_period",
               "rebalance", "costs", "code_snapshot")
#: Canonical measurement code every experiment depends on. Snapshotted with the
#: campaign's own files, so an edit to a book after pre-registration refuses.
CANONICAL_SOURCE = ("alpha_agent/agents_v2/runner.py", "alpha_agent/agents_v2/books.py",
                    "alpha_agent/agents_v2/event_book.py", "alpha_agent/agents_v2/provenance.py",
                    "alpha_agent/r59/native.py", "alpha_agent/r59/engines.py",
                    "alpha_agent/r57/engine.py", "research/agents/campaign_r56_v2/evaluator.py")

#: Session year used by every canonical book (alpha_agent.r59.native._layer_stats,
#: research.agents.campaign_r56_v2.evaluator.run_futures_book): ppy = 252 / horizon.
SESSIONS_PER_YEAR = 252.0

FROZEN_SOURCE_KEY = "frozen_source"
ADMISSION_PASS = "PASS"
EXC_POST_MEASUREMENT_WITHDRAWAL = "GOVERNANCE_EXCEPTION_POST_MEASUREMENT_WITHDRAWAL"
EXC_MEASURED_WITHOUT_ADMISSION = "GOVERNANCE_EXCEPTION_MEASURED_WITHOUT_ADMISSION"


class ProvenanceRefusal(RuntimeError):
    """A measurement is not bound to what its pre-registration named."""

    def __init__(self, code: str, message: str):
        super().__init__("%s: %s" % (code, message))
        self.code = code


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _rel(path: Path, repo_root: Path) -> str:
    return Path(path).resolve().relative_to(Path(repo_root).resolve()).as_posix()


# --------------------------------------------------------------------------- #
# 1. Frozen, content-addressed source                                          #
# --------------------------------------------------------------------------- #
def snapshot(paths: Iterable, *, repo_root, store_dir) -> dict:
    """Copy each file into the write-once content store and return the manifest
    to embed as ``parameters.frozen_source`` in the pre-registration.

    Idempotent: a blob that already exists is re-hashed and must match (a store
    that was tampered with refuses rather than silently re-binding)."""
    store = Path(store_dir)
    store.mkdir(parents=True, exist_ok=True)
    files = {}
    for p in sorted(Path(x).resolve() for x in paths):
        data = p.read_bytes()
        h = _sha256(data)
        blob = store / h
        if blob.exists():
            if _sha256(blob.read_bytes()) != h:
                raise ProvenanceRefusal("FROZEN_SOURCE_STORE_CORRUPT",
                                        "blob %s does not hash to its name" % h)
        else:
            tmp = store / (".%s.tmp" % h)
            tmp.write_bytes(data)
            os.replace(tmp, blob)
        files[_rel(p, repo_root)] = h
    body = {"files": files, "algorithm": "sha256", "store": "content-addressed <store>/<sha256>"}
    body["manifest_sha256"] = _sha256(json.dumps(files, sort_keys=True).encode("utf-8"))
    return body


def manifest_of(frozen_spec: Optional[dict]) -> Optional[dict]:
    params = (frozen_spec or {}).get("parameters") or {}
    m = params.get(FROZEN_SOURCE_KEY)
    return m if isinstance(m, dict) else None


def verify(manifest: dict, *, repo_root, store_dir=None) -> list:
    """Every problem that unbinds a measurement from its frozen source; ``[]``
    means bound. Checks the manifest's own hash, every live file, and (when a
    store is given) that every blob is present and intact."""
    problems = []
    files = (manifest or {}).get("files")
    if not isinstance(files, dict) or not files:
        return ["frozen_source manifest names no files"]
    want = _sha256(json.dumps(files, sort_keys=True).encode("utf-8"))
    if manifest.get("manifest_sha256") != want:
        problems.append("manifest_sha256 does not match its file list")
    root = Path(repo_root)
    for rel, h in sorted(files.items()):
        live = root / rel
        if not live.exists():
            problems.append("%s: missing" % rel)
        elif _sha256(live.read_bytes()) != h:
            problems.append("%s: live bytes %s != frozen %s" % (rel, _sha256(live.read_bytes())[:12], h[:12]))
        if store_dir is not None:
            blob = Path(store_dir) / h
            if not blob.exists():
                problems.append("%s: blob %s absent from the store (not reconstructable)" % (rel, h[:12]))
            elif _sha256(blob.read_bytes()) != h:
                problems.append("%s: blob %s corrupt" % (rel, h[:12]))
    return problems


def materialize(manifest: dict, *, store_dir, dest_dir) -> Path:
    """Reconstruct the exact frozen source tree under ``dest_dir``."""
    dest = Path(dest_dir)
    for rel, h in sorted(manifest["files"].items()):
        blob = Path(store_dir) / h
        if not blob.exists() or _sha256(blob.read_bytes()) != h:
            raise ProvenanceRefusal("FROZEN_SOURCE_NOT_RECONSTRUCTABLE", "%s: blob %s" % (rel, h[:12]))
        out = dest / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(blob, out)
    return dest


def require_frozen_source(frozen_spec: dict, *, required: bool, repo_root, store_dir) -> Optional[dict]:
    """The runner's door. Returns the verified manifest (or None when the
    campaign does not require one and none was frozen); raises on any drift."""
    m = manifest_of(frozen_spec)
    if m is None:
        if required:
            raise ProvenanceRefusal("FROZEN_SOURCE_NOT_PREREGISTERED",
                                    "the campaign requires parameters.frozen_source and the "
                                    "pre-registration carries none")
        return None
    if store_dir is None:
        raise ProvenanceRefusal("FROZEN_SOURCE_STORE_UNDECLARED",
                                "a frozen_source manifest is verified against a declared store")
    problems = verify(m, repo_root=repo_root, store_dir=store_dir)
    if problems:
        raise ProvenanceRefusal("FROZEN_SOURCE_MISMATCH", "; ".join(problems))
    return m


# --------------------------------------------------------------------------- #
# 2. One authoritative horizon and cadence                                     #
# --------------------------------------------------------------------------- #
def executed_horizon_cadence(plan: dict, *, default_horizon: int, default_cadence: int) -> tuple:
    """What the book will ACTUALLY run: the plan's book kwargs, else the book's
    own defaults (``books.run_stage``)."""
    kw = plan.get("book_kwargs") or {}
    return int(kw.get("horizon", default_horizon)), int(kw.get("cadence", default_cadence))


def frozen_horizon_cadence(frozen_spec: dict) -> tuple:
    f = frozen_spec or {}
    params = f.get("parameters") or {}
    h = f.get("horizon_sessions", params.get("horizon_sessions"))
    c = f.get("cadence_sessions", params.get("cadence_sessions"))
    return (None if h is None else int(h)), (None if c is None else int(c))


def check_horizon_cadence(frozen_spec: dict, plan: dict, *, default_horizon: int,
                          default_cadence: int) -> dict:
    """Refuse unless the executed horizon/cadence equal the frozen ones. Returns
    the single authoritative pair that every downstream artifact must carry."""
    eh, ec = executed_horizon_cadence(plan, default_horizon=default_horizon, default_cadence=default_cadence)
    fh, fc = frozen_horizon_cadence(frozen_spec)
    problems = []
    params = (frozen_spec or {}).get("parameters") or {}
    for key in ("horizon_sessions", "cadence_sessions"):
        top, inner = (frozen_spec or {}).get(key), params.get(key)
        if top is not None and inner is not None and int(top) != int(inner):
            problems.append("the pre-registration states %s twice (%s vs parameters %s)" % (key, top, inner))
    if fh is None:
        problems.append("the pre-registration states no horizon_sessions")
    elif fh != eh:
        problems.append("frozen horizon_sessions %d != executed horizon %d" % (fh, eh))
    if fc is not None and fc != ec:
        problems.append("frozen cadence_sessions %d != executed cadence %d" % (fc, ec))
    if problems:
        raise ProvenanceRefusal("HORIZON_NOT_PREREGISTERED", "; ".join(problems))
    return {"horizon_sessions": eh, "cadence_sessions": ec}


# --------------------------------------------------------------------------- #
# 3. Director admission (G7)                                                   #
# --------------------------------------------------------------------------- #
def _g7_of(entry) -> Optional[str]:
    if not isinstance(entry, dict):
        return None
    g7 = entry.get("G7")
    if isinstance(g7, dict):
        g7 = g7.get("verdict")
    return None if g7 is None else str(g7).upper()


def admission_history(cell_id: str, ruling_files: Iterable) -> list:
    """Every ruling that names ``cell_id``, in the ORDER the campaign declares
    its ruling files (the declaration is the authority on precedence, never
    file mtimes). Each item: ``{ruling_file, g7, mtime_utc}``."""
    out = []
    for f in ruling_files:
        p = Path(f)
        doc = json.loads(p.read_text(encoding="utf-8-sig"))
        entry = (doc.get("rulings") or {}).get(cell_id)
        if entry is None:
            continue
        out.append({"ruling_file": p.name, "g7": _g7_of(entry),
                    "mtime_epoch": p.stat().st_mtime})
    return out


def latest_admission(cell_id: str, ruling_files: Iterable) -> Optional[dict]:
    hist = admission_history(cell_id, ruling_files)
    return hist[-1] if hist else None


def require_admission(cell_id: Optional[str], ruling_files: Optional[Iterable]) -> Optional[dict]:
    """The runner's door: the latest authoritative ruling naming the cell must be
    G7 PASS. ``ruling_files=None`` means the campaign declares no admission
    ledger (pre-R100 campaigns); an empty declaration is a ledger with nothing
    admitted and refuses."""
    if ruling_files is None:
        return None
    if not cell_id:
        raise ProvenanceRefusal("ADMISSION_UNRESOLVABLE", "the experiment row names no cell_id")
    latest = latest_admission(cell_id, list(ruling_files))
    if latest is None:
        raise ProvenanceRefusal("ADMISSION_NOT_G7_PASS", "no director ruling names %s" % cell_id)
    if latest["g7"] != ADMISSION_PASS:
        raise ProvenanceRefusal("ADMISSION_NOT_G7_PASS", "latest ruling %s rules %s G7 %s"
                                % (latest["ruling_file"], cell_id, latest["g7"]))
    return latest


def admission_exceptions(measured_cells: Iterable, ruling_files: Iterable) -> list:
    """Measured cells whose CURRENT latest ruling is not G7 PASS. A withdrawal
    after measurement stays visible here; such a cell is not a clean admissible
    experiment, whatever its counters say."""
    files = list(ruling_files)
    out = []
    for cell in measured_cells:
        hist = admission_history(cell, files)
        latest = hist[-1] if hist else None
        if latest is None:
            out.append({"cell": cell, "exception": EXC_MEASURED_WITHOUT_ADMISSION, "history": hist})
        elif latest["g7"] != ADMISSION_PASS:
            out.append({"cell": cell, "exception": EXC_POST_MEASUREMENT_WITHDRAWAL,
                        "latest_ruling": latest["ruling_file"], "latest_g7": latest["g7"],
                        "history": [{"ruling_file": h["ruling_file"], "g7": h["g7"]} for h in hist]})
    return out


# --------------------------------------------------------------------------- #
# 4. The complete frozen recipe (R100 repair)                                  #
# --------------------------------------------------------------------------- #
def _feed(h, key: str, v, skipped: list) -> None:
    """Feed one panel/kwargs value into a running hash, exactly and
    deterministically (a numpy ``repr`` truncates, so arrays hash their bytes)."""
    import numpy as np
    h.update(("\x00%s\x00" % key).encode("utf-8"))
    if isinstance(v, np.ndarray):
        a = np.ascontiguousarray(v)
        h.update(("%s|%s|" % (a.dtype, a.shape)).encode("utf-8"))
        if a.dtype == object:
            h.update(json.dumps([str(x) for x in a.ravel().tolist()]).encode("utf-8"))
        else:
            h.update(a.tobytes())
    elif isinstance(v, np.generic):
        h.update(repr(v.item()).encode("utf-8"))
    elif isinstance(v, dict):
        for k in sorted(v, key=str):
            _feed(h, "%s.%s" % (key, k), v[k], skipped)
    elif isinstance(v, (list, tuple)):
        if all(isinstance(x, (str, int, float, bool)) or x is None for x in v):
            h.update(json.dumps(list(v)).encode("utf-8"))
        else:
            for i, x in enumerate(v):
                _feed(h, "%s[%d]" % (key, i), x, skipped)
    elif isinstance(v, (str, int, float, bool)) or v is None:
        h.update(json.dumps(v).encode("utf-8"))
    else:
        skipped.append(key)


def fingerprint(obj: dict) -> dict:
    """``{sha256, keys, unhashed_keys}`` of a panel or kwargs mapping. A callable
    or opaque object is listed as unhashed rather than silently ignored."""
    h = hashlib.sha256()
    skipped: list = []
    for k in sorted(obj or {}, key=str):
        _feed(h, str(k), obj[k], skipped)
    return {"sha256": h.hexdigest(), "keys": sorted(str(k) for k in (obj or {})),
            "unhashed_keys": sorted(skipped)}


_TIMING_KWARGS = ("horizon", "cadence", "hold", "event_rule")
_COST_KWARGS = ("cost_rate", "charge_rolls", "cost_mult")


def executed_recipe(plan: dict, row: dict, frozen_spec: dict, manifest: Optional[dict], *,
                    default_horizon: int, default_cadence: int, repo_root=None) -> dict:
    """The eight-part recipe this ``plan`` WILL execute, read from the objects the
    book is handed - never from what anyone says about them."""
    kw = dict(plan.get("book_kwargs") or {})
    panel = plan.get("panel") or {}
    book = plan.get("book")
    files = {}
    for f in plan.get("data_files") or []:
        p = Path(f) if Path(f).is_absolute() or repo_root is None else Path(repo_root) / f
        files[Path(f).as_posix()] = _sha256(p.read_bytes()) if p.exists() else None
    pfp = fingerprint(panel)
    syms = None
    for k in ("symbols", "markets", "tickers"):
        if panel.get(k) is not None:
            syms = [str(x) for x in list(panel[k])]
            break
    if book in ("FUTURES_DATED_CONTRACT", "EQUITY_TOPN"):
        holding = int(kw.get("horizon", default_horizon))
        rebalance = {"cadence_sessions": int(kw.get("cadence", default_cadence))}
    elif book == "FUTURES_EVENT_WINDOW":
        holding = int(kw.get("hold") or 1)
        rebalance = {"event_rule": fingerprint({"event_rule": kw.get("event_rule")})["sha256"]}
    else:
        holding = int(kw.get("hold") or 5)
        rebalance = {"daily_tranche": True}
    cps = panel.get("cost_per_side")
    costs = {"cost_model": (frozen_spec or {}).get("cost_model"),
             "cost_rate": kw.get("cost_rate"),
             "charge_rolls": bool(kw.get("charge_rolls", True)),
             "cost_mult": float(kw.get("cost_mult", 1.0)),
             "cost_per_side_sha256": (None if cps is None else fingerprint({"c": cps})["sha256"])}
    construction = {k: v for k, v in kw.items() if k not in _TIMING_KWARGS + _COST_KWARGS}
    return {
        "data": {"dataset_id": (frozen_spec or {}).get("dataset_id"),
                 "panel_sha256": pfp["sha256"], "panel_keys": pfp["keys"],
                 "panel_unhashed_keys": pfp["unhashed_keys"], "files": files},
        "signal": {"executor": row.get("executor"), "book": book,
                   "construction_sha256": fingerprint(construction)["sha256"],
                   "construction_keys": sorted(construction)},
        "instruments": syms,
        "direction": int(plan.get("signal_sign", (frozen_spec or {}).get("expected_sign", 1))),
        "holding_period": holding,
        "rebalance": rebalance,
        "costs": costs,
        "code_snapshot": (manifest or {}).get("manifest_sha256"),
    }


def recipe_sha256(recipe: dict) -> str:
    return _sha256(json.dumps({k: recipe.get(k) for k in RECIPE_KEYS}, sort_keys=True,
                              default=str).encode("utf-8"))


def recipe_of(frozen_spec: Optional[dict]) -> Optional[dict]:
    r = ((frozen_spec or {}).get("parameters") or {}).get(RECIPE_KEY)
    return r if isinstance(r, dict) else None


def recipe_problems(recipe: Optional[dict], frozen_spec: dict) -> list:
    """Internal consistency of a frozen recipe with the spec that carries it.
    Checked at pre-registration AND again before measurement."""
    if not isinstance(recipe, dict):
        return ["the pre-registration carries no parameters.recipe"]
    out = ["recipe is missing %s" % k for k in RECIPE_KEYS if k not in recipe]
    if out:
        return out
    if recipe.get("recipe_sha256") != recipe_sha256(recipe):
        out.append("recipe_sha256 does not match its contents")
    sign = (frozen_spec or {}).get("expected_sign")
    if sign is not None and int(recipe["direction"]) != int(sign):
        out.append("recipe direction %s != expected_sign %s" % (recipe["direction"], sign))
    hz = (frozen_spec or {}).get("horizon_sessions")
    if hz is not None and int(recipe["holding_period"]) != int(hz):
        out.append("recipe holding_period %s != horizon_sessions %s" % (recipe["holding_period"], hz))
    m = manifest_of(frozen_spec)
    if m is None:
        out.append("the pre-registration carries no parameters.frozen_source")
    elif recipe.get("code_snapshot") != m.get("manifest_sha256"):
        out.append("recipe code_snapshot != frozen_source manifest_sha256")
    elif not set(CANONICAL_SOURCE) <= set((m.get("files") or {})):
        out.append("frozen_source omits canonical measurement code: %s"
                   % ", ".join(sorted(set(CANONICAL_SOURCE) - set(m.get("files") or {}))))
    return out


def require_recipe(frozen_spec: dict, executed: dict) -> dict:
    """The runner's door under FULL_RECIPE_V1: the frozen recipe must be complete
    and consistent, and every one of its eight parts must equal what is about to
    run. Raises BEFORE any layer is read."""
    frozen = recipe_of(frozen_spec)
    problems = recipe_problems(frozen, frozen_spec)
    if problems:
        raise ProvenanceRefusal("RECIPE_NOT_PREREGISTERED", "; ".join(problems))
    diff = []
    for k in RECIPE_KEYS:
        a = json.dumps(frozen.get(k), sort_keys=True, default=str)
        b = json.dumps(executed.get(k), sort_keys=True, default=str)
        if a != b:
            diff.append(k)
    if executed["costs"].get("cost_mult") != 1.0:
        diff.append("costs.cost_mult")
    if diff:
        raise ProvenanceRefusal("RECIPE_MISMATCH",
                                "executed recipe differs from the frozen one in: %s"
                                % ", ".join(sorted(set(diff))))
    return {"recipe_sha256": frozen["recipe_sha256"], "matched": list(RECIPE_KEYS)}
