"""alpha_agent.r31 - Release 31, the Mathematical Alpha Frontier campaign.

RESEARCH ONLY. This package answers one question:

    "What is the best mathematically defensible decision function from the
     information we currently own to capital allocation?"

It creates NO signal authority, NO portfolio target, NO proposal, NO decision,
NO order and NO model promotion. It reads OWNED data and writes immutable
research artifacts under its own research root. Nothing in this package imports
``paper_trader.api``; the only operational modules it may read are the pure
stdlib canonical owners of COST, RISK PRICES and PORTFOLIO CONSTRAINTS
(``engine.zero_base_allocator``), because the campaign is required to judge every
candidate on the SAME canonical economics the operator would face.

Ownership map - each concern has exactly one owner in this package:

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            :mod:`alpha_agent.r31.contract`
data snapshot + frozen panel :mod:`alpha_agent.r31.snapshot`
evidence partition           :mod:`alpha_agent.r31.partition`
research judge               :mod:`alpha_agent.r31.judge`
learners (mathematics)       :mod:`alpha_agent.r31.learners`
candidate registry + budgets :mod:`alpha_agent.r31.registry`
known-method families        :mod:`alpha_agent.r31.methods`
bounded novel discovery      :mod:`alpha_agent.r31.novel`
multiple-testing control     :mod:`alpha_agent.r31.multiple_testing`
lockbox access               :mod:`alpha_agent.r31.lockbox`
orchestration + verdict      :mod:`alpha_agent.r31.campaign`
===========================  =================================================

There is deliberately no second portfolio optimiser, no second HOC engine, no
second cost model and no second forward-evidence system here. Where the campaign
needs one of those it reads the canonical owner.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

RELEASE = "release31"
CAMPAIGN_FAMILY = "mathematical_alpha_frontier"

#: The isolated research root. NO operational store is ever written by this
#: package; ``tests/test_release31_mathematical_alpha_frontier.py`` asserts that
#: every path this package writes is under this root.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R31_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\mathematical_alpha_frontier")

#: Safety badges every artifact carries.
SAFETY = ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY", "NO ORDERS",
          "NO LIVE ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
          "NO OPERATIONAL WRITE", "NO MODEL PROMOTION"]

#: Declared once, here, so no module can quietly widen it.
AUTOMATIC_PROMOTION_ALLOWED = False


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str) -> Path:
    return research_root() / str(campaign_id)


# --------------------------------------------------------------------------- #
# Hashing / fingerprints
# --------------------------------------------------------------------------- #
def sha(obj: Any) -> str:
    """Stable SHA-256 of any JSON-serialisable object."""
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def sha_file(path) -> Optional[str]:
    """SHA-256 of a file's bytes, streamed. ``None`` when the file is absent.

    The campaign binds its inputs by CONTENT, not by mtime: a rebuilt panel with
    the same bytes is the same snapshot, and a silently edited one is not.
    """
    p = Path(path)
    try:
        h = hashlib.sha256()
        with open(p, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def file_fingerprint(path, *, content_hash: bool = True) -> dict:
    """Path, size, mtime and (by default) the content hash of one input file."""
    p = Path(path)
    try:
        st = p.stat()
    except OSError:
        return {"path": str(p), "exists": False}
    out = {"path": str(p), "exists": True, "size_bytes": int(st.st_size),
           "mtime": int(st.st_mtime)}
    if content_hash:
        out["sha256"] = sha_file(p)
    return out


# --------------------------------------------------------------------------- #
# Immutable artifact writes
# --------------------------------------------------------------------------- #
class ArtifactImmutable(RuntimeError):
    """Raised when a campaign artifact would be overwritten with new content."""


def write_json(path, payload: Any, *, immutable: bool = True) -> Path:
    """Write one campaign artifact atomically.

    ``immutable`` is the campaign's central discipline: an artifact that already
    exists with DIFFERENT content is never silently replaced, because a campaign
    that can rewrite its own evidence after seeing a result is not evidence. Byte
    identical rewrites are permitted so a resumed run is idempotent.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, indent=2, sort_keys=True, default=str)
    if immutable and p.exists():
        try:
            existing = p.read_text(encoding="utf-8")
        except OSError:
            existing = None
        if existing is not None and existing != body:
            raise ArtifactImmutable(
                "refusing to overwrite immutable campaign artifact %s; a new "
                "result requires a new campaign id" % (p,))
        return p
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(body)
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return p


def read_json(path) -> Optional[Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def safety_block() -> dict:
    return {"safety": list(SAFETY),
            "automatic_promotion_allowed": AUTOMATIC_PROMOTION_ALLOWED,
            "creates_signal_authority": False,
            "creates_portfolio_target": False,
            "creates_proposal": False,
            "creates_decision": False,
            "creates_order": False,
            "writes_operational_store": False}
