"""alpha_agent.r46.sec - the ONE seam the Release-46 lanes use to reach EDGAR.

The SEC serves its archives free of charge under a fair-access policy: a
declared User-Agent carrying a contact, and no more than ten requests a
second. Both are enforced here rather than remembered per lane. The contact
follows the estate's existing convention (``alpha_agent.collectors.sec_edgar``):
the operator's ``git config user.email``, never a literal in source, never
written into an artifact unmasked.

Two callers: the earnings lane (``data.sec.gov/submissions`` for 8-K Item
2.02 acceptance instants) and the Form-4 lane (the daily form index and the
full submission text of each Form 4). Both capture raw bytes with the
acquisition instant and sha256 and never overwrite a capture.

Spends nothing. Creates no account. Accepts no licence.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import time
import urllib.request
from typing import Optional

CALCULATION_OWNER = "alpha_agent.r46.sec"

PRODUCT = "paper-trader-research/46.5"
#: At most ten requests per second under the SEC fair-access policy; the
#: seam paces itself below that whatever the caller does.
REQUEST_INTERVAL_SECONDS = 0.12
HTTP_TIMEOUT = 60
CONTACT_ENV = ("PAPER_TRADER_SEC_CONTACT", "SEC_EDGAR_CONTACT")
BLOCKED_NO_CONTACT = "BLOCKED_MISSING_USER_AGENT_CONTACT"

_last_request_at = [0.0]


def contact() -> Optional[str]:
    for n in CONTACT_ENV:
        v = os.environ.get(n)
        if v and "@" in v:
            return v.strip()
    try:
        out = subprocess.run(["git", "config", "user.email"],
                             capture_output=True, text=True, timeout=10)
        v = (out.stdout or "").strip()
        if "@" in v:
            return v
    except Exception:                           # noqa: BLE001 - reported by caller
        pass
    return None


def mask(email: Optional[str]) -> Optional[str]:
    if not email or "@" not in email:
        return None
    local, dom = email.split("@", 1)
    return "%s***@%s" % (local[:1], dom)


def user_agent() -> Optional[str]:
    c = contact()
    return ("%s %s" % (PRODUCT, c)) if c else None


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def get(url: str, *, ua: str = None, timeout: int = HTTP_TIMEOUT) -> dict:
    """Paced GET. Returns {status, body, error}; never raises."""
    agent = ua or user_agent()
    if not agent:
        return {"status": None, "body": None, "error": BLOCKED_NO_CONTACT}
    wait = REQUEST_INTERVAL_SECONDS - (time.monotonic() - _last_request_at[0])
    if wait > 0:
        time.sleep(wait)
    _last_request_at[0] = time.monotonic()
    req = urllib.request.Request(url, headers={
        "User-Agent": agent, "Accept-Encoding": "gzip, deflate",
        "Host": url.split("/")[2]})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as fh:
            body = fh.read()
            enc = (fh.headers.get("Content-Encoding") or "").lower()
            if "gzip" in enc:
                import gzip
                body = gzip.decompress(body)
            return {"status": fh.status, "body": body, "error": None}
    except urllib.error.HTTPError as exc:       # type: ignore[attr-defined]
        return {"status": exc.code, "body": None,
                "error": "HTTPError %s" % exc.code}
    except Exception as exc:                    # noqa: BLE001 - reported
        return {"status": None, "body": None,
                "error": "%s: %s" % (type(exc).__name__, str(exc)[:120])}


__all__ = ["CALCULATION_OWNER", "PRODUCT", "REQUEST_INTERVAL_SECONDS",
           "BLOCKED_NO_CONTACT", "contact", "mask", "user_agent", "sha256",
           "get"]
