#!/usr/bin/env python
"""Alpha Agent Stage 4 — standalone Gmail OAuth token-exchange DIAGNOSTIC.

READ-ONLY. This probe performs ONE OAuth token-exchange (grant_type=
refresh_token) to classify the current credential state. It NEVER sends an
email, NEVER opens a browser / authorizes, NEVER writes a file, and NEVER prints
the refresh token, an access token, the client secret or any DPAPI plaintext.

The DPAPI refresh token is decrypted by the PowerShell wrapper
(diagnose_alpha_agent_gmail.ps1) and handed to this process on STDIN only — never
on the command line, in the environment, or in a file (identical to the real
sender's contract).

It emits exactly ONE JSON line, e.g.

    {"classification":"TOKEN_EXCHANGE_INVALID_GRANT","google_error":"invalid_grant",
     "google_error_description":"Token has been expired or revoked.","http_status":400}

Distinguishable classifications (a future failure is never a mystery):
    TOKEN_FILE_NOT_FOUND               no refresh token was available on stdin
    TOKEN_EXCHANGE_OK                  the refresh token minted an access token
    TOKEN_EXCHANGE_INVALID_GRANT       refresh token expired / revoked
    TOKEN_EXCHANGE_CLIENT_MISMATCH     OAuth client id/secret rejected (invalid_client)
    TOKEN_EXCHANGE_ACCOUNT_MISMATCH    minted token is for a different Google account
    TOKEN_EXCHANGE_POLICY_REJECTION    another OAuth policy/consent rejection (4xx)
    TOKEN_EXCHANGE_UNREACHABLE         could not reach the Google token endpoint

Standard library only. Exit code 0 on TOKEN_EXCHANGE_OK, 1 otherwise.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

_DEFAULT_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
_TOKENINFO_ENDPOINT = "https://oauth2.googleapis.com/tokeninfo"

TOKEN_FILE_NOT_FOUND = "TOKEN_FILE_NOT_FOUND"
TOKEN_EXCHANGE_OK = "TOKEN_EXCHANGE_OK"
TOKEN_EXCHANGE_INVALID_GRANT = "TOKEN_EXCHANGE_INVALID_GRANT"
TOKEN_EXCHANGE_CLIENT_MISMATCH = "TOKEN_EXCHANGE_CLIENT_MISMATCH"
TOKEN_EXCHANGE_ACCOUNT_MISMATCH = "TOKEN_EXCHANGE_ACCOUNT_MISMATCH"
TOKEN_EXCHANGE_POLICY_REJECTION = "TOKEN_EXCHANGE_POLICY_REJECTION"
TOKEN_EXCHANGE_UNREACHABLE = "TOKEN_EXCHANGE_UNREACHABLE"


def _safe_slug(value, limit: int = 40) -> str:
    """Whitelist an OAuth error slug: short, letters/underscores only."""
    if not isinstance(value, str):
        return ""
    s = value.strip()
    if 0 < len(s) <= limit and all(c.isalpha() or c == "_" for c in s):
        return s
    return ""


def _safe_description(value, limit: int = 160) -> str:
    """A short, whitespace-normalised English description with any long
    token-like run redacted. Google's OAuth error_description values are fixed
    English sentences (e.g. 'Token has been expired or revoked.'); this guards
    against any unexpected echo of secret-looking material."""
    if not isinstance(value, str):
        return ""
    out = []
    for tok in value.split():
        out.append(tok if len(tok) <= 24 else "[...]")
    text = " ".join(out).strip()
    return text[:limit]


def classify_token_error(err_slug: str, http_status):
    """Pure mapping from a Google OAuth error slug + HTTP status to a diagnostic
    classification. No I/O — unit-testable."""
    if err_slug == "invalid_grant":
        return TOKEN_EXCHANGE_INVALID_GRANT
    if err_slug == "invalid_client":
        return TOKEN_EXCHANGE_CLIENT_MISMATCH
    if err_slug in ("invalid_scope", "access_denied", "unauthorized_client",
                    "admin_policy_enforced", "org_internal"):
        return TOKEN_EXCHANGE_POLICY_REJECTION
    try:
        code = int(http_status)
    except (TypeError, ValueError):
        code = 0
    if 400 <= code < 500:
        return TOKEN_EXCHANGE_POLICY_REJECTION
    return TOKEN_EXCHANGE_UNREACHABLE


def _emit(classification: str, *, google_error: str = "",
          google_error_description: str = "", http_status=None,
          account_ok=None) -> int:
    payload = {
        "classification": classification,
        "google_error": google_error,
        "google_error_description": google_error_description,
        "http_status": http_status,
        "account_ok": account_ok,
    }
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()
    return 0 if classification == TOKEN_EXCHANGE_OK else 1


def _read_refresh_token_from_stdin() -> str:
    line = sys.stdin.readline()
    return line.rstrip("\r\n")


def _tokeninfo_account(access_token: str, *, timeout: int):
    """Best-effort: return the email tokeninfo reports for the access token, or
    None (the gmail.send-only scope usually carries no email claim)."""
    url = _TOKENINFO_ENDPOINT + "?" + urllib.parse.urlencode(
        {"access_token": access_token})
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            obj = json.loads(resp.read().decode("utf-8", "replace"))
        return obj.get("email") if isinstance(obj, dict) else None
    except Exception:  # noqa: BLE001 — best effort, never fatal
        return None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Alpha Agent Gmail OAuth token-exchange diagnostic.")
    parser.add_argument("--oauth-client-path", required=True)
    parser.add_argument("--expected-account", default="")
    parser.add_argument("--token-endpoint", default=_DEFAULT_TOKEN_ENDPOINT)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    args = parser.parse_args(argv)

    refresh_token = _read_refresh_token_from_stdin()
    if not refresh_token:
        return _emit(TOKEN_FILE_NOT_FOUND,
                     google_error_description="No refresh token on stdin.")

    try:
        client_raw = json.loads(
            Path(args.oauth_client_path).read_text(encoding="utf-8-sig"))
        block = (client_raw.get("installed") or client_raw.get("web")
                 if isinstance(client_raw, dict) else None) or {}
        client_id = str(block.get("client_id") or "")
        client_secret = str(block.get("client_secret") or "")
        token_uri = str(block.get("token_uri") or args.token_endpoint)
    except (OSError, ValueError):
        return _emit(TOKEN_EXCHANGE_CLIENT_MISMATCH,
                     google_error="client_file_unreadable",
                     google_error_description="OAuth client file unreadable.")
    if not client_id or not client_secret:
        return _emit(TOKEN_EXCHANGE_CLIENT_MISMATCH,
                     google_error="client_incomplete",
                     google_error_description="OAuth client is incomplete.")

    data = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }).encode("ascii")
    req = urllib.request.Request(
        token_uri, data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=args.timeout_seconds) as resp:
            body = json.loads(resp.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as exc:
        try:
            err_obj = json.loads(exc.read().decode("utf-8", "replace"))
        except Exception:  # noqa: BLE001
            err_obj = {}
        slug = _safe_slug(err_obj.get("error"))
        desc = _safe_description(err_obj.get("error_description"))
        return _emit(classify_token_error(slug, exc.code),
                     google_error=slug, google_error_description=desc,
                     http_status=exc.code)
    except (urllib.error.URLError, TimeoutError, OSError):
        return _emit(TOKEN_EXCHANGE_UNREACHABLE,
                     google_error_description="Token endpoint unreachable.")

    access_token = body.get("access_token") if isinstance(body, dict) else None
    if not access_token:
        return _emit(TOKEN_EXCHANGE_POLICY_REJECTION,
                     google_error="no_access_token",
                     google_error_description="No access token returned.")

    account_ok = None
    if args.expected_account:
        who = _tokeninfo_account(str(access_token),
                                 timeout=args.timeout_seconds)
        if who:
            account_ok = (who.lower() == args.expected_account.strip().lower())
            if not account_ok:
                return _emit(TOKEN_EXCHANGE_ACCOUNT_MISMATCH,
                             google_error_description="Token is for a "
                             "different Google account.", account_ok=False)
    return _emit(TOKEN_EXCHANGE_OK,
                 google_error_description="Refresh token is valid.",
                 account_ok=account_ok)


if __name__ == "__main__":
    sys.exit(main())
