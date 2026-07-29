#!/usr/bin/env python
"""Alpha Agent Stage 4 — one-time Gmail API OAuth authorization (Desktop flow).

This is a REAL, standalone Python source file. It is NEVER invoked through
``python -c``. It performs the OAuth 2.0 *Desktop application* authorization-code
flow with PKCE against Google, entirely with the Python standard library:

  * bind a loopback listener on 127.0.0.1 with a random available port;
  * generate a cryptographically random ``state`` and a PKCE (S256) verifier +
    challenge;
  * open the system browser at Google's authorization endpoint with
    ``access_type=offline``, ``prompt=consent`` and ``login_hint``;
  * wait (bounded) for the browser to redirect back to the loopback listener;
  * validate the returned ``state`` EXACTLY and reject any OAuth error;
  * exchange the authorization code at ``https://oauth2.googleapis.com/token``
    using ``urllib.request``;
  * require a ``refresh_token`` and verify the granted scope includes
    ``gmail.send``.

Output contract
---------------
Exactly ONE compact JSON object is written to stdout. On success it carries the
refresh token so the PowerShell configure wrapper can DPAPI-encrypt it in memory
(the token is returned ONLY inside that captured JSON — it is never printed on
its own, logged, or included in any error diagnostic):

    {"status":"GMAIL_AUTHORIZATION_OK","refresh_token":"...","account":"...","scope":"..."}

On failure a safe object is emitted with no token and a fixed, secret-free
diagnostic:

    {"status":"AUTHORIZATION_TIMEOUT","refresh_token":null,"diagnostic":"..."}

The authorization code, access token, refresh token, client secret and the raw
Google token response are never printed except for the single success line's
refresh token. Process exit code is 0 for GMAIL_AUTHORIZATION_OK, 1 otherwise.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import http.server
import json
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

# Terminal statuses.
GMAIL_AUTHORIZATION_OK = "GMAIL_AUTHORIZATION_OK"
AUTHORIZATION_CLIENT_INVALID = "AUTHORIZATION_CLIENT_INVALID"
AUTHORIZATION_LISTENER_FAILED = "AUTHORIZATION_LISTENER_FAILED"
AUTHORIZATION_TIMEOUT = "AUTHORIZATION_TIMEOUT"
AUTHORIZATION_STATE_MISMATCH = "AUTHORIZATION_STATE_MISMATCH"
AUTHORIZATION_DENIED = "AUTHORIZATION_DENIED"
AUTHORIZATION_NO_CODE = "AUTHORIZATION_NO_CODE"
AUTHORIZATION_NO_REFRESH_TOKEN = "AUTHORIZATION_NO_REFRESH_TOKEN"
AUTHORIZATION_SCOPE_INSUFFICIENT = "AUTHORIZATION_SCOPE_INSUFFICIENT"
AUTHORIZATION_TOKEN_EXCHANGE_FAILED = "AUTHORIZATION_TOKEN_EXCHANGE_FAILED"

_LOOPBACK = "127.0.0.1"
_GMAIL_SEND_SCOPE = "https://www.googleapis.com/auth/gmail.send"
_DEFAULT_AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
_DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token"
_COMPLETION_HTML = (
    "<!doctype html><html><head><meta charset=\"utf-8\">"
    "<title>Alpha Agent</title></head><body>"
    "<p>Alpha Agent authorization completed. You may close this window.</p>"
    "</body></html>")


# --------------------------------------------------------------------------- #
# Pure, deterministically testable helpers (no network, no browser).
# --------------------------------------------------------------------------- #
def _b64url(raw: bytes) -> str:
    """URL-safe Base64 with padding stripped (RFC 7636 style)."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _pkce_pair() -> tuple[str, str]:
    """Return a (verifier, S256 challenge) PKCE pair.

    The verifier is 43-128 chars from the unreserved set; the challenge is the
    URL-safe, unpadded Base64 of SHA-256(verifier).
    """
    verifier = _b64url(secrets.token_bytes(64))          # ~86 chars, in range
    challenge = _b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


def _random_state() -> str:
    """A cryptographically random, URL-safe OAuth state value."""
    return secrets.token_urlsafe(32)


def _emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _fail(status: str, diagnostic: str) -> int:
    _emit({"status": status, "refresh_token": None, "diagnostic": diagnostic})
    return 1


def _load_client(path: str):
    """Return (client_dict, error_diagnostic). client_dict has no secret echoed."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None, "OAuth client secrets file is missing or unreadable."
    if not isinstance(data, dict):
        return None, "OAuth client secrets file is not a JSON object."
    block = data.get("installed") or data.get("web")
    if not isinstance(block, dict):
        return None, "OAuth client secrets file is not a Desktop/Web client."
    if not block.get("client_id") or not block.get("client_secret"):
        return None, "OAuth client secrets file lacks client_id/client_secret."
    return ({"client_id": str(block["client_id"]),
             "client_secret": str(block["client_secret"]),
             "auth_uri": str(block.get("auth_uri") or _DEFAULT_AUTH_URI),
             "token_uri": str(block.get("token_uri") or _DEFAULT_TOKEN_URI)},
            None)


def _build_auth_url(client: dict, *, redirect_uri: str, scope: str, state: str,
                    challenge: str, account: str) -> str:
    params = {
        "client_id": client["client_id"],
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": scope,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "access_type": "offline",
        "prompt": "consent",
        "login_hint": account,
    }
    return client["auth_uri"] + "?" + urllib.parse.urlencode(params)


def _classify_redirect(params: dict, expected_state: str):
    """Return (error_status, code, diagnostic).

    error_status is None when the redirect is a valid, state-matched code.
    """
    if params.get("error"):
        return (AUTHORIZATION_DENIED, None,
                "Authorization was denied or failed at Google.")
    if params.get("state") != expected_state:
        return (AUTHORIZATION_STATE_MISMATCH, None,
                "OAuth state did not match; authorization aborted.")
    code = params.get("code")
    if not code:
        return (AUTHORIZATION_NO_CODE, None,
                "Authorization response contained no code.")
    return (None, code, "")


def _finalize_token(token_obj, *, required_scope: str):
    """Return (status, refresh_token, granted_scope, diagnostic)."""
    if not isinstance(token_obj, dict):
        return (AUTHORIZATION_TOKEN_EXCHANGE_FAILED, None, None,
                "Token endpoint returned no JSON object.")
    refresh = token_obj.get("refresh_token")
    if not refresh:
        return (AUTHORIZATION_NO_REFRESH_TOKEN, None, None,
                "Token response contained no refresh token; re-consent required.")
    granted = str(token_obj.get("scope") or "")
    if required_scope not in granted.split():
        return (AUTHORIZATION_SCOPE_INSUFFICIENT, None, None,
                "Granted scope does not include gmail.send.")
    return (GMAIL_AUTHORIZATION_OK, str(refresh), granted, "")


# --------------------------------------------------------------------------- #
# Network (exercised only in real use; monkeypatched/never called in tests).
# --------------------------------------------------------------------------- #
def _exchange_code(client: dict, *, code: str, redirect_uri: str,
                   verifier: str, timeout: int) -> dict:
    data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client["client_id"],
        "client_secret": client["client_secret"],
        "code_verifier": verifier,
    }).encode("ascii")
    req = urllib.request.Request(
        client["token_uri"], data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
    return json.loads(body)


# --------------------------------------------------------------------------- #
# Loopback listener (single-shot capture of the browser redirect).
# --------------------------------------------------------------------------- #
class _RedirectHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - http.server API
        parsed = urllib.parse.urlparse(self.path)
        params = {k: v[0] for k, v in
                  urllib.parse.parse_qs(parsed.query).items()}
        # Only latch a request that actually carries an OAuth result.
        if params.get("code") or params.get("error") or params.get("state"):
            self.server.captured_params = params  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(_COMPLETION_HTML.encode("utf-8"))

    def log_message(self, *args):  # silence — never log query strings
        return


def _wait_for_redirect(server, *, timeout_seconds: int):
    """Handle loopback requests until an OAuth result arrives or time is up."""
    server.captured_params = None
    server.timeout = min(timeout_seconds, 30)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        server.handle_request()  # returns after one request OR socket timeout
        if server.captured_params:
            return server.captured_params
    return None


# --------------------------------------------------------------------------- #
# Entry point.
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Alpha Agent Stage 4 Gmail API OAuth authorization.")
    parser.add_argument("--client-secrets-path", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--scope", default=_GMAIL_SEND_SCOPE)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args(argv)

    client, err = _load_client(args.client_secrets_path)
    if client is None:
        return _fail(AUTHORIZATION_CLIENT_INVALID, err)

    verifier, challenge = _pkce_pair()
    state = _random_state()

    # Bind ONLY to loopback with a random available port.
    try:
        server = http.server.HTTPServer((_LOOPBACK, 0), _RedirectHandler)
    except OSError:
        return _fail(AUTHORIZATION_LISTENER_FAILED,
                     "Could not bind a loopback authorization listener.")
    try:
        port = server.server_address[1]
        redirect_uri = "http://%s:%d/" % (_LOOPBACK, port)
        auth_url = _build_auth_url(
            client, redirect_uri=redirect_uri, scope=args.scope, state=state,
            challenge=challenge, account=args.account)
        try:
            webbrowser.open(auth_url)
        except Exception:  # noqa: BLE001 - browser is best-effort
            pass
        params = _wait_for_redirect(server, timeout_seconds=args.timeout_seconds)
    finally:
        try:
            server.server_close()
        except OSError:
            pass

    if not params:
        return _fail(AUTHORIZATION_TIMEOUT,
                     "Timed out waiting for browser authorization.")

    err_status, code, diag = _classify_redirect(params, state)
    if err_status is not None:
        return _fail(err_status, diag)

    try:
        token_obj = _exchange_code(
            client, code=code, redirect_uri=redirect_uri, verifier=verifier,
            timeout=min(args.timeout_seconds, 120))
    except urllib.error.HTTPError:
        return _fail(AUTHORIZATION_TOKEN_EXCHANGE_FAILED,
                     "Google rejected the authorization-code exchange.")
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return _fail(AUTHORIZATION_TOKEN_EXCHANGE_FAILED,
                     "Could not reach the Google token endpoint.")

    status, refresh, granted, fdiag = _finalize_token(
        token_obj, required_scope=_GMAIL_SEND_SCOPE)
    if status != GMAIL_AUTHORIZATION_OK:
        return _fail(status, fdiag)

    # The refresh token appears ONLY here, in the single captured JSON line.
    _emit({"status": GMAIL_AUTHORIZATION_OK, "refresh_token": refresh,
           "account": args.account, "scope": granted})
    return 0


if __name__ == "__main__":
    sys.exit(main())
