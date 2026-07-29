#!/usr/bin/env python
"""Alpha Agent Stage 4 — standalone Gmail API sender (OAuth 2.0).

This is a REAL, standalone Python source file. It is NEVER invoked through
``python -c`` and NEVER receives the OAuth refresh token on the command line, in
the environment, or from a file. The PowerShell wrapper
(``send_alpha_agent_email.ps1``) decrypts the DPAPI refresh token in memory and
passes it to this process on STDIN only.

SMTP is not used anywhere in this module — delivery is exclusively the Gmail API
``users.messages.send`` REST endpoint over HTTPS, authorized with a short-lived
Bearer access token minted from the refresh token.

Contract
--------
Command-line arguments (all non-secret):

    --job-path           path to the outbox job JSON
    --oauth-client-path  path to the Desktop OAuth client JSON (client_id/secret)
    --account            the Gmail sender address (userId=me is the same account)
    --gmail-endpoint     Gmail API send endpoint (default users/me/messages/send)
    --token-endpoint     OAuth token endpoint (default oauth2.googleapis.com)
    --timeout-seconds    bounded HTTP timeout (default 120)

The refresh token is read from STDIN only (one line). It is never echoed, never
persisted, and never included in any diagnostic string. The minted access token
is held only in memory for the single send and is likewise never printed.

Standard library only: ``urllib.request``/``urllib.error``/``urllib.parse``,
``base64``, ``json``, ``email.message.EmailMessage`` (plus argparse/mimetypes/
sys/pathlib). Delivery sequence: refresh access token -> build MIME -> base64url
encode -> POST to the Gmail API.

Output: exactly ONE JSON object on stdout, e.g.

    {"status":"EMAIL_SENT","message_id":"18f...","diagnostic":"Report delivered through Gmail API."}
    {"status":"OAUTH_REAUTHORIZATION_REQUIRED","message_id":null,"diagnostic":"Refresh token was rejected; re-authorization required."}

Failure diagnostics contain only a fixed, safe message. Raw Google responses —
which may carry account or token data — are never printed. Process exit code is 0
for EMAIL_SENT, 1 otherwise.
"""
from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import sys
import urllib.error
import urllib.parse
import urllib.request
from email.message import EmailMessage
from pathlib import Path

# Machine-readable terminal statuses. These MUST stay in sync with the set the
# runtime recognises in alpha_agent/runtime.py.
EMAIL_SENT = "EMAIL_SENT"
OAUTH_REAUTHORIZATION_REQUIRED = "OAUTH_REAUTHORIZATION_REQUIRED"
OAUTH_TOKEN_REFRESH_REJECTED = "OAUTH_TOKEN_REFRESH_REJECTED"
OAUTH_CLIENT_INVALID = "OAUTH_CLIENT_INVALID"
GMAIL_API_PERMISSION_DENIED = "GMAIL_API_PERMISSION_DENIED"
GMAIL_API_RATE_LIMITED = "GMAIL_API_RATE_LIMITED"
GMAIL_API_RETRYABLE_FAILURE = "GMAIL_API_RETRYABLE_FAILURE"
EMAIL_JOB_INVALID = "EMAIL_JOB_INVALID"
EMAIL_ATTACHMENT_INVALID = "EMAIL_ATTACHMENT_INVALID"
EMAIL_PERMANENT_FAILURE = "EMAIL_PERMANENT_FAILURE"

# Guard rail: report attachments are small; refuse anything implausibly large.
_MAX_ATTACHMENT_BYTES = 15 * 1024 * 1024
_DEFAULT_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
_DEFAULT_GMAIL_ENDPOINT = \
    "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"

# Fixed, secret-free diagnostics for each Gmail-API send failure class.
_SEND_DIAGNOSTICS = {
    OAUTH_REAUTHORIZATION_REQUIRED:
        "Gmail API rejected the access token; re-authorization required.",
    GMAIL_API_PERMISSION_DENIED:
        "Gmail API denied permission for this account or scope.",
    GMAIL_API_RATE_LIMITED:
        "Gmail API rate-limited the request; retry later.",
    GMAIL_API_RETRYABLE_FAILURE:
        "Gmail API had a temporary server error; retry later.",
    EMAIL_PERMANENT_FAILURE:
        "Gmail API rejected the message.",
}


def _emit(status: str, *, message_id=None, diagnostic: str = "") -> int:
    """Print exactly one safe JSON line; return the process exit code."""
    payload = {"status": status,
               "message_id": message_id if isinstance(message_id, str)
               and message_id else None,
               "diagnostic": diagnostic}
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()
    return 0 if status == EMAIL_SENT else 1


def _read_refresh_token_from_stdin() -> str:
    """Read a single line from stdin, stripping only the trailing EOL."""
    line = sys.stdin.readline()
    return line.rstrip("\r\n")


def _safe_error_code(exc) -> str:
    """Return only a short, whitelisted OAuth error slug from an HTTPError body.

    Used purely for internal status mapping. The raw body is never emitted.
    """
    try:
        raw = exc.read().decode("utf-8", "replace")
        obj = json.loads(raw)
    except Exception:  # noqa: BLE001 - malformed error bodies are ignored
        return ""
    code = obj.get("error") if isinstance(obj, dict) else None
    if isinstance(code, str):
        s = code.strip()
        if 0 < len(s) <= 40 and all(c.isalpha() or c == "_" for c in s):
            return s
    return ""


def _map_send_status(http_code: int) -> str:
    """Map a Gmail API send HTTP status to a terminal status."""
    if http_code == 401:
        return OAUTH_REAUTHORIZATION_REQUIRED
    if http_code == 403:
        return GMAIL_API_PERMISSION_DENIED
    if http_code == 429:
        return GMAIL_API_RATE_LIMITED
    if http_code in (500, 502, 503, 504):
        return GMAIL_API_RETRYABLE_FAILURE
    return EMAIL_PERMANENT_FAILURE


def _load_client(path: str):
    """Return (client_dict, error_status, error_diagnostic)."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None, OAUTH_CLIENT_INVALID, \
            "OAuth client file is missing or unreadable."
    block = data.get("installed") or data.get("web") \
        if isinstance(data, dict) else None
    if not isinstance(block, dict) or not block.get("client_id") \
            or not block.get("client_secret"):
        return None, OAUTH_CLIENT_INVALID, \
            "OAuth client file is not a valid Desktop client."
    return ({"client_id": str(block["client_id"]),
             "client_secret": str(block["client_secret"]),
             "token_uri": str(block.get("token_uri")
                               or _DEFAULT_TOKEN_ENDPOINT)},
            None, None)


def _load_job(job_path: str):
    """Return (job_dict, error_status, error_diagnostic)."""
    try:
        raw = Path(job_path).read_text(encoding="utf-8-sig")
    except OSError:
        return None, EMAIL_JOB_INVALID, "Outbox job could not be read."
    try:
        job = json.loads(raw)
    except (ValueError, TypeError):
        return None, EMAIL_JOB_INVALID, "Outbox job is not valid JSON."
    if not isinstance(job, dict):
        return None, EMAIL_JOB_INVALID, "Outbox job is not a JSON object."
    return job, None, None


def _build_message(job: dict, sender: str):
    """Build the multipart message. Return (message, error_status, error_msg)."""
    recipient = job.get("recipient")
    subject = job.get("subject")
    html_path = job.get("html_path")
    text_path = job.get("text_path")
    for name, value in (("recipient", recipient), ("subject", subject),
                        ("html_path", html_path), ("text_path", text_path)):
        if not isinstance(value, str) or not value.strip():
            return None, EMAIL_JOB_INVALID, (
                "Outbox job missing required field: %s." % name)

    try:
        html_body = Path(html_path).read_text(encoding="utf-8")
        text_body = Path(text_path).read_text(encoding="utf-8")
    except OSError:
        return None, EMAIL_JOB_INVALID, "Report HTML or text file is missing."

    message = EmailMessage()
    message["From"] = sender
    message["To"] = recipient
    message["Subject"] = subject
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")

    # Attach only explicitly listed, existing files. Missing optional
    # attachments are skipped; a malformed or unreadable entry is an error.
    for attach_value in (job.get("attach_markdown") or []):
        if not isinstance(attach_value, str):
            return None, EMAIL_ATTACHMENT_INVALID, (
                "Attachment entry is not a path string.")
        attach_path = Path(attach_value)
        if not attach_path.is_file():
            continue
        try:
            data = attach_path.read_bytes()
        except OSError:
            return None, EMAIL_ATTACHMENT_INVALID, (
                "Attachment could not be read.")
        if len(data) > _MAX_ATTACHMENT_BYTES:
            return None, EMAIL_ATTACHMENT_INVALID, (
                "Attachment exceeds the size limit.")
        mime, _ = mimetypes.guess_type(attach_path.name)
        if mime and "/" in mime:
            maintype, subtype = mime.split("/", 1)
        else:
            maintype, subtype = "application", "octet-stream"
        message.add_attachment(data, maintype=maintype, subtype=subtype,
                               filename=attach_path.name)

    return message, None, None


def _encode_mime(message: EmailMessage) -> str:
    """URL-safe Base64 (no padding) of the raw RFC 2822 message, per Gmail API."""
    return base64.urlsafe_b64encode(message.as_bytes()).rstrip(b"=") \
        .decode("ascii")


def _refresh_access_token(client: dict, refresh_token: str, *, timeout: int):
    """Mint an access token from the refresh token. Return (token, status, diag)."""
    data = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client["client_id"],
        "client_secret": client["client_secret"],
    }).encode("ascii")
    req = urllib.request.Request(
        client["token_uri"], data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        err = _safe_error_code(exc)
        if err == "invalid_grant":
            return None, OAUTH_REAUTHORIZATION_REQUIRED, \
                "Refresh token was rejected; re-authorization required."
        if err == "invalid_client":
            return None, OAUTH_CLIENT_INVALID, \
                "OAuth client credentials were rejected."
        return None, OAUTH_TOKEN_REFRESH_REJECTED, \
            "The token endpoint rejected the refresh request."
    except (urllib.error.URLError, TimeoutError, OSError):
        return None, GMAIL_API_RETRYABLE_FAILURE, \
            "Could not reach the Google token endpoint."
    try:
        obj = json.loads(body)
        token = obj.get("access_token") if isinstance(obj, dict) else None
    except (ValueError, TypeError):
        token = None
    if not token:
        return None, OAUTH_TOKEN_REFRESH_REJECTED, \
            "The token endpoint returned no access token."
    return str(token), None, None


def _gmail_send(raw_message: str, access_token: str, endpoint: str, *,
                timeout: int):
    """POST the encoded message to the Gmail API. Return (message_id, status, diag)."""
    body = json.dumps({"raw": raw_message}).encode("utf-8")
    req = urllib.request.Request(endpoint, data=body, headers={
        "Authorization": "Bearer %s" % access_token,
        "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        status = _map_send_status(exc.code)
        return None, status, _SEND_DIAGNOSTICS.get(
            status, "Gmail API rejected the send.")
    except (urllib.error.URLError, TimeoutError, OSError):
        return None, GMAIL_API_RETRYABLE_FAILURE, \
            "Could not reach the Gmail API endpoint."
    try:
        obj = json.loads(payload)
        mid = obj.get("id") if isinstance(obj, dict) else None
    except (ValueError, TypeError):
        mid = None
    return (str(mid) if isinstance(mid, str) else ""), None, None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Alpha Agent Stage 4 standalone Gmail API sender.")
    parser.add_argument("--job-path", required=True)
    parser.add_argument("--oauth-client-path", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--gmail-endpoint", default=_DEFAULT_GMAIL_ENDPOINT)
    parser.add_argument("--token-endpoint", default=_DEFAULT_TOKEN_ENDPOINT)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)

    refresh_token = _read_refresh_token_from_stdin()
    if not refresh_token:
        return _emit(OAUTH_REAUTHORIZATION_REQUIRED,
                     diagnostic="No refresh token was provided on stdin.")

    client, err_status, err_msg = _load_client(args.oauth_client_path)
    if client is None:
        return _emit(err_status, diagnostic=err_msg)
    if not client.get("token_uri"):
        client["token_uri"] = args.token_endpoint

    job, err_status, err_msg = _load_job(args.job_path)
    if job is None:
        return _emit(err_status, diagnostic=err_msg)

    message, err_status, err_msg = _build_message(job, args.account)
    if message is None:
        return _emit(err_status, diagnostic=err_msg)

    access_token, err_status, err_msg = _refresh_access_token(
        client, refresh_token, timeout=args.timeout_seconds)
    if access_token is None:
        return _emit(err_status, diagnostic=err_msg)

    raw = _encode_mime(message)
    message_id, err_status, err_msg = _gmail_send(
        raw, access_token, args.gmail_endpoint, timeout=args.timeout_seconds)
    if err_status is not None:
        return _emit(err_status, diagnostic=err_msg)

    return _emit(EMAIL_SENT, message_id=message_id,
                 diagnostic="Report delivered through Gmail API.")


if __name__ == "__main__":
    sys.exit(main())
