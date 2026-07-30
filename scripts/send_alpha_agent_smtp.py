#!/usr/bin/env python
"""Alpha Agent Stage 4 — standalone Gmail SMTP sender (App Password).

This is a REAL, standalone Python source file. It is NEVER invoked through
``python -c`` and NEVER receives the Gmail App Password on the command line, in
the environment, or from a file. The PowerShell wrapper
(``send_alpha_agent_smtp.ps1``) decrypts the Windows DPAPI App Password in memory
and passes it to this process on STDIN only.

Transport is Gmail SMTP with an App Password — the replacement for the retired
Gmail API OAuth transport:

    host      smtp.gmail.com
    port      587
    security  STARTTLS (ehlo -> starttls(secure ctx) -> ehlo -> login)

The App Password is read from STDIN only (one line). It is never echoed, never
persisted, never included in any diagnostic string, and SMTP debug output is
never enabled, so the AUTH exchange is never printed.

Because SMTP does not return a Gmail API message id, this sender GENERATES an
RFC 5322 ``Message-ID`` and returns it as the canonical, non-empty email message
identifier.

Standard library only: ``smtplib``, ``ssl``, ``socket``, ``email.message``
(plus argparse/json/sys/pathlib/mimetypes).

Output: exactly ONE JSON object on stdout, e.g.

    {"status":"EMAIL_SENT","message_id":"<...@gmail.com>","transport":"gmail_smtp","diagnostic":"Gmail SMTP accepted the message."}
    {"status":"EMAIL_SMTP_AUTHENTICATION_REJECTED","message_id":null,"transport":"gmail_smtp","diagnostic":"Gmail SMTP rejected the App Password."}

Failure diagnostics are fixed, safe strings. Process exit code is 0 for
EMAIL_SENT, 1 otherwise.
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import smtplib
import socket
import ssl
import sys
from email.message import EmailMessage
from email.utils import format_datetime, make_msgid
from pathlib import Path

# Machine-readable terminal statuses. These MUST stay in sync with the set the
# runtime recognises in alpha_agent/runtime.py.
EMAIL_SENT = "EMAIL_SENT"
EMAIL_ALREADY_SENT = "EMAIL_ALREADY_SENT"
EMAIL_SMTP_CREDENTIAL_MISSING = "EMAIL_SMTP_CREDENTIAL_MISSING"
EMAIL_SMTP_AUTHENTICATION_REJECTED = "EMAIL_SMTP_AUTHENTICATION_REJECTED"
EMAIL_SMTP_TLS_FAILED = "EMAIL_SMTP_TLS_FAILED"
EMAIL_SMTP_CONNECTION_FAILED = "EMAIL_SMTP_CONNECTION_FAILED"
EMAIL_SEND_FAILED = "EMAIL_SEND_FAILED"

TRANSPORT = "gmail_smtp"

_DEFAULT_SMTP_HOST = "smtp.gmail.com"
_DEFAULT_SMTP_PORT = 587
# Guard rail: report attachments are small; refuse anything implausibly large.
_MAX_ATTACHMENT_BYTES = 15 * 1024 * 1024

# Fixed, secret-free diagnostics for each SMTP failure class.
_DIAGNOSTICS = {
    EMAIL_SENT: "Gmail SMTP accepted the message.",
    EMAIL_SMTP_CREDENTIAL_MISSING:
        "No Gmail App Password was provided on stdin.",
    EMAIL_SMTP_AUTHENTICATION_REJECTED:
        "Gmail SMTP rejected the App Password.",
    EMAIL_SMTP_TLS_FAILED:
        "STARTTLS negotiation with Gmail SMTP failed.",
    EMAIL_SMTP_CONNECTION_FAILED:
        "Could not connect to the Gmail SMTP server.",
    EMAIL_SEND_FAILED:
        "Gmail SMTP did not accept the message for delivery.",
}


def _emit(status: str, *, message_id=None, diagnostic: str = "") -> int:
    """Print exactly one safe JSON line; return the process exit code."""
    payload = {
        "status": status,
        "message_id": message_id if isinstance(message_id, str)
        and message_id else None,
        "transport": TRANSPORT,
        "diagnostic": diagnostic or _DIAGNOSTICS.get(status, ""),
    }
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()
    return 0 if status == EMAIL_SENT else 1


def _read_app_password_from_stdin() -> str:
    """Read a single line from stdin. Some Windows stdin pipes prepend a UTF-8
    BOM (U+FEFF), which str.strip() does NOT remove; strip it plus any stray
    surrounding whitespace (Gmail App Passwords contain none)."""
    line = sys.stdin.readline()
    return line.replace("\ufeff", "").strip()


def _load_job(job_path: str):
    """Return (job_dict, error_diagnostic)."""
    try:
        raw = Path(job_path).read_text(encoding="utf-8-sig")
    except OSError:
        return None, "Outbox job could not be read."
    try:
        job = json.loads(raw)
    except (ValueError, TypeError):
        return None, "Outbox job is not valid JSON."
    if not isinstance(job, dict):
        return None, "Outbox job is not a JSON object."
    return job, None


def build_message(job: dict, sender: str, *, message_id: str = None):
    """Build the multipart/alternative message with an RFC 5322 Message-ID.

    Returns ``(message, message_id, error_diagnostic)``. The message carries
    Date, From, To, Subject and Message-ID headers and a UTF-8 plain-text body
    plus a UTF-8 HTML alternative. Any explicitly listed, existing attachment is
    added (nesting the alternative inside a multipart/mixed container). The
    generated Message-ID is the canonical delivery identifier.
    """
    recipient = job.get("recipient")
    subject = job.get("subject")
    html_path = job.get("html_path")
    text_path = job.get("text_path")
    for name, value in (("recipient", recipient), ("subject", subject),
                        ("html_path", html_path), ("text_path", text_path)):
        if not isinstance(value, str) or not value.strip():
            return None, None, "Outbox job missing required field: %s." % name

    try:
        html_body = Path(html_path).read_text(encoding="utf-8")
        text_body = Path(text_path).read_text(encoding="utf-8")
    except OSError:
        return None, None, "Report HTML or text file is missing."
    if not text_body.strip() or not html_body.strip():
        return None, None, "Report HTML or text body is empty."

    domain = sender.split("@")[-1] if "@" in sender else "localhost"
    if message_id is None:
        message_id = make_msgid(domain=domain)

    message = EmailMessage()
    message["From"] = sender
    message["To"] = recipient
    message["Subject"] = subject
    message["Date"] = format_datetime(_now())
    message["Message-ID"] = message_id
    # multipart/alternative: plain text first, HTML alternative second. UTF-8 is
    # applied automatically by EmailMessage for non-ASCII content.
    message.set_content(text_body, charset="utf-8")
    message.add_alternative(html_body, subtype="html", charset="utf-8")

    for attach_value in (job.get("attach_markdown") or []):
        if not isinstance(attach_value, str):
            return None, None, "Attachment entry is not a path string."
        attach_path = Path(attach_value)
        if not attach_path.is_file():
            continue
        try:
            data = attach_path.read_bytes()
        except OSError:
            return None, None, "Attachment could not be read."
        if len(data) > _MAX_ATTACHMENT_BYTES:
            return None, None, "Attachment exceeds the size limit."
        mime, _ = mimetypes.guess_type(attach_path.name)
        if mime and "/" in mime:
            maintype, subtype = mime.split("/", 1)
        else:
            maintype, subtype = "application", "octet-stream"
        message.add_attachment(data, maintype=maintype, subtype=subtype,
                               filename=attach_path.name)

    return message, message_id, None


def _now():
    """Local timezone-aware 'now' for the Date header (isolated for tests)."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).astimezone()


def deliver(message, *, account, app_password, host, port, timeout,
            smtp_factory=None, ssl_context=None):
    """Deliver ``message`` over SMTP+STARTTLS. Return ``(status, diagnostic)``.

    The delivery sequence is exactly: connect -> ehlo -> starttls(secure ctx) ->
    ehlo -> login -> send_message -> quit. ``smtp_factory`` is injectable so unit
    tests can supply a fake SMTP client (never touching the network); it defaults
    to ``smtplib.SMTP``. Neither the App Password nor the SMTP AUTH exchange is
    ever printed — SMTP debug output is never enabled.
    """
    if not app_password:
        return EMAIL_SMTP_CREDENTIAL_MISSING, \
            _DIAGNOSTICS[EMAIL_SMTP_CREDENTIAL_MISSING]
    factory = smtp_factory or smtplib.SMTP
    context = ssl_context or ssl.create_default_context()

    # --- connect --------------------------------------------------------- #
    try:
        server = factory(host, port, timeout=timeout)
    except (smtplib.SMTPConnectError, socket.gaierror, socket.timeout,
            ConnectionError, OSError):
        return EMAIL_SMTP_CONNECTION_FAILED, \
            _DIAGNOSTICS[EMAIL_SMTP_CONNECTION_FAILED]

    try:
        # --- STARTTLS ---------------------------------------------------- #
        try:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
        except (smtplib.SMTPNotSupportedError, smtplib.SMTPException,
                ssl.SSLError, RuntimeError, ValueError):
            return EMAIL_SMTP_TLS_FAILED, _DIAGNOSTICS[EMAIL_SMTP_TLS_FAILED]

        # --- authenticate ----------------------------------------------- #
        try:
            server.login(account, app_password)
        except smtplib.SMTPAuthenticationError:
            return EMAIL_SMTP_AUTHENTICATION_REJECTED, \
                _DIAGNOSTICS[EMAIL_SMTP_AUTHENTICATION_REJECTED]
        except (smtplib.SMTPException, UnicodeError):
            # Any other failure during the AUTH phase (including a non-ASCII
            # credential) is treated as a credential rejection, never surfacing
            # the server response or the credential.
            return EMAIL_SMTP_AUTHENTICATION_REJECTED, \
                _DIAGNOSTICS[EMAIL_SMTP_AUTHENTICATION_REJECTED]

        # --- send -------------------------------------------------------- #
        try:
            server.send_message(message)
        except (smtplib.SMTPException, socket.timeout, ConnectionError,
                OSError):
            return EMAIL_SEND_FAILED, _DIAGNOSTICS[EMAIL_SEND_FAILED]
    finally:
        try:
            server.quit()
        except Exception:  # noqa: BLE001 - closing must never mask the result
            pass

    return EMAIL_SENT, _DIAGNOSTICS[EMAIL_SENT]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Alpha Agent Stage 4 standalone Gmail SMTP sender.")
    parser.add_argument("--job-path", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--smtp-host", default=_DEFAULT_SMTP_HOST)
    parser.add_argument("--smtp-port", type=int, default=_DEFAULT_SMTP_PORT)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)

    app_password = _read_app_password_from_stdin()
    if not app_password:
        return _emit(EMAIL_SMTP_CREDENTIAL_MISSING)

    job, err = _load_job(args.job_path)
    if job is None:
        return _emit(EMAIL_SEND_FAILED, diagnostic=err)

    message, message_id, err = build_message(job, args.account)
    if message is None:
        return _emit(EMAIL_SEND_FAILED, diagnostic=err)

    status, diagnostic = deliver(
        message, account=args.account, app_password=app_password,
        host=args.smtp_host, port=args.smtp_port,
        timeout=args.timeout_seconds)
    # The App Password is now out of scope; drop the reference immediately.
    app_password = None

    if status == EMAIL_SENT:
        return _emit(EMAIL_SENT, message_id=message_id, diagnostic=diagnostic)
    return _emit(status, diagnostic=diagnostic)


if __name__ == "__main__":
    sys.exit(main())
