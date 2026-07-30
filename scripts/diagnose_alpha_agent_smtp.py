#!/usr/bin/env python
"""Alpha Agent Stage 4 — standalone Gmail SMTP App Password DIAGNOSTIC.

READ-ONLY. This probe authenticates to Gmail SMTP to classify the current App
Password state WITHOUT sending an email. It connects to smtp.gmail.com:587,
performs ``ehlo -> starttls(secure ctx) -> ehlo -> login -> NOOP -> quit`` and
never issues MAIL/RCPT/DATA. It NEVER opens a browser, NEVER writes a file, and
NEVER prints the App Password, the SMTP AUTH exchange, or any DPAPI plaintext
(SMTP debug output is never enabled).

The DPAPI App Password is decrypted by the PowerShell wrapper
(diagnose_alpha_agent_smtp.ps1) and handed to this process on STDIN only — never
on the command line, in the environment, or in a file (identical to the real
sender's contract).

It emits exactly ONE JSON line, e.g.

    {"classification":"SMTP_AUTHENTICATION_OK"}
    {"classification":"SMTP_AUTHENTICATION_REJECTED"}

Distinguishable classifications:
    SMTP_CREDENTIAL_MISSING        no App Password was available on stdin
    SMTP_AUTHENTICATION_OK         the App Password authenticated successfully
    SMTP_AUTHENTICATION_REJECTED   Gmail SMTP rejected the App Password (535)
    SMTP_TLS_FAILED                STARTTLS negotiation failed
    SMTP_CONNECTION_FAILED         could not reach / connect to the SMTP server

Standard library only. Exit code 0 on SMTP_AUTHENTICATION_OK, 1 otherwise.
"""
from __future__ import annotations

import argparse
import json
import smtplib
import socket
import ssl
import sys

_DEFAULT_SMTP_HOST = "smtp.gmail.com"
_DEFAULT_SMTP_PORT = 587

SMTP_CREDENTIAL_MISSING = "SMTP_CREDENTIAL_MISSING"
SMTP_AUTHENTICATION_OK = "SMTP_AUTHENTICATION_OK"
SMTP_AUTHENTICATION_REJECTED = "SMTP_AUTHENTICATION_REJECTED"
SMTP_TLS_FAILED = "SMTP_TLS_FAILED"
SMTP_CONNECTION_FAILED = "SMTP_CONNECTION_FAILED"


def _emit(classification: str) -> int:
    sys.stdout.write(json.dumps({"classification": classification},
                                separators=(",", ":")) + "\n")
    sys.stdout.flush()
    return 0 if classification == SMTP_AUTHENTICATION_OK else 1


def _read_app_password_from_stdin() -> str:
    line = sys.stdin.readline()
    # Some Windows stdin pipes prepend a UTF-8 BOM (U+FEFF); it is not stripped
    # by str.strip(). Remove it plus any surrounding whitespace. Gmail App
    # Passwords are 16 ASCII alphanumerics.
    return line.replace("\ufeff", "").strip()


def probe(*, account, app_password, host, port, timeout,
          smtp_factory=None, ssl_context=None) -> str:
    """Authenticate (no send) and return a classification. ``smtp_factory`` is
    injectable so unit tests can supply a fake SMTP client."""
    if not app_password:
        return SMTP_CREDENTIAL_MISSING
    factory = smtp_factory or smtplib.SMTP
    context = ssl_context or ssl.create_default_context()

    try:
        server = factory(host, port, timeout=timeout)
    except (smtplib.SMTPConnectError, socket.gaierror, socket.timeout,
            ConnectionError, OSError):
        return SMTP_CONNECTION_FAILED

    try:
        try:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
        except (smtplib.SMTPNotSupportedError, smtplib.SMTPException,
                ssl.SSLError, RuntimeError, ValueError):
            return SMTP_TLS_FAILED

        try:
            server.login(account, app_password)
        except smtplib.SMTPAuthenticationError:
            return SMTP_AUTHENTICATION_REJECTED
        except (smtplib.SMTPException, UnicodeError):
            return SMTP_AUTHENTICATION_REJECTED

        # Read-only liveness check; never MAIL/RCPT/DATA.
        try:
            server.noop()
        except smtplib.SMTPException:
            pass
    finally:
        try:
            server.quit()
        except Exception:  # noqa: BLE001 - closing must never mask the result
            pass

    return SMTP_AUTHENTICATION_OK


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Alpha Agent Gmail SMTP App Password diagnostic.")
    parser.add_argument("--account", required=True)
    parser.add_argument("--smtp-host", default=_DEFAULT_SMTP_HOST)
    parser.add_argument("--smtp-port", type=int, default=_DEFAULT_SMTP_PORT)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    args = parser.parse_args(argv)

    app_password = _read_app_password_from_stdin()
    if not app_password:
        return _emit(SMTP_CREDENTIAL_MISSING)

    classification = probe(
        account=args.account, app_password=app_password,
        host=args.smtp_host, port=args.smtp_port,
        timeout=args.timeout_seconds)
    app_password = None
    return _emit(classification)


if __name__ == "__main__":
    sys.exit(main())
