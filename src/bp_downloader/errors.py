"""Classify sda-cli error output by how the downloader must react to it.

The patterns come from the per-dataset logs of an earlier deployment's bash scripts, over
several months of runs. Retrying was the only reaction those scripts had, and most of the
damage came from retrying errors that a retry can never fix: an expired token (one run sat
on it for days) and the unrouted download2 endpoint (149 failed attempts in one run).
"""

from __future__ import annotations

import enum
import re


class ErrorKind(enum.Enum):
    TOKEN_EXPIRED = "token expired"
    """The download conf no longer works. Stop the run; a person must fetch a new conf."""

    BAD_PASSPHRASE = "bad passphrase"
    """The private key cannot be unlocked. Stop the run."""

    DISK_FULL = "disk full"
    """The share is full. Stop the run: retrying fills nothing, and no file is to blame."""

    WRONG_KEY = "encrypted to another key"
    """Files on disk were encrypted to a key we don't hold. Hold that dataset, touch nothing."""

    ENDPOINT_DEAD = "endpoint dead"
    """The endpoint does not answer at all (TLS, DNS, connect). Stop using it for this run."""

    WRONG_ENDPOINT = "wrong endpoint"
    """`unexpected EOF`: with several endpoints, the dataset may live behind another one
    (NBISweden/sda-cli#695); otherwise, or once the endpoint has delivered files, a dropped
    connection (the runner decides). Since one endpoint serves every dataset, it is the latter."""

    NOT_FOUND = "not found"
    """HTTP 404/403: not granted to this conf's user, or a passing listing error."""

    TRANSIENT = "transient"
    """Server-side or transport errors that clear on retry, and anything unrecognised."""


# Most severe first: when an output holds several errors, the first match wins.
_PATTERNS: list[tuple[ErrorKind, re.Pattern[str]]] = [
    (
        ErrorKind.TOKEN_EXPIRED,
        re.compile(r"access token has expired|status 401\b|could not parse token", re.I),
    ),
    (ErrorKind.BAD_PASSPHRASE, re.compile(r"chacha20poly1305: message authentication failed")),
    (ErrorKind.DISK_FULL, re.compile(r"no space left on device|disk quota exceeded", re.I)),
    (ErrorKind.WRONG_KEY, re.compile(r"could not find matching public key header")),
    (
        ErrorKind.ENDPOINT_DEAD,
        re.compile(
            r"failed to verify certificate|x509:|dial tcp [^\n]*(i/o timeout|connection refused)"
            r"|no such host",
            re.I,
        ),
    ),
    (ErrorKind.WRONG_ENDPOINT, re.compile(r"unexpected EOF")),
    (ErrorKind.NOT_FOUND, re.compile(r"status (404|403)\b")),
]


def classify(output: str) -> ErrorKind:
    """Return the kind of the most severe error in a failed sda-cli call's output."""
    for kind, pattern in _PATTERNS:
        if pattern.search(output):
            return kind
    return ErrorKind.TRANSIENT


def last_error_line(output: str) -> str:
    """The last line that looks like an error, for the status database and the log."""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for line in reversed(lines):
        if re.search(r"error|fail|warning", line, re.I):
            return line[:500]
    return lines[-1][:500] if lines else "(no output)"
