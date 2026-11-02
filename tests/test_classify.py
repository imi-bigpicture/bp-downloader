"""sda-cli error output is mapped to the reaction it needs.

Every input line is copied from real per-dataset logs (April to September 2026) or from
the 2026-10-06 test on the production host, with only IDs and IP addresses shortened.
"""

import pytest

from bp_downloader.errors import ErrorKind, classify

CASES = [
    # An expired token can never be fixed by retrying: the whole run must stop.
    ("Error: the provided access token has expired, please renew it", ErrorKind.TOKEN_EXPIRED),
    ("Error: failed to get files, reason: server returned status 401", ErrorKind.TOKEN_EXPIRED),
    # A wrong passphrase fails every decryption the same way.
    (
        "Error: chacha20poly1305: message authentication failed, file: other-key.sec.pem",
        ErrorKind.BAD_PASSPHRASE,
    ),
    # Files on disk encrypted to a key we don't hold (another operator's, or an older key).
    (
        "Error decrypting file IMAGES/IMAGE_a/image-level-0.dcm.c4gh: could not create cryp4gh"
        " reader: could not find matching public key header, decryption failed",
        ErrorKind.WRONG_KEY,
    ),
    # A full share: retrying fills nothing, and blaming the file would abandon a good one.
    (
        "Error: write IMAGES/IMAGE_a/image-level-0.dcm.c4gh.part: no space left on device",
        ErrorKind.DISK_FULL,
    ),
    ("Error: could not create output file x.dcm: disk quota exceeded", ErrorKind.DISK_FULL),
    # download2 has not been routed since mid-2026; it answers with a placeholder certificate.
    (
        'Error: failed to get files, reason: failed to get response, reason: Get '
        '"https://download2.bp.nbis.se/metadata/datasets/aa-Dataset-x/files": tls: failed to '
        "verify certificate: x509: certificate is valid for ingress.local, not download2.bp.nbis.se",
        ErrorKind.ENDPOINT_DEAD,
    ),
    (
        'Error: failed to get response, reason: Get "https://download2.bp.nbis.se/metadata/'
        'datasets/aa-Dataset-x/files": dial tcp 10.0.0.1:443: i/o timeout',
        ErrorKind.ENDPOINT_DEAD,
    ),
    # Asking the endpoint that doesn't hold the dataset (NBISweden/sda-cli#695).
    ("Error: unexpected EOF", ErrorKind.WRONG_ENDPOINT),
    (
        'Error: failed to get response, reason: Get "https://download.bp.nbis.se/s3/aa-Dataset-x/'
        'IMAGES/IMAGE_a/image-level-1.dcm.c4gh": unexpected EOF',
        ErrorKind.WRONG_ENDPOINT,
    ),
    # Not granted in REMS, or (in the May-June waves) a passing listing error.
    ("Error: failed to get files, reason: server returned status 404", ErrorKind.NOT_FOUND),
    ("Error: failed to get files, reason: server returned status 403", ErrorKind.NOT_FOUND),
    # Server-side and transport errors that clear on retry.
    ("Error: server returned status 500", ErrorKind.TRANSIENT),
    ("Error: failed to get files, reason: server returned status 502", ErrorKind.TRANSIENT),
    ("Error: failed to get files, reason: server returned status 503", ErrorKind.TRANSIENT),
    (
        "Error: stream error: stream ID 11; INTERNAL_ERROR; received from peer",
        ErrorKind.TRANSIENT,
    ),
    (
        "Error: read tcp 10.0.0.1:50000->10.0.0.2:443: read: connection reset by peer",
        ErrorKind.TRANSIENT,
    ),
    # Anything unrecognised counts as transient, so it is retried a bounded number of times.
    ("Error: something nobody has seen before", ErrorKind.TRANSIENT),
]


@pytest.mark.parametrize(("output", "kind"), CASES)
def test_classifies_real_sda_cli_errors(output: str, kind: ErrorKind) -> None:
    assert classify(output) is kind


def test_progress_lines_around_the_error_do_not_change_the_kind() -> None:
    output = (
        "Remote server (host_base): download.bp.nbis.se\n"
        "Downloading files from file list\n"
        "IMAGES/IMAGE_a/image-level-0.dcm.c4gh  100% |████████| (5/5 MB)\n"
        "Error: the provided access token has expired, please renew it\n"
    )
    assert classify(output) is ErrorKind.TOKEN_EXPIRED


def test_token_expiry_wins_over_a_transient_error_in_the_same_output() -> None:
    output = (
        "Error: server returned status 503\n"
        "Error: the provided access token has expired, please renew it\n"
    )
    assert classify(output) is ErrorKind.TOKEN_EXPIRED
