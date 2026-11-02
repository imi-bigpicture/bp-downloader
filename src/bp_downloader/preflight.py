"""Checks that run before a run touches the network or the share.

Each one stands for a way an unattended run went wrong before, or would have: downloading
into an unmounted network share (which fills the local disk), an sda-cli whose flags differ
from the ones this code passes, and a token that expired days earlier.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Config, ConfigError
from .runner import EXIT_CONFIG, EXIT_FATAL
from .sdacli import SdaCli


class PreflightError(Exception):
    def __init__(self, message: str, exit_code: int = EXIT_CONFIG) -> None:
        super().__init__(message)
        self.exit_code = exit_code


@dataclass(frozen=True)
class Ready:
    password: str
    token_expires: datetime
    access_key: str
    """The conf's user, which is what REMS grants access to. Not a secret; the token is."""


def conf_value(conf: Path, name: str) -> str | None:
    """A `name = value` line of an s3cmd conf."""
    value = None
    for line in conf.read_text().splitlines():
        key, sep, rest = line.partition("=")
        if sep and key.strip() == name:
            value = rest.strip()
    return value or None


def token_expiry(conf: Path) -> datetime:
    """When the conf's access token expires, from its `exp` claim. Never prints the token."""
    token = conf_value(conf, "access_token")
    if not token:
        raise PreflightError(f"SDA_CONFIG has no access_token: {conf}")
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return datetime.fromtimestamp(int(claims["exp"]), tz=timezone.utc)
    except (IndexError, KeyError, ValueError, binascii.Error):
        raise PreflightError(f"SDA_CONFIG's access_token has no readable expiry: {conf}") from None


def check(config: Config, cli: SdaCli) -> Ready:
    if config.require_mount is not None:
        if not os.path.ismount(config.require_mount):
            raise PreflightError(
                f"REQUIRE_MOUNT {config.require_mount} is not a mount point. Check"
                f" `findmnt {config.require_mount}`: unmounted, downloads would fill the local disk"
            )
        if not config.datasets_root.is_relative_to(config.require_mount):
            raise PreflightError(
                f"DATASETS_ROOT {config.datasets_root} is not on {config.require_mount}"
            )
    if not config.datasets_root.is_dir():
        raise PreflightError(f"DATASETS_ROOT is not a folder: {config.datasets_root}")
    free = shutil.disk_usage(config.datasets_root).free
    if free < config.min_free_bytes:
        raise PreflightError(
            f"only {free / 1e9:,.1f} GB free on {config.datasets_root}, below MIN_FREE_GB"
            f" ({config.min_free_bytes / 1e9:,.0f} GB)"
        )

    for name, path in (
        ("SDA_CONFIG", config.sda_config),
        ("PUBLIC_KEY", config.public_key),
        ("PRIVATE_KEY", config.private_key),
    ):
        if not os.access(path, os.R_OK):
            raise PreflightError(f"{name} is not readable: {path}")
    if not os.access(config.sda_cli, os.X_OK):
        raise PreflightError(f"SDA_CLI is not executable: {config.sda_cli}")
    try:
        password = config.c4gh_password()
    except ConfigError as exc:
        raise PreflightError(str(exc)) from None

    try:
        version = cli.version()
    except RuntimeError as exc:
        raise PreflightError(str(exc)) from None
    if version != config.sda_cli_version:
        raise PreflightError(
            f"sda-cli is {version}, but this code is written for {config.sda_cli_version}"
            " (SDA_CLI_VERSION). 0.3.4 renamed --continue to --ignore-existing; check the"
            " flags before changing the pin"
        )

    expires = token_expiry(config.sda_config)
    now = datetime.now(timezone.utc)
    renew = "Fetch a new s3cmd-download.conf (REMS / LS Login, with MFA) and put it at SDA_CONFIG"
    if expires <= now:
        raise PreflightError(
            f"The download token expired on {expires:%Y-%m-%d %H:%M} UTC. {renew}", EXIT_FATAL
        )
    if expires - timedelta(hours=config.token_margin_hours) <= now:
        raise PreflightError(
            f"The download token expires on {expires:%Y-%m-%d %H:%M} UTC, within"
            f" TOKEN_MARGIN_HOURS ({config.token_margin_hours:g} h). {renew}",
            EXIT_FATAL,
        )
    access_key = conf_value(config.sda_config, "access_key")
    if not access_key:
        raise PreflightError(f"SDA_CONFIG has no access_key: {config.sda_config}")
    return Ready(password, expires, access_key)
