"""Settings, read from the environment, then `.env`, then the defaults below.

A variable set in the environment always wins, so a one-off run can override `.env`
without editing it. Relative paths resolve against the repo folder (the folder holding
`.env`), not the current directory.

Site settings (SITE_SETTINGS) have no default: each site says where its own conf, keys,
data and status database live, so a run can never start against another site's paths.
A missing one is a configuration error naming it.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

DEFAULTS: dict[str, str] = {
    "SDA_CLI": "./sda-cli",
    "SDA_CLI_VERSION": "0.3.5",
    "PRIVATE_KEY": "",  # empty: derived from PUBLIC_KEY (.pub.pem -> .sec.pem)
    # Since the 2026-06-01 release this one endpoint reads every storage backend, so the old
    # second endpoint (download2) was retired. More can still be listed, space-separated.
    "DOWNLOAD_URLS": "https://download.bp.nbis.se",
    # v1 is sda-cli's default too. Passed explicitly so a change of default can't switch
    # API under a running download. download-v2 has no production address yet.
    "API_VERSION": "v1",
    # Off by default. Set it to a mount point and the run refuses to start unless that
    # is mounted: for a network mount with `nofail`, where a failed mount would make
    # downloads fill the local disk.
    "REQUIRE_MOUNT": "",
    "DOWNLOAD_BATCH_SIZE": "50",
    # Far below ARG_MAX: one call over ~15k files failed with `Argument list too long`.
    "DECRYPT_BATCH_SIZE": "500",
    "MAX_FILE_FAILURES": "5",
    "MAX_CONSECUTIVE_FAILURES": "10",
    "MAX_ENDPOINT_SWITCHES": "3",
    "LISTING_ATTEMPTS": "3",
    "BASE_SLEEP": "30",
    "MAX_SLEEP": "600",
    "MAX_RUNTIME_HOURS": "72",
    # Keep this much free on the share (GB = 10^9 bytes), for whatever else it holds.
    "MIN_FREE_GB": "1000",
    # Write the status table here at the end of every run, however it ends. Empty: don't.
    # Also the database's only backup, unless the site backs up the local disk.
    "EXPORT_CSV": "",
    # Stop this many hours before the download token expires, between calls rather than in
    # the middle of a batch. A new token needs a manual login with MFA.
    "TOKEN_MARGIN_HOURS": "6",
}

# Settings with no default: they name this site's own files and folders.
# SDA_CONFIG: the download conf. PUBLIC_KEY (and its .sec.pem pair): the Crypt4GH keys.
# C4GH_PASSWORD_FILE: the key's passphrase file (ignored when C4GH_PASSWORD is set).
# DATASETS_ROOT: one folder per dataset. STATUS_DB: the status database, on local disk
# because SQLite locking is not safe over CIFS.
SITE_SETTINGS: frozenset[str] = frozenset(
    {"SDA_CONFIG", "PUBLIC_KEY", "C4GH_PASSWORD_FILE", "DATASETS_ROOT", "STATUS_DB"}
)

# Stands in for a site setting that a command doesn't need and the site left unset.
UNSET = Path("<unset>")


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Config:
    sda_cli: Path
    sda_cli_version: str
    sda_config: Path
    public_key: Path
    private_key: Path
    c4gh_password_env: str | None
    c4gh_password_file: Path | None
    download_urls: tuple[str, ...]
    api_version: str
    datasets_root: Path
    require_mount: Path | None
    status_db: Path
    download_batch_size: int
    decrypt_batch_size: int
    max_file_failures: int
    max_consecutive_failures: int
    max_endpoint_switches: int
    listing_attempts: int
    base_sleep: float
    max_sleep: float
    max_runtime_hours: float
    min_free_bytes: int
    export_csv: Path | None
    token_margin_hours: float

    def __repr__(self) -> str:  # keep the passphrase out of tracebacks and logs
        return f"Config(datasets_root={self.datasets_root}, status_db={self.status_db})"

    def c4gh_password(self) -> str:
        """The private key's passphrase, from C4GH_PASSWORD or C4GH_PASSWORD_FILE."""
        if self.c4gh_password_env is not None:
            return self.c4gh_password_env
        if self.c4gh_password_file is None:
            raise ConfigError("Set C4GH_PASSWORD or C4GH_PASSWORD_FILE")
        try:
            return self.c4gh_password_file.read_text().rstrip("\n")
        except OSError as exc:
            raise ConfigError(f"Cannot read C4GH_PASSWORD_FILE: {exc.strerror}") from None


def read_env_file(path: Path) -> dict[str, str]:
    """Parse KEY=VALUE lines. Supports comments, blank lines, `export ` and quotes."""
    values: dict[str, str] = {}
    for number, raw in enumerate(path.read_text().splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.removeprefix("export ").strip()
        key, sep, value = line.partition("=")
        if not sep or not key.strip():
            raise ConfigError(f"{path}:{number}: expected KEY=VALUE")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def load_config(
    environ: Mapping[str, str] = os.environ,
    env_file: Path | None = None,
    needs: Iterable[str] = SITE_SETTINGS,
) -> Config:
    """Build the settings. `env_file` defaults to `.env` in the repo folder.

    `needs` are the site settings the caller will use; any of them missing is an error.
    A site setting outside `needs` that is missing reads as UNSET.
    """
    if env_file is None:
        env_file = Path(__file__).resolve().parents[2] / ".env"
    base_dir = env_file.parent
    file_values = read_env_file(env_file) if env_file.is_file() else {}

    unknown = sorted(set(file_values) - set(DEFAULTS) - SITE_SETTINGS - {"C4GH_PASSWORD"})
    if unknown:
        raise ConfigError(f"Unknown setting(s) in {env_file}: {', '.join(unknown)}")
    if "C4GH_PASSWORD" in file_values:
        raise ConfigError(
            f"Don't put C4GH_PASSWORD in {env_file}; point C4GH_PASSWORD_FILE at a mode-600 file"
        )

    def get(name: str) -> str:
        if name in environ:
            return environ[name]
        if name in file_values:
            return file_values[name]
        return DEFAULTS.get(name, "")

    required = set(needs) & SITE_SETTINGS
    if "C4GH_PASSWORD" in environ:
        required.discard("C4GH_PASSWORD_FILE")
    unset = sorted(n for n in required if not get(n))
    if unset:
        raise ConfigError(
            f"Missing site setting(s): {', '.join(unset)}. Set them in {env_file} "
            "or the environment; .env.example shows each"
        )

    def path(name: str) -> Path:
        value = get(name)
        if not value:
            if name in SITE_SETTINGS:
                return UNSET
            raise ConfigError(f"{name} must not be empty")
        return (base_dir / Path(value).expanduser()).resolve()

    def optional_path(name: str) -> Path | None:
        return path(name) if get(name) else None

    def integer(name: str) -> int:
        value = get(name)
        try:
            number = int(value)
        except ValueError:
            raise ConfigError(f"{name} must be a whole number (got {value!r})") from None
        if number < 1:
            raise ConfigError(f"{name} must be at least 1 (got {number})")
        return number

    def number(name: str) -> float:
        value = get(name)
        try:
            result = float(value)
        except ValueError:
            raise ConfigError(f"{name} must be a number (got {value!r})") from None
        if result < 0:
            raise ConfigError(f"{name} must not be negative (got {value})")
        return result

    public_key = path("PUBLIC_KEY")
    if get("PRIVATE_KEY"):
        private_key = path("PRIVATE_KEY")
    elif public_key == UNSET:
        private_key = UNSET
    elif public_key.name.endswith(".pub.pem"):
        private_key = public_key.with_name(public_key.name.removesuffix(".pub.pem") + ".sec.pem")
    else:
        raise ConfigError("PUBLIC_KEY must end in .pub.pem, or set PRIVATE_KEY")

    urls = tuple(get("DOWNLOAD_URLS").split())
    if not urls:
        raise ConfigError("DOWNLOAD_URLS must name at least one endpoint")
    api_version = get("API_VERSION")
    if api_version not in ("v1", "v2"):
        raise ConfigError(f"API_VERSION must be v1 or v2 (got {api_version!r})")

    return Config(
        sda_cli=path("SDA_CLI"),
        sda_cli_version=get("SDA_CLI_VERSION").removeprefix("v"),
        sda_config=path("SDA_CONFIG"),
        public_key=public_key,
        private_key=private_key,
        c4gh_password_env=environ.get("C4GH_PASSWORD"),
        c4gh_password_file=optional_path("C4GH_PASSWORD_FILE"),
        download_urls=urls,
        api_version=api_version,
        datasets_root=path("DATASETS_ROOT"),
        require_mount=optional_path("REQUIRE_MOUNT"),
        status_db=path("STATUS_DB"),
        download_batch_size=integer("DOWNLOAD_BATCH_SIZE"),
        decrypt_batch_size=integer("DECRYPT_BATCH_SIZE"),
        max_file_failures=integer("MAX_FILE_FAILURES"),
        max_consecutive_failures=integer("MAX_CONSECUTIVE_FAILURES"),
        max_endpoint_switches=integer("MAX_ENDPOINT_SWITCHES"),
        listing_attempts=integer("LISTING_ATTEMPTS"),
        base_sleep=number("BASE_SLEEP"),
        max_sleep=number("MAX_SLEEP"),
        max_runtime_hours=number("MAX_RUNTIME_HOURS"),
        min_free_bytes=int(number("MIN_FREE_GB") * 1e9),
        export_csv=optional_path("EXPORT_CSV"),
        token_margin_hours=number("TOKEN_MARGIN_HOURS"),
    )
