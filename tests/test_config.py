"""Settings come from the environment, then .env, then built-in defaults."""

from pathlib import Path

import pytest

from bp_downloader.config import DEFAULTS, SITE_SETTINGS, ConfigError, load_config

SITE = {
    "SDA_CONFIG": "/site/secrets/s3cmd.conf",
    "PUBLIC_KEY": "/site/secrets/key.pub.pem",
    "C4GH_PASSWORD_FILE": "/site/secrets/pass",
    "DATASETS_ROOT": "/site/datasets",
    "STATUS_DB": "/site/state/status.db",
}


def write_env(tmp_path: Path, text: str) -> Path:
    env_file = tmp_path / ".env"
    env_file.write_text(text)
    return env_file


def test_environment_beats_env_file_beats_default(tmp_path: Path) -> None:
    env_file = write_env(tmp_path, "DOWNLOAD_BATCH_SIZE=20\nMAX_FILE_FAILURES=7\n")

    config = load_config({**SITE, "DOWNLOAD_BATCH_SIZE": "10"}, env_file)

    assert config.download_batch_size == 10  # environment
    assert config.max_file_failures == 7  # .env
    assert config.decrypt_batch_size == 500  # default


def test_relative_paths_resolve_against_the_env_file_folder_not_the_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "workspace" / "repo"
    repo.mkdir(parents=True)
    env_file = write_env(repo, (
        "STATUS_DB=../state/status.db\nSDA_CONFIG=../secrets/s3cmd-download.conf\n"
        "PUBLIC_KEY=../secrets/key.pub.pem\nC4GH_PASSWORD_FILE=../secrets/pass\n"
        "DATASETS_ROOT=../data\n"
    ))
    monkeypatch.chdir(tmp_path)

    config = load_config({}, env_file)

    assert config.status_db == tmp_path / "workspace" / "state" / "status.db"
    assert config.sda_config == tmp_path / "workspace" / "secrets" / "s3cmd-download.conf"


def test_private_key_is_derived_from_the_public_key(tmp_path: Path) -> None:
    config = load_config({**SITE, "PUBLIC_KEY": "/keys/site.pub.pem"}, tmp_path / ".env")

    assert config.private_key == Path("/keys/site.sec.pem")


def test_public_key_without_pub_pem_needs_an_explicit_private_key(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="PUBLIC_KEY"):
        load_config({**SITE, "PUBLIC_KEY": "/keys/mykey.pem"}, tmp_path / ".env")

    config = load_config(
        {**SITE, "PUBLIC_KEY": "/keys/mykey.pem", "PRIVATE_KEY": "/keys/secret.pem"}, tmp_path / ".env"
    )
    assert config.private_key == Path("/keys/secret.pem")


def test_env_file_accepts_comments_export_and_quotes(tmp_path: Path) -> None:
    env_file = write_env(
        tmp_path,
        "# endpoints\n"
        "\n"
        'export DOWNLOAD_URLS="https://a.example https://b.example"\n'
        "API_VERSION='v2'\n",
    )

    config = load_config(SITE, env_file)

    assert config.download_urls == ("https://a.example", "https://b.example")
    assert config.api_version == "v2"


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("MAX_FILE_FAILURES", "five"),
        ("DOWNLOAD_BATCH_SIZE", "0"),
        ("BASE_SLEEP", "-1"),
        ("API_VERSION", "v3"),
        ("DOWNLOAD_URLS", "  "),
    ],
)
def test_invalid_values_are_rejected_naming_the_variable(
    tmp_path: Path, variable: str, value: str
) -> None:
    with pytest.raises(ConfigError, match=variable):
        load_config({**SITE, variable: value}, tmp_path / ".env")


def test_unknown_names_in_env_file_are_rejected(tmp_path: Path) -> None:
    env_file = write_env(tmp_path, "MAX_RETRIES=3\n")  # an old name; it no longer exists

    with pytest.raises(ConfigError, match="MAX_RETRIES"):
        load_config({}, env_file)


def test_passphrase_is_refused_in_env_file(tmp_path: Path) -> None:
    env_file = write_env(tmp_path, "C4GH_PASSWORD=hunter2\n")

    with pytest.raises(ConfigError, match="C4GH_PASSWORD_FILE") as excinfo:
        load_config({}, env_file)
    assert "hunter2" not in str(excinfo.value)


def test_passphrase_comes_from_environment_or_from_file(tmp_path: Path) -> None:
    pass_file = tmp_path / "c4gh-pass"
    pass_file.write_text("from-file\n")

    from_file = load_config({**SITE, "C4GH_PASSWORD_FILE": str(pass_file)}, tmp_path / ".env")
    from_env = load_config(
        {**SITE, "C4GH_PASSWORD_FILE": str(pass_file), "C4GH_PASSWORD": "from-env"}, tmp_path / ".env"
    )

    assert from_file.c4gh_password() == "from-file"
    assert from_env.c4gh_password() == "from-env"
    assert "from-env" not in repr(from_env)


def test_the_default_endpoint_is_the_one_that_serves_every_dataset(tmp_path: Path) -> None:
    # Since the 2026-06-01 release one endpoint reads every storage backend; download2 is gone.
    config = load_config(SITE, write_env(tmp_path, ""))

    assert config.download_urls == ("https://download.bp.nbis.se",)


def test_no_site_setting_has_a_default() -> None:
    assert SITE_SETTINGS.isdisjoint(DEFAULTS)
    assert SITE_SETTINGS == {
        "SDA_CONFIG", "PUBLIC_KEY", "C4GH_PASSWORD_FILE", "DATASETS_ROOT", "STATUS_DB"
    }


def test_missing_site_settings_are_all_named_in_one_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as excinfo:
        load_config({}, tmp_path / ".env")

    for name in SITE_SETTINGS:
        assert name in str(excinfo.value)


def test_site_settings_can_come_from_the_env_file(tmp_path: Path) -> None:
    env_file = write_env(tmp_path, "".join(f"{k}={v}\n" for k, v in SITE.items()))

    config = load_config({}, env_file)

    assert config.datasets_root == Path("/site/datasets")


def test_a_command_that_needs_only_the_status_database_asks_for_only_that(
    tmp_path: Path,
) -> None:
    env = {"STATUS_DB": "/site/state/status.db"}
    config = load_config(env, tmp_path / ".env", needs={"STATUS_DB"})

    assert config.status_db == Path("/site/state/status.db")
    with pytest.raises(ConfigError, match="STATUS_DB"):
        load_config({}, tmp_path / ".env", needs={"STATUS_DB"})


def test_the_mount_check_is_off_unless_configured(tmp_path: Path) -> None:
    assert load_config(SITE, tmp_path / ".env").require_mount is None
    on = load_config({**SITE, "REQUIRE_MOUNT": "/mnt/data"}, tmp_path / ".env")
    assert on.require_mount == Path("/mnt/data")


def test_env_example_documents_every_setting_and_site_ones_are_placeholders(
    tmp_path: Path,
) -> None:
    example = Path(__file__).resolve().parents[1] / ".env.example"
    text = example.read_text()
    settings = [
        line.removeprefix("# ")
        for line in text.splitlines()
        if line.startswith("# ") and "=" in line.split()[1]
    ]
    names = {line.split("=", 1)[0] for line in settings}
    assert names == set(DEFAULTS) | SITE_SETTINGS

    # Defaults listed in the example are the real defaults.
    env_file = write_env(tmp_path, "\n".join(s for s in settings if s.split("=", 1)[0] in DEFAULTS) + "\n")
    assert vars(load_config(SITE, env_file)) == vars(load_config(SITE, tmp_path / "absent.env"))

    # Site settings show a placeholder, not any real site's value.
    for name in SITE_SETTINGS:
        line = next(s for s in settings if s.startswith(name + "="))
        assert "/path/to/" in line
    for forbidden in ("ru" + "mc", "/mnt/download", "../secrets", "../state"):
        assert forbidden not in text.lower()


def test_the_passphrase_file_is_not_required_when_the_passphrase_is_in_the_environment(
    tmp_path: Path,
) -> None:
    without_file = {k: v for k, v in SITE.items() if k != "C4GH_PASSWORD_FILE"}

    config = load_config({**without_file, "C4GH_PASSWORD": "pw"}, tmp_path / ".env")
    assert config.c4gh_password() == "pw"

    with pytest.raises(ConfigError, match="C4GH_PASSWORD_FILE"):
        load_config(without_file, tmp_path / ".env")


def test_an_empty_site_setting_counts_as_missing(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="SDA_CONFIG"):
        load_config({**SITE, "SDA_CONFIG": ""}, tmp_path / ".env")
