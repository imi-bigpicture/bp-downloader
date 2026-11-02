"""The status database upgrades in place and refuses a newer one."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from harness import World

# The schema as first released (088859a), before versioning: no user_version.
FIRST_SCHEMA = """
CREATE TABLE datasets (
    id TEXT PRIMARY KEY, status TEXT NOT NULL, endpoint TEXT, files_total INTEGER,
    files_done INTEGER NOT NULL DEFAULT 0, files_abandoned INTEGER NOT NULL DEFAULT 0,
    bytes_total INTEGER, key_name TEXT, source TEXT NOT NULL, note TEXT, last_error TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE files (
    dataset_id TEXT NOT NULL REFERENCES datasets(id), remote_path TEXT NOT NULL,
    local_path TEXT NOT NULL, file_id TEXT NOT NULL, size INTEGER NOT NULL,
    state TEXT NOT NULL, verified INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0, last_error TEXT,
    PRIMARY KEY (dataset_id, remote_path)
);
"""

DS = "aa-Dataset-abc123-def456"


@pytest.fixture
def world(tmp_path: Path) -> World:
    return World(tmp_path)


def write_db(path: Path, script: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(script)
    conn.commit()
    conn.close()


def test_a_database_from_before_versioning_is_upgraded_and_keeps_its_rows(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    write_db(
        Path(world.settings["STATUS_DB"]),
        FIRST_SCHEMA
        + f"INSERT INTO datasets (id, status, source, updated_at)"
        f" VALUES ('{DS}', 'complete', 'legacy-csv', '2026-10-12T00:00:00+00:00');",
    )

    assert world.cli("status") == 0

    assert DS in capsys.readouterr().out
    assert world.dataset(DS).status == "complete"
    # Every later migration ran: their columns and tables are in use.
    assert world.cli("prioritise", DS, "3") == 0
    assert world.cli("runs") == 0
    assert world.dataset(DS).priority == 3


def test_a_database_from_a_newer_version_is_refused(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    write_db(Path(world.settings["STATUS_DB"]), FIRST_SCHEMA + "PRAGMA user_version = 999;")

    assert world.cli("status") == 2

    assert "newer" in capsys.readouterr().err


def test_a_new_database_is_created_at_the_latest_schema_version(world: World) -> None:
    from bp_downloader.status import MIGRATIONS

    assert world.cli("status") == 0

    assert world.db().schema_version() == len(MIGRATIONS)


ROW_WITH_AN_ERROR = (
    "INSERT INTO datasets (id, status, source, note, last_error, updated_at)"
    f" VALUES ('{DS}', 'pending', 'import', 'Imported from a list',"
    " 'Failed: Failed to list dataset files from both URLs.', '2026-10-12T00:00:00+00:00');"
)


def schema_at(version: int) -> str:
    from bp_downloader.status import MIGRATIONS

    return "".join(MIGRATIONS[:version]) + f"PRAGMA user_version = {version};"


def test_upgrading_from_version_3_leaves_every_dataset_row_as_it_was(world: World) -> None:
    write_db(Path(world.settings["STATUS_DB"]), schema_at(3) + ROW_WITH_AN_ERROR)

    record = world.dataset(DS)

    assert record.last_error == "Failed: Failed to list dataset files from both URLs."
    assert record.note == "Imported from a list"
    assert world.db().schema_version() == 4


def test_a_database_at_version_4_opens_without_changes(world: World) -> None:
    path = Path(world.settings["STATUS_DB"])
    write_db(path, schema_at(4) + ROW_WITH_AN_ERROR)
    before = path.read_bytes()

    assert world.cli("status") == 0

    assert path.read_bytes() == before
    record = world.dataset(DS)
    assert record.last_error == "Failed: Failed to list dataset files from both URLs."
    assert record.note == "Imported from a list"
