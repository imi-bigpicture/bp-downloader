"""`import` queues the dataset IDs in a file and, on request, adopts folders already on disk."""

from __future__ import annotations

from pathlib import Path

import pytest

from harness import World

A = "aa-Dataset-aaaaa1-bbbbb1"
B = "aa-Dataset-aaaaa2-bbbbb2"
C = "aa-Dataset-aaaaa3-bbbbb3"
FOLDER = "aa-Dataset-fffff1-ggggg1"
FOLDER2 = "aa-Dataset-fffff2-ggggg2"


@pytest.fixture
def world(tmp_path: Path) -> World:
    return World(tmp_path)


def listing(world: World, text: str, *, encoding: str = "utf-8") -> str:
    path = world.root / "ids.txt"
    path.write_bytes(text.encode(encoding))
    return str(path)


def statuses(world: World) -> dict[str, str]:
    return {record.id: record.status for record in world.db().datasets()}


def test_one_id_per_line_queues_every_id_and_skips_blank_lines(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    path = listing(world, f"{A}\n\n{B}\n  \n{C}\n")

    assert world.cli("import", path) == 0

    assert statuses(world) == {A: "pending", B: "pending", C: "pending"}
    assert "3 added, 0 already known, 0 adopted, 0 rejected" in capsys.readouterr().out


def test_a_csv_takes_the_id_from_the_first_column_and_skips_the_header(world: World) -> None:
    path = listing(world, f"﻿DatasetID,Files,SizeGB\n{A},65,12.25\n{B}, 7 ,1\n")

    assert world.cli("import", path) == 0

    assert statuses(world) == {A: "pending", B: "pending"}


def test_the_status_export_can_be_imported_again(world: World) -> None:
    world.queue(A, B)
    export = world.root / "export.csv"
    assert world.cli("export-csv", str(export)) == 0
    (world.root / "other").mkdir()
    other = World(world.root / "other")  # an empty site

    assert other.cli("import", str(export)) == 0

    assert statuses(other) == {A: "pending", B: "pending"}


def test_an_id_the_database_knows_is_left_alone_and_counted(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.queue(A)
    world.db().update_dataset(A, status="complete")
    capsys.readouterr()

    assert world.cli("import", listing(world, f"{A}\n{B}\n")) == 0

    assert statuses(world) == {A: "complete", B: "pending"}
    assert "1 added, 1 already known, 0 adopted, 0 rejected" in capsys.readouterr().out


def test_a_line_that_is_not_an_id_refuses_the_whole_file_and_names_the_line(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    path = listing(world, f"{A}\n\n{B}\nnot-a-dataset\n{C}\n")

    assert world.cli("import", path) == 1

    assert world.db().datasets() == []
    err = capsys.readouterr().err
    assert "line 4" in err and "not-a-dataset" in err and "nothing written" in err


def test_only_the_first_line_may_be_a_header(world: World, capsys: pytest.CaptureFixture[str]) -> None:
    assert world.cli("import", listing(world, f"{A}\nDatasetID\n")) == 1

    assert world.db().datasets() == []
    assert "line 2" in capsys.readouterr().err


def test_adopt_holds_unknown_folders_and_leaves_known_ones(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.queue(A)
    world.put(A, "f.dcm", "x")  # known: untouched
    world.put(FOLDER, "f.dcm", "x")
    (world.datasets_root / FOLDER2).mkdir()
    (world.datasets_root / "scratch").mkdir()  # not a dataset folder
    capsys.readouterr()

    assert world.cli("import", "--adopt", listing(world, f"{B}\n")) == 0

    assert statuses(world) == {A: "pending", B: "pending", FOLDER: "held", FOLDER2: "held"}
    assert world.dataset(FOLDER).source == "adopted"
    assert world.dataset(B).source == "import"
    assert "1 added, 0 already known, 2 adopted, 0 rejected" in capsys.readouterr().out
    assert world.on_disk(FOLDER) == {"f.dcm": "x"}  # no file was touched


def test_a_folder_named_in_the_file_is_queued_not_held(world: World) -> None:
    world.put(FOLDER, "f.dcm", "x")

    assert world.cli("import", "--adopt", listing(world, f"{FOLDER}\n")) == 0

    assert statuses(world) == {FOLDER: "pending"}


def test_without_adopt_folders_are_ignored_and_the_dataset_root_is_not_needed(
    world: World,
) -> None:
    world.put(FOLDER, "f.dcm", "x")
    del world.settings["DATASETS_ROOT"]

    assert world.cli("import", listing(world, f"{A}\n")) == 0

    assert statuses(world) == {A: "pending"}


def test_adopt_needs_the_dataset_root(world: World, capsys: pytest.CaptureFixture[str]) -> None:
    del world.settings["DATASETS_ROOT"]

    assert world.cli("import", "--adopt", listing(world, f"{A}\n")) == 2

    assert "DATASETS_ROOT" in capsys.readouterr().err
    assert world.db().datasets() == []


def test_a_missing_file_is_refused(world: World, capsys: pytest.CaptureFixture[str]) -> None:
    assert world.cli("import", str(world.root / "nope.txt")) == 1

    assert "nothing written" in capsys.readouterr().err


def test_the_old_migration_command_is_gone(world: World) -> None:
    with pytest.raises(SystemExit):
        world.cli("migrate-csv", "x.csv")


def test_add_and_import_agree_on_what_an_id_is(world: World) -> None:
    assert world.cli("add", "aa-Dataset-short") == 1
    assert world.db().datasets() == []
