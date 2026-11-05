"""`run --metadata-only`: fetch only each dataset's METADATA/, leaving its status alone."""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from harness import World
from bp_downloader.config import DEFAULTS, SITE_SETTINGS
from bp_downloader.sdacli import parse_dataset_listing

DS = "aa-Dataset-abc123-def456"
DS2 = "aa-Dataset-zzz999-yyy888"
NEW = "aa-Dataset-new111-new222"
EXIT_FATAL = 3


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[World]:
    for name in [*DEFAULTS, *SITE_SETTINGS, "C4GH_PASSWORD"]:
        monkeypatch.delenv(name, raising=False)
    yield World(tmp_path)


def metadata_of(files: dict[str, str]) -> dict[str, str]:
    return {path: text for path, text in files.items() if path.startswith("METADATA/")}


def last_run_reason(world: World) -> str:
    runs = world.db().runs(1)
    assert runs
    return runs[0].stop_reason or ""


def test_the_real_dataset_listing_is_parsed() -> None:
    # sda-cli 0.3.5's `list --datasets` output, as captured on 2026-11-05 (first rows).
    output = (
        "The provided access token expires on Wed, 02 Dec 2026 09:53:22 UTC.\n"
        "Remote server (host_base): https://inbox.bp.nbis.se\n"
        "DatasetID                                \t Files \t Size\n"
        "aa-Dataset-dt574f-2fjhh3 \t 13937 \t 2.5 TB\n"
        "aa-Dataset-u5mztq-vccqxd \t 1246 \t 163 GB\n"
    )

    assert parse_dataset_listing(output) == [
        "aa-Dataset-dt574f-2fjhh3", "aa-Dataset-u5mztq-vccqxd"
    ]


def test_only_the_metadata_is_fetched_and_the_status_is_left_alone(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1"]})
    world.queue(DS)

    assert world.cli("run", "--metadata-only", DS) == 0

    assert world.on_disk(DS) == metadata_of(files)  # decrypted, no .c4gh, no images
    assert all("/METADATA/" in path for batch in world.download_lists() for path in batch)
    record = world.dataset(DS)
    assert record.status == "pending"  # still in the backlog for the full download
    states = {path: f.state for path, f in world.files(DS).items()}
    assert states["METADATA/image.xml.c4gh"] == "done"
    assert states["METADATA/dataset.xml.c4gh"] == "done"
    assert states["IMAGES/IMAGE_a/image-level-0.dcm.c4gh"] == "missing"
    assert (record.files_done, record.files_total) == (2, 4)
    assert "metadata only" in last_run_reason(world)


def test_a_later_full_run_reuses_the_metadata(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.queue(DS)
    world.cli("run", "--metadata-only", DS)
    metadata_downloads = len(world.download_lists())

    assert world.run() == 0

    later = world.download_lists()[metadata_downloads:]
    assert later and all("/IMAGES/" in path for batch in later for path in batch)
    assert world.on_disk(DS) == files
    assert world.dataset(DS).status == "complete"


def test_without_ids_every_dataset_the_conf_lists_is_covered(world: World) -> None:
    known = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    new = world.remote_dataset(NEW, {"IMAGE_n": ["n0"]})
    done = world.remote_dataset(DS2, {"IMAGE_b": ["b0"]})
    world.queue(DS)
    world.db().add_dataset(DS2, status="complete")

    assert world.cli("run", "--metadata-only") == 0

    assert world.on_disk(DS) == metadata_of(known)
    assert world.on_disk(NEW) == metadata_of(new)
    assert not (world.datasets_root / DS2).exists()  # complete: its metadata is there already
    assert world.dataset(DS).status == "pending"
    added = world.dataset(NEW)
    # Not in the full-download backlog: a plain run leaves it alone until it is requeued.
    assert added.status == "held"
    assert added.source == "listed"
    assert world.dataset(DS2).status == "complete"
    assert done  # listed, but never fetched


def test_a_plain_run_does_not_pick_up_datasets_found_by_a_metadata_run(world: World) -> None:
    world.remote_dataset(NEW, {"IMAGE_n": ["n0"]})
    world.cli("run", "--metadata-only")
    downloads = len(world.download_lists())

    assert world.run() == 0

    assert len(world.download_lists()) == downloads
    assert world.dataset(NEW).status == "held"


def test_a_failure_leaves_the_status_alone_and_is_reported(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.queue(DS, DS2)  # DS2 is on no endpoint: 404
    world.db().update_dataset(DS2, status="failed", last_error="earlier error")

    assert world.cli("run", "--metadata-only", DS, DS2) == 0

    record = world.dataset(DS2)
    assert (record.status, record.last_error) == ("failed", "earlier error")
    assert world.dataset(DS).status == "pending"
    reason = last_run_reason(world)
    assert "1 with metadata" in reason and "1 without" in reason


def test_an_expired_token_leaves_a_held_dataset_held(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.db().add_dataset(DS, status="held")
    world.scenario["expire_token_after_calls"] = 1

    assert world.cli("run", "--metadata-only", DS) == EXIT_FATAL

    assert world.dataset(DS).status == "held"


def test_existing_metadata_that_fails_its_check_is_left_in_place(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.put(DS, "METADATA/dataset.xml", "an older copy, another size")
    world.queue(DS)

    assert world.cli("run", "--metadata-only", DS) == 0

    on_disk = world.on_disk(DS)
    assert on_disk["METADATA/dataset.xml"] == "an older copy, another size"
    assert "METADATA/dataset.xml.c4gh" not in on_disk
    assert not (world.datasets_root / DS / "_download" / "quarantine").exists()
    assert not any("dataset.xml" in path for batch in world.download_lists() for path in batch)
    assert world.files(DS)["METADATA/dataset.xml.c4gh"].failures == 0
    assert "1 without" in last_run_reason(world)
    assert files  # the full run, not this one, decides what to do with it


def test_metadata_fetched_now_that_fails_its_check_is_fetched_again(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    bad = world.datasets_root / DS / "METADATA/dataset.xml"
    world.fault("decrypt", path=str(bad), content="cut", times=1)
    world.queue(DS)

    assert world.cli("run", "--metadata-only", DS) == 0

    assert world.on_disk(DS) == metadata_of(files)  # only this run's own copy was moved aside


def test_metadata_encrypted_to_another_key_is_left_as_it_is(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.put(DS, "METADATA/dataset.xml", files["METADATA/dataset.xml"], other_key=True)
    world.db().add_dataset(DS, status="held")

    assert world.cli("run", "--metadata-only", DS) == 0

    assert world.dataset(DS).status == "held"
    assert world.on_disk(DS)["METADATA/dataset.xml.c4gh"].startswith("OTHERKEY:")
    out = capsys.readouterr().out
    assert "another key" in out and "requeue" not in out


def test_complete_datasets_named_are_skipped(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.db().add_dataset(DS, status="complete")

    assert world.cli("run", "--metadata-only", DS) == 0

    assert not world.calls_to("list")
    assert "already" in capsys.readouterr().out


def test_another_conf_is_used_through_the_environment(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.queue(DS)
    other = world.root / "secrets" / "s3cmd-download_colleague.conf"
    other.write_text(world.conf.read_text().replace("someone@", "colleague@"))
    monkeypatch.setenv("SDA_CONFIG", str(other))

    assert world.cli("run", "--metadata-only", DS) == 0

    configs = {argv[argv.index("--config") + 1] for argv in world.calls_to("list")}
    assert configs == {str(other)}
    assert world.on_disk(DS) == metadata_of(files)


def test_a_failed_listing_of_the_confs_datasets_stops_the_run(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.fault("list-datasets", error="Error: server returned status 500", times=99)

    assert world.cli("run", "--metadata-only") == EXIT_FATAL

    assert world.db().datasets() == []
    assert not world.download_lists()
    assert "listing" in last_run_reason(world)


def test_the_listing_of_the_confs_datasets_is_retried(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.fault("list-datasets", error="Error: server returned status 500", times=1)

    assert world.cli("run", "--metadata-only") == 0

    assert world.on_disk(DS) == metadata_of(files)
    assert len([a for a in world.calls_to("list") if "--datasets" in a]) == 2


def test_an_empty_listing_of_the_confs_datasets_ends_cleanly(world: World) -> None:
    assert world.cli("run", "--metadata-only") == 0

    assert world.db().datasets() == []
    assert "metadata only" in last_run_reason(world)


def test_the_token_expiry_check_still_runs_first(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.set_token_expiry(time.time() - 60)

    assert world.cli("run", "--metadata-only") == EXIT_FATAL

    assert not world.calls_to("list")  # stopped by the pre-flight check, before any request
