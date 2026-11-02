"""End-to-end runs of the downloader against a fake sda-cli, temp folders and a temp DB."""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from harness import PRIMARY, SECONDARY, World
from bp_downloader.cli import main
from bp_downloader.config import DEFAULTS, SITE_SETTINGS

DS = "aa-Dataset-abc123-def456"
DS2 = "aa-Dataset-zzz999-yyy888"
EXIT_FATAL = 3


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[World]:
    for name in [*DEFAULTS, *SITE_SETTINGS, "C4GH_PASSWORD"]:
        monkeypatch.delenv(name, raising=False)
    yield World(tmp_path)


def test_a_dataset_downloads_decrypts_and_verifies(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1"], "IMAGE_b": ["b0"]})
    world.queue(DS)

    assert world.run() == 0

    assert world.on_disk(DS) == files  # decrypted, and no .c4gh left
    record = world.dataset(DS)
    assert record.status == "complete"
    assert (record.files_total, record.files_done, record.files_abandoned) == (5, 5, 0)
    logs = list((world.datasets_root / DS / "_download").glob("download_*.log"))
    assert len(logs) == 1
    # The API version is passed explicitly, so a change of sda-cli's default can't switch it.
    network_calls = world.calls_to("list") + world.calls_to("download")
    assert all(argv[argv.index("--api-version") + 1] == "v1" for argv in network_calls)


def test_metadata_is_downloaded_before_any_image(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1"], "IMAGE_b": ["b0"]})
    world.queue(DS)

    world.run()

    first, *rest = world.download_lists()
    assert first and all("/METADATA/" in path for path in first)
    assert rest and all("/IMAGES/" in path for batch in rest for path in batch)


def test_a_checksum_mismatch_is_quarantined_and_fetched_again(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"], "IMAGE_b": ["b0"]})
    bad = world.datasets_root / DS / "IMAGES/IMAGE_a/image-level-0.dcm"
    # Same size, different bytes: only the checksum can tell.
    world.fault("decrypt", path=str(bad), content="a9", times=1)
    world.queue(DS)

    assert world.run() == 0

    assert world.on_disk(DS) == files
    assert world.dataset(DS).status == "complete"
    record = world.files(DS)["IMAGES/IMAGE_a/image-level-0.dcm.c4gh"]
    assert (record.failures, record.verified) == (1, True)
    assert "checksum" in (record.last_error or "")
    # Nothing is deleted on a mismatch: the bad copy is kept aside for inspection.
    quarantined = list((world.datasets_root / DS / "_download" / "quarantine").rglob("*.dcm"))
    assert [path.read_text() for path in quarantined] == ["a9"]


def test_an_expired_token_stops_the_whole_run_without_retrying(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.remote_dataset(DS2, {"IMAGE_b": ["b0"]})
    world.scenario["expire_token_after_calls"] = 1  # the first listing works, then expiry
    world.queue(DS, DS2)

    assert world.run() == EXIT_FATAL

    assert len(world.calls_to("download")) == 1  # no retries on an expired token
    assert DS2 not in str(world.calls())  # the next dataset is never started
    record = world.dataset(DS)
    assert record.status == "pending"  # the dataset is not to blame
    assert "expired" in (record.last_error or "")
    assert all(f.failures == 0 for f in world.files(DS).values())
    assert world.dataset(DS2).status == "pending"


def test_listing_falls_back_to_the_second_endpoint(world: World) -> None:
    world.scenario["endpoints"][SECONDARY] = {}
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]}, on=SECONDARY)
    world.queue(DS)

    assert world.run() == 0

    assert world.urls_used("list") == [PRIMARY, SECONDARY]
    assert set(world.urls_used("download")) == {SECONDARY}
    assert world.on_disk(DS) == files
    assert world.dataset(DS).endpoint == SECONDARY


def test_a_dead_endpoint_is_tried_once_per_run_not_once_per_dataset(world: World) -> None:
    # download2 is dead (the default scene). Neither dataset is on the live endpoint.
    world.queue(DS, DS2)

    assert world.run() == 0

    assert world.urls_used("list").count(SECONDARY) == 1
    for dataset_id in (DS, DS2):
        record = world.dataset(dataset_id)
        assert record.status == "no_access"
        assert "404" in (record.last_error or "")


def test_unexpected_eof_switches_to_the_endpoint_that_holds_the_data(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    # Listed on the primary, but its files live behind the other endpoint (NBISweden/sda-cli#695).
    world.scenario["endpoints"][PRIMARY]["serves"] = []
    world.scenario["endpoints"][SECONDARY] = {"serves": [DS]}
    world.queue(DS)

    assert world.run() == 0

    assert world.urls_used("download")[:2] == [PRIMARY, SECONDARY]
    assert world.on_disk(DS) == files
    # Asking the wrong endpoint is not the file's fault.
    assert all(f.failures == 0 for f in world.files(DS).values())


def test_endpoint_switching_is_bounded(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.scenario["endpoints"][PRIMARY]["serves"] = []  # no endpoint serves the files
    world.scenario["endpoints"][SECONDARY] = {}
    world.settings["MAX_ENDPOINT_SWITCHES"] = "3"
    world.settings["MAX_CONSECUTIVE_FAILURES"] = "100"
    world.queue(DS)

    assert world.run() == 0

    assert world.urls_used("download") == [PRIMARY, SECONDARY, PRIMARY, SECONDARY]
    record = world.dataset(DS)
    assert record.status == "failed"
    assert "unexpected EOF" in (record.last_error or "")


def test_unexpected_eof_from_the_endpoint_already_delivering_is_retried_there(
    world: World,
) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1", "a2"]})
    world.scenario["endpoints"][SECONDARY] = {"serves": [DS]}
    # A dropped connection mid-download: a0 has just landed from the primary.
    world.fault(
        "download", url=PRIMARY, path="IMAGES/IMAGE_a/image-level-1.dcm",
        error="Error: unexpected EOF", times=2,
    )
    world.queue(DS)

    assert world.run() == 0

    assert set(world.urls_used("download")) == {PRIMARY}
    assert world.on_disk(DS) == files
    # A dropped connection is not the file's fault.
    assert all(f.failures == 0 for f in world.files(DS).values())


def test_unexpected_eof_with_no_live_endpoint_left_is_retried_not_given_up(
    world: World,
) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1"]})
    world.scenario["endpoints"][SECONDARY] = {"serves": [DS]}
    tls_error = (
        'Error: failed to get response, reason: Get "https://download2.bp.nbis.se/s3/x": '
        "tls: failed to verify certificate: x509: certificate is valid for ingress.local"
    )
    world.fault("download", url=SECONDARY, error=tls_error, times=99)
    # EOF before anything lands: the switch finds the other endpoint dead, and the
    # primary drops the connection once more before it delivers.
    world.fault("download", url=PRIMARY, error="Error: unexpected EOF", times=2)
    world.queue(DS)

    assert world.run() == 0

    assert world.dataset(DS).status == "complete"
    assert world.on_disk(DS) == files
    assert world.urls_used("download").count(SECONDARY) == 1


def test_an_endpoint_that_dies_mid_download_is_dropped_for_the_run(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1"]})
    world.scenario["endpoints"][PRIMARY]["serves"] = []
    world.scenario["endpoints"][SECONDARY] = {"serves": [DS]}
    world.remote_dataset(DS2, {"IMAGE_b": ["b0"]})
    tls_error = (
        'Error: failed to get response, reason: Get "https://download2.bp.nbis.se/s3/x": '
        "tls: failed to verify certificate: x509: certificate is valid for ingress.local"
    )
    world.fault("download", url=SECONDARY, error=tls_error, times=99)
    world.queue(DS, DS2)

    assert world.run() == 0

    assert world.urls_used("download").count(SECONDARY) == 1
    # The primary is the only live endpoint left: retried until MAX_CONSECUTIVE_FAILURES.
    assert world.dataset(DS).status == "failed"
    assert "failed calls in a row" in (world.dataset(DS).last_error or "")
    assert world.dataset(DS2).status == "complete"


def test_a_transient_500_is_retried_and_counted_against_that_file_only(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1", "a2"]})
    world.fault(
        "download", path="IMAGES/IMAGE_a/image-level-1.dcm",
        error="Error: server returned status 500", times=2,
    )
    world.queue(DS)

    assert world.run() == 0

    assert world.on_disk(DS) == files
    failures = {path: f.failures for path, f in world.files(DS).items()}
    assert failures["IMAGES/IMAGE_a/image-level-1.dcm.c4gh"] == 2
    assert sum(failures.values()) == 2


def test_a_file_that_keeps_failing_is_abandoned_counting_across_runs(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1", "a2"]})
    broken = "IMAGES/IMAGE_a/image-level-0.dcm"
    world.fault("download", path=broken, error="Error: server returned status 500", times=99)
    world.settings["MAX_FILE_FAILURES"] = "5"
    world.settings["MAX_CONSECUTIVE_FAILURES"] = "3"
    world.queue(DS)

    world.run()  # three failures in a row: this run gives the dataset up

    assert world.dataset(DS).status == "failed"
    assert world.files(DS)[broken + ".c4gh"].failures == 3

    world.run()  # two more: abandoned, and the rest of the dataset comes in

    record = world.dataset(DS)
    assert record.status == "incomplete"
    assert (record.files_done, record.files_abandoned) == (4, 1)
    assert broken not in world.on_disk(DS)
    assert "IMAGES/IMAGE_a/image-level-2.dcm" in world.on_disk(DS)

    world.run()  # an incomplete dataset is not picked up again by itself
    assert world.dataset(DS).status == "incomplete"

    assert world.cli("requeue", DS) == 0
    assert world.dataset(DS).status == "pending"
    assert world.files(DS)[broken + ".c4gh"].failures == 0


def test_decryption_runs_in_bounded_batches(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": [f"a{i}" for i in range(7)]})
    world.settings["DECRYPT_BATCH_SIZE"] = "3"
    world.settings["DOWNLOAD_BATCH_SIZE"] = "10"
    world.queue(DS)

    assert world.run() == 0

    sizes = [len([a for a in argv if a.endswith(".c4gh")]) for argv in world.calls_to("decrypt")]
    assert max(sizes) <= 3
    assert sum(sizes) == len(files)
    assert world.on_disk(DS) == files


def test_files_already_on_disk_as_x_or_x_c4gh_are_not_downloaded_again(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1", "a2"]})
    for local in ("METADATA/image.xml", "METADATA/dataset.xml"):
        world.put(DS, local, files[local], encrypted=True)  # downloaded, never decrypted
    world.put(DS, "IMAGES/IMAGE_a/image-level-0.dcm", "a0")  # decrypted
    world.put(DS, "IMAGES/IMAGE_a/image-level-1.dcm", "a1", encrypted=True)
    world.queue(DS)

    assert world.run() == 0

    fetched = [path for batch in world.download_lists() for path in batch]
    assert [path.split("/", 1)[1] for path in fetched] == ["IMAGES/IMAGE_a/image-level-2.dcm.c4gh"]
    assert world.on_disk(DS) == files
    assert all(f.verified for path, f in world.files(DS).items() if path.startswith("IMAGES/"))


def test_a_truncated_decryption_beside_its_c4gh_is_decrypted_again(world: World) -> None:
    # An interrupted decryption leaves a short `x` next to `x.c4gh`. sda-cli would skip it
    # as "already decrypted", and with --clean then delete the only good copy.
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0-full-content"]})
    world.put(DS, "IMAGES/IMAGE_a/image-level-0.dcm", "a0-fu")
    world.put(DS, "IMAGES/IMAGE_a/image-level-0.dcm", "a0-full-content", encrypted=True)
    world.queue(DS)

    assert world.run() == 0

    assert world.on_disk(DS) == files
    fetched = [path for batch in world.download_lists() for path in batch]
    assert not any("IMAGES/" in path for path in fetched)
    assert any("--force-overwrite" in argv for argv in world.calls_to("decrypt"))


def test_a_failed_decryption_refetches_only_that_file(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1", "a2"]})
    bad = world.datasets_root / DS / "IMAGES/IMAGE_a/image-level-1.dcm"
    world.fault("decrypt", path=str(bad), error="could not create cryp4gh reader: EOF", times=1)
    world.queue(DS)

    assert world.run() == 0

    assert world.on_disk(DS) == files
    fetched = [path.split("/", 1)[1] for batch in world.download_lists() for path in batch]
    assert fetched.count("IMAGES/IMAGE_a/image-level-1.dcm.c4gh") == 2
    assert fetched.count("IMAGES/IMAGE_a/image-level-0.dcm.c4gh") == 1
    assert world.files(DS)["IMAGES/IMAGE_a/image-level-1.dcm.c4gh"].failures == 1


def test_a_wrong_passphrase_stops_the_run(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.scenario["passphrase"] = "something else"
    world.queue(DS)

    assert world.run() == EXIT_FATAL

    record = world.dataset(DS)
    assert record.status == "pending"
    assert "authentication failed" in (record.last_error or "")
    assert all(f.failures == 0 for f in world.files(DS).values())
    # The encrypted downloads stay, ready for a run with the right passphrase.
    assert "METADATA/image.xml.c4gh" in world.on_disk(DS)


EXIT_CONFIG = 2


@pytest.mark.parametrize(
    ("setup", "message"),
    [
        # A mount with `nofail`: unmounted, it is an empty folder on the local disk.
        (lambda w: w.settings.update(REQUIRE_MOUNT=str(w.root)), "not a mount point"),
        (lambda w: w.scenario.update(version="v0.3.3"), "0.3.3"),
        (lambda w: w.settings.update(SDA_CONFIG=str(w.root / "nope.conf")), "SDA_CONFIG"),
        (lambda w: (w.root / "secrets" / "key.sec.pem").unlink(), "PRIVATE_KEY"),
        (lambda w: w.settings.update(C4GH_PASSWORD_FILE=str(w.root / "nope")), "C4GH_PASSWORD"),
        (lambda w: w.settings.update(DATASETS_ROOT=str(w.root / "nope")), "DATASETS_ROOT"),
    ],
)
def test_preflight_refuses_to_start(
    world: World, capsys: pytest.CaptureFixture[str], setup: object, message: str
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.queue(DS)
    setup(world)  # type: ignore[operator]

    assert world.run() == EXIT_CONFIG

    assert message in capsys.readouterr().err
    assert not world.calls_to("list") and not world.calls_to("download")
    assert world.dataset(DS).status == "pending"


def test_an_already_expired_token_is_caught_before_any_request(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.set_token_expiry(1_700_000_000)  # 2023
    world.queue(DS)

    assert world.run() == EXIT_FATAL

    assert not world.calls_to("list")
    err = capsys.readouterr().err
    assert "expired" in err and "2023-11-14" in err
    assert "access_token" not in err  # the token itself is never printed


def test_the_runtime_cap_stops_a_hanging_call_and_the_run(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.remote_dataset(DS2, {"IMAGE_b": ["b0"]})
    world.fault("download", path="IMAGES/IMAGE_a/image-level-0.dcm", sleep=60)
    world.settings["MAX_RUNTIME_HOURS"] = str(2 / 3600)  # two seconds
    world.queue(DS, DS2)

    started = time.monotonic()
    assert world.run() == 0  # reaching the cap is a normal end of a run
    assert time.monotonic() - started < 20

    record = world.dataset(DS)
    assert record.status == "interrupted"
    assert "runtime cap" in (record.last_error or "")
    assert DS2 not in str(world.calls())


def wait_for(path: Path, timeout: float = 20) -> None:
    end = time.monotonic() + timeout
    while not path.exists():
        assert time.monotonic() < end, f"timed out waiting for {path}"
        time.sleep(0.05)


def test_first_interrupt_stops_gracefully_after_the_current_call(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.remote_dataset(DS2, {"IMAGE_b": ["b0"]})
    marker = world.root / "downloading"
    world.fault("download", path="IMAGES/IMAGE_a/image-level-0.dcm", touch=str(marker), sleep=60)
    world.queue(DS, DS2)

    process = world.spawn("run")
    wait_for(marker)
    process.send_signal(signal.SIGINT)
    output, _ = process.communicate(timeout=20)

    assert process.returncode == 130, output
    record = world.dataset(DS)
    assert record.status == "interrupted"
    assert "SIGINT" in (record.last_error or "")
    assert DS2 not in str(world.calls())
    assert world.dataset(DS2).status == "pending"
    # METADATA landed before the interrupt and is kept.
    assert "METADATA/image.xml" in world.on_disk(DS)


def test_second_interrupt_kills_a_call_that_ignores_the_first(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    marker = world.root / "downloading"
    world.fault(
        "download", path="IMAGES/IMAGE_a/image-level-0.dcm",
        touch=str(marker), sleep=60, ignore_sigint=True,
    )
    world.queue(DS)

    process = world.spawn("run")
    wait_for(marker)
    process.send_signal(signal.SIGINT)
    time.sleep(1)
    assert process.poll() is None  # still waiting for the call to finish
    process.send_signal(signal.SIGINT)
    output, _ = process.communicate(timeout=20)

    assert process.returncode == 130, output
    assert world.dataset(DS).status == "interrupted"


def test_files_encrypted_to_another_key_hold_the_dataset_untouched(world: World) -> None:
    # Two undecrypted datasets, and partial downloads, are encrypted to a key we don't hold.
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1"]})
    for local in ("METADATA/image.xml", "METADATA/dataset.xml"):
        world.put(DS, local, files[local], other_key=True)
    world.remote_dataset(DS2, {"IMAGE_b": ["b0"]})
    world.queue(DS, DS2)

    assert world.run() == 0

    record = world.dataset(DS)
    assert record.status == "held"
    assert "another key" in (record.last_error or "")
    assert not any(DS in str(argv) for argv in world.calls_to("download"))
    assert sorted(world.on_disk(DS)) == ["METADATA/dataset.xml.c4gh", "METADATA/image.xml.c4gh"]
    assert all(f.failures == 0 for f in world.files(DS).values())
    assert world.dataset(DS2).status == "complete"  # the run goes on


def test_a_held_dataset_runs_only_when_named(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.db().add_dataset(DS, status="held")

    world.run()
    assert world.dataset(DS).status == "held"
    assert not world.calls_to("list")

    world.run(DS)
    assert world.dataset(DS).status == "complete"


def test_naming_a_complete_dataset_does_not_run_it_again(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.db().add_dataset(DS, status="complete")

    assert world.run(DS) == 0

    assert not world.calls_to("list")
    assert "requeue" in capsys.readouterr().out


def test_status_and_export_show_every_dataset(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.queue(DS, DS2)
    world.run(DS)
    capsys.readouterr()

    assert world.cli("status") == 0
    out = capsys.readouterr().out
    assert DS in out and DS2 in out and "complete" in out and "pending" in out

    export = world.root / "export" / "datasets.csv"
    assert world.cli("export-csv", str(export)) == 0
    lines = export.read_text().splitlines()
    assert lines[0].startswith("dataset_id,status,")
    assert len(lines) == 3


def test_an_unreadable_image_xml_fails_that_dataset_and_the_run_goes_on(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.scenario["datasets"][DS]["files"]["METADATA/image.xml"] = "<IMAGE_SET><FILES"  # cut off
    world.remote_dataset(DS2, {"IMAGE_b": ["b0"]})
    world.queue(DS, DS2)

    assert world.run() == 0

    record = world.dataset(DS)
    assert record.status == "failed"
    assert "image.xml" in (record.last_error or "")
    assert world.files(DS)["METADATA/image.xml.c4gh"].failures == 1  # fetched again next run
    assert not any("/IMAGES/" in p for batch in world.download_lists()[:2] for p in batch)
    assert world.dataset(DS2).status == "complete"


def test_a_second_run_is_refused_while_one_is_running(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    marker = world.root / "downloading"
    world.fault("download", path="IMAGES/IMAGE_a/image-level-0.dcm", touch=str(marker), sleep=60)
    world.queue(DS)

    first = world.spawn("run")
    try:
        wait_for(marker)
        calls_before = len(world.calls())

        assert world.run() == EXIT_CONFIG
        assert world.cli("requeue", DS) == EXIT_CONFIG

        err = capsys.readouterr().err
        assert "another run" in err and str(first.pid) in err
        assert len(world.calls()) == calls_before
        assert world.cli("status") == 0  # reading is always allowed
        ids = world.root / "ids.txt"
        ids.write_text("aa-Dataset-qqqqq1-rrrrr1\n")
        assert world.cli("import", str(ids)) == 0  # rows only, like `add`: no lock
    finally:
        first.send_signal(signal.SIGINT)
        first.communicate(timeout=20)

    assert world.run() == 0  # the lock goes with the process


RESERVE_BYTES = 1000  # MIN_FREE_GB below, in bytes


@pytest.fixture
def share(world: World, monkeypatch: pytest.MonkeyPatch) -> Callable[[int], None]:
    """Give the share a capacity: free space is capacity minus what lies under DATASETS_ROOT."""
    world.settings["MIN_FREE_GB"] = str(RESERVE_BYTES / 1e9)

    def set_capacity(capacity: int) -> None:
        def disk_usage(path: object) -> shutil._ntuple_diskusage:
            # Data only: on the real share the logs are noise next to terabytes of images.
            used = sum(
                p.stat().st_size
                for p in world.datasets_root.rglob("*")
                if p.is_file() and "_download" not in p.parts
            )
            return shutil._ntuple_diskusage(capacity, used, capacity - used)

        monkeypatch.setattr(shutil, "disk_usage", disk_usage)

    return set_capacity


def test_a_nearly_full_share_refuses_the_run(
    world: World, share: Callable[[int], None], capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.queue(DS)
    share(RESERVE_BYTES - 1)

    assert world.run() == EXIT_CONFIG

    assert "MIN_FREE_GB" in capsys.readouterr().err
    assert not world.calls_to("list")


def test_a_dataset_that_cannot_fit_is_skipped_for_one_that_can(
    world: World, share: Callable[[int], None]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["x" * 5000]})
    small = world.remote_dataset(DS2, {"IMAGE_b": ["b0"]})
    world.queue(DS, DS2)
    share(RESERVE_BYTES + 3 * sum(len(c) for c in small.values()))

    assert world.run() == 0

    record = world.dataset(DS)
    assert record.status == "pending"
    assert "needs" in (record.last_error or "")
    assert not any(DS in str(argv) for argv in world.calls_to("download"))
    assert world.dataset(DS2).status == "complete"


def test_the_run_stops_before_a_batch_that_would_overfill_the_share(
    world: World, share: Callable[[int], None]
) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["x" * 1000, "y" * 1000, "z" * 1000]})
    world.settings["DOWNLOAD_BATCH_SIZE"] = "1"
    world.queue(DS)
    # Room for the dataset, but not for a batch's .c4gh and decrypted copy near the end.
    share(RESERVE_BYTES + sum(len(c) for c in files.values()) + 100)

    assert world.run() == EXIT_FATAL

    record = world.dataset(DS)
    assert record.status == "pending"
    assert "MIN_FREE_GB" in (record.last_error or "")
    assert all(f.failures == 0 for f in world.files(DS).values())
    images = [p for p in world.on_disk(DS) if p.startswith("IMAGES/")]
    assert 0 < len(images) < 3


def test_no_space_left_on_device_stops_the_run_without_blaming_the_file(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.fault(
        "download", path="IMAGES/IMAGE_a/image-level-0.dcm", times=99,
        error="Error: write IMAGES/IMAGE_a/image-level-0.dcm.c4gh.part: no space left on device",
    )
    world.queue(DS)

    assert world.run() == EXIT_FATAL

    assert len(world.calls_to("download")) == 2  # metadata, then the one failed image call
    assert world.dataset(DS).status == "pending"
    assert all(f.failures == 0 for f in world.files(DS).values())


# What some of the older datasets hold in place of every image: an S3 bucket listing.
BUCKET_LISTING = (
    '<?xml version="1.0"?><ListAllMyBucketsResult>' + " " * 5560 + "</ListAllMyBucketsResult>"
)


def test_metadata_with_the_wrong_size_is_quarantined_and_fetched_again(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    dataset_xml = world.datasets_root / DS / "METADATA/dataset.xml"
    world.fault("decrypt", path=str(dataset_xml), content="<DATASET>", times=1)  # cut short
    world.queue(DS)

    assert world.run() == 0

    assert world.on_disk(DS) == files
    record = world.files(DS)["METADATA/dataset.xml.c4gh"]
    assert record.failures == 1 and "size" in (record.last_error or "")


def test_an_image_without_a_checksum_is_still_checked_by_size(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0" * 100]}, checksums={})
    image = world.datasets_root / DS / "IMAGES/IMAGE_a/image-level-0.dcm"
    world.fault("decrypt", path=str(image), content=BUCKET_LISTING, times=1)
    world.queue(DS)

    assert world.run() == 0

    assert world.on_disk(DS) == files
    record = world.files(DS)["IMAGES/IMAGE_a/image-level-0.dcm.c4gh"]
    assert (record.failures, record.verified) == (1, False)


def test_a_relisting_notices_files_that_changed_size_or_disappeared(world: World) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1", "a2"]})
    world.queue(DS)
    assert world.run() == 0

    # Bigpicture republishes the dataset: level 1 changes, level 2 is withdrawn.
    new = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1-revised"]})
    world.cli("requeue", DS)
    assert world.run() == 0

    record = world.dataset(DS)
    assert record.status == "complete"
    assert (record.files_total, record.files_done) == (len(new), len(new))
    on_disk = world.on_disk(DS)
    assert on_disk["IMAGES/IMAGE_a/image-level-1.dcm"] == "a1-revised"
    states = {path: f.state for path, f in world.files(DS).items()}
    assert states["IMAGES/IMAGE_a/image-level-2.dcm.c4gh"] == "gone"
    # Withdrawn files are left where they are: deleting is a person's decision.
    assert on_disk["IMAGES/IMAGE_a/image-level-2.dcm"] == "a2"


def test_no_access_is_remembered_for_this_conf_user_only(world: World) -> None:
    world.queue(DS)  # on no endpoint this user can see

    world.run()
    assert world.dataset(DS).status == "no_access"
    lists_after_first_run = len(world.calls_to("list"))

    world.run()  # same conf: not asked again
    assert len(world.calls_to("list")) == lists_after_first_run
    assert world.dataset(DS).status == "no_access"

    # Someone with access installs their conf.
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.set_token_expiry(time.time() + 30 * 86400, access_key="colleague@example.org")
    world.run()
    assert world.dataset(DS).status == "complete"
    assert world.on_disk(DS) == files


def test_requeue_retries_a_no_access_dataset_with_the_same_conf(world: World) -> None:
    world.queue(DS)
    world.run()
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})  # access was granted in REMS meanwhile

    world.cli("requeue", DS)
    world.run()

    assert world.dataset(DS).status == "complete"


def test_datasets_run_in_priority_order(world: World) -> None:
    third = "aa-Dataset-aaa000-aaa000"
    for dataset_id in (DS, DS2, third):
        world.remote_dataset(dataset_id, {"IMAGE_a": ["a0"]})
    world.queue(DS, DS2, third)

    assert world.cli("prioritise", DS2, "10") == 0
    assert world.cli("prioritise", DS, "5") == 0
    world.run()

    listed = [argv[argv.index("--dataset") + 1] for argv in world.calls_to("list")]
    assert listed == [DS2, DS, third]


def test_the_export_leaves_out_who_the_conf_belongs_to(world: World) -> None:
    world.queue(DS)
    world.run()
    export = world.root / "datasets.csv"

    world.cli("export-csv", str(export))

    text = export.read_text()
    assert "no_access" in text
    assert "someone@example.org" not in text


def last_run_line(world: World, capsys: pytest.CaptureFixture[str]) -> str:
    capsys.readouterr()
    assert world.cli("runs", "--last", "1") == 0
    return capsys.readouterr().out.strip().splitlines()[-1]


def last_run(world: World, capsys: pytest.CaptureFixture[str]) -> dict[str, str]:
    """The `runs` columns of the last run, by header name."""
    fields = last_run_line(world, capsys).split(None, 9)
    names = ["run", "date", "time", "hours", "exit", "sets", "files", "gb", "mbs", "why"]
    return dict(zip(names, fields))


def test_a_run_is_recorded_and_exported(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1"]})
    export = world.root / "share" / "status" / "datasets.csv"
    world.settings["EXPORT_CSV"] = str(export)
    world.queue(DS)

    assert world.run() == 0

    run = last_run(world, capsys)
    assert (run["exit"], run["sets"], run["files"]) == ("0", "1", str(len(files)))
    assert run["why"] == "finished"
    assert DS in export.read_text()


def test_a_run_stopped_by_an_expired_token_is_recorded_and_exported(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.scenario["expire_token_after_calls"] = 1
    export = world.root / "datasets.csv"
    world.settings["EXPORT_CSV"] = str(export)
    world.queue(DS)

    assert world.run() == EXIT_FATAL

    run = last_run(world, capsys)
    assert run["exit"] == "3" and run["why"].startswith("token expired")
    assert export.is_file()


def test_an_interrupted_run_is_recorded_and_exported(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    marker = world.root / "downloading"
    world.fault("download", path="IMAGES/IMAGE_a/image-level-0.dcm", touch=str(marker), sleep=60)
    export = world.root / "datasets.csv"
    world.settings["EXPORT_CSV"] = str(export)
    world.queue(DS)

    process = world.spawn("run")
    wait_for(marker)
    process.send_signal(signal.SIGINT)
    process.communicate(timeout=20)

    run = last_run(world, capsys)
    assert run["exit"] == "130" and run["why"] == "interrupted by SIGINT"
    assert "interrupted" in export.read_text()


def test_status_ends_with_the_last_run(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.queue(DS)
    world.run()
    capsys.readouterr()

    world.cli("status")

    assert capsys.readouterr().out.strip().splitlines()[-1].startswith("Last run:")


def test_the_run_stops_cleanly_before_the_token_expires(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.remote_dataset(DS2, {"IMAGE_b": ["b0"]})
    world.fault("download", path="IMAGES/IMAGE_a/image-level-0.dcm", sleep=60)
    world.settings["TOKEN_MARGIN_HOURS"] = "0"
    world.set_token_expiry(time.time() + 3)
    world.queue(DS, DS2)

    started = time.monotonic()
    assert world.run() == EXIT_FATAL  # a person has to renew the token
    assert time.monotonic() - started < 20

    record = world.dataset(DS)
    assert record.status == "interrupted"  # resumes as it is next run
    assert "the download token expires at" in (record.last_error or "")
    assert all(f.failures == 0 for f in world.files(DS).values())
    assert DS2 not in str(world.calls())
    assert last_run(world, capsys)["why"].startswith("the download token expires at")


def test_a_token_inside_the_margin_is_refused_before_any_request(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0"]})
    world.settings["TOKEN_MARGIN_HOURS"] = "2"
    world.set_token_expiry(time.time() + 3600)
    world.queue(DS)

    assert world.run() == EXIT_FATAL

    assert "TOKEN_MARGIN_HOURS" in capsys.readouterr().err
    assert not world.calls_to("list")


def test_a_warning_when_the_token_ends_before_the_runtime_cap(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.settings["TOKEN_MARGIN_HOURS"] = "0"
    world.settings["MAX_RUNTIME_HOURS"] = str(40 * 24)
    world.run()  # nothing queued: the warning comes first anyway

    assert "before the runtime cap" in capsys.readouterr().out


def test_throughput_counts_only_what_this_run_downloaded(world: World) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a" * 100, "b" * 100, "c" * 1000]})
    for local, content in files.items():
        if not local.endswith("image-level-2.dcm"):
            world.put(DS, local, content)  # an earlier run fetched these
    world.queue(DS)

    assert world.run() == 0

    run = world.db().runs(1)[0]
    assert run.files_done == len(files)  # every file was settled this run
    assert run.bytes_downloaded == 1000  # but only one was downloaded


def test_a_dataset_mostly_on_disk_is_not_skipped_for_space(
    world: World, share: Callable[[int], None]
) -> None:
    # Like a large dataset: many .c4gh files on disk, never decrypted.
    files = world.remote_dataset(DS, {"IMAGE_a": ["x" * 1000, "y" * 1000, "z" * 1000]})
    for local, content in files.items():
        if local.startswith("IMAGES/"):
            world.put(DS, local, content, encrypted=True)
    metadata = sum(len(c) for local, c in files.items() if local.startswith("METADATA/"))
    on_disk = 3 * len("C4GH:" + "x" * 1000)
    world.settings["DECRYPT_BATCH_SIZE"] = "1"
    world.queue(DS)
    # Room for the metadata batch, then for decrypting one image at a time: not for all three.
    share(RESERVE_BYTES + on_disk + 2 * metadata + 1100)

    assert world.run() == 0

    assert world.dataset(DS).status == "complete"
    assert world.on_disk(DS) == files


def test_status_shows_progress_while_a_dataset_is_running(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1", "a2"]})
    marker = world.root / "downloading"
    world.fault("download", path="IMAGES/IMAGE_a/image-level-1.dcm", touch=str(marker), sleep=60)
    world.settings["DOWNLOAD_BATCH_SIZE"] = "1"
    world.queue(DS)

    process = world.spawn("run")
    try:
        wait_for(marker)
        capsys.readouterr()
        assert world.cli("status") == 0
        line = next(l for l in capsys.readouterr().out.splitlines() if l.startswith(DS))
        # Two metadata files and the first image are done; five files in all.
        assert "3/5" in line.split()
    finally:
        process.send_signal(signal.SIGINT)
        process.communicate(timeout=20)


def status_row(world: World, capsys: pytest.CaptureFixture[str], dataset_id: str) -> list[str]:
    """The FILES and CHECKED columns of a dataset's `status` line."""
    capsys.readouterr()
    assert world.cli("status") == 0
    line = next(l for l in capsys.readouterr().out.splitlines() if l.startswith(dataset_id))
    columns = line.split()
    return [columns[3], columns[5]]


def test_status_shows_migrated_datasets_as_done_but_unchecked(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    # From a legacy CSV: its file counts, but no per-file records here.
    db = world.db()
    db.add_dataset(DS, status="complete", source="legacy-csv", files_total=12)
    db.add_dataset(DS2, status="pending", source="legacy-csv", files_total=34)
    third = "aa-Dataset-aaa000-aaa000"
    db.add_dataset(third, status="held", source="share-folder")

    assert status_row(world, capsys, DS) == ["12/12", "no"]
    assert status_row(world, capsys, DS2) == ["0/34", "-"]
    assert status_row(world, capsys, third) == ["-/?", "no"]


def test_status_shows_a_downloaded_dataset_as_checked(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    files = world.remote_dataset(DS, {"IMAGE_a": ["a0", "a1"]})
    world.queue(DS)
    world.run()

    n = len(files)
    assert status_row(world, capsys, DS) == [f"{n}/{n}", "yes"]


def test_status_survives_a_closed_pipe(world: World) -> None:
    db = world.db()
    for number in range(2000):  # more than a pipe buffer holds
        db.add_dataset(f"aa-Dataset-{number:06d}-xxxxxx")
    env_file = world.write()
    result = subprocess.run(
        f"{sys.executable} -m bp_downloader --env-file {env_file} status | head -1",
        shell=True, capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    )
    assert "DATASET" in result.stdout
    assert "Traceback" not in result.stderr and "BrokenPipe" not in result.stderr


def test_run_without_any_site_settings_names_each_and_exits_with_the_config_code(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    empty_env = world.root / "empty.env"
    empty_env.write_text("")

    assert main(["--env-file", str(empty_env), "run"]) == EXIT_CONFIG

    err = capsys.readouterr().err
    for name in ("SDA_CONFIG", "PUBLIC_KEY", "C4GH_PASSWORD_FILE", "DATASETS_ROOT", "STATUS_DB"):
        assert name in err
    assert not (world.root / "state").exists()  # nothing was created, nothing was called


def test_status_needs_only_the_status_database(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    env_file = world.root / "status-only.env"
    env_file.write_text(f"STATUS_DB={world.settings['STATUS_DB']}\n")

    assert main(["--env-file", str(env_file), "status"]) == 0
    assert main(["--env-file", str(env_file), "run"]) == EXIT_CONFIG
    assert "DATASETS_ROOT" in capsys.readouterr().err
