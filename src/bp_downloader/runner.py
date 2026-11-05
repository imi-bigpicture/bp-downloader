"""One run: work through the runnable datasets, one at a time.

Per dataset: list it, then download, decrypt and verify its metadata, then its images in
batches. Each batch is decrypted and checked against image.xml as soon as it lands.

A metadata-only run (`run --metadata-only`) stops each dataset after its metadata and leaves
every dataset's status alone. Without named datasets it first lists every dataset the conf
can see.
"""

from __future__ import annotations

import shutil
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .checksums import IMAGE_XML, MetadataError, read_checksums, sha256_file
from .config import Config
from .errors import ErrorKind, classify, last_error_line
from .sdacli import CallResult, SdaCli, parse_dataset_listing, parse_listing
from .status import (
    COMPLETE,
    DONE,
    FAILED,
    GONE,
    HELD,
    IN_PROGRESS,
    INCOMPLETE,
    INTERRUPTED,
    MISSING,
    NO_ACCESS,
    PENDING,
    FileRecord,
    StatusDB,
)

EXIT_OK = 0
EXIT_CONFIG = 2  # bad settings, or a pre-flight check failed: nothing was attempted
EXIT_FATAL = 3  # stopped on an error no retry can fix; see FATAL_ADVICE
EXIT_INTERRUPTED = 130

FATAL_KINDS = (ErrorKind.TOKEN_EXPIRED, ErrorKind.BAD_PASSPHRASE, ErrorKind.DISK_FULL)
FATAL_ADVICE = {
    ErrorKind.TOKEN_EXPIRED: (
        "Fetch a new s3cmd-download.conf (REMS / LS Login, with MFA), put it at SDA_CONFIG,"
        " and start the run again. Datasets resume where they stopped."
    ),
    ErrorKind.BAD_PASSPHRASE: (
        "The private key could not be unlocked. Check C4GH_PASSWORD_FILE against PRIVATE_KEY."
    ),
    ErrorKind.DISK_FULL: (
        "The share is (nearly) full. Free space, or lower MIN_FREE_GB, and start the run again."
    ),
}


def gb(size: int) -> str:
    return f"{size / 1e9:,.1f} GB"


class Fatal(Exception):
    """An error no retry can fix. The run stops."""

    def __init__(self, kind: ErrorKind, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class DatasetStopped(Exception):
    """This dataset can't go on in this run. The run moves on to the next one."""

    def __init__(self, status: str, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


class ListingFailed(Exception):
    """The conf's datasets could not be listed, so a metadata-only run has nothing to do."""


def raise_if_fatal(result: CallResult) -> None:
    kind = classify(result.output)
    if kind in FATAL_KINDS:
        raise Fatal(kind, last_error_line(result.output))


def is_metadata(record: FileRecord) -> bool:
    return record.local_path.startswith("METADATA/")


@dataclass
class Listed:
    """What asking each live endpoint in turn came to."""

    url: str | None = None  # the endpoint that answered, with `result`; None if none did
    result: CallResult | None = None
    not_found: list[str] = field(default_factory=list)  # "url: error", 404/403 answers
    errors: list[str] = field(default_factory=list)  # "url: error", everything else


class DatasetLog:
    """The per-dataset log, kept next to the data in `<dataset>/_download/`."""

    def __init__(self, folder: Path, echo: Callable[[str], None]) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        self.folder = folder
        self.stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.path = folder / f"download_{self.stamp}.log"
        self.echo = echo

    def __call__(self, message: str) -> None:
        line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}"
        with self.path.open("a") as handle:
            handle.write(line + "\n")
        self.echo(line)

    def output(self, what: str, result: CallResult) -> None:
        with self.path.open("a") as handle:
            handle.write(f"--- {what}: exit {result.returncode}\n{result.output}")
            if result.output and not result.output.endswith("\n"):
                handle.write("\n")
            handle.write("---\n")


def encrypted(path: Path) -> Path:
    return path.with_name(path.name + ".c4gh")


class Runner:
    def __init__(
        self,
        config: Config,
        db: StatusDB,
        cli: SdaCli,
        access_key: str,
        token_expires: datetime,
        echo: Callable[[str], None] = print,
        metadata_only: bool = False,
    ) -> None:
        self.config = config
        # Fetch each dataset's METADATA/ and nothing else, and leave every dataset's status
        # as it was: a metadata pass (often with another person's conf) must not change what
        # the next full run does.
        self.metadata_only = metadata_only
        self.with_metadata = 0
        self.without_metadata: list[str] = []
        self.access_key = access_key
        self.db = db
        self.cli = cli
        self.echo = echo
        # The run ends at the runtime cap, or TOKEN_MARGIN_HOURS before the token expires,
        # whichever comes first.
        self.deadline = time.monotonic() + config.max_runtime_hours * 3600
        token_left = (token_expires - datetime.now(timezone.utc)).total_seconds()
        token_deadline = time.monotonic() + token_left - config.token_margin_hours * 3600
        self.deadline_reason = (
            f"runtime cap of {config.max_runtime_hours:g} h reached (MAX_RUNTIME_HOURS)"
        )
        self.token_ends_run = token_deadline < self.deadline
        if self.token_ends_run:
            self.deadline = token_deadline
            self.deadline_reason = (
                f"the download token expires at {token_expires:%Y-%m-%d %H:%M} UTC"
                f" (stopping TOKEN_MARGIN_HOURS={config.token_margin_hours:g} h before)"
            )
            echo(
                f"Warning: the token runs out before the runtime cap; this run stops at"
                f" {token_expires - timedelta(hours=config.token_margin_hours):%Y-%m-%d %H:%M}"
                " UTC. Renew the conf to go on after that."
            )
        self.dead: dict[str, str] = {}  # endpoint -> the error that showed it dead
        self._wake = threading.Event()
        self.stop_reason: str | None = None
        self.interrupted = False
        self._signals = 0
        self.datasets_attempted = 0
        self.files_done = 0
        self.bytes_downloaded = 0

    def request_stop(self, signal_name: str) -> None:
        """Two-stage stop, as in the earlier bash loop.

        The first signal is forwarded to the running sda-cli call; the dataset is recorded
        as interrupted and no new one starts. The second kills the call outright.
        """
        self._signals += 1
        if self._signals == 1:
            self.echo(f"{signal_name}: stopping after the current call. Again to force.")
            self.stop_reason = f"interrupted by {signal_name}"
            self.interrupted = True
            self._wake.set()
            self.cli.interrupt_child()
        else:
            self.echo(f"{signal_name} again: killing the current call.")
            self.cli.interrupt_child(force=True)

    def check_stop(self, result: CallResult | None = None) -> None:
        """Raise if the run must end now: a signal came in, or the runtime cap was reached."""
        if self.stop_reason is None and (
            (result is not None and result.timed_out) or time.monotonic() >= self.deadline
        ):
            self.stop_reason = self.deadline_reason
        if self.stop_reason is not None:
            raise DatasetStopped(INTERRUPTED, self.stop_reason)

    def free_bytes(self) -> int:
        return shutil.disk_usage(self.config.datasets_root).free

    def ensure_room(self, size: int) -> None:
        """Stop the run before writing `size` bytes would eat into MIN_FREE_GB."""
        free = self.free_bytes()
        if free - size < self.config.min_free_bytes:
            raise Fatal(
                ErrorKind.DISK_FULL,
                f"{gb(free)} free; the next batch needs up to {gb(size)} and MIN_FREE_GB"
                f" keeps {gb(self.config.min_free_bytes)}",
            )

    def mark_dead(self, url: str, error: str) -> None:
        """Stop using an endpoint for the rest of this run."""
        if url not in self.dead:
            self.dead[url] = error
            self.echo(f"Endpoint {url} is not answering; not using it again this run: {error}")

    def backoff(self, failures: int) -> None:
        """Exponential backoff, capped at MAX_SLEEP and at the runtime cap."""
        delay = min(self.config.base_sleep * 2 ** (failures - 1), self.config.max_sleep)
        delay = min(delay, max(self.deadline - time.monotonic(), 0.0))
        if delay > 0:
            self.echo(f"Retrying in {delay:.0f}s")
            self._wake.wait(delay)
        self.check_stop()

    def run(self, password: str, dataset_ids: Sequence[str] = ()) -> int:
        """Work through the datasets, and record the run in the `runs` table."""
        run_id = self.db.start_run()
        exit_code: int | None = None
        reason: str | None = None
        try:
            exit_code, reason = self._run(password, dataset_ids)
            return exit_code
        except BaseException as exc:
            reason = f"crashed: {type(exc).__name__}: {exc}"
            raise
        finally:
            self.db.finish_run(
                run_id,
                exit_code=exit_code,
                stop_reason=reason,
                datasets_attempted=self.datasets_attempted,
                files_done=self.files_done,
                bytes_downloaded=self.bytes_downloaded,
            )

    def _run(self, password: str, dataset_ids: Sequence[str]) -> tuple[int, str]:
        exit_code, reason = self._run_datasets(password, dataset_ids)
        if self.metadata_only:
            reason = (
                f"{reason} (metadata only: {self.with_metadata} with metadata,"
                f" {len(self.without_metadata)} without)"
            )
            if self.without_metadata:
                self.echo("Without metadata: " + ", ".join(self.without_metadata))
        return exit_code, reason

    def record_metadata(self, dataset_id: str, fetched: bool) -> None:
        if fetched:
            self.with_metadata += 1
        else:
            self.without_metadata.append(dataset_id)

    def _run_datasets(self, password: str, dataset_ids: Sequence[str]) -> tuple[int, str]:
        try:
            if self.metadata_only and not dataset_ids:
                dataset_ids = self.datasets_for_metadata()
            elif not dataset_ids:
                dataset_ids = self.db.runnable(self.access_key)
            for dataset_id in dataset_ids:
                try:
                    self.check_stop()
                except DatasetStopped:
                    break
                self.datasets_attempted += 1
                DatasetJob(self, dataset_id, password).run()
        except Fatal as exc:
            self.echo(f"Run stopped: {exc.kind.value}: {exc}")
            self.echo(FATAL_ADVICE[exc.kind])
            return EXIT_FATAL, f"{exc.kind.value}: {exc}"
        except ListingFailed as exc:
            self.echo(f"Run stopped: {exc}")
            return EXIT_FATAL, str(exc)
        except DatasetStopped:
            pass  # stopped while listing the conf's datasets; stop_reason says why
        if self.stop_reason:
            self.echo(f"Run stopped: {self.stop_reason}")
            if self.interrupted:
                return EXIT_INTERRUPTED, self.stop_reason
            if self.token_ends_run:
                self.echo(FATAL_ADVICE[ErrorKind.TOKEN_EXPIRED])
                return EXIT_FATAL, self.stop_reason  # a person has to renew the token
            return EXIT_OK, self.stop_reason
        return EXIT_OK, "finished"

    def datasets_for_metadata(self) -> list[str]:
        """Every dataset the conf can list, minus those whose metadata is already here.

        Ones the database doesn't know are added as `held`: found through someone's conf,
        they don't join the full-download backlog until a person `requeue`s them.
        """
        listed = self.list_conf_datasets()
        added = 0
        wanted: list[str] = []
        for dataset_id in listed:
            if self.db.add_dataset(
                dataset_id,
                status=HELD,
                source="listed",
                note="found by a metadata-only run; `requeue` it to download it in full",
            ):
                added += 1
            record = self.db.dataset(dataset_id)
            if record is not None and record.status in (COMPLETE, INCOMPLETE):
                continue  # downloaded already, metadata included
            wanted.append(dataset_id)
        self.echo(
            f"The conf lists {len(listed)} datasets: {added} new (added as held),"
            f" {len(listed) - len(wanted)} already downloaded, {len(wanted)} to fetch metadata for"
        )
        return wanted

    def list_conf_datasets(self) -> list[str]:
        """`list --datasets` on the first live endpoint that answers."""

        def call(url: str) -> CallResult:
            self.echo(f"Listing the conf's datasets on {url} (this takes minutes)")
            return self.cli.list_datasets(url, deadline=self.deadline)

        def note(url: str, attempt: int, result: CallResult) -> None:
            if not result.ok:
                self.echo(f"Listing the conf's datasets failed: {last_error_line(result.output)}")

        listed = self.list_on_live_endpoints(self.config.download_urls, call, note)
        if listed.result is not None:
            return parse_dataset_listing(listed.result.output)
        errors = listed.errors + listed.not_found
        raise ListingFailed(
            "listing the conf's datasets failed: " + ("; ".join(errors) or "no live endpoint")
        )

    def list_on_live_endpoints(
        self,
        urls: Sequence[str],
        call: Callable[[str], CallResult],
        note: Callable[[str, int, CallResult], None],
    ) -> Listed:
        """Ask each live endpoint in turn, retrying each up to LISTING_ATTEMPTS times.

        Stops at the first that answers. A dead endpoint is dropped for the run and a 404/403
        moves on at once; other errors are retried after a backoff. `note` sees every result.
        """
        listed = Listed()
        for url in urls:
            if url in self.dead:
                continue
            for attempt in range(1, self.config.listing_attempts + 1):
                result = call(url)
                note(url, attempt, result)
                self.check_stop(result)
                if result.ok:
                    listed.url, listed.result = url, result
                    return listed
                raise_if_fatal(result)
                kind = classify(result.output)
                error = last_error_line(result.output)
                if kind is ErrorKind.ENDPOINT_DEAD:
                    self.mark_dead(url, error)
                    break
                if kind is ErrorKind.NOT_FOUND:
                    listed.not_found.append(f"{url}: {error}")
                    break
                listed.errors.append(f"{url}: {error}")
                if attempt < self.config.listing_attempts:
                    self.backoff(attempt)
        return listed


class DatasetJob:
    def __init__(self, runner: Runner, dataset_id: str, password: str) -> None:
        self.runner = runner
        self.config = runner.config
        self.db = runner.db
        self.cli = runner.cli
        self.dataset_id = dataset_id
        self.password = password
        self.folder = self.config.datasets_root / dataset_id
        self.log = DatasetLog(self.folder / "_download", runner.echo)
        self.checksums: dict[str, str] = {}
        # Files with `x` or `x.c4gh` on disk before this run fetched anything. A metadata-only
        # run never moves these: they may be another tool's or person's copies.
        self.on_disk_before: set[str] = set()

    def run(self) -> None:
        if self.runner.metadata_only:
            self.run_metadata_only()
            return
        self.db.update_dataset(self.dataset_id, status=IN_PROGRESS, last_error=None)
        self.log(f"Dataset {self.dataset_id}")
        try:
            self.download_dataset()
        except Fatal as exc:
            # Not this dataset's fault: leave it for the next run, untouched.
            self.db.update_dataset(self.dataset_id, status=PENDING, last_error=f"stopped: {exc}")
            self.log(f"Run stopped: {exc.kind.value}: {exc}")
            raise
        except DatasetStopped as exc:
            self.db.refresh_counts(self.dataset_id)
            self.db.update_dataset(
                self.dataset_id,
                status=exc.status,
                last_error=exc.reason,
                no_access_for=self.runner.access_key if exc.status == NO_ACCESS else None,
            )
            self.log(f"{exc.status}: {exc.reason}")
            return
        record = self.db.refresh_counts(self.dataset_id)
        if record.files_abandoned:
            reason = f"{record.files_abandoned} file(s) abandoned; `requeue` to try them again"
            self.db.update_dataset(self.dataset_id, status=INCOMPLETE, last_error=reason)
            self.log(f"Incomplete: {record.files_done} files done, {reason}")
        else:
            self.db.update_dataset(self.dataset_id, status=COMPLETE)
            self.log(f"Complete: {record.files_done} files")

    def run_metadata_only(self) -> None:
        """Fetch METADATA/ only. The dataset's status and last error stay as they were."""
        self.log(f"Dataset {self.dataset_id}: metadata only")
        try:
            self.download_dataset()
        except Fatal as exc:
            self.log(f"Run stopped: {exc.kind.value}: {exc}")
            raise
        except DatasetStopped as exc:
            self.db.refresh_counts(self.dataset_id)
            self.log(f"Metadata not fetched: {exc.reason}")
            if exc.status != INTERRUPTED:
                self.runner.record_metadata(self.dataset_id, fetched=False)
            return
        self.db.refresh_counts(self.dataset_id)
        metadata = [
            f for f in self.db.files(self.dataset_id) if is_metadata(f) and f.state != GONE
        ]
        done = sum(f.state == DONE for f in metadata)
        # 0 of 0 is not "with metadata": some datasets are published without any.
        fetched = bool(metadata) and done == len(metadata)
        self.runner.record_metadata(self.dataset_id, fetched=fetched)
        if not metadata:
            self.log("Metadata: none; the server lists no METADATA/ files for this dataset")
        elif fetched:
            self.log(f"Metadata: {done} files")
        else:
            self.log(f"Metadata: {done} of {len(metadata)} files; the rest were abandoned")

    def download_dataset(self) -> None:
        self.url = self.list_files()
        self.switches = 0
        self.delivered: set[str] = set()  # endpoints that delivered files of it this run
        files = [f for f in self.db.files(self.dataset_id) if f.state != GONE]
        self.on_disk_before = {
            f.remote_path for f in files
            if self.plain(f).exists() or encrypted(self.plain(f)).exists()
        }
        metadata = [f for f in files if is_metadata(f)]
        images = [f for f in files if not is_metadata(f)]
        # What still has to come over the network: files with neither x nor x.c4gh on disk.
        needed = sum(
            f.size
            for f in (metadata if self.runner.metadata_only else files)
            if f.state == MISSING
            and not self.plain(f).exists()
            and not encrypted(self.plain(f)).exists()
        )
        room = self.runner.free_bytes() - self.config.min_free_bytes
        if needed > room:
            # A smaller dataset further down may still fit.
            raise DatasetStopped(
                PENDING, f"needs {gb(needed)}, only {gb(max(room, 0))} free above MIN_FREE_GB"
            )
        self.fetch(metadata)
        if self.runner.metadata_only:
            return
        image_xml = self.folder / IMAGE_XML
        if image_xml.is_file():
            try:
                self.checksums = read_checksums(image_xml)
            except MetadataError as exc:
                record = next(
                    f for f in self.current(metadata) if f.local_path == IMAGE_XML + ".c4gh"
                )
                self.reject(record, str(exc))
                raise DatasetStopped(FAILED, f"{exc}; it is fetched again next run") from None
        self.fetch(images)
        unverified = [
            f for f in self.current(images) if f.state == DONE and not f.verified
        ]
        if unverified:
            self.log(
                f"{len(unverified)} image file(s) have no checksum in {IMAGE_XML}:"
                " checked by size only"
            )

    def other_live_endpoint(self) -> str | None:
        urls = self.config.download_urls
        start = urls.index(self.url) if self.url in urls else -1
        for offset in range(1, len(urls)):
            url = urls[(start + offset) % len(urls)]
            if url != self.url and url not in self.runner.dead:
                return url
        return None

    def switch_endpoint(self, error: str) -> None:
        """React to an endpoint error by moving to another live endpoint, or give up.

        The earlier bash script switched only on `unexpected EOF`, so once it had switched to
        the unrouted download2 (which fails on TLS instead) it never switched back, and retried
        it for hours. Here a dead endpoint is dropped for the run, and switches are capped.
        """
        kind = classify(error)
        if kind is ErrorKind.ENDPOINT_DEAD:
            self.runner.mark_dead(self.url, error)
        elif self.switches >= self.config.max_endpoint_switches:
            raise DatasetStopped(
                FAILED, f"{error} (switched endpoints {self.switches} times, giving up)"
            )
        target = self.other_live_endpoint()
        if target is None:
            raise DatasetStopped(FAILED, f"{error} (no live endpoint left to switch to)")
        if kind is ErrorKind.WRONG_ENDPOINT:
            self.switches += 1
        self.log(f"Switching from {self.url} to {target}: {error}")
        self.url = target

    def list_files(self) -> str:
        """List the dataset on the first endpoint that has it. Returns that endpoint.

        With several endpoints on different storage, which one holds a dataset isn't shown
        anywhere (NBISweden/sda-cli#695), so each is asked. The dataset's last good endpoint
        goes first.
        """
        record = self.db.dataset(self.dataset_id)
        known = record.endpoint if record else None
        urls = sorted(self.config.download_urls, key=lambda url: url != known)
        outcome = self.runner.list_on_live_endpoints(
            urls,
            lambda url: self.cli.list_dataset(url, self.dataset_id, deadline=self.runner.deadline),
            lambda url, attempt, result: self.log.output(
                f"list on {url} (attempt {attempt})", result
            ),
        )
        if outcome.url is not None and outcome.result is not None:
            url, result = outcome.url, outcome.result
            listed = parse_listing(result.output)
            if not listed:
                # bp-slurm-downloader read an empty listing as "complete".
                raise DatasetStopped(FAILED, f"listing on {url} returned no files")
            (self.log.folder / f"file_listing_{self.log.stamp}.txt").write_text(result.output)
            for remote_path in self.db.sync_listing(self.dataset_id, listed):
                self.log(f"{remote_path}: no longer listed; left on disk as it is")
            self.db.update_dataset(self.dataset_id, endpoint=url)
            self.log(f"Listed {len(listed)} files on {url}")
            return url
        if outcome.not_found and not outcome.errors:
            raise DatasetStopped(
                NO_ACCESS,
                "not granted to this conf's user, or not there: " + "; ".join(outcome.not_found),
            )
        errors = outcome.errors + outcome.not_found
        if not errors:
            raise DatasetStopped(FAILED, "no live endpoint left to list it on")
        raise DatasetStopped(FAILED, "listing failed: " + "; ".join(errors))

    def plain(self, record: FileRecord) -> Path:
        return self.folder / record.local_path.removesuffix(".c4gh")

    def fetch(self, group: list[FileRecord]) -> None:
        """Download the group's missing files in batches, settling each batch as it lands."""
        self.settle(group)
        consecutive_failures = 0
        while True:
            missing = [f for f in self.current(group) if f.state == MISSING]
            if not missing:
                return
            batch = missing[: self.config.download_batch_size]
            self.runner.ensure_room(2 * sum(f.size for f in batch))  # .c4gh + decrypted copy
            result = self.cli.download(
                self.url, self.dataset_id, self.folder, [f.remote_path for f in batch],
                deadline=self.runner.deadline,
            )
            self.log.output(f"download {len(batch)} files from {self.url}", result)
            # A batch holds only files with nothing on disk, so whatever is there came now.
            self.runner.bytes_downloaded += sum(
                f.size for f in batch if encrypted(self.plain(f)).is_file()
            )
            if not result.ok:
                raise_if_fatal(result)
            # Settle what landed even when stopping: those files are done, not pending.
            failed_here = self.settle(batch)
            self.db.refresh_counts(self.dataset_id)  # so `status` shows progress mid-run
            self.runner.check_stop(result)
            after = {f.remote_path: f for f in self.current(batch)}
            if any(after[f.remote_path].state == DONE for f in batch):
                consecutive_failures = 0
                self.delivered.add(self.url)
            if result.ok:
                continue
            error = last_error_line(result.output)
            kind = classify(result.output)
            if kind is ErrorKind.ENDPOINT_DEAD or (
                kind is ErrorKind.WRONG_ENDPOINT
                and self.url not in self.delivered
                and self.other_live_endpoint() is not None
            ):
                # The endpoint's fault, not the file's: no blame, no wait.
                self.switch_endpoint(error)
                continue
            if kind is ErrorKind.WRONG_ENDPOINT:
                # An endpoint that has delivered this dataset's files, or the only one left,
                # dropped the connection: retry it after a wait. Not the file's fault.
                self.log(f"Connection dropped by {self.url}, retrying there: {error}")
            else:
                # sda-cli stops at the first file it can't fetch: that one takes the blame.
                culprit = next(
                    (f for f in batch
                     if after[f.remote_path].state == MISSING
                     and f.remote_path not in failed_here),
                    None,
                )
                if culprit is not None:
                    abandoned = self.db.record_file_failure(
                        self.dataset_id, culprit.remote_path, error, self.config.max_file_failures
                    )
                    if abandoned:
                        self.log(
                            f"{culprit.local_path}: abandoned after repeated failures: {error}"
                        )
            consecutive_failures += 1
            if consecutive_failures >= self.config.max_consecutive_failures:
                raise DatasetStopped(
                    FAILED, f"{consecutive_failures} failed calls in a row; last: {error}"
                )
            self.runner.backoff(consecutive_failures)

    def current(self, group: list[FileRecord]) -> list[FileRecord]:
        wanted = {f.remote_path for f in group}
        return [f for f in self.db.files(self.dataset_id) if f.remote_path in wanted]

    def settle(self, group: list[FileRecord]) -> set[str]:
        """Bring the database in line with the disk: decrypt what landed, then verify it.

        A file counts as present when `x` or `x.c4gh` exists, so a decryption problem never
        causes a re-download of files already on disk. A note in the earlier bash loop said:
        "In case the download is done but the decryption is pending, then it should only
        invoke the decryption algorithm. The --continue flag is not robust to evaluate
        whether a file is present but decrypted already and will try to download it." That
        loop's `decrypt --clean` deleted each `.c4gh`, after which `--continue` saw nothing
        and fetched the file again.

        Returns the remote paths that had a failure counted against them here.
        """
        # One decrypt batch at a time, verified (and its .c4gh deleted) before the next: the
        # encrypted and decrypted copies of a whole dataset never sit on the share together.
        failed: set[str] = set()
        size = self.config.decrypt_batch_size
        for start in range(0, len(group), size):
            chunk = group[start : start + size]
            self.decrypt(chunk)
            failed |= self.verify(chunk)
        return failed

    def verify(self, group: list[FileRecord]) -> set[str]:
        """Mark what decrypted and checks out as done; reject the rest. Returns the rejected."""
        failed: set[str] = set()
        for record in group:
            plain = self.plain(record)
            enc = encrypted(plain)
            if record.state == DONE and plain.is_file() and not enc.exists():
                continue  # verified on an earlier pass; don't hash it again
            if enc.exists() and self.runner.stop_reason:
                continue  # decryption was cut short; the next run picks it up as it is
            if not plain.is_file():
                if enc.is_file():
                    self.reject(record, "decryption failed (see the log)")
                    failed.add(record.remote_path)
                elif record.state == DONE:
                    self.db.update_file(self.dataset_id, record.remote_path, state=MISSING)
                continue
            # Every file gets this check: the listing's size is the decrypted size.
            size = plain.stat().st_size
            if size != record.size:
                self.reject(record, f"size mismatch: {size} bytes, the listing says {record.size}")
                failed.add(record.remote_path)
                continue
            expected = self.checksums.get(record.local_path.removesuffix(".c4gh"))
            if expected is not None:
                actual = sha256_file(plain)
                if actual != expected:
                    self.reject(
                        record, f"checksum mismatch: image.xml has {expected}, got {actual}"
                    )
                    failed.add(record.remote_path)
                    continue
            # The .c4gh goes only now: decryption, and verification where possible, passed.
            enc.unlink(missing_ok=True)
            self.db.update_file(
                self.dataset_id, record.remote_path, state=DONE, verified=expected is not None
            )
            self.runner.files_done += 1
        return failed

    def decrypt(self, group: list[FileRecord]) -> None:
        """Decrypt every `x.c4gh` in the group, in bounded batches.

        Never `--clean`. sda-cli writes `x` in place, without a `.part`, so an interrupted
        decryption leaves a truncated `x`. On the next run, sda-cli would skip it as "already
        decrypted" and `--clean` would delete the good `x.c4gh`. So when both exist, `x` is
        untrusted and decrypted again with `--force-overwrite`, and the `.c4gh` is removed
        only once `x` has passed verification.
        """
        if self.runner.stop_reason:
            return  # files left as .c4gh are picked up by the next run
        pending = [f for f in group if encrypted(self.plain(f)).is_file()]
        # Split before decrypting: the first pass creates the `x` the split looks at.
        fresh = [f for f in pending if not self.plain(f).exists()]
        untrusted = [f for f in pending if self.plain(f).exists()]
        for force, chunk_files in ((False, fresh), (True, untrusted)):
            size = self.config.decrypt_batch_size
            for start in range(0, len(chunk_files), size):
                records = chunk_files[start : start + size]
                self.runner.ensure_room(sum(f.size for f in records))
                chunk = [encrypted(self.plain(f)) for f in records]
                result = self.cli.decrypt(
                    chunk, force=force, password=self.password, deadline=self.runner.deadline
                )
                self.log.output(
                    f"decrypt {len(chunk)} files{' (force-overwrite)' if force else ''}", result
                )
                # sda-cli exits 0 when single files fail, so look at the output either way.
                raise_if_fatal(result)  # a wrong passphrase fails every file alike
                if classify(result.output) is ErrorKind.WRONG_KEY:
                    # Re-downloading would replace terabytes that someone holding that key
                    # (an earlier tool's or person's) could still decrypt. Leave it to a person.
                    if self.runner.metadata_only:
                        raise DatasetStopped(
                            HELD, "files on disk are encrypted to another key; left as they are"
                        )
                    raise DatasetStopped(
                        HELD,
                        "files on disk are encrypted to another key, not PRIVATE_KEY: decrypt"
                        " them with that key, or move them aside and `requeue` to download again",
                    )
                if result.timed_out or self.runner.stop_reason:
                    break  # files left as .c4gh are picked up by the next run

    def reject(self, record: FileRecord, reason: str) -> None:
        """Move a bad file (and its .c4gh) aside, so it is fetched again, and count a failure.

        A metadata-only run moves only what it fetched itself: a copy that was already on
        disk is left where it is, and the dataset stops here for this run.
        """
        if self.runner.metadata_only and record.remote_path in self.on_disk_before:
            raise DatasetStopped(
                FAILED,
                f"{record.local_path}: {reason}; left in place (a metadata-only run moves"
                " no file it did not fetch)",
            )
        quarantine = self.log.folder / "quarantine" / self.log.stamp
        for path in (self.plain(record), encrypted(self.plain(record))):
            if path.exists():
                target = quarantine / path.relative_to(self.folder)
                target.parent.mkdir(parents=True, exist_ok=True)
                path.replace(target)
        abandoned = self.db.record_file_failure(
            self.dataset_id, record.remote_path, reason, self.config.max_file_failures
        )
        self.log(f"{record.local_path}: {reason}{'; abandoned' if abandoned else ''}")
        if not abandoned:
            self.db.update_file(self.dataset_id, record.remote_path, state=MISSING)
