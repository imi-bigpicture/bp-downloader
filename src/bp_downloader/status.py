"""The status database: one row per dataset, one row per file of a listed dataset.

It replaces the status CSV of the earlier bash scripts, which they rewrote in place with awk
while the loop was still reading it, and which held free-text statuses such as
`Failed: <last error line>`.
It lives on local disk, because SQLite's locking is not safe over CIFS.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Dataset statuses
PENDING = "pending"  # never tried, or worth trying again
IN_PROGRESS = "in_progress"  # a run is on it; if no run is, the run was killed
COMPLETE = "complete"  # every file decrypted, and verified where image.xml has a checksum
INCOMPLETE = "incomplete"  # everything else is done, but some files were abandoned
FAILED = "failed"  # this run gave up on it; the next run tries again
NO_ACCESS = "no_access"  # every endpoint answered 404/403: not granted to this conf's user
INTERRUPTED = "interrupted"  # stopped by Ctrl-C, SIGTERM or the runtime cap
HELD = "held"  # left alone until someone runs it by name

STATUSES = (PENDING, IN_PROGRESS, COMPLETE, INCOMPLETE, FAILED, NO_ACCESS, INTERRUPTED, HELD)
# What a run picks up by itself. HELD only runs when named; COMPLETE and INCOMPLETE never do
# (`requeue` puts them back).
RUNNABLE = (PENDING, IN_PROGRESS, FAILED, NO_ACCESS, INTERRUPTED)

# File states
MISSING = "missing"  # neither x nor x.c4gh on disk
DONE = "done"  # x on disk, decrypted; verified when image.xml has its checksum
ABANDONED = "abandoned"  # failed MAX_FILE_FAILURES times; `requeue` resets it
GONE = "gone"  # no longer in the dataset's listing; left on disk, left out of the counts


class SchemaError(Exception):
    pass


# Numbered upgrades: MIGRATIONS[n] takes a database from user_version n to n + 1. Append
# only; never edit one that a database may still have to run (4 was emptied once every
# database it concerned had passed it). The first is the schema as released before
# versioning, written with IF NOT EXISTS so that such a database upgrades to version 1.
MIGRATIONS: list[str] = [
    """
CREATE TABLE IF NOT EXISTS datasets (
    id              TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    endpoint        TEXT,
    files_total     INTEGER,
    files_done      INTEGER NOT NULL DEFAULT 0,
    files_abandoned INTEGER NOT NULL DEFAULT 0,
    bytes_total     INTEGER,
    key_name        TEXT,
    source          TEXT NOT NULL,
    note            TEXT,
    last_error      TEXT,
    updated_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
    dataset_id  TEXT NOT NULL REFERENCES datasets(id),
    remote_path TEXT NOT NULL,
    local_path  TEXT NOT NULL,
    file_id     TEXT NOT NULL,
    size        INTEGER NOT NULL,
    state       TEXT NOT NULL,
    verified    INTEGER NOT NULL DEFAULT 0,
    failures    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    PRIMARY KEY (dataset_id, remote_path)
);
""",
    # 2: REMS grants access per person, so `no_access` is remembered per conf user; and an
    # order in which datasets are worked on.
    """
ALTER TABLE datasets ADD COLUMN no_access_for TEXT;
ALTER TABLE datasets ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
""",
    # 3: one row per run, for throughput, finish estimates and why each run stopped.
    """
CREATE TABLE runs (
    id                 INTEGER PRIMARY KEY,
    started_at         TEXT NOT NULL,
    ended_at           TEXT,
    exit_code          INTEGER,
    stop_reason        TEXT,
    datasets_attempted INTEGER NOT NULL DEFAULT 0,
    files_done         INTEGER NOT NULL DEFAULT 0,
    bytes_downloaded   INTEGER NOT NULL DEFAULT 0
);
""",
    # 4: retired, now a no-op. It once tidied rows written by a site-specific import that
    # no longer exists, and every database that held such rows is already past it. It
    # stays so the numbering holds: a database at version 4 still opens without running
    # anything, and a new one passes through it unchanged.
    "",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class DatasetRecord:
    id: str
    status: str
    endpoint: str | None
    files_total: int | None
    files_done: int
    files_abandoned: int
    bytes_total: int | None
    key_name: str | None
    source: str
    note: str | None
    last_error: str | None
    updated_at: str
    no_access_for: str | None
    """The conf user (access_key) that got 404/403 everywhere. Kept off the shared export."""
    priority: int


@dataclass(frozen=True)
class FileRecord:
    dataset_id: str
    remote_path: str
    local_path: str
    """Where the decrypted file lives, relative to the dataset folder."""
    file_id: str
    size: int
    state: str
    verified: bool
    failures: int
    last_error: str | None


@dataclass(frozen=True)
class RunRecord:
    id: int
    started_at: str
    ended_at: str | None
    """None: still running, or killed outright (SIGKILL, power loss)."""
    exit_code: int | None
    stop_reason: str | None
    datasets_attempted: int
    files_done: int
    """Files settled (decrypted and checked) in this run, including ones fetched earlier."""
    bytes_downloaded: int
    """Bytes fetched over the network in this run: the basis for throughput."""


@dataclass(frozen=True)
class ListedFile:
    file_id: str
    size: int
    remote_path: str
    local_path: str


class StatusDB:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._migrate()

    def schema_version(self) -> int:
        version: int = self._conn.execute("PRAGMA user_version").fetchone()[0]
        return version

    def _migrate(self) -> None:
        version = self.schema_version()
        if version > len(MIGRATIONS):
            self._conn.close()
            raise SchemaError(
                f"{self.path} is at schema version {version}, newer than this code's"
                f" {len(MIGRATIONS)}: update the code before using it"
            )
        if version == len(MIGRATIONS):
            return
        with self.transaction():
            for number in range(version, len(MIGRATIONS)):
                for statement in MIGRATIONS[number].split(";"):
                    if statement.strip():
                        self._conn.execute(statement)
            # PRAGMA takes no parameters; the value is our own integer.
            self._conn.execute(f"PRAGMA user_version = {len(MIGRATIONS)}")

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    # --- datasets ---------------------------------------------------------------------

    def add_dataset(
        self,
        dataset_id: str,
        *,
        status: str = PENDING,
        source: str = "added",
        note: str | None = None,
        key_name: str | None = None,
        files_total: int | None = None,
        bytes_total: int | None = None,
        last_error: str | None = None,
    ) -> bool:
        """Add a dataset. Returns False, changing nothing, if it is already there."""
        if status not in STATUSES:
            raise ValueError(f"unknown status {status!r}")
        cursor = self._conn.execute(
            "INSERT OR IGNORE INTO datasets (id, status, source, note, key_name, files_total,"
            " bytes_total, last_error, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (dataset_id, status, source, note, key_name, files_total, bytes_total, last_error,
             now()),
        )
        return cursor.rowcount == 1

    def dataset(self, dataset_id: str) -> DatasetRecord | None:
        row = self._conn.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,)).fetchone()
        return DatasetRecord(**row) if row else None

    def datasets(self) -> list[DatasetRecord]:
        rows = self._conn.execute("SELECT * FROM datasets ORDER BY id").fetchall()
        return [DatasetRecord(**row) for row in rows]

    def runnable(self, access_key: str) -> list[str]:
        """What a run picks up by itself, highest priority first.

        A `no_access` dataset is skipped while the same conf user is running: asking again
        can't change REMS. Another user's conf gets to try it.
        """
        marks = ",".join("?" * len(RUNNABLE))
        rows = self._conn.execute(
            f"SELECT id FROM datasets WHERE status IN ({marks})"
            " AND NOT (status = ? AND no_access_for IS ?)"
            " ORDER BY priority DESC, id",
            (*RUNNABLE, NO_ACCESS, access_key),
        ).fetchall()
        return [row["id"] for row in rows]

    def update_dataset(self, dataset_id: str, **fields: object) -> None:
        if "status" in fields and fields["status"] not in STATUSES:
            raise ValueError(f"unknown status {fields['status']!r}")
        fields["updated_at"] = now()
        assignments = ", ".join(f"{name} = ?" for name in fields)
        self._conn.execute(
            f"UPDATE datasets SET {assignments} WHERE id = ?", (*fields.values(), dataset_id)
        )

    def refresh_counts(self, dataset_id: str) -> DatasetRecord:
        """Recompute the file counts of a dataset from its file rows."""
        row = self._conn.execute(
            "SELECT COUNT(*) AS total, COALESCE(SUM(size), 0) AS bytes,"
            " SUM(state = ?) AS done, SUM(state = ?) AS abandoned"
            " FROM files WHERE dataset_id = ? AND state != ?",
            (DONE, ABANDONED, dataset_id, GONE),
        ).fetchone()
        self.update_dataset(
            dataset_id,
            files_total=row["total"],
            bytes_total=row["bytes"],
            files_done=row["done"] or 0,
            files_abandoned=row["abandoned"] or 0,
        )
        record = self.dataset(dataset_id)
        assert record is not None
        return record

    def requeue(self, dataset_id: str) -> int:
        """Give abandoned files a fresh start and make the dataset runnable again."""
        with self.transaction():
            cursor = self._conn.execute(
                "UPDATE files SET state = ?, failures = 0 WHERE dataset_id = ? AND state = ?",
                (MISSING, dataset_id, ABANDONED),
            )
            self.update_dataset(dataset_id, status=PENDING, last_error=None, no_access_for=None)
        self.refresh_counts(dataset_id)
        return cursor.rowcount

    # --- runs -------------------------------------------------------------------------

    def start_run(self) -> int:
        cursor = self._conn.execute("INSERT INTO runs (started_at) VALUES (?)", (now(),))
        assert cursor.lastrowid is not None
        return cursor.lastrowid

    def finish_run(
        self,
        run_id: int,
        *,
        exit_code: int | None,
        stop_reason: str | None,
        datasets_attempted: int,
        files_done: int,
        bytes_downloaded: int,
    ) -> None:
        self._conn.execute(
            "UPDATE runs SET ended_at = ?, exit_code = ?, stop_reason = ?,"
            " datasets_attempted = ?, files_done = ?, bytes_downloaded = ? WHERE id = ?",
            (now(), exit_code, stop_reason, datasets_attempted, files_done, bytes_downloaded,
             run_id),
        )

    def runs(self, last: int) -> list[RunRecord]:
        """The most recent runs, oldest first."""
        rows = self._conn.execute(
            "SELECT * FROM (SELECT * FROM runs ORDER BY id DESC LIMIT ?) ORDER BY id", (last,)
        ).fetchall()
        return [RunRecord(**row) for row in rows]

    # --- files ------------------------------------------------------------------------

    def sync_listing(self, dataset_id: str, listed: Iterable[ListedFile]) -> list[str]:
        """Record a fresh listing. Returns the remote paths that dropped out of it.

        Known files keep their state and failure count, unless their listed size changed
        (or they come back after being gone): then they are fetched again. Files missing
        from the listing become GONE.
        """
        with self.transaction():
            seen: set[str] = set()
            for item in listed:
                seen.add(item.remote_path)
                self._conn.execute(
                    "INSERT INTO files (dataset_id, remote_path, local_path, file_id, size, state)"
                    " VALUES (?, ?, ?, ?, ?, ?)"
                    " ON CONFLICT (dataset_id, remote_path) DO UPDATE SET"
                    " file_id = excluded.file_id, local_path = excluded.local_path,"
                    " state = CASE WHEN files.state = ? OR files.size != excluded.size"
                    "   THEN ? ELSE files.state END,"
                    " verified = CASE WHEN files.size != excluded.size"
                    "   THEN 0 ELSE files.verified END,"
                    " size = excluded.size",
                    (dataset_id, item.remote_path, item.local_path, item.file_id, item.size,
                     MISSING, GONE, MISSING),
                )
            known = self._conn.execute(
                "SELECT remote_path FROM files WHERE dataset_id = ? AND state != ?",
                (dataset_id, GONE),
            ).fetchall()
            gone = sorted(row["remote_path"] for row in known if row["remote_path"] not in seen)
            for remote_path in gone:
                self.update_file(dataset_id, remote_path, state=GONE)
        return gone

    def files(self, dataset_id: str) -> list[FileRecord]:
        rows = self._conn.execute(
            "SELECT * FROM files WHERE dataset_id = ? ORDER BY remote_path", (dataset_id,)
        ).fetchall()
        return [FileRecord(**{**row, "verified": bool(row["verified"])}) for row in rows]

    def update_file(self, dataset_id: str, remote_path: str, **fields: object) -> None:
        assignments = ", ".join(f"{name} = ?" for name in fields)
        self._conn.execute(
            f"UPDATE files SET {assignments} WHERE dataset_id = ? AND remote_path = ?",
            (*fields.values(), dataset_id, remote_path),
        )

    def record_file_failure(
        self, dataset_id: str, remote_path: str, error: str, max_failures: int
    ) -> bool:
        """Count a failure against a file. Returns True if that abandons it."""
        self._conn.execute(
            "UPDATE files SET failures = failures + 1, last_error = ?,"
            " state = CASE WHEN failures + 1 >= ? THEN ? ELSE state END"
            " WHERE dataset_id = ? AND remote_path = ?",
            (error, max_failures, ABANDONED, dataset_id, remote_path),
        )
        row = self._conn.execute(
            "SELECT state FROM files WHERE dataset_id = ? AND remote_path = ?",
            (dataset_id, remote_path),
        ).fetchone()
        return bool(row["state"] == ABANDONED)
