"""One writer at a time: an exclusive lock next to the status database.

Two runs at once would pick the same datasets (a stale `in_progress` is runnable), and
download, decrypt and quarantine the same files. The kernel drops the lock when the
process ends, however it ends, so a killed run never leaves it behind.
"""

from __future__ import annotations

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


class LockedError(Exception):
    pass


@contextmanager
def exclusive(db_path: Path) -> Iterator[None]:
    path = db_path.with_name(db_path.name + ".lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.seek(0)
            holder = handle.read().strip() or "unknown process"
            raise LockedError(f"another run holds {path}: {holder}") from None
        handle.seek(0)
        handle.truncate()
        started = datetime.now(timezone.utc).isoformat(timespec="seconds")
        handle.write(f"pid {os.getpid()}, since {started}\n")
        handle.flush()
        try:
            yield
        finally:
            handle.seek(0)
            handle.truncate()
