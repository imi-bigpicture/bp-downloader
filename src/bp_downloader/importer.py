"""Queue the dataset IDs in a file, and adopt dataset folders already on disk.

Nothing on disk is changed. Datasets the database already knows are left alone, so an import
can be run again safely; it only ever adds.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

from .status import HELD, PENDING, StatusDB

DATASET_ID = re.compile(r"aa-Dataset-[a-z0-9]{6}-[a-z0-9]{6}")
HEADER_NAMES = {"datasetid", "dataset", "id"}  # `export-csv` writes "dataset_id"
MAX_NAMED = 5


@dataclass
class Report:
    added: int = 0
    already_known: int = 0
    adopted: int = 0


def is_header(cell: str) -> bool:
    return re.sub(r"[^a-z]", "", cell.lower()) in HEADER_NAMES


def read_ids(path: Path) -> list[str]:
    """The IDs in a file: one per line, or the first column of a CSV.

    Blank lines are skipped, and so is a header on the first non-blank line. Any other line
    that is not a dataset ID refuses the whole file, naming the lines.
    """
    ids: list[str] = []
    bad: list[str] = []
    first = True
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue
            cell = row[0].strip()
            is_first, first = first, False
            if is_first and is_header(cell):
                continue
            if DATASET_ID.fullmatch(cell):
                ids.append(cell)
            else:
                bad.append(f"line {reader.line_num}: not a dataset ID: {cell!r}")
    if bad:
        shown = "; ".join(bad[:MAX_NAMED])
        more = f" (and {len(bad) - MAX_NAMED} more)" if len(bad) > MAX_NAMED else ""
        raise ValueError(f"{path}: {len(bad)} rejected: {shown}{more}")
    return ids


def import_datasets(path: Path, db: StatusDB, datasets_root: Path | None = None) -> Report:
    """Queue the file's IDs as `pending`; with `datasets_root`, hold the folders nobody knows.

    Raises ValueError, before anything is written, if a line is not a dataset ID.
    """
    ids = read_ids(path)  # raises before anything is written
    report = Report()
    with db.transaction():
        for dataset_id in ids:
            if db.add_dataset(dataset_id, status=PENDING, source="import"):
                report.added += 1
            else:
                report.already_known += 1
        if datasets_root is not None and datasets_root.is_dir():
            for folder in sorted(datasets_root.iterdir()):
                if folder.is_dir() and DATASET_ID.fullmatch(folder.name) and db.add_dataset(
                    folder.name, status=HELD, source="adopted",
                    note="Folder found on disk, not queued: check it, then `run` it by name"
                    " to download it",
                ):
                    report.adopted += 1
    return report
