"""bp-download: download and decrypt Bigpicture datasets, tracking them in a status database.

  bp-download add DATASET_ID...         queue datasets
  bp-download run [DATASET_ID...]       download every runnable dataset, or the named ones
  bp-download run --metadata-only [ID...]  only METADATA/, of the named or all listable datasets
  bp-download status                    one line per dataset
  bp-download requeue DATASET_ID...     reset abandoned files, make datasets runnable again
  bp-download import FILE [--adopt]     queue the dataset IDs in a file; adopt folders on disk
  bp-download export-csv PATH           write the status table as CSV, for Windows users

Exit codes: 0 done, 1 bad arguments, 2 settings or pre-flight check failed, 3 stopped on an
error no retry can fix (expired token, wrong passphrase), 130 interrupted.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import signal
import sys
from contextlib import ExitStack
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType

from . import lock, preflight
from .config import SITE_SETTINGS, Config, ConfigError, load_config
from .importer import DATASET_ID, import_datasets
from .runner import EXIT_CONFIG, Runner
from .sdacli import SdaCli
from .status import COMPLETE, HELD, INCOMPLETE, DatasetRecord, RunRecord, SchemaError, StatusDB


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bp-download", description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        help="settings file (default: .env in the checkout; needed when installed)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    add = commands.add_parser("add", help="queue datasets for download")
    add.add_argument("dataset_ids", nargs="+", metavar="DATASET_ID")

    run = commands.add_parser("run", help="download every runnable dataset, or the named ones")
    run.add_argument("dataset_ids", nargs="*", metavar="DATASET_ID")
    run.add_argument(
        "--metadata-only", action="store_true",
        help="fetch only METADATA/ and leave each dataset's status as it is; without"
        " DATASET_IDs, every dataset the conf can list (unknown ones are added as held)",
    )

    commands.add_parser("status", help="show every dataset's status")

    requeue = commands.add_parser(
        "requeue", help="reset abandoned files and make datasets runnable again"
    )
    requeue.add_argument("dataset_ids", nargs="+", metavar="DATASET_ID")

    import_ = commands.add_parser(
        "import", help="queue the dataset IDs in a file (one per line, or a CSV's first column)"
    )
    import_.add_argument("file", type=Path, metavar="FILE")
    import_.add_argument(
        "--adopt", action="store_true",
        help="also hold every dataset folder under DATASETS_ROOT that the database doesn't know",
    )

    prioritise = commands.add_parser(
        "prioritise", help="set a dataset's priority: higher runs first (default 0)"
    )
    prioritise.add_argument("dataset_id", metavar="DATASET_ID")
    prioritise.add_argument("priority", type=int, metavar="N")

    runs = commands.add_parser("runs", help="show recent runs: how long, how much, why they ended")
    runs.add_argument("--last", type=int, default=20, metavar="N", help="default 20")

    export = commands.add_parser("export-csv", help="write the status table to a CSV file")
    export.add_argument("path", type=Path, metavar="PATH")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_config(env_file=args.env_file, needs=needs(args))
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    try:
        with ExitStack() as stack:
            if args.command in LOCKED_COMMANDS:
                stack.enter_context(lock.exclusive(config.status_db))
            db = StatusDB(config.status_db)
            stack.callback(db.close)
            return COMMANDS[args.command](config, db, args)
    except lock.LockedError as exc:
        print(f"Not starting: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    except SchemaError as exc:
        print(f"Status database: {exc}", file=sys.stderr)
        return EXIT_CONFIG


def unknown_ids(db: StatusDB, dataset_ids: Sequence[str]) -> bool:
    unknown = [i for i in dataset_ids if db.dataset(i) is None]
    if unknown:
        print(f"Not in the status database (use `add`): {', '.join(unknown)}", file=sys.stderr)
    return bool(unknown)


def cmd_add(config: Config, db: StatusDB, args: argparse.Namespace) -> int:
    for dataset_id in args.dataset_ids:
        if not DATASET_ID.fullmatch(dataset_id):
            print(f"Not a dataset ID: {dataset_id}", file=sys.stderr)
            return 1
    for dataset_id in args.dataset_ids:
        added = db.add_dataset(dataset_id)
        print(f"{dataset_id}: {'queued' if added else 'already known, left as it is'}")
    return 0


def cmd_run(config: Config, db: StatusDB, args: argparse.Namespace) -> int:
    if unknown_ids(db, args.dataset_ids):
        return 1
    dataset_ids = []
    for dataset_id in args.dataset_ids:
        record = db.dataset(dataset_id)
        if record is not None and record.status in (COMPLETE, INCOMPLETE):
            if args.metadata_only:
                print(f"{dataset_id} is {record.status}: its metadata is already here")
            else:
                print(f"{dataset_id} is {record.status}: `requeue` it first to run it again")
        else:
            dataset_ids.append(dataset_id)
    if args.dataset_ids and not dataset_ids:
        return 0

    cli = SdaCli(config)
    try:
        ready = preflight.check(config, cli)
    except preflight.PreflightError as exc:
        print(f"Not starting: {exc}", file=sys.stderr)
        return exc.exit_code
    left = ready.token_expires - datetime.now(timezone.utc)
    print(f"Download token valid until {ready.token_expires:%Y-%m-%d %H:%M} UTC ({left.days} days)")
    runner = Runner(
        config, db, cli, ready.access_key, ready.token_expires, metadata_only=args.metadata_only
    )

    def on_signal(number: int, _frame: FrameType | None) -> None:
        runner.request_stop(signal.Signals(number).name)

    previous = {sig: signal.signal(sig, on_signal) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        return runner.run(ready.password, dataset_ids)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        if config.export_csv is not None:
            try:
                export_csv(db, config.export_csv)
            except OSError as exc:  # e.g. the share went away; the database still has it all
                print(f"Could not write EXPORT_CSV: {exc}", file=sys.stderr)


def cmd_status(config: Config, db: StatusDB, args: argparse.Namespace) -> int:
    records = db.datasets()
    print(
        f"{'DATASET':<28} {'STATUS':<12} {'PRIO':>4} {'FILES':>13} {'GB':>9}"
        f" {'CHECKED':<7}  LAST ERROR"
    )
    for record in records:
        files, checked = files_and_checked(record)
        size = f"{record.bytes_total / 1e9:.1f}" if record.bytes_total else "?"
        print(
            f"{record.id:<28} {record.status:<12} {record.priority:>4} {files:>13} {size:>9}"
            f" {checked:<7}  {(record.last_error or '')[:80]}"
        )
    print(
        "CHECKED: yes = listed and downloaded by this tool, every done file checked by size"
        " (and checksum where image.xml has one); no = files from earlier runs, not checked"
        " here; - = nothing downloaded yet"
    )
    counts: dict[str, int] = {}
    for record in records:
        counts[record.status] = counts.get(record.status, 0) + 1
    print(f"{len(records)} datasets: " + ", ".join(f"{n} {s}" for s, n in sorted(counts.items())))
    last = db.runs(1)
    print(f"Last run: {format_run(last[0]).strip()}" if last else "Last run: none yet")
    return 0


def files_and_checked(record: DatasetRecord) -> tuple[str, str]:
    """The FILES and CHECKED columns of `status`."""
    total = str(record.files_total) if record.files_total else "?"
    if record.endpoint is not None:  # listed by this tool: it has a record of every file
        files = f"{record.files_done}/{total}"
        if record.files_abandoned:
            files += f" -{record.files_abandoned}"
        return files, "yes"
    # Never listed here: the total, if any, came from an imported list or an earlier tool.
    if record.status == COMPLETE:
        return f"{total}/{total}", "no"
    if record.status == HELD:
        return f"-/{total}", "no"  # files on disk in some state, from earlier runs
    return f"0/{total}", "-"


def cmd_requeue(config: Config, db: StatusDB, args: argparse.Namespace) -> int:
    if unknown_ids(db, args.dataset_ids):
        return 1
    for dataset_id in args.dataset_ids:
        reset = db.requeue(dataset_id)
        print(f"{dataset_id}: pending ({reset} abandoned file(s) reset)")
    return 0


def cmd_import(config: Config, db: StatusDB, args: argparse.Namespace) -> int:
    try:
        report = import_datasets(args.file, db, config.datasets_root if args.adopt else None)
    except (OSError, ValueError) as exc:
        print(f"Import refused, nothing written: {exc}", file=sys.stderr)
        return 1
    print(
        f"{report.added} added, {report.already_known} already known,"
        f" {report.adopted} adopted, 0 rejected"
    )
    return 0


def cmd_export_csv(config: Config, db: StatusDB, args: argparse.Namespace) -> int:
    export_csv(db, args.path)
    return 0


def export_csv(db: StatusDB, path: Path) -> None:
    # The conf user stays in the local database: the export goes to a shared folder.
    names = [f.name for f in dataclasses.fields(DatasetRecord) if f.name != "no_access_for"]
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    with partial.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["dataset_id", *names[1:]])
        for record in db.datasets():
            writer.writerow([getattr(record, name) for name in names])
    partial.replace(path)
    print(f"Wrote {path}")


RUNS_HEADER = (
    f"{'RUN':>5} {'STARTED':<20} {'HOURS':>6} {'EXIT':>4} {'SETS':>4} {'FILES':>7}"
    f" {'GB':>9} {'MB/S':>6}  WHY IT ENDED"
)


def format_run(run: RunRecord) -> str:
    started = datetime.fromisoformat(run.started_at)
    if run.ended_at is None:
        hours, exit_code, speed = "-", "-", "-"
        reason = "still running, or killed outright"
    else:
        seconds = (datetime.fromisoformat(run.ended_at) - started).total_seconds()
        hours = f"{seconds / 3600:.1f}"
        exit_code = str(run.exit_code) if run.exit_code is not None else "-"
        speed = f"{run.bytes_downloaded / 1e6 / seconds:.1f}" if seconds > 0 else "-"
        reason = run.stop_reason or ""
    return (
        f"{run.id:>5} {started:%Y-%m-%d %H:%M:%S} {hours:>6} {exit_code:>4}"
        f" {run.datasets_attempted:>4} {run.files_done:>7} {run.bytes_downloaded / 1e9:>9.1f}"
        f" {speed:>6}  {reason}"
    )


def cmd_runs(config: Config, db: StatusDB, args: argparse.Namespace) -> int:
    print(RUNS_HEADER)
    for run in db.runs(args.last):
        print(format_run(run))
    return 0


def cmd_prioritise(config: Config, db: StatusDB, args: argparse.Namespace) -> int:
    if unknown_ids(db, [args.dataset_id]):
        return 1
    db.update_dataset(args.dataset_id, priority=args.priority)
    print(f"{args.dataset_id}: priority {args.priority}")
    return 0


COMMANDS = {
    "add": cmd_add,
    "run": cmd_run,
    "status": cmd_status,
    "requeue": cmd_requeue,
    "import": cmd_import,
    "export-csv": cmd_export_csv,
    "prioritise": cmd_prioritise,
    "runs": cmd_runs,
}
# Commands that change files or file states take the run lock; reading never waits.
# `add`, `import` and `prioritise` don't (`import` only inserts rows and never touches a file,
# like `add`, and adopting a folder is only a row too): a running run reads its dataset list once, at the start,
# and a months-long run must not block reordering the queue.
LOCKED_COMMANDS = {"run", "requeue"}

# Site settings a command reads. `run` needs them all; the rest need the status database.
STATUS_ONLY = frozenset({"STATUS_DB"})
NEEDS: dict[str, frozenset[str]] = {"run": SITE_SETTINGS}


def needs(args: argparse.Namespace) -> frozenset[str]:
    if args.command == "import" and args.adopt:
        return STATUS_ONLY | {"DATASETS_ROOT"}
    return NEEDS.get(args.command, STATUS_ONLY)
