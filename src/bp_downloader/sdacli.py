"""Calls to the sda-cli binary, and parsing of what it prints.

Every call runs with stdin closed: sda-cli prompts for an overwrite or a passphrase when it
lacks a flag or C4GH_PASSWORD, and a prompt would hang an unattended run. The child gets its
own session, so a Ctrl-C at the terminal reaches only the downloader, which then decides
what to forward.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .status import ListedFile


@dataclass(frozen=True)
class CallResult:
    returncode: int
    output: str
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out


class SdaCli:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._child: subprocess.Popen[str] | None = None

    # --- process control --------------------------------------------------------------

    def interrupt_child(self, force: bool = False) -> None:
        """Forward an interrupt to the running call, or kill it outright."""
        child = self._child
        if child is None or child.poll() is not None:
            return
        try:
            child.send_signal(signal.SIGKILL if force else signal.SIGINT)
        except ProcessLookupError:
            pass

    def _call(
        self,
        args: Sequence[str],
        *,
        deadline: float | None = None,
        env: dict[str, str] | None = None,
    ) -> CallResult:
        timeout = None if deadline is None else max(deadline - time.monotonic(), 0.0)
        child = subprocess.Popen(
            [str(self.config.sda_cli), *args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            env=env,
            start_new_session=True,
        )
        self._child = child
        try:
            try:
                output, _ = child.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                child.send_signal(signal.SIGINT)
                try:
                    output, _ = child.communicate(timeout=30)
                except subprocess.TimeoutExpired:
                    child.kill()
                    output, _ = child.communicate()
                return CallResult(child.returncode, output, timed_out=True)
            return CallResult(child.returncode, output)
        finally:
            self._child = None

    # --- commands ---------------------------------------------------------------------

    def version(self) -> str:
        result = self._call(["version"])
        match = re.search(r"v?(\d+\.\d+\.\d+)", result.output)
        if not result.ok or not match:
            raise RuntimeError(f"`sda-cli version` failed: {result.output.strip()}")
        return match.group(1)

    def list_dataset(self, url: str, dataset_id: str, *, deadline: float | None) -> CallResult:
        return self._call(
            ["--config", str(self.config.sda_config), "list", "--dataset", dataset_id,
             "--url", url, "--bytes", "--api-version", self.config.api_version],
            deadline=deadline,
        )

    def download(
        self,
        url: str,
        dataset_id: str,
        outdir: Path,
        remote_paths: Sequence[str],
        *,
        deadline: float | None,
    ) -> CallResult:
        """Download the given files. sda-cli fetches them in order and stops at the first error.

        `--ignore-existing` is what 0.3.3 called `--continue`. The downloader only passes
        files that are missing, but without the flag sda-cli would prompt if one appeared.
        """
        with tempfile.NamedTemporaryFile("w", prefix="files-", suffix=".txt") as file_list:
            file_list.write("".join(path + "\n" for path in remote_paths))
            file_list.flush()
            return self._call(
                ["--config", str(self.config.sda_config), "download",
                 "--pubkey", str(self.config.public_key), "--dataset-id", dataset_id,
                 "--url", url, "--outdir", str(outdir), "--api-version", self.config.api_version,
                 "--ignore-existing", "--from-file", file_list.name],
                deadline=deadline,
            )

    def decrypt(
        self, files: Sequence[Path], *, force: bool, password: str, deadline: float | None
    ) -> CallResult:
        """Decrypt files next to themselves. Never `--clean`: see runner.decrypt_and_verify.

        Exits 0 even when single files fail, so callers must check the results on disk.
        """
        args = ["decrypt", "--key", str(self.config.private_key)]
        if force:
            args.append("--force-overwrite")
        env = {**os.environ, "C4GH_PASSWORD": password}
        return self._call([*args, *map(str, files)], deadline=deadline, env=env)


# --- parsing ------------------------------------------------------------------------------

_EMAIL_LIKE = re.compile(r"[^@\s]+@[^@\s]+")


def local_path(remote_path: str) -> str:
    """Where sda-cli puts a listed file, relative to --outdir.

    sda-cli drops a leading folder that reads as an e-mail address once `_` becomes `@`
    (`helpers.AnonymizeFilepath`). That is how `DATASET_xxx/IMAGES/...` lands as
    `IMAGES/...`.
    """
    first, sep, rest = remote_path.partition("/")
    if sep and "_" in first and _EMAIL_LIKE.fullmatch(first.replace("_", "@")):
        return rest
    return remote_path


def parse_listing(output: str) -> list[ListedFile]:
    """Parse `sda-cli list --dataset --bytes`: a header, `ID \\t size \\t path` rows, a total.

    The format is the same in 0.3.3 and 0.3.5 (v1 API).
    """
    files: list[ListedFile] = []
    for line in output.splitlines():
        columns = [column.strip() for column in line.split("\t")]
        if len(columns) != 3 or columns[0] == "FileID" or not columns[1].isdigit():
            continue
        file_id, size, remote = columns
        files.append(ListedFile(file_id, int(size), remote, local_path(remote)))
    return files
