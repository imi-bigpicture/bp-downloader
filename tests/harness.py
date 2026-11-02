"""A throwaway world for end-to-end runs: datasets root, status DB, keys, conf, fake sda-cli."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from bp_downloader.cli import main
from bp_downloader.status import DatasetRecord, FileRecord, StatusDB

PRIMARY = "https://download.bp.nbis.se"
SECONDARY = "https://download2.bp.nbis.se"
PASSPHRASE = "correct horse"
SRC = Path(__file__).resolve().parents[1] / "src"


def jwt_with_exp(exp: float) -> str:
    def part(data: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")

    return f"{part({'alg': 'none'})}.{part({'exp': int(exp)})}.sig"


def image_xml(checksums: dict[str, str]) -> str:
    rows = "\n".join(
        f'<FILE checksum="enc" checksum_method="SHA256" filename="{name}" filetype="dcm" '
        f'unencrypted_checksum="{digest}"/>'
        for name, digest in checksums.items()
    )
    return f'<?xml version="1.0"?><IMAGE_SET><IMAGE alias="I"><FILES>{rows}</FILES></IMAGE></IMAGE_SET>'


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass
class World:
    root: Path
    settings: dict[str, str] = field(default_factory=dict)
    scenario: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.datasets_root = self.root / "datasets"
        self.datasets_root.mkdir()
        self.fake_dir = self.root / "fake"
        self.fake_dir.mkdir()
        secrets = self.root / "secrets"
        secrets.mkdir()
        (secrets / "key.pub.pem").write_text("PUBLIC\n")
        (secrets / "key.sec.pem").write_text("PRIVATE\n")
        (secrets / "c4gh-pass").write_text(PASSPHRASE + "\n")
        self.conf = secrets / "s3cmd-download.conf"
        self.set_token_expiry(time.time() + 30 * 86400)
        wrapper = self.root / "sda-cli"
        wrapper.write_text(
            "#!/bin/sh\n"
            f'FAKE_SDA_DIR="{self.fake_dir}" exec "{sys.executable}" '
            f'"{Path(__file__).with_name("fake_sda_cli.py")}" "$@"\n'
        )
        wrapper.chmod(0o755)
        self.settings = {
            "SDA_CLI": str(wrapper),
            "SDA_CONFIG": str(self.conf),
            "PUBLIC_KEY": str(secrets / "key.pub.pem"),
            "C4GH_PASSWORD_FILE": str(secrets / "c4gh-pass"),
            "DOWNLOAD_URLS": f"{PRIMARY} {SECONDARY}",
            "DATASETS_ROOT": str(self.datasets_root),
            "REQUIRE_MOUNT": "",
            "STATUS_DB": str(self.root / "state" / "status.db"),
            "BASE_SLEEP": "0",
            "MAX_SLEEP": "0",
            "MIN_FREE_GB": "0",
        }
        self.scenario = {
            "version": "v0.3.5",
            "passphrase": PASSPHRASE,
            "datasets": {},
            "endpoints": {PRIMARY: {"lists": [], "serves": []}, SECONDARY: {"dead": True}},
            "faults": [],
        }

    # --- setting the scene ------------------------------------------------------------

    def set_token_expiry(self, exp: float, access_key: str = "someone@example.org") -> None:
        self.conf.write_text(
            "[default]\n"
            f"access_key = {access_key}\n"
            f"access_token = {jwt_with_exp(exp)}\n"
            "host_base = inbox.bp.nbis.se\n"
        )

    def remote_dataset(
        self,
        dataset_id: str,
        images: dict[str, list[str]],
        *,
        on: str = PRIMARY,
        checksums: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Publish a dataset on an endpoint. Returns {decrypted local path: content}.

        Images come first in the listing and metadata last, the way round that would
        break checksum verification if the downloader took the listing order.
        """
        files: dict[str, str] = {}
        for image, levels in images.items():
            for level, content in enumerate(levels):
                files[f"IMAGES/{image}/image-level-{level}.dcm"] = content
        if checksums is None:
            checksums = {path: sha256(content) for path, content in files.items()}
        files["METADATA/image.xml"] = image_xml(checksums)
        files["METADATA/dataset.xml"] = f"<DATASET>{dataset_id}</DATASET>"
        self.scenario["datasets"][dataset_id] = {
            "prefix": "DATASET_" + dataset_id[-6:].upper(),
            "files": files,
        }
        endpoint = self.scenario["endpoints"][on]
        endpoint.setdefault("lists", []).append(dataset_id)
        endpoint.setdefault("serves", []).append(dataset_id)
        return files

    def put(
        self, dataset_id: str, local: str, content: str, *, encrypted: bool = False,
        other_key: bool = False,
    ) -> Path:
        """Leave a file on disk as an earlier run would have: `x`, or `x.c4gh` if encrypted.

        `other_key` makes the `.c4gh` one our private key can't open, like another operator's key files.
        """
        encrypted = encrypted or other_key
        path = self.datasets_root / dataset_id / (local + ".c4gh" if encrypted else local)
        path.parent.mkdir(parents=True, exist_ok=True)
        marker = "OTHERKEY:" if other_key else "C4GH:"
        path.write_text(marker + content if encrypted else content)
        return path

    def fault(self, op: str, **fault: Any) -> None:
        self.scenario["faults"].append({"op": op, **fault})

    def queue(self, *dataset_ids: str) -> int:
        return self.cli("add", *dataset_ids)

    # --- acting -----------------------------------------------------------------------

    def write(self) -> Path:
        (self.fake_dir / "scenario.json").write_text(json.dumps(self.scenario))
        env_file = self.root / "repo" / ".env"
        env_file.parent.mkdir(exist_ok=True)
        env_file.write_text("".join(f"{k}={v}\n" for k, v in self.settings.items()))
        return env_file

    def cli(self, *args: str) -> int:
        return main(["--env-file", str(self.write()), *args])

    def run(self, *dataset_ids: str) -> int:
        return self.cli("run", *dataset_ids)

    def spawn(self, *args: str) -> subprocess.Popen[str]:
        """Start the CLI as its own process, for signal tests."""
        env = {**os.environ, "PYTHONPATH": str(SRC)}
        return subprocess.Popen(
            [sys.executable, "-m", "bp_downloader", "--env-file", str(self.write()), *args],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    # --- observing --------------------------------------------------------------------

    def db(self) -> StatusDB:
        return StatusDB(Path(self.settings["STATUS_DB"]))

    def dataset(self, dataset_id: str) -> DatasetRecord:
        record = self.db().dataset(dataset_id)
        assert record is not None, f"{dataset_id} is not in the status database"
        return record

    def files(self, dataset_id: str) -> dict[str, FileRecord]:
        return {record.local_path: record for record in self.db().files(dataset_id)}

    def calls(self) -> list[dict[str, Any]]:
        log = self.fake_dir / "calls.jsonl"
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    def calls_to(self, command: str) -> list[list[str]]:
        """argv of each call to an sda-cli subcommand, in call order."""
        return [call["argv"] for call in self.calls() if command in call["argv"]]

    def download_lists(self) -> list[list[str]]:
        """The remote paths handed to each `download --from-file` call, in call order."""
        return [call["files"] for call in self.calls() if "download" in call["argv"]]

    def urls_used(self, command: str) -> list[str]:
        return [argv[argv.index("--url") + 1] for argv in self.calls_to(command)]

    def on_disk(self, dataset_id: str) -> dict[str, str]:
        folder = self.datasets_root / dataset_id
        return {
            str(path.relative_to(folder)): path.read_text()
            for path in sorted(folder.rglob("*"))
            if path.is_file() and "_download" not in path.parts
        }
