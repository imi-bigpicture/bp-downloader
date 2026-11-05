"""A stand-in for sda-cli 0.3.5, driven by a JSON scenario. No network.

It copies the real binary's behaviour where the downloader depends on it (read from the
v0.3.5 source and from real logs):

- `list --dataset` prints `FileID \\t Size \\t Path` rows, paths prefixed `DATASET_xxx/`.
- `list --datasets` prints two info lines, then `DatasetID \\t Files \\t Size` rows.
- `download --from-file` fetches files in list order and aborts at the first error. It
  drops the `DATASET_xxx/` prefix on disk, writes `<name>.part` then renames, and without
  `--ignore-existing` it prompts on an existing file (EOF on stdin is an error).
- The token's `exp` is checked before any request.
- `decrypt` writes `x` next to `x.c4gh`, skips an `x` that already exists unless
  `--force-overwrite`, and exits 0 even when single files fail.

Encryption is simulated: a downloaded `.c4gh` holds `C4GH:` + the plaintext.

Scenario keys:
  version                  printed by `version`
  passphrase               the private key's passphrase
  datasets                 {id: {"prefix": "DATASET_X", "files": {local_path: plaintext}}}
  endpoints                {url: {"dead": bool, "lists": [ids], "serves": [ids]}};
                           "lists" feeds both `list --dataset` and `list --datasets`
  faults                   [{"op", "url"?, "dataset"?, "path"?, "times", "error"?,
                             "sleep"?, "content"?, "touch"?,
                             "ignore_sigint"?}]
                           op: "list", "list-datasets", "download" or "decrypt"
  expire_token_after_calls the token reads as expired after this many list/download calls
"""

from __future__ import annotations

import base64
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(os.environ["FAKE_SDA_DIR"])
SCENARIO: dict[str, Any] = json.loads((HERE / "scenario.json").read_text())
STATE_FILE = HERE / "state.json"
EXPIRED = "Error: the provided access token has expired, please renew it"


def load_state() -> dict[str, Any]:
    if STATE_FILE.exists():
        state: dict[str, Any] = json.loads(STATE_FILE.read_text())
        return state
    return {"calls": 0, "fault_hits": {}}


STATE = load_state()


def save_state() -> None:
    STATE_FILE.write_text(json.dumps(STATE))


def fail(message: str, code: int = 1) -> None:
    save_state()
    print(message, file=sys.stderr)
    sys.exit(code)


def take_fault(op: str, **context: str) -> dict[str, Any] | None:
    """Return the first matching fault that has hits left, and use one hit."""
    for index, fault in enumerate(SCENARIO.get("faults", [])):
        if fault["op"] != op:
            continue
        if any(key in fault and fault[key] != value for key, value in context.items()):
            continue
        used = STATE["fault_hits"].get(str(index), 0)
        if used >= fault.get("times", 1):
            continue
        STATE["fault_hits"][str(index)] = used + 1
        return dict(fault)
    return None


def apply_side_effects(fault: dict[str, Any]) -> None:
    if fault.get("ignore_sigint"):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    if "touch" in fault:
        Path(fault["touch"]).touch()
    if "sleep" in fault:
        save_state()
        time.sleep(fault["sleep"])


def parse(argv: list[str]) -> tuple[list[str], dict[str, str], set[str]]:
    """Split argv into positional args, --key value options and bare --flags."""
    flags_with_values = {
        "--config", "--dataset", "--url", "--pubkey", "--dataset-id", "--outdir", "--key",
        "--api-version",
    }
    positional: list[str] = []
    options: dict[str, str] = {}
    flags: set[str] = set()
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in flags_with_values:
            options[arg] = argv[i + 1]
            i += 2
        elif arg.startswith("--"):
            flags.add(arg)
            i += 1
        else:
            positional.append(arg)
            i += 1
    return positional, options, flags


def token_expired(config_path: str) -> bool:
    after = SCENARIO.get("expire_token_after_calls")
    if after is not None and STATE["calls"] > after:  # this call is already counted
        return True
    for line in Path(config_path).read_text().splitlines():
        key, _, value = line.partition("=")
        if key.strip() == "access_token":
            payload = value.strip().split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
            return bool(claims["exp"] < time.time())
    fail("Error: could not parse token, reason: no access_token")
    return False


def check_endpoint(url: str, dataset: str) -> None:
    endpoint = SCENARIO["endpoints"].get(url, {"dead": True})
    if endpoint.get("dead"):
        host = url.removeprefix("https://")
        fail(
            f'Error: failed to get files, reason: failed to get response, reason: Get '
            f'"{url}/metadata/datasets/{dataset}/files": tls: failed to verify certificate: '
            f"x509: certificate is valid for ingress.local, not {host}"
        )


def cmd_list_datasets(options: dict[str, str]) -> None:
    """`list --datasets`: one row per dataset the conf's user can see, sizes human-readable."""
    url = options["--url"]
    check_endpoint(url, "")
    fault = take_fault("list-datasets", url=url)
    if fault:
        apply_side_effects(fault)
        if "error" in fault:
            fail(fault["error"])
    host = options.get("--config", "")
    print("The provided access token expires on Wed, 02 Dec 2026 09:53:22 UTC.")
    print(f"Remote server (host_base): https://inbox.bp.nbis.se ({Path(host).name})")
    print(f"{'DatasetID':<41}\t Files \t Size")
    for dataset in SCENARIO["endpoints"][url].get("lists", []):
        spec = SCENARIO["datasets"][dataset]
        size = sum(len(content.encode()) for content in spec["files"].values())
        print(f"{dataset} \t {len(spec['files'])} \t {size} B")


def cmd_list(options: dict[str, str]) -> None:
    url, dataset = options["--url"], options["--dataset"]
    check_endpoint(url, dataset)
    fault = take_fault("list", url=url, dataset=dataset)
    if fault:
        apply_side_effects(fault)
        if "error" in fault:
            fail(fault["error"])
    if dataset not in SCENARIO["endpoints"][url].get("lists", []):
        fail("Error: failed to get files, reason: server returned status 404")
    spec = SCENARIO["datasets"][dataset]
    print(f"{'FileID':<20} \t {'Size':<10} \t Path")
    total = 0
    for number, (local, content) in enumerate(spec["files"].items()):
        size = len(content.encode())
        total += size
        print(f"aa-File-{number:06d}-x \t {size} \t {spec['prefix']}/{local}.c4gh")
    print(f"Dataset size: {total}")


def cmd_download(options: dict[str, str], flags: set[str], positional: list[str]) -> None:
    url, dataset = options["--url"], options["--dataset-id"]
    outdir = Path(options["--outdir"])
    check_endpoint(url, dataset)
    spec = SCENARIO["datasets"][dataset]
    if "--from-file" not in flags:
        fail("Error: this fake only supports --from-file")
    for remote in Path(positional[0]).read_text().split():
        prefix, _, local_c4gh = remote.partition("/")
        local = local_c4gh.removesuffix(".c4gh")
        fault = take_fault("download", url=url, dataset=dataset, path=local)
        if fault:
            apply_side_effects(fault)
            if "error" in fault:
                fail(fault["error"])
        if dataset not in SCENARIO["endpoints"][url].get("serves", []):
            fail("Error: unexpected EOF")
        if prefix != spec["prefix"] or local not in spec["files"]:
            fail("Error: server returned status 404")
        target = outdir / local_c4gh
        if target.exists():
            if "--ignore-existing" in flags:
                continue
            # The real prompt reads stdin; with stdin closed it can never be answered.
            fail("Error: failed to prompt for overwrite: EOF")
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + ".part")
        part.write_text("C4GH:" + spec["files"][local])
        part.rename(target)
        print(f"{local_c4gh}  100% |████████████|")


def cmd_decrypt(options: dict[str, str], flags: set[str], files: list[str]) -> None:
    password = os.environ.get("C4GH_PASSWORD")
    if password is None:
        fail("Error: failed to read password: EOF")  # the prompt, with stdin closed
    if password != SCENARIO["passphrase"]:
        fail(f"Error: chacha20poly1305: message authentication failed, file: {options['--key']}")
    decrypted = 0
    for name in files:
        encrypted = Path(name)
        plain = encrypted.with_name(encrypted.name.removesuffix(".c4gh"))
        if not encrypted.is_file():
            print(f"Error: cannot read input file {name}", file=sys.stderr)
            continue
        if plain.exists() and "--force-overwrite" not in flags:
            print(f"Warning: file {plain} is already decrypted, skipping", file=sys.stderr)
            continue
        fault = take_fault("decrypt", path=str(plain))
        if fault:
            apply_side_effects(fault)
            if "error" in fault:
                print(f"Error decrypting file {name}: {fault['error']}", file=sys.stderr)
                continue
        if not encrypted.read_text().startswith("C4GH:"):  # encrypted to another key
            print(
                f"Error decrypting file {name}: could not create cryp4gh reader: could not find"
                " matching public key header, decryption failed",
                file=sys.stderr,
            )
            continue
        text = encrypted.read_text().removeprefix("C4GH:")
        if fault and "content" in fault:
            text = fault["content"]
        plain.write_text(text)
        decrypted += 1
    if decrypted != len(files):
        print(f"WARNING: {len(files) - decrypted} file(s) could not be decrypted")


def main() -> None:
    argv = sys.argv[1:]
    positional, options, flags = parse(argv)
    record: dict[str, Any] = {"argv": argv}
    if "--from-file" in flags:
        record["files"] = Path(positional[-1]).read_text().split()
    with (HERE / "calls.jsonl").open("a") as log:
        log.write(json.dumps(record) + "\n")
    command = positional.pop(0)
    if command == "version":
        print(f"sda-cli version:  {SCENARIO.get('version', 'v0.3.5')}")
    elif command in ("list", "download"):
        STATE["calls"] += 1  # requests to the server; `version` and `decrypt` make none
        if token_expired(options["--config"]):
            fail(EXPIRED)
        if command == "list" and "--datasets" in flags:
            cmd_list_datasets(options)
        elif command == "list":
            cmd_list(options)
        else:
            cmd_download(options, flags, positional)
    elif command == "decrypt":
        cmd_decrypt(options, flags, positional)
    else:
        fail(f"Error: unknown command {command}")
    save_state()


if __name__ == "__main__":
    main()
