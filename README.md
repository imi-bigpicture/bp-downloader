# bp-downloader

Downloads and decrypts Bigpicture datasets with
[`sda-cli`](https://github.com/NBISweden/sda-cli), one dataset at a time. It keeps the state of
every dataset and every file in a SQLite status database, so a run can stop at any point and
the next one carries on.

It replaces a pair of bash scripts (a download loop and a per-dataset retry script) that an
earlier deployment ran for several months. It keeps what worked in them: listing on more than one
endpoint, retry with backoff, a two-stage Ctrl-C, a runtime cap, and logs next to the data.
It also fixes what went wrong in their runs (see [Failure handling](#failure-handling)).

Python 3.10+, standard library only. Tests need `pytest`.

## Install

With [pipx](https://pipx.pypa.io/), straight from the repo:

```bash
pipx install git+https://github.com/imi-bigpicture/bp-downloader
bp-download --help
```

Installed this way, the command has no folder of its own, so give it your settings file on
every call: `bp-download --env-file /path/to/site/.env status`. Relative paths in that file
resolve against its folder, and so does the default `SDA_CLI=./sda-cli`: put the sda-cli
binary next to the `.env`, or set `SDA_CLI` to its path.

Or work from a checkout, with no install at all: `git clone` the repo and use `./bp-download`.
It reads `.env` from the checkout's folder, and sda-cli sits next to it.

### Platform limits

- **Linux and macOS.** The run lock uses `fcntl`, which Windows doesn't have. Tests and CI
  run on Linux.
- **Download API v1 only.** `API_VERSION=v2` is accepted and passed on to sda-cli, but no
  run has used it: the v2 download service has no production address yet. Treat it as
  untested.
- **One sda-cli version**, the pinned one below.

## sda-cli version: 0.3.5 (pinned)

The code passes flags that differ between releases. v0.3.4 renamed `download --continue` to
`--ignore-existing`, and without it sda-cli prompts before overwriting. So the run refuses to
start unless `sda-cli version` matches `SDA_CLI_VERSION`.

Install the binary into the folder holding your `.env` (here the Linux x86_64 one; the
release also has Linux arm64 and macOS builds, with their hashes in its `checksums.txt`):

```bash
cd /path/to/site    # the folder holding your .env (in a checkout: the checkout)
gh release download v0.3.5 -R NBISweden/sda-cli -p 'sda-cli_.0.3.5_Linux_x86_64.tar.gz'
sha256sum sda-cli_.0.3.5_Linux_x86_64.tar.gz
# expected: a1bfa741d92ff454ffbd34fb51697ff0345a62275da40aeed74849f9d041daf7
tar xzf sda-cli_.0.3.5_Linux_x86_64.tar.gz sda-cli
sha256sum sda-cli
# expected: 3208dbc468841150f0b10e8d9270bae296857b7710258cd0889c4be73c698086
```

### Bumping the pinned version

A new sda-cli release can change what this code relies on without any error. Before moving
the pin, read the release notes and the diff since 0.3.5, and check:

1. **The `list` output format**: `list --bytes` output is parsed by `sdacli.parse_listing`,
   and `sdacli.local_path` mirrors where sda-cli puts each listed file on disk.
2. **The `download` and `decrypt` flags** this code passes (`sdacli.py`): `--pubkey`,
   `--dataset-id`, `--outdir`, `--api-version`, `--ignore-existing` and `--from-file` for
   `download`; `--key` and `--force-overwrite` for `decrypt`. Every call runs with stdin
   closed, so a new prompt would fail the call rather than wait. `decrypt` exits 0 when single
   files fail; the code relies on checking the disk, not the exit code.
3. **The error strings** `errors.py` matches (expired token, wrong passphrase, disk full,
   another key, TLS and EOF failures). A changed message silently falls through to
   "transient" and is retried.

Then:

1. Update `tests/fake_sda_cli.py` first, so the fake behaves like the new release.
2. Run the tests (`python3 -m pytest`); fix the code until they pass.
3. Move the pin: `SDA_CLI_VERSION` in `config.py` and `.env.example`, and the version,
   asset name and both hashes in this README.
4. Try it on one small dataset before an unattended run.

## Usage

```bash
./bp-download add aa-Dataset-xxxxxx-xxxxxx   # queue a dataset
./bp-download run                            # work through every runnable dataset
./bp-download run aa-Dataset-xxxxxx-xxxxxx   # just this one (also runs a held dataset)
./bp-download status                         # one line per dataset
./bp-download requeue aa-Dataset-...         # retry abandoned files, or re-run a dataset
./bp-download prioritise aa-Dataset-... 10   # higher runs first (default 0)
./bp-download runs                           # recent runs: how long, how much, why they ended
./bp-download export-csv /path/to/share/status/datasets.csv
```

From a pipx install, the command is `bp-download --env-file ... <command>` instead.

Exit codes: `0` done (failures are recorded per dataset), `1` bad arguments, `2` settings or
a pre-flight check failed, or another run is going (nothing was attempted), `3` stopped on
something a person must fix (token, passphrase, a full share), `130` interrupted.

Only one `run` (or `requeue`) at a time: they take a lock next to the status
database, and a second one is refused with the holder's PID. `status`, `runs` and
`export-csv` never wait.

**Ctrl-C** once: the current sda-cli call is interrupted, the dataset is recorded as
`interrupted`, and no new dataset starts. Twice: the call is killed. SIGTERM does the same.

To see which datasets your conf can reach:
`./sda-cli --config /path/to/secrets/s3cmd-download.conf list --datasets --url https://download.bp.nbis.se`.

## Settings

Settings come from the environment first, then the settings file (`.env` in the checkout, or
the file `--env-file` names), then the defaults.
[`.env.example`](.env.example) lists every setting. Copy it to `.env` and fill in the five
site settings, which have no default so that a run never starts against another site's
paths: `SDA_CONFIG`, `PUBLIC_KEY`, `C4GH_PASSWORD_FILE`, `DATASETS_ROOT` and `STATUS_DB`.
A missing one is a configuration error (exit 2) that names it, before any network call.
`run` needs all five; `status`, `runs`, `export-csv`, `add`, `import`, `prioritise` and `requeue` need
only `STATUS_DB`, and `import --adopt` also `DATASETS_ROOT`. Every other setting has a default.
Relative paths are relative to the settings file's folder.

`REQUIRE_MOUNT` is off by default. Turn it on (set it to the mount point) when
`DATASETS_ROOT` is on a network mount with `nofail` in `/etc/fstab`: if that mount fails at
boot, the folder is a plain local directory and a download would fill the local disk.

The private key's passphrase comes from `C4GH_PASSWORD`, or from the file `C4GH_PASSWORD_FILE`
names. It is never accepted inside `.env`.

## What a run does

For each runnable dataset (`pending`, `failed`, `no_access`, `interrupted`, or a stale
`in_progress`), highest priority first, then by ID:

1. **List** it on each endpoint in `DOWNLOAD_URLS`, the one that last worked first. The
   default is the single endpoint `download.bp.nbis.se`, which has served every dataset since
   the 2026-06-01 release. Before that, data sat behind two endpoints on different storage,
   and which one held a dataset wasn't shown anywhere
   ([NBISweden/sda-cli#695](https://github.com/NBISweden/sda-cli/issues/695)). The listing is
   saved in `_download/`. A file whose listed size changed since the last listing is fetched
   again; one no longer listed becomes `gone` and stays on disk. If the dataset's missing
   files won't fit above `MIN_FREE_GB`, it is skipped for now and the run moves on.
2. **Metadata first.** Download `METADATA/`, decrypt it, and read the checksums from
   `METADATA/image.xml`.
3. **Images**, in batches of `DOWNLOAD_BATCH_SIZE` files passed with `--from-file`. Only
   missing files are passed: sda-cli sends one request per listed file even when
   `--ignore-existing` then skips it.
   Before each batch, the run stops (exit 3) if writing it (`.c4gh` plus decrypted copy)
   would eat into `MIN_FREE_GB`.
4. After each batch, **decrypt** what landed, in calls of at most `DECRYPT_BATCH_SIZE` files
   (one call over ~15k files failed with `Argument list too long`). Then **check** every file:
   its size against the listing (which gives decrypted sizes), and each image against its
   `unencrypted_checksum` in `image.xml`. The `.c4gh` is deleted only once its decrypted copy
   has passed. Metadata files, and images that `image.xml` doesn't list (the log counts
   these), are checked by size only. That still catches a failure seen in older downloads: the
   same 5,616-byte S3 bucket listing saved under every image's name.
5. The dataset ends `complete`, or `incomplete` if some files were abandoned.

A file counts as present when `x` or `x.c4gh` is on disk, so a decryption problem never
causes a re-download. `decrypt --clean` is never used: sda-cli decrypts in place without a
`.part`, so an interrupted decryption leaves a truncated `x`. The next run would skip it as
"already decrypted", and `--clean` would then delete the only good copy. Instead, when `x` and
`x.c4gh` both exist, `x` is decrypted again with `--force-overwrite`.

## Folders and files

| Where | What |
|---|---|
| `DATASETS_ROOT/<dataset>/` | The data, as sda-cli lays it out: `METADATA/`, `IMAGES/IMAGE_<id>/` |
| `DATASETS_ROOT/<dataset>/_download/` | `download_<time>.log` per run (each sda-cli call's output included), `file_listing_<time>.txt`, and `quarantine/` |
| `_download/quarantine/<time>/` | Files that failed verification or decryption, moved aside rather than deleted, before being fetched again. Delete them once you've looked |
| `STATUS_DB` | The status database: datasets, files and runs. On local disk, because SQLite locking is not safe over CIFS. Its schema is versioned and upgrades itself when newer code opens it; older code refuses a newer database. It is not backed up: set `EXPORT_CSV` so every run writes the status table to the share |
| `STATUS_DB.lock` | The run lock. Holds the running process's PID |

### Statuses

| Dataset | Meaning | Picked up by `run`? |
|---|---|---|
| `pending` | Not tried yet, or stopped by an expired token or a wrong passphrase | yes |
| `in_progress` | A run is on it. If none is, that run was killed | yes |
| `interrupted` | Stopped by Ctrl-C, SIGTERM, the runtime cap, or the token running out | yes |
| `failed` | This run gave up on it (too many failures in a row, or no live endpoint) | yes |
| `no_access` | Every endpoint that answered said 404/403: not granted to this conf's user in REMS | not with the same conf user; yes once another user's conf is installed, or after `requeue` |
| `complete` | Every file decrypted, and verified where `image.xml` has a checksum | no |
| `incomplete` | Done, apart from abandoned files. `requeue` resets them | no |
| `held` | Needs a person. Usually its files on disk are encrypted to another key | only when named |

A file is `missing`, `done`, `abandoned`, or `gone` (dropped from the listing). Its failures
are counted across runs.

In `status`, **FILES** is done/total and **CHECKED** says whether this tool vouches for those
files. `yes`: it listed and downloaded the dataset and checked every done file by size, and
by checksum where `image.xml` has one. `no`: the files come from earlier runs by other tools (an
adopted folder, or a dataset recorded as complete before this tool) and are not checked here;
such a `complete` dataset shows as `N/N  no`. `-`: nothing downloaded yet.

## Failure handling

An earlier deployment's runs hit errors in waves, and most of the damage came from how the scripts reacted to
them. Every sda-cli failure is now classified (`errors.py`) and handled by kind:

| Kind | Seen as | Reaction |
|---|---|---|
| Token expired | `access token has expired`, HTTP 401 | **Stop the run** (exit 3). The dataset goes back to `pending`, and no file is blamed. Also checked before the run, from the token's `exp` claim. An earlier deployment's run retried an expired token for days, and one morning of listing with an expired token marked dozens of datasets failed that were never lost |
| Wrong passphrase | `chacha20poly1305: message authentication failed` | Stop the run (exit 3) |
| Disk full | `no space left on device`, `disk quota exceeded` | Stop the run (exit 3), and blame no file. Retrying would abandon healthy files |
| Encrypted to another key | `could not find matching public key header` | Hold the dataset, and touch nothing. Datasets downloaded earlier by another tool or person are often encrypted to that person's key, and re-downloading them would replace terabytes that key can still decrypt |
| Endpoint dead | TLS/x509 failure, connect timeout | Don't use that endpoint again this run, and switch. `download2.bp.nbis.se` was retired in mid-2026 and fails this way. An earlier deployment's script switched to it and then retried it 149 times in one run |
| Wrong endpoint, or a dropped connection | `unexpected EOF` | With the default single endpoint, always a dropped connection. With several: before an endpoint has delivered any of the dataset's files, switch to the other live endpoint, at most `MAX_ENDPOINT_SWITCHES` times per dataset. Once it has delivered files this run, or when no other endpoint is alive, the EOF is a dropped connection: back off and retry the same endpoint, counted towards `MAX_CONSECUTIVE_FAILURES`. No file is blamed either way. Before this, one dropped connection with download2 dead ended the dataset for the run |
| Not found | HTTP 404/403 while listing | Try the next endpoint; if all say so, `no_access` |
| Transient | HTTP 500/502/503, HTTP/2 stream errors, connection resets, anything unknown | Count a failure against the file sda-cli stopped at, back off (`BASE_SLEEP` doubling, capped at `MAX_SLEEP`) and retry. A file is abandoned after `MAX_FILE_FAILURES`; a dataset is given up for this run after `MAX_CONSECUTIVE_FAILURES` failed calls in a row |

A decryption that leaves no `x` (sda-cli then still exits 0), and an `x` whose size or
checksum doesn't match, all count as a failure of that file. The file is moved to quarantine
and fetched again.

### Pre-flight checks

`run` doesn't start (exit 2) unless:
- `REQUIRE_MOUNT`, if set, is a mount point (see Settings).
- `DATASETS_ROOT` exists on that mount.
- The conf and both keys are readable.
- A passphrase is available.
- sda-cli is the pinned version.
- `DATASETS_ROOT` has more than `MIN_FREE_GB` free.
- No other run holds the lock.

A token that has expired, or expires within `TOKEN_MARGIN_HOURS`, stops it with exit 3. The
token's expiry date is printed at the start of every run; the token itself is never printed.

### The token's runway

A download token lasts about 30 days, and a new one needs a manual login with MFA. A run ends
at the runtime cap or `TOKEN_MARGIN_HOURS` before the token expires, whichever comes first,
and warns at the start when it will be the token. Ending on the token stops between calls,
leaves the dataset `interrupted` (it resumes) and exits 3. `runs` shows the throughput, for
planning when the next renewal is due.

## Importing a list of datasets

```bash
./bp-download import datasets.txt           # queue every ID in the file
./bp-download import datasets.txt --adopt   # ...and hold the folders already on disk
```

The file has one dataset ID per line, or is a CSV with the ID in the first column (the
`export-csv` output works). A header row on the first line, blank lines and a UTF-8 BOM are
fine. IDs look like `aa-Dataset-xxxxxx-xxxxxx`. Any other line refuses the whole file: nothing
is written and the error names the lines.

Every new ID is queued as `pending`, with source `import`. An ID the database already has is
left exactly as it is. With `--adopt`, every `aa-Dataset-*` folder under `DATASETS_ROOT` that
the database still doesn't know is added as `held` (source `adopted`), so a person looks at it
before a `run` touches it; `run` it by name when it should be downloaded. Folders are never
changed. The command prints how many were added, already known and adopted, and 0 rejected
(a rejected line refuses the file instead). It only ever adds, so it is safe to run twice.

## Running on a network share

The data usually goes to a network share, while the status database stays on local disk.

- Mount the share in `/etc/fstab`. With `nofail`, a failed mount leaves the mount point as
  a plain local folder and the machine still boots. Then set `REQUIRE_MOUNT` to the mount
  point, so a run refuses to start rather than fill the local disk. Check by hand with
  `findmnt <mount point>`.
- Keep the conf, the key pair and the passphrase file in a mode-700 folder outside the
  checkout, and point `SDA_CONFIG`, `PUBLIC_KEY` and `C4GH_PASSWORD_FILE` at them.
- Set `EXPORT_CSV` to a folder on the share: the status database is not backed up there.

Things a first test on a new host is likely to show:

- A single HTTP 500 aborts a whole sda-cli download call. Retrying gets through: the same
  file failed and then succeeded two minutes later.
- `sda-cli encrypt` writes `checksum_*` files into the current folder. Don't run it from the
  checkout.
- A download conf only sees the datasets its user has been granted in REMS.

## Development

```bash
python3 -m pytest              # no network: tests drive tests/fake_sda_cli.py
uvx --with pytest mypy         # strict type check
```

`tests/fake_sda_cli.py` stands in for sda-cli 0.3.5. It mimics the behaviour this code
depends on, and each test drives it with a scenario of datasets, endpoints and faults. When
sda-cli's behaviour changes, change the fake first.

GitHub Actions (`.github/workflows/ci.yml`) runs the same two checks on every push and pull
request, on Python 3.10 and 3.12. It installs only `pytest` and `mypy`, and uses no secrets.

## Licence

Copyright 2026 Bigpicture contributors. Licensed under the Apache License, Version 2.0. See
[LICENSE](LICENSE).
