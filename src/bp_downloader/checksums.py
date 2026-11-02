"""Checksums from a dataset's METADATA/image.xml.

Each `<FILE filename=... unencrypted_checksum=...>` gives the SHA-256 of a decrypted image,
with `filename` relative to the dataset folder (no `DATASET_xxx/` prefix). The `checksum`
attribute is of the file as submitted, encrypted to Bigpicture's key, so it can't match a
download re-encrypted to ours. Metadata files have no checksum.
"""

from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

IMAGE_XML = "METADATA/image.xml"


class MetadataError(Exception):
    pass


def read_checksums(image_xml: Path) -> dict[str, str]:
    """Map each image's path (relative to the dataset folder) to its lowercase SHA-256."""
    try:
        root = ET.parse(image_xml).getroot()
    except (ET.ParseError, OSError) as exc:
        raise MetadataError(f"cannot read {image_xml}: {exc}") from None
    checksums: dict[str, str] = {}
    for element in root.iter("FILE"):
        filename = element.get("filename", "").strip()
        checksum = element.get("unencrypted_checksum", "").strip()
        if filename and checksum:
            checksums[filename] = checksum.lower()
    return checksums


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
