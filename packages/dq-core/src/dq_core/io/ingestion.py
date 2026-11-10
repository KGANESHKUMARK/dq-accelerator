"""Managed Bronze ingestion for the local POC.

The source file is preserved and a small lineage manifest is written beside the
Bronze landing. Cloud object-store connectors remain deferred; this contract is
storage-path agnostic so the same manifest can later point at DBFS, Volumes, or
cloud URIs.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame

from dq_core.runtime.utils import utc_now_iso


@dataclass(frozen=True)
class IngestReceipt:
    dataset: str
    source_path: str
    bronze_path: str
    checksum_sha256: str
    row_count: int
    schema: list[dict[str, str]]
    ingested_at: str
    manifest_path: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset, "source_path": self.source_path, "bronze_path": self.bronze_path,
            "checksum_sha256": self.checksum_sha256, "row_count": self.row_count, "schema": self.schema,
            "ingested_at": self.ingested_at, "manifest_path": self.manifest_path,
        }


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_path(path: str | Path) -> str:
    """Checksum a file, or a directory's files in sorted order.

    Directory checksums cover every regular file except ``_ingest.json`` itself,
    so re-ingesting the same landed data is stable and self-referential-free.
    """
    root = Path(path)
    if root.is_file():
        return sha256_file(root)
    digest = hashlib.sha256()
    files = sorted(f for f in root.rglob("*") if f.is_file() and f.name != "_ingest.json")
    for f in files:
        digest.update(str(f.relative_to(root)).replace("\\", "/").encode("utf-8"))
        with f.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    return digest.hexdigest()


def write_ingest_manifest(dataset: str, source_path: str | Path, bronze_path: str,
                          dataframe: DataFrame) -> IngestReceipt:
    """Write `_ingest.json` for a managed Bronze landing."""
    source = Path(source_path)
    manifest = Path(bronze_path) / "_ingest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    receipt = IngestReceipt(
        dataset=dataset, source_path=str(source), bronze_path=bronze_path,
        checksum_sha256=sha256_path(source), row_count=dataframe.count(),
        schema=[{"name": field.name, "type": field.dataType.simpleString()} for field in dataframe.schema.fields],
        ingested_at=utc_now_iso(), manifest_path=str(manifest))
    manifest.write_text(json.dumps(receipt.to_dict(), indent=2) + "\n", encoding="utf-8")
    return receipt
