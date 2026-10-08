"""Bronze manifest: an append-only Delta table with one row per landed version of a match file.

Written with delta-rs (``deltalake``) so ingestion needs no JVM; Spark reads the same table.
The latest row per ``match_id`` is the current version; older rows are lineage.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from ipl_lakehouse.query import read_table

MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("match_id", pa.string(), nullable=False),
        pa.field("sha256", pa.string(), nullable=False),
        pa.field("size_bytes", pa.int64(), nullable=False),
        pa.field("bronze_path", pa.string(), nullable=False),
        pa.field("data_version", pa.string()),
        pa.field("season", pa.int32()),
        pa.field("match_date", pa.string()),
        pa.field("change_type", pa.string(), nullable=False),
        pa.field("source_url", pa.string()),
        pa.field("source_archive_sha256", pa.string()),
        pa.field("source_last_modified", pa.string()),
        pa.field("ingest_run_id", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


@dataclass(frozen=True)
class ManifestRecord:
    match_id: str
    sha256: str
    size_bytes: int
    bronze_path: str  # relative to the data dir, POSIX separators
    data_version: str | None
    season: int | None
    match_date: str | None
    change_type: str  # "new" | "changed"
    source_url: str | None
    source_archive_sha256: str | None
    source_last_modified: str | None
    ingest_run_id: str
    ingested_at: datetime


def manifest_exists(path: Path) -> bool:
    return DeltaTable.is_deltatable(str(path))


def append_records(path: Path, records: Iterable[ManifestRecord]) -> int:
    rows = [asdict(r) for r in records]
    if not rows:
        return 0
    table = pa.Table.from_pylist(rows, schema=MANIFEST_SCHEMA)
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), table, mode="append")
    return len(rows)


def read_all(path: Path) -> pa.Table:
    if not manifest_exists(path):
        return MANIFEST_SCHEMA.empty_table()
    return read_table(path)


def latest_versions(path: Path) -> dict[str, dict]:
    """match_id -> latest manifest row (as a dict)."""
    latest: dict[str, dict] = {}
    for row in read_all(path).to_pylist():
        current = latest.get(row["match_id"])
        if current is None or row["ingested_at"] >= current["ingested_at"]:
            latest[row["match_id"]] = row
    return latest
