"""Small, explicit Delta Lake write patterns used by the batch layers."""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

_ID = re.compile(r"^[0-9A-Za-z_-]+$")


def exists(spark: SparkSession, path: Path) -> bool:
    return DeltaTable.isDeltaTable(spark, str(path))


def read(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format("delta").load(str(path))


def _compact(df: DataFrame, partition_by: Sequence[str]) -> DataFrame:
    """One file per partition (or one file) - these tables are small, avoid small-file sprawl."""
    return df.repartition(*partition_by) if partition_by else df.coalesce(1)


def overwrite(df: DataFrame, path: Path, partition_by: Sequence[str] = ()) -> None:
    """Atomic full refresh (one Delta commit); readers see the old or the new version."""
    writer = (
        _compact(df, partition_by)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))


def id_predicate(column: str, ids: Iterable[str]) -> str:
    values = sorted(set(ids))
    bad = [v for v in values if not _ID.match(v)]
    if bad:
        raise ValueError(f"refusing to build a predicate from unsafe ids: {bad[:3]}")
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def replace_for_ids(
    spark: SparkSession,
    df: DataFrame,
    path: Path,
    *,
    column: str,
    ids: Iterable[str],
    partition_by: Sequence[str] = (),
) -> None:
    """Replace every row whose ``column`` is in ``ids`` with ``df`` in a single atomic commit.

    ``replaceWhere`` also deletes rows for ids that produce no output rows (e.g. a corrected
    match with no wickets any more), which a plain MERGE would leave behind.
    """
    ids = list(ids)
    df = _compact(df, partition_by)
    if not exists(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(str(path))
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", id_predicate(column, ids))
        .option("mergeSchema", "true")
        .save(str(path))
    )
