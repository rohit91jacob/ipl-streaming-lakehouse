"""Batch orchestration: ingest -> silver -> DQ -> gold -> DQ, each step idempotent."""

from __future__ import annotations

import uuid
from pathlib import Path

from pyspark.sql import SparkSession

from ipl_lakehouse.batch import delta_io
from ipl_lakehouse.batch.gold import GOLD_TABLES, build_gold
from ipl_lakehouse.batch.silver import SILVER_TABLES, build_silver
from ipl_lakehouse.config import Settings
from ipl_lakehouse.ingest.cricsheet import run_ingest
from ipl_lakehouse.logs import get_logger
from ipl_lakehouse.quality.checks import (
    CheckResult,
    enforce,
    gold_checks,
    persist,
    run_checks,
    silver_checks,
)

log = get_logger(__name__)


def run_dq(
    spark: SparkSession, settings: Settings, layer: str, run_id: str | None = None
) -> list[CheckResult]:
    run_id = run_id or uuid.uuid4().hex[:12]
    results: list[CheckResult] = []
    lake = settings.lake
    if layer in ("silver", "all"):
        tables = {t: delta_io.read(spark, lake.silver(t)) for t in SILVER_TABLES}
        results += run_checks(silver_checks(), tables, layer="silver", run_id=run_id)
    if layer in ("gold", "all"):
        tables = {t: delta_io.read(spark, lake.gold(t)) for t in GOLD_TABLES}
        results += run_checks(gold_checks(), tables, layer="gold", run_id=run_id)
    persist(spark, settings, results)
    failed = [r.name for r in results if not r.passed]
    log.info(
        "dq finished",
        extra={"layer": layer, "checks": len(results), "failed": failed, "run_id": run_id},
    )
    enforce(results, settings.dq_fail_on_error)
    return results


def run_batch(
    spark: SparkSession,
    settings: Settings,
    *,
    full_refresh: bool = False,
    skip_ingest: bool = False,
    archive_path: Path | None = None,
    include_register: bool = True,
) -> dict:
    run_id = uuid.uuid4().hex[:12]
    summary: dict = {"run_id": run_id}
    if not skip_ingest:
        summary["ingest"] = run_ingest(
            settings, archive_path=archive_path, include_register=include_register
        )
    silver = build_silver(spark, settings, full_refresh=full_refresh)
    summary["silver_matches_processed"] = len(silver.processed_match_ids)
    run_dq(spark, settings, "silver", run_id)
    summary["gold_tables"] = build_gold(spark, settings)
    run_dq(spark, settings, "gold", run_id)
    log.info("batch finished", extra=summary)
    return summary
