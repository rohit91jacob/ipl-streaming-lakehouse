import pytest
from pyspark.sql import functions as F

from ipl_lakehouse.batch.gold import GOLD_TABLES
from ipl_lakehouse.batch.pipeline import run_dq
from ipl_lakehouse.batch.silver import SILVER_TABLES
from ipl_lakehouse.quality.checks import (
    DataQualityError,
    enforce,
    gold_checks,
    run_checks,
    silver_checks,
    too_many_legal_balls,
)
from support import FINAL_2025

pytestmark = pytest.mark.spark


def test_all_checks_pass_on_real_data(silver, gold):
    s = run_checks(
        silver_checks(), {t: silver(t) for t in SILVER_TABLES}, layer="silver", run_id="t"
    )
    g = run_checks(gold_checks(), {t: gold(t) for t in GOLD_TABLES}, layer="gold", run_id="t")
    failed = [(r.name, r.violation_count, r.sample) for r in s + g if not r.passed]
    assert failed == []


def test_corrupted_delivery_fails_loudly(silver):
    tables = {t: silver(t) for t in SILVER_TABLES}
    broken = tables["deliveries"].withColumn(
        "runs_total",
        F.when(
            (F.col("match_id") == FINAL_2025)
            & (F.col("innings_number") == 1)
            & (F.col("over_index") == 0)
            & (F.col("delivery_seq") == 1),
            F.col("runs_total") + 4,
        ).otherwise(F.col("runs_total")),
    )
    results = run_checks(
        silver_checks(), {**tables, "deliveries": broken}, layer="silver", run_id="t"
    )
    by_name = {r.name: r for r in results}
    assert not by_name["deliveries_runs_total_consistent"].passed
    assert by_name["deliveries_runs_total_consistent"].violation_count == 1
    assert not by_name["innings_runs_reconcile"].passed, "cross-table reconciliation catches it too"
    with pytest.raises(DataQualityError, match="deliveries_runs_total_consistent"):
        enforce(results)
    enforce(results, fail_on_error=False)  # report-only mode never raises


def test_seven_ball_over_needs_an_umpire_miscount_flag(spark):
    rows = [("m", 1, 0, True, False)] * 7 + [("m", 1, 1, True, True)] * 7
    df = spark.createDataFrame(
        rows,
        "match_id string, innings_number int, over_index int, is_legal boolean, umpire_miscount boolean",
    )
    bad = too_many_legal_balls(df).collect()
    assert [(r.over_index, r.legal_balls) for r in bad] == [(0, 7)]


def test_run_dq_persists_results(spark, lake):
    results = run_dq(spark, lake, "all", run_id="pytest-run")
    stored = (
        spark.read.format("delta").load(str(lake.lake.dq_results)).filter("run_id = 'pytest-run'")
    )
    assert stored.count() == len(results) == len(silver_checks()) + len(gold_checks())
    assert stored.filter("NOT passed AND severity = 'error'").count() == 0
