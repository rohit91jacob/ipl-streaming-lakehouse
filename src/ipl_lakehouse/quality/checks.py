"""Declarative data-quality checks.

A check is a function returning the *violating rows* of a table; zero rows means it passed.
Results are appended to ``ops/dq_results`` (Delta) for monitoring and any failed ``error``
check raises :class:`DataQualityError`, which makes the pipeline (or the streaming query) fail
loudly instead of publishing bad numbers.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ipl_lakehouse import reference
from ipl_lakehouse.config import Settings
from ipl_lakehouse.cricket import MAX_WICKETS, SUPER_OVER_MAX_WICKETS
from ipl_lakehouse.logs import get_logger

log = get_logger(__name__)

ERROR, WARN = "error", "warn"
Tables = dict[str, DataFrame]


class DataQualityError(RuntimeError):
    def __init__(self, failures: list[CheckResult]):
        self.failures = failures
        names = ", ".join(f"{f.name} ({f.violation_count})" for f in failures)
        super().__init__(f"data quality checks failed: {names}")


@dataclass(frozen=True)
class Check:
    name: str
    table: str
    description: str
    violations: Callable[[Tables], DataFrame]
    severity: str = ERROR


@dataclass
class CheckResult:
    name: str
    table: str
    severity: str
    description: str
    passed: bool
    violation_count: int
    sample: str  # JSON list of up to N violating rows
    layer: str
    run_id: str
    checked_at: datetime = field(default_factory=lambda: datetime.now(UTC))


RESULT_SCHEMA = StructType(
    [
        StructField("name", StringType(), False),
        StructField("table", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("description", StringType(), True),
        StructField("passed", BooleanType(), False),
        StructField("violation_count", LongType(), False),
        StructField("sample", StringType(), True),
        StructField("layer", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("checked_at", TimestampType(), False),
    ]
)


def run_checks(
    checks: Iterable[Check], tables: Tables, *, layer: str, run_id: str, sample_size: int = 5
) -> list[CheckResult]:
    results = []
    for check in checks:
        bad = check.violations(tables)
        count = bad.count()
        sample = (
            [r.asDict(recursive=True) for r in bad.limit(sample_size).collect()] if count else []
        )
        result = CheckResult(
            name=check.name,
            table=check.table,
            severity=check.severity,
            description=check.description,
            passed=count == 0,
            violation_count=count,
            sample=json.dumps(sample, default=str),
            layer=layer,
            run_id=run_id,
        )
        level = "info" if result.passed else ("error" if check.severity == ERROR else "warning")
        getattr(log, level)(
            "dq check",
            extra={
                "check": check.name,
                "passed": result.passed,
                "violations": count,
                "severity": check.severity,
                "layer": layer,
            },
        )
        results.append(result)
    return results


def persist(spark: SparkSession, settings: Settings, results: list[CheckResult]) -> None:
    if not results:
        return
    rows = [tuple(asdict(r)[f.name] for f in RESULT_SCHEMA.fields) for r in results]
    (
        spark.createDataFrame(rows, RESULT_SCHEMA)
        .write.format("delta")
        .mode("append")
        .save(str(settings.lake.dq_results))
    )


def enforce(results: list[CheckResult], fail_on_error: bool = True) -> None:
    failures = [r for r in results if not r.passed and r.severity == ERROR]
    if failures and fail_on_error:
        raise DataQualityError(failures)


# --------------------------------------------------------------------------- reusable rules
def over_ball_counts(deliveries: DataFrame) -> DataFrame:
    """Legal balls per over (and whether Cricsheet flags the over as miscounted by the umpire)."""
    return deliveries.groupBy("match_id", "innings_number", "over_index").agg(
        F.sum(F.col("is_legal").cast("int")).alias("legal_balls"),
        F.max(F.col("umpire_miscount").cast("int")).cast("boolean").alias("umpire_miscount"),
    )


def too_many_legal_balls(deliveries: DataFrame) -> DataFrame:
    return over_ball_counts(deliveries).filter(
        (F.col("legal_balls") > 6) & ~F.col("umpire_miscount")
    )


def too_many_wickets(innings: DataFrame) -> DataFrame:
    limit = F.when(F.col("is_super_over"), SUPER_OVER_MAX_WICKETS).otherwise(MAX_WICKETS)
    return innings.filter(F.col("wickets") > limit)


def runs_total_inconsistent(deliveries: DataFrame) -> DataFrame:
    return deliveries.filter(F.col("runs_total") != F.col("runs_batter") + F.col("runs_extras"))


def _duplicates(df: DataFrame, keys: list[str]) -> DataFrame:
    return df.groupBy(*keys).count().filter("count > 1")


# --------------------------------------------------------------------------- batch suites
def silver_checks() -> list[Check]:
    def extras_breakdown(t: Tables) -> DataFrame:
        d = t["deliveries"]
        parts = (
            F.col("wides") + F.col("noballs") + F.col("byes") + F.col("legbyes") + F.col("penalty")
        )
        return d.filter(F.col("runs_extras") != parts)

    def innings_reconcile(t: Tables) -> DataFrame:
        sums = (
            t["deliveries"]
            .groupBy("match_id", "innings_number")
            .agg(F.sum("runs_total").alias("delivery_runs_check"))
        )
        return (
            t["innings"]
            .join(sums, ["match_id", "innings_number"], "left")
            .filter(
                F.coalesce("delivery_runs_check", F.lit(0))
                + F.col("penalty_runs_pre")
                + F.col("penalty_runs_post")
                != F.col("runs")
            )
        )

    def chase_target(t: Tables) -> DataFrame:
        """Independent cross-check of the source: an unrevised target is first innings + 1."""
        inn = t["innings"].filter(~F.col("is_super_over"))
        first = inn.filter("innings_number = 1").select(
            "match_id", F.col("runs").alias("first_runs")
        )
        second = inn.filter("innings_number = 2 AND target_runs IS NOT NULL").select(
            "match_id", "target_runs", "target_balls", "scheduled_balls"
        )
        matches = t["matches"].filter(F.col("result_method").isNull()).select("match_id")
        return (
            second.join(first, "match_id")
            .join(matches, "match_id")
            .filter(F.col("target_balls") == F.col("scheduled_balls"))
            .filter(F.col("target_runs") != F.col("first_runs") + 1)
        )

    def winner_participates(t: Tables) -> DataFrame:
        m = t["matches"]
        return m.filter(
            F.col("winner").isNotNull()
            & (F.col("winner") != F.col("team1"))
            & (F.col("winner") != F.col("team2"))
        )

    def orphan_deliveries(t: Tables) -> DataFrame:
        return (
            t["deliveries"]
            .select("match_id")
            .distinct()
            .join(t["matches"].select("match_id"), "match_id", "left_anti")
        )

    def unmapped_venues(t: Tables) -> DataFrame:
        known = {r["venue_raw"] for r in reference.venues()}
        return t["matches"].filter(~F.col("venue_raw").isin(*known)).select("match_id", "venue_raw")

    def unmapped_teams(t: Tables) -> DataFrame:
        known = {r["team"] for r in reference.teams()}
        m = t["matches"]
        return m.filter(~F.col("team1").isin(*known) | ~F.col("team2").isin(*known)).select(
            "match_id", "team1", "team2"
        )

    def ball_labels(t: Tables) -> DataFrame:
        return (
            t["deliveries"]
            .filter(
                F.col("source_ball_label").isNotNull()
                & (F.col("ball_label") != F.col("source_ball_label"))
            )
            .select(
                "match_id",
                "innings_number",
                "over_index",
                "delivery_seq",
                "ball_label",
                "source_ball_label",
            )
        )

    def missing_player_ids(t: Tables) -> DataFrame:
        return t["match_players"].filter(F.col("player_id").isNull())

    return [
        Check("deliveries_runs_total_consistent", "deliveries",
              "runs.total == runs.batter + runs.extras", lambda t: runs_total_inconsistent(t["deliveries"])),
        Check("deliveries_extras_breakdown", "deliveries",
              "runs.extras == wides + noballs + byes + legbyes + penalty", extras_breakdown),
        Check("deliveries_unique_key", "deliveries", "one row per (match, innings, over, delivery)",
              lambda t: _duplicates(t["deliveries"], ["match_id", "innings_number", "over_index", "delivery_seq"])),
        Check("deliveries_max_six_legal_balls_per_over", "deliveries",
              "<= 6 legal balls per over unless Cricsheet flags an umpire miscount",
              lambda t: too_many_legal_balls(t["deliveries"])),
        Check("deliveries_belong_to_a_match", "deliveries", "every delivery joins to silver.matches",
              orphan_deliveries),
        Check("innings_max_wickets", "innings", "<= 10 wickets per innings (2 in a super over)",
              lambda t: too_many_wickets(t["innings"])),
        Check("innings_runs_reconcile", "innings", "innings runs == sum of delivery runs + penalties",
              innings_reconcile),
        Check("innings_chase_target_reconciles", "innings",
              "unrevised target == first-innings total + 1 (independent source cross-check)", chase_target),
        Check("matches_unique_id", "matches", "one row per match_id",
              lambda t: _duplicates(t["matches"], ["match_id"])),
        Check("matches_winner_is_a_participant", "matches", "winner is team1 or team2",
              winner_participates),
        Check("matches_venue_mapped", "matches", "venue spelling is in reference/venues.csv",
              unmapped_venues, WARN),
        Check("matches_teams_mapped", "matches", "team names are in reference/teams.csv",
              unmapped_teams, WARN),
        Check("deliveries_ball_label_matches_source", "deliveries",
              "derived over.ball label equals Cricsheet actual_delivery", ball_labels, WARN),
        Check("match_players_have_ids", "match_players", "every player resolves to a registry id",
              missing_player_ids, WARN),
    ]  # fmt: skip


def gold_checks() -> list[Check]:
    def points_balance(t: Tables) -> DataFrame:
        return (
            t["points_table"]
            .groupBy("season")
            .agg(F.sum("won").alias("won"), F.sum("lost").alias("lost"))
            .filter(F.col("won") != F.col("lost"))
        )

    def points_formula(t: Tables) -> DataFrame:
        p = t["points_table"]
        return p.filter(
            (F.col("points") != F.col("won") * 2 + F.col("no_result"))
            | (F.col("played") != F.col("won") + F.col("lost") + F.col("no_result"))
        )

    def batting_reconciles(t: Tables) -> DataFrame:
        bat = (
            t["batting_scorecard"]
            .groupBy("match_id", "innings_number")
            .agg(F.sum("runs").alias("batter_runs"))
        )
        inn = t["innings_summary"].select("match_id", "innings_number", "runs", "extras")
        return inn.join(bat, ["match_id", "innings_number"], "left").filter(
            F.coalesce("batter_runs", F.lit(0)) + F.col("extras") != F.col("runs")
        )

    def bowler_wickets(t: Tables) -> DataFrame:
        bowl = (
            t["bowling_scorecard"]
            .groupBy("match_id", "innings_number")
            .agg(F.sum("wickets").alias("bowler_wickets"))
        )
        inn = t["innings_summary"].select("match_id", "innings_number", "wickets")
        return inn.join(bowl, ["match_id", "innings_number"]).filter(
            F.col("bowler_wickets") > F.col("wickets")
        )

    def one_row_per_team(t: Tables) -> DataFrame:
        return _duplicates(t["points_table"], ["season", "team"])

    def positions_dense(t: Tables) -> DataFrame:
        w = Window.partitionBy("season")
        return (
            t["points_table"]
            .withColumn("_n", F.count(F.lit(1)).over(w))
            .filter((F.col("position") < 1) | (F.col("position") > F.col("_n")))
        )

    return [
        Check("points_wins_equal_losses", "points_table", "per season, total wins == total losses",
              points_balance),
        Check("points_formula", "points_table",
              "points == 2*won + no_result and played == won + lost + no_result", points_formula),
        Check("points_one_row_per_team", "points_table", "one row per (season, team)", one_row_per_team),
        Check("points_positions_valid", "points_table", "positions are 1..n within a season",
              positions_dense),
        Check("batting_runs_reconcile", "batting_scorecard",
              "sum(batter runs) + extras == innings runs", batting_reconciles),
        Check("bowler_wickets_within_team_wickets", "bowling_scorecard",
              "bowler-credited wickets never exceed team wickets", bowler_wickets),
    ]  # fmt: skip
