"""Bronze -> silver: parse Cricsheet match JSON with Spark into conformed, typed Delta tables.

Incremental by default: only matches whose latest bronze version (manifest sha256) differs
from the version recorded in ``silver.matches`` are (re)processed, and their rows are replaced
atomically per table. ``matches`` is written last, so an interrupted run is simply redone.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ipl_lakehouse import reference
from ipl_lakehouse.batch import delta_io
from ipl_lakehouse.batch.schemas import MATCH
from ipl_lakehouse.config import Settings
from ipl_lakehouse.cricket import BOWLER_WICKET_KINDS, NON_DISMISSAL_KINDS
from ipl_lakehouse.logs import get_logger

log = get_logger(__name__)

SILVER_TABLES = (
    "match_players",
    "innings",
    "deliveries",
    "wickets",
    "substitutions",
    "matches",  # must stay last: it is the commit marker for incremental processing
)


@dataclass
class SilverResult:
    processed_match_ids: list[str] = field(default_factory=list)
    full_refresh: bool = False


# --------------------------------------------------------------------------- helpers
def overs_to_balls_col(overs: Column) -> Column:
    """Cricket notation (9.2 = 9 overs 2 balls) to balls, null-safe."""
    whole = F.floor(overs)
    return (whole * 6 + F.round((overs - whole) * 10)).cast("int")


def balls_to_overs_col(balls: Column) -> Column:
    return F.concat(
        F.floor(balls / 6).cast("int").cast("string"), F.lit("."), (balls % 6).cast("string")
    )


def phase_col(over_number: Column, is_super_over: Column) -> Column:
    return (
        F.when(is_super_over, F.lit("super_over"))
        .when(over_number <= 6, F.lit("powerplay"))
        .when(over_number <= 15, F.lit("middle"))
        .otherwise(F.lit("death"))
    )


def reference_frames(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "venues": spark.createDataFrame(list(reference.venues())),
        "teams": spark.createDataFrame(list(reference.teams())),
    }


# --------------------------------------------------------------------------- inputs
def latest_manifest(spark: SparkSession, settings: Settings) -> DataFrame:
    manifest = delta_io.read(spark, settings.lake.manifest)
    order = Window.partitionBy("match_id").orderBy(F.col("ingested_at").desc(), F.col("sha256"))
    return manifest.withColumn("_rn", F.row_number().over(order)).filter("_rn = 1").drop("_rn")


def pending_matches(spark: SparkSession, settings: Settings, full_refresh: bool) -> DataFrame:
    latest = latest_manifest(spark, settings)
    matches_path = settings.lake.silver("matches")
    if full_refresh or not delta_io.exists(spark, matches_path):
        return latest
    current = delta_io.read(spark, matches_path).select(
        "match_id", F.col("source_sha256").alias("sha256")
    )
    return latest.join(current, ["match_id", "sha256"], "left_anti")


def read_bronze(spark: SparkSession, settings: Settings, pending: DataFrame) -> DataFrame:
    rows = pending.select("match_id", "bronze_path", "ingested_at").collect()
    paths = [str((settings.data_dir / r.bronze_path).resolve()) for r in rows]
    raw = (
        spark.read.schema(MATCH)
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .json(paths)
        .withColumn("_file", F.col("_metadata.file_path"))
        .withColumn("match_id", F.regexp_extract("_file", r"match_id=([0-9]+)/", 1))
        .withColumn("source_sha256", F.regexp_extract("_file", r"([0-9a-f]{64})\.json$", 1))
    )
    ingested = pending.select("match_id", F.col("ingested_at").alias("source_ingested_at"))
    return raw.join(F.broadcast(ingested), "match_id").drop("_file")


# --------------------------------------------------------------------------- transforms
def build_matches(raw: DataFrame, refs: dict[str, DataFrame]) -> DataFrame:
    o = "info.outcome"
    winner = F.coalesce(F.col(f"{o}.winner"), F.col(f"{o}.eliminator"), F.col(f"{o}.bowl_out"))
    team1, team2 = F.element_at("info.teams", 1), F.element_at("info.teams", 2)
    base = raw.select(
        "match_id",
        F.year(F.to_date(F.element_at("info.dates", 1))).cast("int").alias("season"),
        F.col("info.season").alias("season_label"),
        F.to_date(F.element_at("info.dates", 1)).alias("match_date"),
        F.to_date(F.element_at("info.dates", -1)).alias("match_end_date"),
        F.size("info.dates").cast("int").alias("match_days"),
        F.col("info.event.name").alias("event_name"),
        F.col("info.event.match_number").cast("int").alias("match_number"),
        F.coalesce(F.col("info.event.stage"), F.lit("League")).alias("stage"),
        F.col("info.event.stage").isNotNull().alias("is_playoff"),
        team1.alias("team1"),
        team2.alias("team2"),
        F.col("info.toss.winner").alias("toss_winner"),
        F.col("info.toss.decision").alias("toss_decision"),
        F.when(F.col(f"{o}.winner").isNotNull(), F.lit("win"))
        .when(F.col(f"{o}.result") == "tie", F.lit("tie"))
        .when(F.col(f"{o}.result") == "no result", F.lit("no_result"))
        .otherwise(F.coalesce(F.col(f"{o}.result"), F.lit("unknown")))
        .alias("result_type"),
        winner.alias("winner"),
        F.when(winner.isNull(), F.lit(None))
        .when(winner == team1, team2)
        .otherwise(team1)
        .alias("loser"),
        F.col(f"{o}.eliminator").alias("super_over_winner"),
        F.col(f"{o}.by.runs").cast("int").alias("win_by_runs"),
        F.col(f"{o}.by.wickets").cast("int").alias("win_by_wickets"),
        F.col(f"{o}.method").alias("result_method"),
        F.coalesce(F.col("info.player_of_match"), F.array()).alias("player_of_match"),
        F.col("info.venue").alias("venue_raw"),
        F.col("info.city").alias("city_raw"),
        F.coalesce(F.col("info.officials.umpires"), F.array()).alias("umpires"),
        F.coalesce(F.col("info.officials.tv_umpires"), F.array()).alias("tv_umpires"),
        F.coalesce(F.col("info.officials.match_referees"), F.array()).alias("match_referees"),
        F.col("info.overs").cast("int").alias("scheduled_overs"),
        F.col("info.balls_per_over").cast("int").alias("balls_per_over"),
        F.col("info.gender").alias("gender"),
        F.col("info.match_type").alias("match_type"),
        F.col("info.team_type").alias("team_type"),
        F.col("meta.data_version").alias("data_version"),
        F.col("meta.revision").cast("int").alias("source_revision"),
        "source_sha256",
        "source_ingested_at",
    )
    venues = refs["venues"].select(
        F.col("venue_raw"), F.col("venue"), F.col("city").alias("_city"), F.col("country")
    )
    teams = refs["teams"]
    out = (
        base.join(F.broadcast(venues), "venue_raw", "left")
        .withColumn("venue", F.coalesce("venue", "venue_raw"))
        .withColumn("city", F.coalesce("_city", "city_raw"))
        .drop("_city")
    )
    for col in ("team1", "team2", "winner"):
        mapping = teams.select(
            F.col("team").alias(col), F.col("franchise").alias(f"{col}_franchise")
        )
        out = out.join(F.broadcast(mapping), col, "left").withColumn(
            f"{col}_franchise", F.coalesce(f"{col}_franchise", col)
        )
    return out.withColumn("silver_built_at", F.current_timestamp())


def _innings_base(raw: DataFrame) -> DataFrame:
    return raw.select(
        "match_id",
        F.year(F.to_date(F.element_at("info.dates", 1))).cast("int").alias("season"),
        F.col("info.teams").alias("_teams"),
        F.col("info.overs").cast("int").alias("scheduled_overs"),
        F.posexplode("innings").alias("_pos", "inn"),
    ).select(
        "match_id",
        "season",
        (F.col("_pos") + 1).cast("int").alias("innings_number"),
        F.col("inn.team").alias("batting_team"),
        F.element_at(F.filter("_teams", lambda t: t != F.col("inn.team")), 1).alias("bowling_team"),
        F.coalesce(F.col("inn.super_over"), F.lit(False)).alias("is_super_over"),
        "scheduled_overs",
        "inn",
    )


def build_deliveries(raw: DataFrame) -> DataFrame:
    overs = _innings_base(raw).select(
        "match_id",
        "season",
        "innings_number",
        "batting_team",
        "bowling_team",
        "is_super_over",
        F.col("inn.powerplays").alias("_powerplays"),
        F.coalesce(F.map_keys("inn.miscounted_overs"), F.array()).alias("_miscounted"),
        F.col("inn.target.runs").cast("int").alias("target_runs"),
        overs_to_balls_col(F.col("inn.target.overs")).alias("target_balls"),
        F.explode("inn.overs").alias("ov"),
    )
    d = overs.select(
        "*",
        F.col("ov.over").cast("int").alias("over_index"),
        F.posexplode("ov.deliveries").alias("_dpos", "d"),
    ).drop("ov")
    e = "d.extras"
    d = d.select(
        "match_id",
        "season",
        "innings_number",
        "is_super_over",
        "batting_team",
        "bowling_team",
        "over_index",
        (F.col("over_index") + 1).alias("over_number"),
        (F.col("_dpos") + 1).cast("int").alias("delivery_seq"),
        F.col("d.actual_delivery").alias("source_ball_label"),
        F.col("d.batter").alias("batter"),
        F.col("d.non_striker").alias("non_striker"),
        F.col("d.bowler").alias("bowler"),
        F.col("d.runs.batter").cast("int").alias("runs_batter"),
        F.col("d.runs.extras").cast("int").alias("runs_extras"),
        F.col("d.runs.total").cast("int").alias("runs_total"),
        F.coalesce(F.col("d.runs.non_boundary"), F.lit(False)).alias("non_boundary"),
        F.coalesce(F.col(f"{e}.wides"), F.lit(0)).cast("int").alias("wides"),
        F.coalesce(F.col(f"{e}.noballs"), F.lit(0)).cast("int").alias("noballs"),
        F.coalesce(F.col(f"{e}.byes"), F.lit(0)).cast("int").alias("byes"),
        F.coalesce(F.col(f"{e}.legbyes"), F.lit(0)).cast("int").alias("legbyes"),
        F.coalesce(F.col(f"{e}.penalty"), F.lit(0)).cast("int").alias("penalty"),
        F.coalesce(F.col("d.wickets"), F.array()).alias("_wickets"),
        F.col("d.review.by").alias("review_by"),
        F.col("d.review.decision").alias("review_decision"),
        F.col("d.review.umpires_call").alias("review_umpires_call"),
        "target_runs",
        "target_balls",
        "_powerplays",
        F.array_contains("_miscounted", F.col("over_index").cast("string")).alias(
            "umpire_miscount"
        ),
    )
    d = (
        d.withColumn("is_wide", F.col("wides") > 0)
        .withColumn("is_noball", F.col("noballs") > 0)
        .withColumn("is_legal", ~F.col("is_wide") & ~F.col("is_noball"))
        .withColumn("batter_faced", ~F.col("is_wide"))
        .withColumn("is_four", (F.col("runs_batter") == 4) & ~F.col("non_boundary"))
        .withColumn("is_six", (F.col("runs_batter") == 6) & ~F.col("non_boundary"))
        .withColumn("is_dot", F.col("is_legal") & (F.col("runs_total") == 0))
        .withColumn("bowler_runs", F.col("runs_batter") + F.col("wides") + F.col("noballs"))
        .withColumn(
            "team_wickets",
            F.size(F.filter("_wickets", lambda w: ~w["kind"].isin(*NON_DISMISSAL_KINDS))),
        )
        .withColumn(
            "bowler_wickets",
            F.size(F.filter("_wickets", lambda w: w["kind"].isin(*BOWLER_WICKET_KINDS))),
        )
        .withColumn("is_wicket", F.size("_wickets") > 0)
        .withColumn("players_out", F.transform("_wickets", lambda w: w["player_out"]))
        .withColumn("wicket_kinds", F.transform("_wickets", lambda w: w["kind"]))
        .withColumn("phase", phase_col(F.col("over_number"), F.col("is_super_over")))
    )
    in_over = Window.partitionBy("match_id", "innings_number", "over_index").orderBy("delivery_seq")
    in_innings = (
        Window.partitionBy("match_id", "innings_number")
        .orderBy("over_index", "delivery_seq")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    d = (
        d.withColumn(
            "legal_ball_in_over",
            F.sum(F.col("is_legal").cast("int")).over(
                in_over.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "ball_label",
            F.concat(
                F.col("over_index").cast("string"),
                F.lit("."),
                (F.col("legal_ball_in_over") + F.when(F.col("is_legal"), 0).otherwise(1)).cast(
                    "string"
                ),
            ),
        )
        .withColumn("legal_ball_number", F.sum(F.col("is_legal").cast("int")).over(in_innings))
        .withColumn("team_runs_after", F.sum("runs_total").over(in_innings))
        .withColumn("team_wickets_after", F.sum("team_wickets").over(in_innings))
    )
    # Official (mandatory/batting) powerplay overs from Cricsheet, keyed as over*10 + ball.
    position = F.col("over_index") * 10 + (
        F.col("legal_ball_in_over") + F.when(F.col("is_legal"), 0).otherwise(1)
    )
    d = d.withColumn(
        "in_powerplay",
        F.coalesce(
            F.exists(
                "_powerplays",
                lambda p: (
                    (position >= F.round(p["from"] * 10)) & (position <= F.round(p["to"] * 10))
                ),
            ),
            F.lit(False),
        ),
    )
    d = d.withColumn(
        "delivery_id",
        F.concat_ws(
            "-",
            *(
                F.col(c).cast("string")
                for c in ("match_id", "innings_number", "over_index", "delivery_seq")
            ),
        ),
    )
    # `_wickets` is kept for build_wickets and dropped before the table is written.
    return d.drop("_powerplays")


DELIVERY_OUTPUT_DROP = ("_wickets",)


def build_wickets(deliveries: DataFrame, raw: DataFrame) -> DataFrame:
    registry = raw.select("match_id", F.col("info.registry.people").alias("_people"))
    w = deliveries.select(
        "match_id",
        "season",
        "innings_number",
        "is_super_over",
        "batting_team",
        "bowling_team",
        "over_index",
        "delivery_seq",
        "ball_label",
        "bowler",
        "team_runs_after",
        F.posexplode("_wickets").alias("_wpos", "w"),
    )
    w = w.select(
        "*",
        (F.col("_wpos") + 1).cast("int").alias("wicket_seq"),
        F.col("w.player_out").alias("player_out"),
        F.col("w.kind").alias("kind"),
        F.coalesce(F.transform("w.fielders", lambda f: f["name"]), F.array()).alias("fielders"),
        F.coalesce(
            F.exists("w.fielders", lambda f: F.coalesce(f["substitute"], F.lit(False))),
            F.lit(False),
        ).alias("fielder_is_substitute"),
    ).drop("w", "_wpos")
    w = (
        w.withColumn("is_bowler_wicket", F.col("kind").isin(*BOWLER_WICKET_KINDS))
        .withColumn("counts_as_team_wicket", ~F.col("kind").isin(*NON_DISMISSAL_KINDS))
        .withColumn("credited_bowler", F.when(F.col("is_bowler_wicket"), F.col("bowler")))
    )
    order = Window.partitionBy("match_id", "innings_number").orderBy(
        "over_index", "delivery_seq", "wicket_seq"
    )
    w = w.withColumn(
        "team_wicket_number",
        F.when(
            F.col("counts_as_team_wicket"),
            F.sum(F.col("counts_as_team_wicket").cast("int")).over(
                order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        ),
    ).withColumnRenamed("team_runs_after", "team_runs_at_fall")
    w = w.join(F.broadcast(registry), "match_id", "left")
    return w.withColumn("player_out_id", F.element_at("_people", F.col("player_out"))).drop(
        "_people"
    )


def build_innings(raw: DataFrame, deliveries: DataFrame) -> DataFrame:
    base = _innings_base(raw).select(
        "match_id",
        "season",
        "innings_number",
        "batting_team",
        "bowling_team",
        "is_super_over",
        "scheduled_overs",
        F.col("inn.target.runs").cast("int").alias("target_runs"),
        F.col("inn.target.overs").alias("target_overs"),
        overs_to_balls_col(F.col("inn.target.overs")).alias("target_balls"),
        F.coalesce(F.col("inn.absent_hurt"), F.array()).alias("absent_hurt"),
        F.coalesce(F.col("inn.penalty_runs.pre"), F.lit(0)).cast("int").alias("penalty_runs_pre"),
        F.coalesce(F.col("inn.penalty_runs.post"), F.lit(0)).cast("int").alias("penalty_runs_post"),
        F.coalesce(F.col("inn.declared"), F.lit(False)).alias("declared"),
        F.coalesce(F.col("inn.forfeited"), F.lit(False)).alias("forfeited"),
        F.col("inn.miscounted_overs").alias("miscounted_overs"),
        F.col("inn.powerplays").alias("powerplays"),
    )
    agg = deliveries.groupBy("match_id", "innings_number").agg(
        F.count(F.lit(1)).cast("int").alias("deliveries"),
        F.sum("runs_total").cast("int").alias("delivery_runs"),
        F.sum("team_wickets").cast("int").alias("wickets"),
        F.sum(F.col("is_legal").cast("int")).cast("int").alias("legal_balls"),
        F.sum("runs_extras").cast("int").alias("extras"),
        F.sum("wides").cast("int").alias("wides"),
        F.sum("noballs").cast("int").alias("noballs"),
        F.sum("byes").cast("int").alias("byes"),
        F.sum("legbyes").cast("int").alias("legbyes"),
        F.sum("penalty").cast("int").alias("penalty_extras"),
        F.sum(F.col("is_four").cast("int")).cast("int").alias("fours"),
        F.sum(F.col("is_six").cast("int")).cast("int").alias("sixes"),
        F.sum(F.col("is_dot").cast("int")).cast("int").alias("dot_balls"),
        F.array_distinct(F.flatten(F.collect_list("players_out"))).alias("_out"),
    )
    inn = base.join(agg, ["match_id", "innings_number"], "left")
    for c in (
        "deliveries", "delivery_runs", "wickets", "legal_balls", "extras", "wides", "noballs",
        "byes", "legbyes", "penalty_extras", "fours", "sixes", "dot_balls",
    ):  # fmt: skip
        inn = inn.withColumn(c, F.coalesce(c, F.lit(0)))
    inn = inn.withColumn(
        "runs", F.col("delivery_runs") + F.col("penalty_runs_pre") + F.col("penalty_runs_post")
    )
    out_or_absent = F.size(F.coalesce("_out", F.array())) + F.size("absent_hurt")
    inn = inn.withColumn(
        "all_out",
        F.when(F.col("is_super_over"), F.col("wickets") >= 2).otherwise(out_or_absent >= 10),
    ).drop("_out")
    # Umpires occasionally miscount an over (5 or 7 legal balls). Cricsheet records those overs;
    # official overs/run rates/NRR count every completed over as 6 balls, so adjust for them.
    miscount = F.when(F.col("miscounted_overs").isNull(), F.lit(0)).otherwise(
        F.aggregate(
            F.map_values("miscounted_overs"),
            F.lit(0),
            lambda acc, o: acc + (F.lit(6) - o["balls"].cast("int")),
        )
    )
    return (
        inn.withColumn("official_balls", (F.col("legal_balls") + miscount).cast("int"))
        .withColumn("overs", balls_to_overs_col(F.col("official_balls")))
        .withColumn(
            "run_rate",
            F.when(
                F.col("official_balls") > 0,
                F.round(F.col("runs") * 6 / F.col("official_balls"), 2),
            ),
        )
        .withColumn("scheduled_balls", F.col("scheduled_overs") * 6)
    )


def build_match_players(raw: DataFrame, substitutions: DataFrame) -> DataFrame:
    players = raw.select(
        "match_id",
        F.year(F.to_date(F.element_at("info.dates", 1))).cast("int").alias("season"),
        F.col("info.registry.people").alias("_people"),
        F.explode("info.players").alias("team", "_names"),
    ).select(
        "match_id",
        "season",
        "team",
        "_people",
        F.posexplode("_names").alias("_pos", "player_name"),
    )
    impact = (
        substitutions.filter(F.col("reason") == "impact_player")
        .select("match_id", F.col("team"), F.col("player_in").alias("player_name"))
        .distinct()
        .withColumn("is_impact_substitute", F.lit(True))
    )
    return (
        players.select(
            "match_id",
            "season",
            "team",
            (F.col("_pos") + 1).cast("int").alias("lineup_order"),
            "player_name",
            F.element_at("_people", F.col("player_name")).alias("player_id"),
        )
        .join(impact, ["match_id", "team", "player_name"], "left")
        .withColumn("is_impact_substitute", F.coalesce("is_impact_substitute", F.lit(False)))
    )


def build_substitutions(raw: DataFrame) -> DataFrame:
    rows = (
        _innings_base(raw)
        .select(
            "match_id",
            "season",
            "innings_number",
            F.explode("inn.overs").alias("ov"),
        )
        .select(
            "match_id",
            "season",
            "innings_number",
            F.col("ov.over").cast("int").alias("over_index"),
            F.posexplode("ov.deliveries").alias("_dpos", "d"),
        )
        .select(
            "match_id",
            "season",
            "innings_number",
            "over_index",
            (F.col("_dpos") + 1).cast("int").alias("delivery_seq"),
            F.explode("d.replacements.match").alias("r"),
        )
    )
    return rows.select(
        "match_id",
        "season",
        "innings_number",
        "over_index",
        "delivery_seq",
        F.col("r.team").alias("team"),
        F.col("r.in").alias("player_in"),
        F.col("r.out").alias("player_out"),
        F.col("r.reason").alias("reason"),
    )


def build_people(spark: SparkSession, settings: Settings) -> DataFrame | None:
    """The latest Cricsheet people register landed in bronze (None if never ingested)."""
    state_file = settings.lake.ingest_state_file
    if not state_file.exists():
        return None
    register = json.loads(state_file.read_text(encoding="utf-8")).get("register", {})
    if not register.get("bronze_path"):
        return None
    return (
        spark.read.option("header", "true")
        .csv(str((settings.data_dir / register["bronze_path"]).resolve()))
        .select(
            F.col("identifier").alias("player_id"),
            "name",
            "unique_name",
            F.col("key_cricinfo").alias("cricinfo_id"),
            F.col("key_cricbuzz").alias("cricbuzz_id"),
        )
    )


# --------------------------------------------------------------------------- driver
def transform(raw: DataFrame, refs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    deliveries = build_deliveries(raw)
    substitutions = build_substitutions(raw)
    return {
        "matches": build_matches(raw, refs),
        "innings": build_innings(raw, deliveries),
        "deliveries": deliveries.drop(*DELIVERY_OUTPUT_DROP),
        "wickets": build_wickets(deliveries, raw),
        "substitutions": substitutions,
        "match_players": build_match_players(raw, substitutions),
    }


def build_silver(
    spark: SparkSession, settings: Settings, *, full_refresh: bool = False
) -> SilverResult:
    lake = settings.lake
    if not delta_io.exists(spark, lake.manifest):
        raise RuntimeError(f"no bronze manifest at {lake.manifest}; run `ipl ingest` first")
    pending = pending_matches(spark, settings, full_refresh).cache()
    ids = sorted(r.match_id for r in pending.select("match_id").collect())
    if not ids:
        log.info("silver is up to date")
        pending.unpersist()
        return SilverResult()
    log.info("building silver", extra={"matches": len(ids), "full_refresh": full_refresh})
    raw = read_bronze(spark, settings, pending).cache()
    frames = transform(raw, reference_frames(spark))
    for table in SILVER_TABLES:
        path: Path = lake.silver(table)
        df = frames[table]
        if full_refresh:
            delta_io.overwrite(df, path, partition_by=("season",))
        else:
            delta_io.replace_for_ids(
                spark, df, path, column="match_id", ids=ids, partition_by=("season",)
            )
        log.info("silver table written", extra={"table": table, "matches": len(ids)})
    _write_people(spark, settings)
    raw.unpersist()
    pending.unpersist()
    return SilverResult(processed_match_ids=ids, full_refresh=full_refresh)


def _write_people(spark: SparkSession, settings: Settings) -> None:
    """Rebuild silver.people from the latest register for every player id seen so far."""
    lake = settings.lake
    people = build_people(spark, settings)
    if people is None:
        log.warning("no people register landed; skipping silver.people")
        return
    ids = (
        delta_io.read(spark, lake.silver("match_players"))
        .select("player_id")
        .union(
            delta_io.read(spark, lake.silver("wickets")).select(
                F.col("player_out_id").alias("player_id")
            )
        )
        .where(F.col("player_id").isNotNull())
        .distinct()
    )
    delta_io.overwrite(people.join(ids, "player_id", "inner"), lake.silver("people"))
