"""Silver -> gold marts. Every mart is a pure function of silver DataFrames (unit-testable) and is
rebuilt in full on each run: the whole IPL history is ~300k deliveries, so a full, atomic
overwrite is cheaper and simpler than incremental aggregation.

Conventions: super overs are excluded from every statistic except the per-innings tables
(flagged ``is_super_over``); season stats and the points table only use the main innings.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ipl_lakehouse import reference
from ipl_lakehouse.batch import delta_io
from ipl_lakehouse.batch.silver import balls_to_overs_col
from ipl_lakehouse.config import Settings
from ipl_lakehouse.logs import get_logger

log = get_logger(__name__)

GOLD_TABLES = (
    "innings_summary",
    "match_summary",
    "batting_scorecard",
    "bowling_scorecard",
    "player_season_batting",
    "player_season_bowling",
    "points_table",
    "venue_stats",
    "team_phase_stats",
    "head_to_head",
)


def _rate(num: Column, den: Column, scale: float = 1.0, digits: int = 2) -> Column:
    return F.when(den > 0, F.round(num * scale / den, digits))


def _score_text(runs: Column, wickets: Column, overs: Column) -> Column:
    return F.concat(
        runs.cast("string"), F.lit("/"), wickets.cast("string"), F.lit(" ("), overs, F.lit(")")
    )


# --------------------------------------------------------------------------- per match
def innings_summary(matches: DataFrame, innings: DataFrame) -> DataFrame:
    m = matches.select("match_id", "match_date", "stage", "venue", "result_type")
    return innings.join(m, "match_id").select(
        "match_id",
        "season",
        "match_date",
        "stage",
        "venue",
        "innings_number",
        "is_super_over",
        "batting_team",
        "bowling_team",
        "runs",
        "wickets",
        "legal_balls",
        "official_balls",
        "overs",
        "run_rate",
        "extras",
        "wides",
        "noballs",
        "byes",
        "legbyes",
        "fours",
        "sixes",
        "dot_balls",
        "all_out",
        "target_runs",
        "target_balls",
        _score_text(F.col("runs"), F.col("wickets"), F.col("overs")).alias("score_text"),
    )


def match_summary(matches: DataFrame, innings: DataFrame) -> DataFrame:
    main = innings.filter(~F.col("is_super_over"))
    first = main.filter("innings_number = 1").select(
        "match_id",
        F.col("batting_team").alias("first_batting_team"),
        _score_text(F.col("runs"), F.col("wickets"), F.col("overs")).alias("first_innings_score"),
    )
    second = main.filter("innings_number = 2").select(
        "match_id",
        F.col("batting_team").alias("second_batting_team"),
        _score_text(F.col("runs"), F.col("wickets"), F.col("overs")).alias("second_innings_score"),
    )
    dls = F.when(F.col("result_method").isNotNull(), F.lit(" (DLS)")).otherwise(F.lit(""))

    def plural(n: Column, word: str) -> Column:
        return F.concat(
            n.cast("string"), F.lit(f" {word}"), F.when(n != 1, F.lit("s")).otherwise(F.lit(""))
        )

    result_text = (
        F.when(
            F.col("win_by_runs").isNotNull(),
            F.concat(F.col("winner"), F.lit(" won by "), plural(F.col("win_by_runs"), "run"), dls),
        )
        .when(
            F.col("win_by_wickets").isNotNull(),
            F.concat(
                F.col("winner"), F.lit(" won by "), plural(F.col("win_by_wickets"), "wicket"), dls
            ),
        )
        .when(
            F.col("result_type") == "tie",
            F.concat(
                F.lit("Match tied ("), F.col("super_over_winner"), F.lit(" won the super over)")
            ),
        )
        .when(F.col("result_type") == "no_result", F.lit("No result"))
        .otherwise(F.coalesce(F.col("winner"), F.col("result_type")))
    )
    return (
        matches.join(first, "match_id", "left")
        .join(second, "match_id", "left")
        .select(
            "match_id",
            "season",
            "match_date",
            "match_number",
            "stage",
            "is_playoff",
            "venue",
            "city",
            "team1",
            "team2",
            "toss_winner",
            "toss_decision",
            "first_batting_team",
            "first_innings_score",
            "second_batting_team",
            "second_innings_score",
            "result_type",
            "winner",
            "result_method",
            result_text.alias("result_text"),
            F.array_join("player_of_match", ", ").alias("player_of_match"),
        )
    )


def _with_player_ids(df: DataFrame, players: DataFrame, team_col: str, name_col: str) -> DataFrame:
    ids = players.select(
        "match_id",
        F.col("team").alias(team_col),
        F.col("player_name").alias(name_col),
        "player_id",
    )
    return df.join(ids, ["match_id", team_col, name_col], "left")


def batting_scorecard(deliveries: DataFrame, wickets: DataFrame, players: DataFrame) -> DataFrame:
    keys = ["match_id", "season", "innings_number", "is_super_over", "batting_team"]
    appear = deliveries.select(
        *keys,
        F.col("batter").alias("batter"),
        F.col("over_index"),
        F.col("delivery_seq"),
        F.lit(0).alias("_slot"),
    ).unionByName(
        deliveries.select(
            *keys,
            F.col("non_striker").alias("batter"),
            F.col("over_index"),
            F.col("delivery_seq"),
            F.lit(1).alias("_slot"),
        )
    )
    first_seen = appear.groupBy(*keys, "batter").agg(
        F.min(F.struct("over_index", "delivery_seq", "_slot")).alias("_first")
    )
    order = Window.partitionBy("match_id", "innings_number").orderBy("_first")
    positions = first_seen.withColumn("batting_position", F.row_number().over(order)).drop("_first")

    stats = deliveries.groupBy("match_id", "innings_number", "batter").agg(
        F.sum("runs_batter").cast("int").alias("runs"),
        F.sum(F.col("batter_faced").cast("int")).cast("int").alias("balls"),
        F.sum(F.col("is_four").cast("int")).cast("int").alias("fours"),
        F.sum(F.col("is_six").cast("int")).cast("int").alias("sixes"),
        F.sum((F.col("batter_faced") & (F.col("runs_batter") == 0)).cast("int"))
        .cast("int")
        .alias("dots"),
    )
    # A batter can appear in `wickets` twice (retired hurt, then out later): keep the last entry.
    last = Window.partitionBy("match_id", "innings_number", "player_out").orderBy(
        F.col("over_index").desc(), F.col("delivery_seq").desc(), F.col("wicket_seq").desc()
    )
    dismissals = (
        wickets.withColumn("_rn", F.row_number().over(last))
        .filter("_rn = 1")
        .select(
            "match_id",
            "innings_number",
            F.col("player_out").alias("batter"),
            F.col("kind").alias("dismissal_kind"),
            F.col("credited_bowler").alias("dismissal_bowler"),
            F.col("fielders").alias("dismissal_fielders"),
            F.col("counts_as_team_wicket").alias("is_out"),
            F.col("team_wicket_number").alias("fall_of_wicket_number"),
            F.col("team_runs_at_fall").alias("fall_of_wicket_score"),
            F.col("ball_label").alias("fall_of_wicket_ball"),
        )
    )
    card = (
        positions.join(stats, ["match_id", "innings_number", "batter"], "left")
        .join(dismissals, ["match_id", "innings_number", "batter"], "left")
        .fillna(0, subset=["runs", "balls", "fours", "sixes", "dots"])
        .withColumn("is_out", F.coalesce("is_out", F.lit(False)))
        .withColumn("strike_rate", _rate(F.col("runs"), F.col("balls"), 100.0))
    )
    return _with_player_ids(card, players, "batting_team", "batter")


def bowling_scorecard(deliveries: DataFrame, players: DataFrame) -> DataFrame:
    keys = ["match_id", "season", "innings_number", "is_super_over", "bowling_team", "bowler"]
    per_over = deliveries.groupBy("match_id", "innings_number", "over_index").agg(
        F.countDistinct("bowler").alias("_bowlers"),
        F.first("bowler").alias("bowler"),
        F.sum(F.col("is_legal").cast("int")).alias("_legal"),
        F.sum("bowler_runs").alias("_conceded"),
    )
    maidens = (
        per_over.filter(
            (F.col("_bowlers") == 1) & (F.col("_legal") >= 6) & (F.col("_conceded") == 0)
        )
        .groupBy("match_id", "innings_number", "bowler")
        .agg(F.count(F.lit(1)).cast("int").alias("maidens"))
    )
    figures = deliveries.groupBy(*keys).agg(
        F.min(F.struct("over_index", "delivery_seq")).alias("_first"),
        F.sum(F.col("is_legal").cast("int")).cast("int").alias("balls"),
        F.sum("bowler_runs").cast("int").alias("runs_conceded"),
        F.sum("bowler_wickets").cast("int").alias("wickets"),
        F.sum((F.col("is_legal") & (F.col("bowler_runs") == 0)).cast("int"))
        .cast("int")
        .alias("dots"),
        F.sum(F.col("is_four").cast("int")).cast("int").alias("fours_conceded"),
        F.sum(F.col("is_six").cast("int")).cast("int").alias("sixes_conceded"),
        F.sum(F.col("is_wide").cast("int")).cast("int").alias("wides"),
        F.sum(F.col("is_noball").cast("int")).cast("int").alias("noballs"),
    )
    order = Window.partitionBy("match_id", "innings_number").orderBy("_first")
    card = (
        figures.withColumn("bowling_order", F.row_number().over(order))
        .drop("_first")
        .join(maidens, ["match_id", "innings_number", "bowler"], "left")
        .withColumn("maidens", F.coalesce("maidens", F.lit(0)))
        .withColumn("overs", balls_to_overs_col(F.col("balls")))
        .withColumn("economy", _rate(F.col("runs_conceded"), F.col("balls"), 6.0))
    )
    return _with_player_ids(card, players, "bowling_team", "bowler")


# --------------------------------------------------------------------------- per season
def _appearances(players: DataFrame) -> DataFrame:
    return (
        players.filter(F.col("player_id").isNotNull())
        .groupBy("season", "player_id")
        .agg(F.countDistinct("match_id").cast("int").alias("matches"))
    )


def _latest_name(name_col: str) -> Column:
    """Most recent spelling of a player's name (Cricsheet ids are stable, names drift)."""
    return F.max_by(F.col(name_col), F.col("match_id").cast("long")).alias("player_name")


def player_season_batting(batting: DataFrame, players: DataFrame) -> DataFrame:
    main = batting.filter(~F.col("is_super_over") & F.col("player_id").isNotNull())
    stats = main.groupBy("season", "player_id").agg(
        _latest_name("batter"),
        F.sort_array(F.collect_set("batting_team")).alias("teams"),
        F.count(F.lit(1)).cast("int").alias("innings"),
        F.sum("runs").cast("int").alias("runs"),
        F.sum("balls").cast("int").alias("balls"),
        F.sum(F.col("is_out").cast("int")).cast("int").alias("outs"),
        F.max(F.struct(F.col("runs"), (~F.col("is_out")).alias("not_out"))).alias("_best"),
        F.sum(((F.col("runs") >= 50) & (F.col("runs") < 100)).cast("int"))
        .cast("int")
        .alias("fifties"),
        F.sum((F.col("runs") >= 100).cast("int")).cast("int").alias("hundreds"),
        F.sum(((F.col("runs") == 0) & F.col("is_out")).cast("int")).cast("int").alias("ducks"),
        F.sum("fours").cast("int").alias("fours"),
        F.sum("sixes").cast("int").alias("sixes"),
    )
    return (
        stats.join(_appearances(players), ["season", "player_id"], "left")
        .withColumn("not_outs", F.col("innings") - F.col("outs"))
        .withColumn("highest_score", F.col("_best.runs").cast("int"))
        .withColumn(
            "highest_score_text",
            F.concat(
                F.col("_best.runs").cast("string"),
                F.when(F.col("_best.not_out"), F.lit("*")).otherwise(F.lit("")),
            ),
        )
        .drop("_best")
        .withColumn("average", _rate(F.col("runs"), F.col("outs")))
        .withColumn("strike_rate", _rate(F.col("runs"), F.col("balls"), 100.0))
    )


def player_season_bowling(bowling: DataFrame, players: DataFrame) -> DataFrame:
    main = bowling.filter(~F.col("is_super_over") & F.col("player_id").isNotNull())
    stats = main.groupBy("season", "player_id").agg(
        _latest_name("bowler"),
        F.sort_array(F.collect_set("bowling_team")).alias("teams"),
        F.count(F.lit(1)).cast("int").alias("innings"),
        F.sum("balls").cast("int").alias("balls"),
        F.sum("runs_conceded").cast("int").alias("runs_conceded"),
        F.sum("wickets").cast("int").alias("wickets"),
        F.sum("maidens").cast("int").alias("maidens"),
        F.sum("dots").cast("int").alias("dots"),
        F.max(F.struct(F.col("wickets"), (-F.col("runs_conceded")).alias("neg_runs"))).alias(
            "_best"
        ),
        F.sum((F.col("wickets") == 4).cast("int")).cast("int").alias("four_wickets"),
        F.sum((F.col("wickets") >= 5).cast("int")).cast("int").alias("five_wickets"),
    )
    return (
        stats.join(_appearances(players), ["season", "player_id"], "left")
        .withColumn("overs", balls_to_overs_col(F.col("balls")))
        .withColumn(
            "best_figures",
            F.concat(
                F.col("_best.wickets").cast("string"),
                F.lit("/"),
                (-F.col("_best.neg_runs")).cast("string"),
            ),
        )
        .drop("_best")
        .withColumn("average", _rate(F.col("runs_conceded"), F.col("wickets")))
        .withColumn("economy", _rate(F.col("runs_conceded"), F.col("balls"), 6.0))
        .withColumn("strike_rate", _rate(F.col("balls"), F.col("wickets")))
    )


def _adjustments(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(list(reference.match_adjustments())).withColumn(
        "season", F.col("season").cast("int")
    )


def nrr_credit_columns(i1: str = "i1", i2: str = "i2") -> dict[str, Column]:
    """Spark version of :func:`ipl_lakehouse.cricket.nrr_credits` (tests keep them in sync)."""
    revised = F.col(f"{i2}.target_runs").isNotNull() & F.col(f"{i2}.target_balls").isNotNull()
    scheduled = F.col(f"{i1}.scheduled_balls")
    first_runs = F.when(revised, F.col(f"{i2}.target_runs") - 1).otherwise(F.col(f"{i1}.runs"))
    first_balls = F.when(revised, F.col(f"{i2}.target_balls")).otherwise(
        F.when(F.col(f"{i1}.all_out"), scheduled).otherwise(F.col(f"{i1}.official_balls"))
    )
    second_quota = F.when(revised, F.col(f"{i2}.target_balls")).otherwise(scheduled)
    second_runs = F.col(f"{i2}.runs")
    second_balls = F.when(F.col(f"{i2}.all_out"), second_quota).otherwise(
        F.col(f"{i2}.official_balls")
    )
    return {
        "first_runs": first_runs,
        "first_balls": first_balls,
        "second_runs": second_runs,
        "second_balls": second_balls,
    }


def points_table(matches: DataFrame, innings: DataFrame, adjustments: DataFrame) -> DataFrame:
    """League-stage standings with official IPL points and net run rate."""
    void = adjustments.filter(F.col("adjustment") == "void").select("match_id")
    league = matches.filter(~F.col("is_playoff")).join(void, "match_id", "left_anti")
    main = innings.filter(~F.col("is_super_over"))
    i1 = main.filter("innings_number = 1").alias("i1")
    i2 = main.filter("innings_number = 2").alias("i2")
    credits = nrr_credit_columns()
    per_match = (
        league.alias("m")
        .join(i1, F.col("m.match_id") == F.col("i1.match_id"), "left")
        .join(i2, F.col("m.match_id") == F.col("i2.match_id"), "left")
        .select(
            F.col("m.match_id").alias("match_id"),
            F.col("m.season").alias("season"),
            F.col("m.team1").alias("team1"),
            F.col("m.team2").alias("team2"),
            F.col("m.result_type").alias("result_type"),
            F.col("m.winner").alias("winner"),
            F.coalesce(F.col("i1.batting_team"), F.col("m.team1")).alias("first_team"),
            *(expr.alias(name) for name, expr in credits.items()),
        )
    )
    # A tie is a result (NRR counts it); the super-over winner takes the two points.
    decided = F.col("result_type").isin("win", "tie")
    rows = None
    for team_col in ("team1", "team2"):
        team = F.col(team_col)
        is_first = team == F.col("first_team")
        won = decided & (F.col("winner") == team)
        lost = decided & F.col("winner").isNotNull() & (F.col("winner") != team)
        points = (
            F.when(won, 2)
            .when(F.col("result_type") == "no_result", 1)
            .when(decided & F.col("winner").isNull(), 1)
            .otherwise(0)
        )

        def credit(first: str, second: str, is_first: Column = is_first) -> Column:
            return F.when(decided, F.when(is_first, F.col(first)).otherwise(F.col(second)))

        side = per_match.select(
            "season",
            "match_id",
            team.alias("team"),
            F.lit(1).alias("played"),
            won.cast("int").alias("won"),
            lost.cast("int").alias("lost"),
            (F.col("result_type") == "tie").cast("int").alias("tied"),
            (~decided).cast("int").alias("no_result"),
            points.alias("points"),
            credit("first_runs", "second_runs").alias("runs_for"),
            credit("first_balls", "second_balls").alias("balls_for"),
            credit("second_runs", "first_runs").alias("runs_against"),
            credit("second_balls", "first_balls").alias("balls_against"),
        )
        rows = side if rows is None else rows.unionByName(side)
    abandoned = adjustments.filter(F.col("adjustment") == "abandoned_no_ball")
    for team_col in ("team1", "team2"):
        rows = rows.unionByName(
            abandoned.select(
                "season",
                "match_id",
                F.col(team_col).alias("team"),
                F.lit(1).alias("played"),
                F.lit(0).alias("won"),
                F.lit(0).alias("lost"),
                F.lit(0).alias("tied"),
                F.lit(1).alias("no_result"),
                F.lit(1).alias("points"),
                F.lit(None).cast("int").alias("runs_for"),
                F.lit(None).cast("int").alias("balls_for"),
                F.lit(None).cast("int").alias("runs_against"),
                F.lit(None).cast("int").alias("balls_against"),
            )
        )
    table = rows.groupBy("season", "team").agg(
        F.sum("played").cast("int").alias("played"),
        F.sum("won").cast("int").alias("won"),
        F.sum("lost").cast("int").alias("lost"),
        F.sum("tied").cast("int").alias("tied"),
        F.sum("no_result").cast("int").alias("no_result"),
        F.sum("points").cast("int").alias("points"),
        F.coalesce(F.sum("runs_for"), F.lit(0)).cast("int").alias("runs_for"),
        F.coalesce(F.sum("balls_for"), F.lit(0)).cast("int").alias("balls_for"),
        F.coalesce(F.sum("runs_against"), F.lit(0)).cast("int").alias("runs_against"),
        F.coalesce(F.sum("balls_against"), F.lit(0)).cast("int").alias("balls_against"),
    )
    table = (
        table.withColumn(
            "net_run_rate",
            F.when(
                (F.col("balls_for") > 0) & (F.col("balls_against") > 0),
                F.round(
                    F.col("runs_for") * 6 / F.col("balls_for")
                    - F.col("runs_against") * 6 / F.col("balls_against"),
                    3,
                ),
            ),
        )
        .withColumn("overs_for", balls_to_overs_col(F.col("balls_for")))
        .withColumn("overs_against", balls_to_overs_col(F.col("balls_against")))
    )
    rank = Window.partitionBy("season").orderBy(
        F.col("points").desc(), F.col("net_run_rate").desc_nulls_last(), F.col("won").desc(), "team"
    )
    return table.withColumn("position", F.row_number().over(rank))


def venue_stats(matches: DataFrame, innings: DataFrame) -> DataFrame:
    main = innings.filter(~F.col("is_super_over"))
    i1 = main.filter("innings_number = 1").select(
        "match_id", F.col("batting_team").alias("bat_first"), F.col("runs").alias("first_runs")
    )
    i2 = main.filter("innings_number = 2").select(
        "match_id",
        F.col("batting_team").alias("chasing"),
        F.col("runs").alias("second_runs"),
        "target_balls",
    )
    m = matches.join(i1, "match_id", "left").join(i2, "match_id", "left")
    decided = F.col("result_type") == "win"
    unreduced = F.col("target_balls") == 120
    per_venue = m.groupBy("venue").agg(
        F.first("city", ignorenulls=True).alias("city"),
        F.count(F.lit(1)).cast("int").alias("matches"),
        F.min("season").alias("first_season"),
        F.max("season").alias("last_season"),
        F.sum(decided.cast("int")).cast("int").alias("decided"),
        F.sum((decided & (F.col("winner") == F.col("bat_first"))).cast("int"))
        .cast("int")
        .alias("bat_first_wins"),
        F.sum((decided & (F.col("winner") == F.col("chasing"))).cast("int"))
        .cast("int")
        .alias("chasing_wins"),
        F.round(F.avg(F.when(unreduced, F.col("first_runs"))), 1).alias("avg_first_innings_runs"),
        F.round(F.avg(F.when(unreduced, F.col("second_runs"))), 1).alias("avg_second_innings_runs"),
        F.sum((decided & (F.col("toss_winner") == F.col("winner"))).cast("int")).alias("_toss_won"),
        F.sum((F.col("toss_decision") == "field").cast("int")).alias("_field_first"),
    )
    extremes = (
        main.join(matches.select("match_id", "venue"), "match_id")
        .groupBy("venue")
        .agg(
            F.max(F.struct("runs", "batting_team", "season")).alias("_hi"),
            F.min(F.when(F.col("all_out"), F.struct("runs", "batting_team", "season"))).alias(
                "_lo"
            ),
        )
    )
    return (
        per_venue.join(extremes, "venue", "left")
        .withColumn("chasing_win_pct", _rate(F.col("chasing_wins"), F.col("decided"), 100.0, 1))
        .withColumn("toss_winner_win_pct", _rate(F.col("_toss_won"), F.col("decided"), 100.0, 1))
        .withColumn("field_first_pct", _rate(F.col("_field_first"), F.col("matches"), 100.0, 1))
        .withColumn("highest_total", F.col("_hi.runs").cast("int"))
        .withColumn("highest_total_team", F.col("_hi.batting_team"))
        .withColumn("highest_total_season", F.col("_hi.season"))
        .withColumn("lowest_all_out_total", F.col("_lo.runs").cast("int"))
        .withColumn("lowest_all_out_team", F.col("_lo.batting_team"))
        .drop("_hi", "_lo", "_toss_won", "_field_first")
    )


def team_phase_stats(deliveries: DataFrame) -> DataFrame:
    main = deliveries.filter(~F.col("is_super_over"))
    frames = []
    for perspective, team in (("batting", "batting_team"), ("bowling", "bowling_team")):
        frames.append(
            main.groupBy("season", F.col(team).alias("team"), "phase")
            .agg(
                F.countDistinct("match_id", "innings_number").cast("int").alias("innings"),
                F.sum("runs_total").cast("int").alias("runs"),
                F.sum(F.col("is_legal").cast("int")).cast("int").alias("legal_balls"),
                F.sum("team_wickets").cast("int").alias("wickets"),
                F.sum(F.col("is_four").cast("int")).cast("int").alias("fours"),
                F.sum(F.col("is_six").cast("int")).cast("int").alias("sixes"),
                F.sum(F.col("is_dot").cast("int")).cast("int").alias("dot_balls"),
            )
            .withColumn("perspective", F.lit(perspective))
        )
    out = frames[0].unionByName(frames[1])
    return (
        out.withColumn("run_rate", _rate(F.col("runs"), F.col("legal_balls"), 6.0))
        .withColumn(
            "boundary_pct", _rate(F.col("fours") + F.col("sixes"), F.col("legal_balls"), 100.0, 1)
        )
        .withColumn("dot_pct", _rate(F.col("dot_balls"), F.col("legal_balls"), 100.0, 1))
        .withColumn("runs_per_wicket", _rate(F.col("runs"), F.col("wickets")))
    )


def head_to_head(matches: DataFrame, adjustments: DataFrame) -> DataFrame:
    void = adjustments.filter(F.col("adjustment") == "void").select("match_id")
    m = matches.join(void, "match_id", "left_anti")
    a = F.least("team1_franchise", "team2_franchise")
    b = F.greatest("team1_franchise", "team2_franchise")
    m = m.withColumn("franchise_a", a).withColumn("franchise_b", b)
    return m.groupBy("franchise_a", "franchise_b").agg(
        F.count(F.lit(1)).cast("int").alias("matches"),
        F.sum((F.col("winner_franchise") == F.col("franchise_a")).cast("int"))
        .cast("int")
        .alias("franchise_a_wins"),
        F.sum((F.col("winner_franchise") == F.col("franchise_b")).cast("int"))
        .cast("int")
        .alias("franchise_b_wins"),
        F.sum((F.col("result_type") == "tie").cast("int")).cast("int").alias("super_over_finishes"),
        F.sum((F.col("result_type") == "no_result").cast("int")).cast("int").alias("no_results"),
        F.min("match_date").alias("first_match_date"),
        F.max("match_date").alias("last_match_date"),
        F.max_by("winner_franchise", "match_date").alias("last_winner"),
    )


# --------------------------------------------------------------------------- driver
def build_marts(spark: SparkSession, silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    adjustments = _adjustments(spark)
    batting = batting_scorecard(silver["deliveries"], silver["wickets"], silver["match_players"])
    bowling = bowling_scorecard(silver["deliveries"], silver["match_players"])
    return {
        "innings_summary": innings_summary(silver["matches"], silver["innings"]),
        "match_summary": match_summary(silver["matches"], silver["innings"]),
        "batting_scorecard": batting,
        "bowling_scorecard": bowling,
        "player_season_batting": player_season_batting(batting, silver["match_players"]),
        "player_season_bowling": player_season_bowling(bowling, silver["match_players"]),
        "points_table": points_table(silver["matches"], silver["innings"], adjustments),
        "venue_stats": venue_stats(silver["matches"], silver["innings"]),
        "team_phase_stats": team_phase_stats(silver["deliveries"]),
        "head_to_head": head_to_head(silver["matches"], adjustments),
    }


def read_silver(spark: SparkSession, settings: Settings) -> dict[str, DataFrame]:
    tables = ("matches", "innings", "deliveries", "wickets", "match_players")
    missing = [t for t in tables if not delta_io.exists(spark, settings.lake.silver(t))]
    if missing:
        raise RuntimeError(f"silver tables missing: {missing}; run `ipl silver` first")
    return {t: delta_io.read(spark, settings.lake.silver(t)) for t in tables}


PARTITIONED = {
    "innings_summary",
    "match_summary",
    "batting_scorecard",
    "bowling_scorecard",
    "player_season_batting",
    "player_season_bowling",
    "team_phase_stats",
}


def build_gold(spark: SparkSession, settings: Settings) -> list[str]:
    marts = build_marts(spark, read_silver(spark, settings))
    for name in GOLD_TABLES:
        delta_io.overwrite(
            marts[name],
            settings.lake.gold(name),
            partition_by=("season",) if name in PARTITIONED else (),
        )
        log.info("gold table written", extra={"table": name})
    return list(GOLD_TABLES)
