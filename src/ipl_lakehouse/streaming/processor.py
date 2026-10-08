"""Spark Structured Streaming: Kafka -> bronze events (+ DLQ) -> silver deliveries -> live scorecard.

Three checkpointed queries:

``ingest``     Kafka -> parse with an explicit schema -> valid events appended to
               ``bronze/stream_events``, invalid ones to ``ops/stream_dlq``. Both writes happen in
               ``foreachBatch`` with Delta's idempotent-writer options (``txnAppId``/``txnVersion``),
               so a batch replayed after a crash is not written twice.
``scorecard``  bronze events -> MERGE (insert-only, keyed on ``event_id``) into
               ``silver/stream_deliveries`` -> recompute the touched innings from that deduplicated
               table -> streaming DQ checks -> MERGE into ``gold/live_scorecard``. Recomputing from a
               deduplicated table (instead of incrementing counters) makes replays, duplicates and
               restarts converge to the same numbers: effectively-once results over an
               at-least-once transport.
``activity``   bronze events -> watermark + dropDuplicatesWithinWatermark -> 1-minute event-time
               windows per match -> ``gold/stream_activity`` (append). The only stateful query; late
               events beyond the watermark are dropped here, which is fine for an ops metric and
               never affects the scorecard path.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ipl_lakehouse.config import Settings
from ipl_lakehouse.cricket import BOWLER_WICKET_KINDS, NON_DISMISSAL_KINDS
from ipl_lakehouse.logs import get_logger
from ipl_lakehouse.quality.checks import (
    Check,
    enforce,
    persist,
    run_checks,
    too_many_legal_balls,
    too_many_wickets,
)
from ipl_lakehouse.streaming.events import EVENT_TYPES, SCHEMA_VERSION
from ipl_lakehouse.streaming.kafka import spark_kafka_options

log = get_logger(__name__)

QUERIES = ("ingest", "scorecard", "activity")


def _f(name: str, dtype) -> StructField:
    return StructField(name, dtype, True)


PAYLOAD_SCHEMA = StructType(
    [
        # delivery
        _f("season", IntegerType()),
        _f("match_date", StringType()),
        _f("innings", IntegerType()),
        _f("is_super_over", BooleanType()),
        _f("batting_team", StringType()),
        _f("bowling_team", StringType()),
        _f("over", IntegerType()),
        _f("ball_seq", IntegerType()),
        _f("ball_label", StringType()),
        _f("batter", StringType()),
        _f("non_striker", StringType()),
        _f("bowler", StringType()),
        _f(
            "runs",
            StructType(
                [
                    _f("batter", IntegerType()),
                    _f("extras", IntegerType()),
                    _f("total", IntegerType()),
                ]
            ),
        ),
        _f(
            "extras",
            StructType(
                [_f(k, IntegerType()) for k in ("wides", "noballs", "byes", "legbyes", "penalty")]
            ),
        ),
        _f("non_boundary", BooleanType()),
        _f(
            "wickets",
            ArrayType(
                StructType(
                    [
                        _f("player_out", StringType()),
                        _f("kind", StringType()),
                        _f("fielders", ArrayType(StringType())),
                    ]
                )
            ),
        ),
        _f("target", StructType([_f("runs", IntegerType()), _f("overs", DoubleType())])),
        _f("scheduled_overs", IntegerType()),
        _f("umpire_miscount", BooleanType()),
        # match_started
        _f("event_name", StringType()),
        _f("match_number", IntegerType()),
        _f("stage", StringType()),
        _f("teams", ArrayType(StringType())),
        _f("venue", StringType()),
        _f("city", StringType()),
        _f("toss", StructType([_f("winner", StringType()), _f("decision", StringType())])),
        # match_completed
        _f("winner", StringType()),
        _f("result", StringType()),
        _f("by_runs", IntegerType()),
        _f("by_wickets", IntegerType()),
        _f("method", StringType()),
        _f("super_over_winner", StringType()),
        _f("player_of_match", ArrayType(StringType())),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        _f("schema_version", IntegerType()),
        _f("event_id", StringType()),
        _f("event_type", StringType()),
        _f("match_id", StringType()),
        _f("sequence", LongType()),
        _f("event_time", TimestampType()),
        _f("produced_at", TimestampType()),
        _f("source", StringType()),
        _f("payload", PAYLOAD_SCHEMA),
    ]
)


KAFKA_SOURCE_SCHEMA = StructType(
    [
        _f("key", BinaryType()),
        _f("value", BinaryType()),
        _f("topic", StringType()),
        _f("partition", IntegerType()),
        _f("offset", LongType()),
        _f("timestamp", TimestampType()),
        _f("timestampType", IntegerType()),
    ]
)


# --------------------------------------------------------------------------- parsing
def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka-shaped rows (key/value/topic/partition/offset/timestamp) -> parsed + error_reason."""
    value = F.col("value").cast("string")
    parsed = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        value.alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(value, ENVELOPE_SCHEMA, {"mode": "PERMISSIVE"}).alias("e"),
    )
    p = "e.payload"
    is_delivery = F.col("e.event_type") == "delivery"
    delivery_incomplete = (
        F.col(f"{p}.innings").isNull()
        | F.col(f"{p}.over").isNull()
        | F.col(f"{p}.ball_seq").isNull()
        | F.col(f"{p}.batting_team").isNull()
        | F.col(f"{p}.runs.total").isNull()
        | F.col(f"{p}.runs.batter").isNull()
        | F.col(f"{p}.runs.extras").isNull()
    )
    reason = (
        F.when(F.expr("try_parse_json(raw_value)").isNull(), F.lit("malformed_json"))
        .when(
            F.col("e.schema_version").isNull() | (F.col("e.schema_version") != SCHEMA_VERSION),
            F.lit("unsupported_schema_version"),
        )
        .when(
            F.col("e.event_id").isNull()
            | F.col("e.match_id").isNull()
            | F.col("e.event_type").isNull()
            | F.col("e.event_time").isNull(),
            F.lit("missing_required_field"),
        )
        .when(~F.col("e.event_type").isin(*EVENT_TYPES), F.lit("unknown_event_type"))
        .when(
            F.col("kafka_key").isNotNull() & (F.col("kafka_key") != F.col("e.match_id")),
            F.lit("key_mismatch"),
        )
        .when(is_delivery & delivery_incomplete, F.lit("invalid_delivery_payload"))
        .when(
            is_delivery
            & (F.col(f"{p}.runs.total") != F.col(f"{p}.runs.batter") + F.col(f"{p}.runs.extras")),
            F.lit("runs_inconsistent"),
        )
    )
    return parsed.withColumn("error_reason", reason)


def valid_events(parsed: DataFrame, batch_id: int) -> DataFrame:
    return parsed.filter(F.col("error_reason").isNull()).select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.schema_version").alias("schema_version"),
        F.col("e.match_id").alias("match_id"),
        F.col("e.sequence").alias("sequence"),
        F.col("e.event_time").alias("event_time"),
        F.col("e.produced_at").alias("produced_at"),
        F.col("e.source").alias("source"),
        F.col("e.payload").alias("payload"),
        "raw_value",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("ingested_at"),
        F.lit(batch_id).cast("long").alias("ingest_batch_id"),
        F.to_date("e.event_time").alias("event_date"),
    )


def invalid_events(parsed: DataFrame, batch_id: int) -> DataFrame:
    return parsed.filter(F.col("error_reason").isNotNull()).select(
        "raw_value",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "error_reason",
        F.current_timestamp().alias("dlq_at"),
        F.lit(batch_id).cast("long").alias("ingest_batch_id"),
    )


# --------------------------------------------------------------------------- helpers
def _txn_app_id(checkpoint: Path, name: str) -> str:
    """Stable per checkpoint: a reset checkpoint gets a new id, so Delta's idempotent-writer
    bookkeeping (txnAppId, txnVersion=batch_id) never skips batches of a *new* query."""
    marker = checkpoint / "_txn_app_id"
    if marker.exists():
        return marker.read_text(encoding="utf-8").strip()
    checkpoint.mkdir(parents=True, exist_ok=True)
    app_id = f"ipl-{name}-{uuid.uuid4().hex[:12]}"
    marker.write_text(app_id, encoding="utf-8")
    return app_id


def _append_idempotent(
    df: DataFrame, path: Path, app_id: str, batch_id: int, partition_by: Sequence[str] = ()
) -> None:
    writer = (
        df.coalesce(1)
        .write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))


def _trigger(settings: Settings, available_now: bool) -> dict:
    if available_now:
        return {"availableNow": True}
    return {"processingTime": f"{settings.stream_trigger_seconds} seconds"}


def _overs(balls: Column) -> Column:
    return F.concat(
        F.floor(balls / 6).cast("int").cast("string"), F.lit("."), (balls % 6).cast("string")
    )


def _overs_to_balls(overs: Column) -> Column:
    whole = F.floor(overs)
    return (whole * 6 + F.round((overs - whole) * 10)).cast("int")


# --------------------------------------------------------------------------- query: ingest
def kafka_source(spark: SparkSession, settings: Settings, topic: str | None = None) -> DataFrame:
    reader = spark.readStream.format("kafka")
    for key, value in spark_kafka_options(settings).items():
        reader = reader.option(key, value)
    return (
        reader.option("subscribe", topic or settings.kafka_topic)
        .option("startingOffsets", settings.kafka_starting_offsets)
        .option("maxOffsetsPerTrigger", settings.kafka_max_offsets_per_trigger)
        .option("failOnDataLoss", str(settings.kafka_fail_on_data_loss).lower())
        .load()
    )


def start_ingest(
    spark: SparkSession,
    settings: Settings,
    source: DataFrame,
    *,
    available_now: bool = False,
) -> StreamingQuery:
    lake = settings.lake
    checkpoint = settings.checkpoint_dir / "ingest"
    app_id = _txn_app_id(checkpoint, "ingest")

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        parsed = parse_events(batch).persist()
        try:
            good = valid_events(parsed, batch_id)
            bad = invalid_events(parsed, batch_id)
            _append_idempotent(good, lake.stream_events, app_id, batch_id, ("event_date",))
            bad_count = bad.count()
            if bad_count:
                _append_idempotent(bad, lake.stream_dlq, app_id, batch_id)
                log.warning("events sent to DLQ", extra={"batch_id": batch_id, "count": bad_count})
        finally:
            parsed.unpersist()

    return (
        parse_passthrough(source)
        .writeStream.queryName("ingest")
        .foreachBatch(write_batch)
        .option("checkpointLocation", str(checkpoint))
        .trigger(**_trigger(settings, available_now))
        .start()
    )


def parse_passthrough(source: DataFrame) -> DataFrame:
    """Keep only the Kafka columns the parser needs (file-based test sources mimic them)."""
    return source.select("key", "value", "topic", "partition", "offset", "timestamp")


# --------------------------------------------------------------------------- query: scorecard
def flatten_deliveries(events: DataFrame) -> DataFrame:
    p = "payload"
    wickets = F.coalesce(F.col(f"{p}.wickets"), F.array())
    wides = F.coalesce(F.col(f"{p}.extras.wides"), F.lit(0))
    noballs = F.coalesce(F.col(f"{p}.extras.noballs"), F.lit(0))
    return events.filter(F.col("event_type") == "delivery").select(
        "event_id",
        "match_id",
        "sequence",
        "event_time",
        F.col(f"{p}.season").alias("season"),
        F.col(f"{p}.innings").alias("innings_number"),
        F.coalesce(F.col(f"{p}.is_super_over"), F.lit(False)).alias("is_super_over"),
        F.col(f"{p}.batting_team").alias("batting_team"),
        F.col(f"{p}.bowling_team").alias("bowling_team"),
        F.col(f"{p}.over").alias("over_index"),
        F.col(f"{p}.ball_seq").alias("delivery_seq"),
        F.col(f"{p}.ball_label").alias("ball_label"),
        F.col(f"{p}.batter").alias("batter"),
        F.col(f"{p}.non_striker").alias("non_striker"),
        F.col(f"{p}.bowler").alias("bowler"),
        F.col(f"{p}.runs.batter").alias("runs_batter"),
        F.col(f"{p}.runs.extras").alias("runs_extras"),
        F.col(f"{p}.runs.total").alias("runs_total"),
        wides.alias("wides"),
        noballs.alias("noballs"),
        ((wides == 0) & (noballs == 0)).alias("is_legal"),
        (
            (F.col(f"{p}.runs.batter") == 4) & ~F.coalesce(F.col(f"{p}.non_boundary"), F.lit(False))
        ).alias("is_four"),
        (
            (F.col(f"{p}.runs.batter") == 6) & ~F.coalesce(F.col(f"{p}.non_boundary"), F.lit(False))
        ).alias("is_six"),
        F.size(F.filter(wickets, lambda w: ~w["kind"].isin(*NON_DISMISSAL_KINDS))).alias(
            "team_wickets"
        ),
        F.size(F.filter(wickets, lambda w: w["kind"].isin(*BOWLER_WICKET_KINDS))).alias(
            "bowler_wickets"
        ),
        F.transform(wickets, lambda w: w["player_out"]).alias("players_out"),
        F.col(f"{p}.target.runs").alias("target_runs"),
        _overs_to_balls(F.col(f"{p}.target.overs")).alias("target_balls"),
        F.col(f"{p}.scheduled_overs").alias("scheduled_overs"),
        F.coalesce(F.col(f"{p}.umpire_miscount"), F.lit(False)).alias("umpire_miscount"),
        F.current_timestamp().alias("first_seen_at"),
    )


def match_updates(events: DataFrame) -> DataFrame:
    p = "payload"
    started = events.filter(F.col("event_type") == "match_started").select(
        "match_id",
        F.col(f"{p}.season").alias("season"),
        F.col(f"{p}.match_date").alias("match_date"),
        F.col(f"{p}.stage").alias("stage"),
        F.col(f"{p}.teams").alias("teams"),
        F.col(f"{p}.venue").alias("venue"),
        F.col(f"{p}.toss.winner").alias("toss_winner"),
        F.col(f"{p}.toss.decision").alias("toss_decision"),
        F.col("event_time").alias("started_at"),
    )
    completed = events.filter(F.col("event_type") == "match_completed").select(
        "match_id",
        F.col(f"{p}.winner").alias("winner"),
        F.col(f"{p}.result").alias("result"),
        F.col(f"{p}.by_runs").alias("by_runs"),
        F.col(f"{p}.by_wickets").alias("by_wickets"),
        F.col(f"{p}.method").alias("method"),
        F.col(f"{p}.super_over_winner").alias("super_over_winner"),
        F.col("event_time").alias("completed_at"),
    )
    touched = events.groupBy("match_id").agg(F.max("event_time").alias("last_event_time"))
    return touched.join(started, "match_id", "left").join(completed, "match_id", "left")


MATCH_COLUMNS = (
    "season", "match_date", "stage", "teams", "venue", "toss_winner", "toss_decision",
    "started_at", "winner", "result", "by_runs", "by_wickets", "method", "super_over_winner",
    "completed_at",
)  # fmt: skip


def compute_scorecard(deliveries: DataFrame, matches: DataFrame | None) -> DataFrame:
    """Innings scorecards from a deduplicated delivery table (deterministic => idempotent)."""
    order = F.struct("over_index", "delivery_seq")
    card = deliveries.groupBy("match_id", "innings_number").agg(
        F.first("season", ignorenulls=True).alias("season"),
        F.first("batting_team", ignorenulls=True).alias("batting_team"),
        F.first("bowling_team", ignorenulls=True).alias("bowling_team"),
        F.max(F.col("is_super_over").cast("int")).cast("boolean").alias("is_super_over"),
        F.sum("runs_total").cast("int").alias("runs"),
        F.sum("team_wickets").cast("int").alias("wickets"),
        F.sum(F.col("is_legal").cast("int")).cast("int").alias("legal_balls"),
        F.sum("runs_extras").cast("int").alias("extras"),
        F.sum(F.col("is_four").cast("int")).cast("int").alias("fours"),
        F.sum(F.col("is_six").cast("int")).cast("int").alias("sixes"),
        F.max("target_runs").alias("target_runs"),
        F.max("target_balls").alias("target_balls"),
        F.count(F.lit(1)).cast("int").alias("deliveries"),
        F.max_by(
            F.struct("event_id", "ball_label", "batter", "non_striker", "bowler"), order
        ).alias("_last"),
        F.max("event_time").alias("last_event_time"),
    )
    runs_required = F.col("target_runs") - F.col("runs")
    balls_left = F.col("target_balls") - F.col("legal_balls")
    card = (
        card.withColumn("overs", _overs(F.col("legal_balls")))
        .withColumn(
            "score_text",
            F.concat_ws(
                "",
                F.col("runs").cast("string"),
                F.lit("/"),
                F.col("wickets").cast("string"),
                F.lit(" ("),
                F.col("overs"),
                F.lit(")"),
            ),
        )
        .withColumn(
            "run_rate",
            F.when(F.col("legal_balls") > 0, F.round(F.col("runs") * 6 / F.col("legal_balls"), 2)),
        )
        .withColumn(
            "runs_required",
            F.when(F.col("target_runs").isNotNull(), F.greatest(runs_required, F.lit(0))),
        )
        .withColumn(
            "balls_remaining",
            F.when(F.col("target_balls").isNotNull(), F.greatest(balls_left, F.lit(0))),
        )
        .withColumn(
            "required_run_rate",
            F.when(
                (balls_left > 0) & (runs_required > 0), F.round(runs_required * 6 / balls_left, 2)
            ),
        )
        .withColumn("last_event_id", F.col("_last.event_id"))
        .withColumn("last_ball", F.col("_last.ball_label"))
        .withColumn("striker", F.col("_last.batter"))
        .withColumn("non_striker", F.col("_last.non_striker"))
        .withColumn("bowler", F.col("_last.bowler"))
        .drop("_last")
    )
    if matches is not None:
        status = matches.select(
            "match_id",
            F.when(F.col("completed_at").isNotNull(), F.lit("completed"))
            .otherwise(F.lit("live"))
            .alias("match_status"),
            F.col("winner").alias("match_winner"),
        )
        card = card.join(status, "match_id", "left")
    else:
        card = card.withColumn("match_status", F.lit("live")).withColumn(
            "match_winner", F.lit(None).cast("string")
        )
    return card.withColumn("match_status", F.coalesce("match_status", F.lit("live"))).withColumn(
        "updated_at", F.current_timestamp()
    )


def stream_checks() -> list[Check]:
    return [
        Check(
            "stream_max_six_legal_balls_per_over",
            "stream_deliveries",
            "<= 6 legal balls per over unless flagged as an umpire miscount",
            lambda t: too_many_legal_balls(t["deliveries"]),
        ),
        Check(
            "stream_innings_max_wickets",
            "live_scorecard",
            "<= 10 wickets per innings (2 in a super over)",
            lambda t: too_many_wickets(t["scorecard"]),
        ),
    ]


def _merge_insert_only(spark: SparkSession, df: DataFrame, path: Path, key: str) -> None:
    if not DeltaTable.isDeltaTable(spark, str(path)):
        df.limit(0).write.format("delta").mode("append").save(str(path))
    (
        DeltaTable.forPath(spark, str(path))
        .alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _merge_matches(spark: SparkSession, df: DataFrame, path: Path) -> None:
    if not DeltaTable.isDeltaTable(spark, str(path)):
        df.limit(0).write.format("delta").mode("append").save(str(path))
    updates = {c: F.coalesce(F.col(f"s.{c}"), F.col(f"t.{c}")) for c in MATCH_COLUMNS}
    updates["last_event_time"] = F.greatest(F.col("t.last_event_time"), F.col("s.last_event_time"))
    (
        DeltaTable.forPath(spark, str(path))
        .alias("t")
        .merge(df.alias("s"), "t.match_id = s.match_id")
        .whenMatchedUpdate(set=updates)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _merge_scorecard(spark: SparkSession, df: DataFrame, path: Path) -> None:
    if not DeltaTable.isDeltaTable(spark, str(path)):
        df.limit(0).write.format("delta").mode("append").save(str(path))
    (
        DeltaTable.forPath(spark, str(path))
        .alias("t")
        .merge(df.alias("s"), "t.match_id = s.match_id AND t.innings_number = s.innings_number")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_scorecard_batch(batch: DataFrame, batch_id: int, settings: Settings) -> None:
    spark = batch.sparkSession
    lake = settings.lake
    events = batch.dropDuplicates(["event_id"]).persist()
    try:
        touched = [r.match_id for r in events.select("match_id").distinct().collect()]
        if not touched:
            return
        deliveries_path = lake.silver("stream_deliveries")
        matches_path = lake.silver("stream_matches")
        _merge_insert_only(spark, flatten_deliveries(events), deliveries_path, "event_id")
        _merge_matches(spark, match_updates(events), matches_path)

        in_scope = F.col("match_id").isin(*touched)
        affected = spark.read.format("delta").load(str(deliveries_path)).filter(in_scope).persist()
        matches = spark.read.format("delta").load(str(matches_path)).filter(in_scope)
        scorecard = compute_scorecard(affected, matches).persist()
        try:
            results = run_checks(
                stream_checks(),
                {"deliveries": affected, "scorecard": scorecard},
                layer="stream",
                run_id=f"scorecard-batch-{batch_id}",
            )
            if any(not r.passed for r in results):
                persist(spark, settings, results)
            enforce(results, settings.stream_dq_fail_on_error)
            _merge_scorecard(spark, scorecard, lake.gold("live_scorecard"))
        finally:
            affected.unpersist()
            scorecard.unpersist()
        log.info("scorecard batch merged", extra={"batch_id": batch_id, "matches": len(touched)})
    finally:
        events.unpersist()


def start_scorecard(
    spark: SparkSession, settings: Settings, *, available_now: bool = False
) -> StreamingQuery:
    source = spark.readStream.format("delta").load(str(settings.lake.stream_events))
    return (
        source.writeStream.queryName("scorecard")
        .foreachBatch(lambda df, batch_id: process_scorecard_batch(df, batch_id, settings))
        .option("checkpointLocation", str(settings.checkpoint_dir / "scorecard"))
        .trigger(**_trigger(settings, available_now))
        .start()
    )


# --------------------------------------------------------------------------- query: activity
def activity_frame(events: DataFrame, watermark: str, window: str) -> DataFrame:
    deduped = events.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(
        ["event_id"]
    )
    return (
        deduped.groupBy(F.window("event_time", window).alias("w"), "match_id")
        .agg(
            F.count(F.lit(1)).cast("long").alias("events"),
            F.sum((F.col("event_type") == "delivery").cast("int")).cast("long").alias("deliveries"),
            F.coalesce(F.sum("payload.runs.total"), F.lit(0)).cast("long").alias("runs"),
            F.sum(
                F.size(
                    F.filter(
                        F.coalesce("payload.wickets", F.array()),
                        lambda w: ~w["kind"].isin(*NON_DISMISSAL_KINDS),
                    )
                )
            )
            .cast("long")
            .alias("wickets"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "match_id",
            "events",
            "deliveries",
            "runs",
            "wickets",
        )
    )


def start_activity(
    spark: SparkSession, settings: Settings, *, available_now: bool = False
) -> StreamingQuery:
    source = spark.readStream.format("delta").load(str(settings.lake.stream_events))
    return (
        activity_frame(source, settings.stream_watermark, settings.stream_activity_window)
        .writeStream.queryName("activity")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(settings.checkpoint_dir / "activity"))
        .trigger(**_trigger(settings, available_now))
        .start(str(settings.lake.gold("stream_activity")))
    )


# --------------------------------------------------------------------------- observability
class ProgressLogger(StreamingQueryListener):
    """Structured log line per micro-batch: throughput, latency, watermark, state size."""

    def onQueryStarted(self, event) -> None:
        log.info(
            "query started",
            extra={"query": event.name, "id": str(event.id), "run_id": str(event.runId)},
        )

    def onQueryProgress(self, event) -> None:
        p = event.progress
        log.info(
            "query progress",
            extra={
                "query": p.name,
                "batch_id": p.batchId,
                "input_rows": p.numInputRows,
                "input_rows_per_s": round(p.inputRowsPerSecond or 0.0, 1),
                "processed_rows_per_s": round(p.processedRowsPerSecond or 0.0, 1),
                "trigger_ms": (p.durationMs or {}).get("triggerExecution"),
                "watermark": (p.eventTime or {}).get("watermark"),
                "state_rows": sum(s.numRowsTotal for s in (p.stateOperators or [])),
            },
        )

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            log.error("query failed", extra={"id": str(event.id), "error": event.exception[:2000]})
        else:
            log.info("query terminated", extra={"id": str(event.id)})


_LISTENER_SESSIONS: set[int] = set()


def _register_listener(spark: SparkSession) -> None:
    key = id(spark)
    if key not in _LISTENER_SESSIONS:
        spark.streams.addListener(ProgressLogger())
        _LISTENER_SESSIONS.add(key)


# --------------------------------------------------------------------------- driver
def run(
    spark: SparkSession,
    settings: Settings,
    *,
    queries: Sequence[str] = QUERIES,
    available_now: bool = False,
    source: DataFrame | None = None,
) -> None:
    unknown = set(queries) - set(QUERIES)
    if unknown:
        raise ValueError(f"unknown queries {sorted(unknown)}; choose from {QUERIES}")
    _register_listener(spark)

    def start(name: str) -> StreamingQuery:
        if name == "ingest":
            src = source if source is not None else kafka_source(spark, settings)
            return start_ingest(spark, settings, src, available_now=available_now)
        if name == "scorecard":
            return start_scorecard(spark, settings, available_now=available_now)
        return start_activity(spark, settings, available_now=available_now)

    ensure_tables(spark, settings)
    ordered = [q for q in QUERIES if q in queries]
    if available_now:
        # Drain everything currently available, one stage after the other, then exit.
        for name in ordered:
            query = start(name)
            query.awaitTermination()
            if query.exception():
                raise RuntimeError(f"query {name} failed: {query.exception()}")
        return

    started: list[StreamingQuery] = []
    try:
        for name in ordered:
            started.append(start(name))
        spark.streams.awaitAnyTermination()
        for q in started:
            if q.exception():
                raise RuntimeError(f"query {q.name} failed: {q.exception()}")
    finally:
        for q in started:
            if q.isActive:
                q.stop()


def ensure_tables(spark: SparkSession, settings: Settings) -> None:
    """Create the empty bronze/DLQ tables up front so downstream queries can start at once."""
    lake = settings.lake
    parsed = parse_events(spark.createDataFrame([], KAFKA_SOURCE_SCHEMA))
    if not DeltaTable.isDeltaTable(spark, str(lake.stream_events)):
        valid_events(parsed, 0).write.format("delta").mode("append").partitionBy("event_date").save(
            str(lake.stream_events)
        )
    if not DeltaTable.isDeltaTable(spark, str(lake.stream_dlq)):
        invalid_events(parsed, 0).write.format("delta").mode("append").save(str(lake.stream_dlq))
