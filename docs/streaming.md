# Streaming semantics

The live path replays historical Cricsheet matches as if they were happening now:

```
ipl produce ──► Kafka topic ipl.deliveries.v1 ──► ipl stream
                (key = match_id)                   ├─ ingest     → bronze/stream_events  (+ ops/stream_dlq)
                                                   ├─ scorecard  → silver/stream_deliveries, silver/stream_matches
                                                   │               → gold/live_scorecard
                                                   └─ activity   → gold/stream_activity
```

## Event contract (`schema_version` 1)

One JSON object per Kafka message:

| Field | Type | Notes |
|---|---|---|
| `schema_version` | int | `1`. A consumer rejects (DLQs) versions it does not know. |
| `event_id` | string | Deterministic idempotency key. A delivery is `<match>-<innings>-<over:02>-<seq:02>` (e.g. `1473511-2-19-04`); lifecycle events are `<match>-match_started` and `<match>-match_completed`. A replay of the same ball always reuses the same id. |
| `event_type` | string | `match_started`, `delivery` or `match_completed` |
| `match_id` | string | Cricsheet match id. It is also the **Kafka key**, so all events of a match land on one partition in order. |
| `sequence` | long | Position of the event within its match (0 = `match_started`) |
| `event_time` | timestamp | When the ball "happened" on the replay clock (UTC, ISO-8601). The original match date is in the payload. |
| `produced_at` | timestamp | When the producer sent the message |
| `source` | string | `cricsheet-replay` |
| `payload` | object | Delivery: innings, over (0-based), `ball_seq` (1-based, counts wides and no-balls), `ball_label` (`"19.4"`), batter, non-striker, bowler, runs, extras, wickets, target, `umpire_miscount`. The lifecycle events carry the teams, venue and toss (start) and the result (completion). |

Every delivery is self-contained: it carries the batting and bowling teams and the target. So the
scorecard can be computed even if a lifecycle event is late or lost.

The producer uses `enable.idempotence=true`, `acks=all` and zstd compression. Its own retries
therefore never duplicate messages inside Kafka. Replays, `--duplicate-every` and restarts
deliberately do create duplicates, and the consumer absorbs them.

## Delivery guarantees

The transport is **at-least-once**: Kafka plus the Spark checkpoint can re-deliver a micro-batch
after a crash. Every sink is idempotent, so the **results are effectively-once**:

| Hop | Mechanism | Why replays are harmless |
|---|---|---|
| Kafka → `bronze/stream_events`, `ops/stream_dlq` | `foreachBatch` appends with Delta's idempotent-writer options `txnAppId` + `txnVersion=batch_id` | If Spark re-runs batch *N* after a crash, Delta sees that `(txnAppId, N)` is already committed and skips the write. `txnAppId` lives inside the checkpoint directory, so a reset checkpoint gets a fresh id. |
| bronze → `silver/stream_deliveries` | `MERGE … ON event_id WHEN NOT MATCHED THEN INSERT` | Each `event_id` is stored exactly once, however often it arrives. |
| silver → `gold/live_scorecard` | For every match touched by the batch, the innings totals are **recomputed from the deduplicated deliveries** and MERGEd on `(match_id, innings_number)` | A recomputation is a pure function of the deduplicated set, so order, duplicates and restarts all converge to the same numbers. Nothing ever increments a counter. |
| bronze → `gold/stream_activity` | `withWatermark(event_time)` + `dropDuplicatesWithinWatermark(event_id)` + 1-minute tumbling windows, append mode | This is the only stateful query. Events later than the watermark are dropped here. That is acceptable for an operational metric, and the late events never affect the scorecard. |

Bronze keeps the raw feed, duplicates included, so any downstream table can be rebuilt from it.

## Malformed data: dead-letter queue

The ingest query parses every message with an explicit schema (`from_json`, no inference). It
writes rejects to `ops/stream_dlq` along with the raw payload, the Kafka coordinates and an
`error_reason`:

| `error_reason` | Meaning |
|---|---|
| `malformed_json` | not parseable JSON (`try_parse_json` is null) |
| `unsupported_schema_version` | missing or unknown `schema_version` |
| `missing_required_field` | no `event_id` / `match_id` / `event_type` / `event_time` |
| `unknown_event_type` | not one of the three event types |
| `key_mismatch` | Kafka key ≠ `match_id` (would break per-match ordering) |
| `invalid_delivery_payload` | delivery without innings/over/ball/team/runs |
| `runs_inconsistent` | `runs.total ≠ runs.batter + runs.extras` |

## Streaming data quality

After each scorecard micro-batch the job checks the recomputed matches before publishing:

* at most 6 legal balls in an over, unless the source flagged an umpire miscount;
* at most 10 wickets in an innings (2 in a super over).

A violation is written to `ops/dq_results` (layer `stream`) and raises `DataQualityError`. The
query then stops, and the bad numbers never reach `gold/live_scorecard`. Set
`IPL_STREAM_DQ_FAIL_ON_ERROR=false` to record violations without stopping. The runbook explains
how to recover.

## Ordering, lateness and restarts

* **Per-match order** comes from the Kafka key. Correctness does not depend on order, because the
  scorecard is recomputed from the set of deliveries. The `striker`, `bowler` and `last_ball`
  columns use the highest `(over, ball_seq)` seen, not the arrival order.
* **Late events.** The scorecard path has no watermark and never drops data. However late a ball
  arrives, the next batch folds it in. Only the activity metric uses a watermark
  (`IPL_STREAM_WATERMARK`, default 2 minutes).
* **Restarts.** `ipl stream` resumes from the checkpoints in `IPL_CHECKPOINT_DIR`. The Kafka
  offsets, the processed Delta versions and the activity state all live there. A starting
  position (`IPL_KAFKA_STARTING_OFFSETS`) only applies to a brand-new checkpoint.
* **`--available-now`** processes everything that is currently available, one query after the
  other, and then exits. It is meant for backfills, CI and the reconciliation drill.

## Reconciliation

`ipl verify-stream --match-id <id>` compares `gold/live_scorecard` with an independent view of the
same match. Pass `--against source` (the default) to recompute from the Cricsheet JSON in plain
Python, or `--against gold` to use the batch `gold/innings_summary`. It exits with code 4 on any
difference in batting team, runs, wickets or legal balls.
