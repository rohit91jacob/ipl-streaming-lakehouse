# Runbook

All commands work locally (`uv run ipl …`) or in Docker. In Docker, replace `uv run ipl` with
`docker compose run --rm batch` for batch commands or `docker compose run --rm stream` for
streaming commands. Exit codes: `0` ok, `1` failure, `2` usage or configuration error, `3`
data-quality failure, `4` reconciliation mismatch.

## Scheduling

| Job | Cadence | Command |
|---|---|---|
| Batch refresh + results site | Daily 05:00 UTC on GitHub Actions ([`refresh.yml`](../.github/workflows/refresh.yml)); run it daily in season if you self-host | `ipl batch` then `ipl report --out site` |
| Keep-alive | 1st and 15th of each month ([`keepalive.yml`](../.github/workflows/keepalive.yml)) | re-enables the scheduled workflows |
| Stream processor | Long-running service (`restart: unless-stopped` in compose) | `ipl stream` |
| Replay / demo feed | On demand | `ipl produce --season 2025` |

`ipl batch` is idempotent and incremental. It sends `If-Modified-Since` (a 304 ends the ingest
step), compares sha256 checksums per match, and processes only new or changed matches into
silver. You can schedule it with cron, Airflow, Dagster or a Kubernetes `CronJob`. The command
returns a non-zero exit code on any failure.

## Scheduled refresh (GitHub Actions)

`refresh.yml` restores the lake from the Actions cache, runs `ipl batch`, saves the lake, builds
the site and deploys it to https://rohit91jacob.github.io/ipl-streaming-lakehouse/. It needs no
secrets.

* **A scheduled run failed.** The `alert` job opens a `Scheduled refresh is failing` issue with
  the run link, or comments on the open one. Open the run and find the failing step:
  * `Batch pipeline` exiting with code 3 is a data-quality failure; follow
    [Data-quality failures](#data-quality-failures-exit-code-3). A new venue spelling (a DQ
    warning) doesn't fail the run.
  * A download error (HTTP 5xx or a timeout) is usually Cricsheet being briefly unavailable. Re-run
    with **Run workflow** in the Actions tab.
  * `deploy` failing means GitHub Pages: check Settings → Pages → Source is "GitHub Actions".

  Close the issue once a run is green. Failed runs never overwrite the cached lake or the site,
  because the save and deploy steps only run after a successful batch.
* **The log says "restored from: nothing".** The cache was evicted. That run downloads the full
  archive and rebuilds every layer (about 2 minutes on a GitHub runner); the output is the same as an incremental
  run.
* **Force a rebuild.** Run workflow with `full_refresh` ticked. That rebuilds silver from
  bronze; to start from an empty lake as well, delete the `ipl-lake-*` caches under Actions →
  Caches first.
* **The schedules stopped.** GitHub disables scheduled workflows after 60 days without
  repository activity. `keepalive.yml` re-enables them twice a month. If they're disabled
  anyway (for example, keep-alive itself was switched off), enable both in the Actions tab, or
  run `keepalive.yml` manually.
* **Credentials.** None to rotate. The workflows only use the per-run `GITHUB_TOKEN`.

## Backfill and reprocessing

| Situation | Action |
|---|---|
| First load / new environment | `ipl batch` (downloads the archive, lands about 1,250 matches, builds silver and gold) |
| Cricsheet corrected some matches | Nothing to do. The next `ipl batch` lands the corrected files as **new versions** (`change_type = changed` in the manifest) and replaces just those matches in silver. |
| Silver logic changed | `ipl silver --full-refresh && ipl gold && ipl dq` (bronze is immutable, so this is always safe) |
| Gold logic or reference data changed | `ipl gold && ipl dq --layer gold`. Gold is always rebuilt in full. |
| Offline / air-gapped rebuild | `ipl batch --archive /path/to/ipl_json.zip --no-register` |
| Inspect what landed | `ipl sql "SELECT change_type, COUNT(*) FROM bronze_manifest GROUP BY change_type"` |

Each bronze file is content-addressed: `bronze/cricsheet/matches/match_id=<id>/<sha256>.json`.
To find the version that silver currently uses, run
`ipl sql "SELECT match_id, source_sha256 FROM matches WHERE match_id = '1473511'"`.

## Data-quality failures (exit code 3)

1. See what failed:
   `ipl sql "SELECT name, layer, violation_count, sample FROM dq_results WHERE NOT passed ORDER BY checked_at DESC LIMIT 20"`.
2. The `sample` column has up to five violating rows as JSON.
3. Decide where the fault lies:
   * **Source.** For example, a corrected Cricsheet file that is internally inconsistent. Report
     it to Cricsheet. As a stopgap, you can rerun with `IPL_DQ_FAIL_ON_ERROR=false` so the run
     records the problem without stopping. Gold still builds, and the failure stays visible in
     `dq_results`.
   * **Reference data.** A new venue spelling or team triggers the `matches_venue_mapped` or
     `matches_teams_mapped` warning. Add a row to `src/ipl_lakehouse/reference/venues.csv` or
     `teams.csv`, then run `ipl silver --full-refresh && ipl gold`.
   * **Code.** Fix it, add a regression test next to the fixture tests, then run
     `ipl silver --full-refresh && ipl gold && ipl dq`.

## Streaming operations

**Health.** The stream logs one JSON line per micro-batch (`"msg": "query progress"`) with
`input_rows`, `processed_rows_per_s`, `trigger_ms`, the watermark and the state size. Alert when
`trigger_ms` stays above the trigger interval, or when progress lines stop arriving. The
dashboard's *Pipeline health* tab shows the DLQ breakdown and the events-per-minute activity.

**Restart.** Stop and start the processor (`docker compose restart stream`). It resumes from its
checkpoints. Producers can keep sending while it is down; Kafka retains 7 days by default
(`IPL_KAFKA_TOPIC_RETENTION_MS`).

**Reconcile a match.** Run `ipl verify-stream --match-id <id>` (or `--against gold`) after the
match is complete.

### DLQ triage

```bash
ipl sql "SELECT error_reason, COUNT(*) AS n FROM stream_dlq GROUP BY error_reason ORDER BY n DESC"
ipl sql "SELECT kafka_partition, kafka_offset, error_reason, raw_value FROM stream_dlq ORDER BY dlq_at DESC LIMIT 20"
```

* `key_mismatch`, `missing_required_field` or `unsupported_schema_version` mean a producer is
  broken or outdated. Fix the producer. The rejected events are not lost: re-publish them from
  `raw_value` once they are corrected.
* `runs_inconsistent` and `invalid_delivery_payload` mean the upstream data is bad. Correct the
  event and re-publish it with the **same `event_id`**. The MERGE deduplicates, so there is no
  risk of double counting.
* `malformed_json` usually means a non-producer client wrote to the topic. Find it from the
  Kafka coordinates.

### Streaming DQ stop (`DataQualityError` in the stream logs)

The query has stopped before it published the offending batch. The event is already in
`bronze/stream_events`.

1. Find it: `ipl sql "SELECT * FROM dq_results WHERE layer = 'stream' AND NOT passed ORDER BY checked_at DESC LIMIT 5"`.
2. If the data is genuinely correct (for example, an umpire really bowled a 7-ball over and the
   event lacks the `umpire_miscount` flag), restart with `IPL_STREAM_DQ_FAIL_ON_ERROR=false` to
   get past it, then switch the setting back.
3. If the data is wrong, publish a corrected event with the same `event_id` before restarting.
   The bronze log keeps both versions. The MERGE keeps the first version it saw, so to replace
   the bad version you must reset (below).

### Reset or rebuild the streaming state

Do this when you change the streaming schema or logic, or when the checkpoints are lost or
corrupted:

```bash
ipl stream-reset            # dry run: lists what would be deleted
ipl stream-reset --yes      # deletes checkpoints + streaming tables (never batch tables)
ipl stream                  # re-reads the topic from IPL_KAFKA_STARTING_OFFSETS (earliest)
```

The rebuild is complete only while the topic still holds the events (7-day retention by
default). Older history stays in the batch tables, and you can always replay it with
`ipl produce`.

### Kafka checkpoint vs. topic mismatch

If the topic was deleted and recreated, or retention removed offsets the checkpoint still points
at, the query fails with `failOnDataLoss`. Either reset the streaming state (above), or accept
the gap with `IPL_KAFKA_FAIL_ON_DATA_LOSS=false` and then reconcile the affected matches.

## Capacity notes

* All IPL history is about 300k deliveries and about 30 MB of Delta. On 4 cores, a full batch
  rebuild takes a few minutes, most of it Spark planning overhead.
* The stream processor needs about 2 GB of driver memory (`IPL_SPARK_DRIVER_MEMORY`). Each
  micro-batch recomputes only the matches it touched.
* For a multi-node deployment, set `IPL_SPARK_MASTER` and put `IPL_DATA_DIR` and
  `IPL_CHECKPOINT_DIR` on a shared POSIX filesystem (for example NFS or EFS). The ingest step and
  `ipl sql` use local file paths, so object storage (S3/ABFS) is on the roadmap rather than
  supported. Use a Kafka cluster with replication factor ≥ 3 (`IPL_KAFKA_REPLICATION_FACTOR`) and
  SASL/SSL (`IPL_KAFKA_SECURITY_PROTOCOL`, `IPL_KAFKA_SASL_*`).
