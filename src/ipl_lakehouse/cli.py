"""``ipl`` command line: one entrypoint for batch, streaming and operations.

Exit codes: 0 ok, 1 failure, 2 usage error, 3 data-quality failure, 4 reconciliation mismatch.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Sequence
from pathlib import Path

from ipl_lakehouse import __version__
from ipl_lakehouse.config import ConfigError, Settings
from ipl_lakehouse.logs import configure_logging, get_logger

log = get_logger("ipl_lakehouse.cli")

EXIT_OK, EXIT_FAILURE, EXIT_USAGE, EXIT_DQ, EXIT_MISMATCH = 0, 1, 2, 3, 4


def _spark(settings: Settings, app: str, *, kafka: bool = False):
    from ipl_lakehouse.spark import build_spark

    return build_spark(settings, f"ipl-{app}", kafka=kafka)


# --------------------------------------------------------------------------- commands
def cmd_ingest(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.ingest.cricsheet import run_ingest

    result = run_ingest(
        settings, force=args.force, archive_path=args.archive, include_register=not args.no_register
    )
    print(json.dumps(result, indent=2, default=str))
    return EXIT_OK


def cmd_silver(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.batch.silver import build_silver

    spark = _spark(settings, "silver")
    try:
        result = build_silver(spark, settings, full_refresh=args.full_refresh)
        print(json.dumps({"matches_processed": len(result.processed_match_ids)}))
    finally:
        spark.stop()
    return EXIT_OK


def cmd_gold(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.batch.gold import build_gold

    spark = _spark(settings, "gold")
    try:
        print(json.dumps({"tables": build_gold(spark, settings)}))
    finally:
        spark.stop()
    return EXIT_OK


def cmd_dq(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.batch.pipeline import run_dq

    spark = _spark(settings, "dq")
    try:
        results = run_dq(spark, settings, args.layer)
        for r in results:
            print(
                f"{'PASS' if r.passed else 'FAIL'}  {r.severity:<5}  {r.name}  ({r.violation_count})"
            )
    finally:
        spark.stop()
    return EXIT_OK


def cmd_batch(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.batch.pipeline import run_batch

    spark = _spark(settings, "batch")
    try:
        summary = run_batch(
            spark,
            settings,
            full_refresh=args.full_refresh,
            skip_ingest=args.skip_ingest,
            archive_path=args.archive,
            include_register=not args.no_register,
        )
        print(json.dumps(summary, indent=2, default=str))
    finally:
        spark.stop()
    return EXIT_OK


def cmd_topics(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.streaming.kafka import ensure_topic

    created = ensure_topic(settings, args.topic)
    print(json.dumps({"topic": args.topic or settings.kafka_topic, "created": created}))
    return EXIT_OK


def cmd_produce(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.ingest import manifest
    from ipl_lakehouse.streaming.producer import (
        ReplayProducer,
        install_signal_handlers,
        load_replay_matches,
    )

    if (
        args.ensure_ingested
        and not args.file
        and not manifest.manifest_exists(settings.lake.manifest)
    ):
        from ipl_lakehouse.ingest.cricsheet import run_ingest

        run_ingest(settings)
    matches = load_replay_matches(
        settings, match_ids=args.match_id or (), season=args.season, files=args.file or ()
    )
    producer = ReplayProducer(settings, topic=args.topic)
    install_signal_handlers(producer)
    stats = producer.replay(
        matches,
        speedup=args.speedup,
        limit=args.limit,
        duplicate_every=args.duplicate_every,
        start_sequence=args.start_sequence,
    )
    print(
        json.dumps(
            {
                "matches": stats.matches,
                "sent": stats.sent,
                "duplicates": stats.duplicates,
                "delivered": stats.delivered,
                "errors": stats.errors[:5],
                "stopped_early": stats.stopped_early,
            }
        )
    )
    return EXIT_FAILURE if stats.errors else EXIT_OK


def cmd_stream(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.streaming.processor import run

    spark = _spark(settings, "stream", kafka=True)
    try:
        run(spark, settings, queries=args.queries, available_now=args.available_now)
    finally:
        spark.stop()
    return EXIT_OK


def cmd_verify_stream(args: argparse.Namespace, settings: Settings) -> int:
    import pyarrow as pa

    from ipl_lakehouse.query import format_table
    from ipl_lakehouse.streaming.verify import (
        expected_from_gold,
        expected_from_source,
        live_scorecard,
        reconcile,
    )

    expected = (
        expected_from_gold(settings, args.match_id)
        if args.against == "gold"
        else expected_from_source(settings, args.match_id, args.file)
    )
    actual = live_scorecard(settings, args.match_id)
    result = reconcile(expected, actual, args.match_id)
    if actual:
        cols = ["innings_number", "batting_team", "score_text", "run_rate", "target_runs",
                "match_status", "last_ball"]  # fmt: skip
        print(format_table(pa.Table.from_pylist([{c: r.get(c) for c in cols} for r in actual])))
    if result.ok:
        print(
            f"OK: live_scorecard for match {args.match_id} matches the {args.against} view "
            f"({len(expected)} innings)"
        )
        return EXIT_OK
    print(f"MISMATCH for match {args.match_id}:")
    for line in result.mismatches:
        print(f"  - {line}")
    return EXIT_MISMATCH


def cmd_sql(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.query import discover_tables, format_table, run_sql

    if args.list:
        for name, path in discover_tables(settings).items():
            print(f"{name:<24} {path}")
        return EXIT_OK
    if not args.query:
        print("provide a SQL query or --list", file=sys.stderr)
        return EXIT_USAGE
    table = run_sql(settings, args.query)
    if args.format == "json":
        print(json.dumps(table.to_pylist(), default=str, indent=2))
    else:
        print(format_table(table, max_rows=args.max_rows))
    return EXIT_OK


def cmd_report(args: argparse.Namespace, settings: Settings) -> int:
    from ipl_lakehouse.report import build_site

    print(json.dumps(build_site(args.out, settings), indent=2))
    return EXIT_OK


def cmd_dashboard(args: argparse.Namespace, settings: Settings) -> int:
    import subprocess

    from ipl_lakehouse.dashboard import app

    cmd = [
        sys.executable, "-m", "streamlit", "run", app.__file__,
        "--server.port", str(args.port), "--server.address", args.address,
        "--server.headless", "true", "--browser.gatherUsageStats", "false",
    ]  # fmt: skip
    return subprocess.call(cmd)


def cmd_stream_reset(args: argparse.Namespace, settings: Settings) -> int:
    lake = settings.lake
    targets = [
        settings.checkpoint_dir,
        lake.stream_events,
        lake.stream_dlq,
        lake.silver("stream_deliveries"),
        lake.silver("stream_matches"),
        lake.gold("live_scorecard"),
        lake.gold("stream_activity"),
    ]
    if not args.yes:
        print("This deletes streaming checkpoints and streaming tables:", *targets, sep="\n  ")
        print("Re-run with --yes to confirm. Batch tables are not touched.")
        return EXIT_USAGE
    for path in targets:
        if Path(path).exists():
            shutil.rmtree(path)
            log.info("deleted", extra={"path": str(path)})
    return EXIT_OK


# --------------------------------------------------------------------------- parser
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ipl", description=__doc__.splitlines()[0])
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser(
        "ingest", help="download Cricsheet IPL data and land new/changed matches in bronze"
    )
    p.add_argument(
        "--force", action="store_true", help="ignore If-Modified-Since / sha256 short-cuts"
    )
    p.add_argument(
        "--archive", type=Path, help="ingest a local ipl_json.zip instead of downloading"
    )
    p.add_argument("--no-register", action="store_true", help="skip the people register download")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("silver", help="bronze -> silver (incremental unless --full-refresh)")
    p.add_argument("--full-refresh", action="store_true")
    p.set_defaults(func=cmd_silver)

    p = sub.add_parser("gold", help="silver -> gold marts (full rebuild)")
    p.set_defaults(func=cmd_gold)

    p = sub.add_parser("dq", help="run data-quality checks and record results in ops/dq_results")
    p.add_argument("--layer", choices=("silver", "gold", "all"), default="all")
    p.set_defaults(func=cmd_dq)

    p = sub.add_parser("batch", help="ingest -> silver -> dq -> gold -> dq")
    p.add_argument("--full-refresh", action="store_true")
    p.add_argument("--skip-ingest", action="store_true")
    p.add_argument("--archive", type=Path, help="use a local ipl_json.zip for the ingest step")
    p.add_argument("--no-register", action="store_true", help="skip the people register download")
    p.set_defaults(func=cmd_batch)

    p = sub.add_parser("topics", help="create the Kafka topic if it does not exist")
    p.add_argument("--topic")
    p.set_defaults(func=cmd_topics)

    p = sub.add_parser("produce", help="replay matches into Kafka as a live feed")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--match-id", nargs="+", help="Cricsheet match id(s) from bronze")
    src.add_argument("--season", type=int, help="replay a whole season from bronze")
    src.add_argument("--file", nargs="+", type=Path, help="Cricsheet JSON file(s) to replay")
    p.add_argument("--speedup", type=float, help="clock compression (0 = as fast as possible)")
    p.add_argument("--limit", type=int, help="stop after N events (restart drills)")
    p.add_argument(
        "--duplicate-every", type=int, help="re-send every Nth event (idempotency drills)"
    )
    p.add_argument(
        "--start-sequence", type=int, default=0, help="resume the first match at this event"
    )
    p.add_argument("--topic")
    p.add_argument(
        "--ensure-ingested", action="store_true", help="run `ipl ingest` first if bronze is empty"
    )
    p.set_defaults(func=cmd_produce)

    p = sub.add_parser("stream", help="run the Structured Streaming queries")
    p.add_argument(
        "--available-now", action="store_true", help="process what is available, then exit"
    )
    p.add_argument(
        "--queries",
        type=lambda s: [q.strip() for q in s.split(",") if q.strip()],
        default=["ingest", "scorecard", "activity"],
        help="comma-separated subset of ingest,scorecard,activity",
    )
    p.set_defaults(func=cmd_stream)

    p = sub.add_parser(
        "verify-stream", help="compare gold/live_scorecard with the batch view of a match"
    )
    p.add_argument("--match-id", required=True)
    p.add_argument("--against", choices=("source", "gold"), default="source")
    p.add_argument("--file", type=Path, help="Cricsheet JSON to compare against (default: bronze)")
    p.set_defaults(func=cmd_verify_stream)

    p = sub.add_parser("sql", help="ad-hoc SQL over silver/gold/ops tables (no JVM)")
    p.add_argument("query", nargs="?")
    p.add_argument("--list", action="store_true", help="list queryable tables")
    p.add_argument("--format", choices=("table", "json"), default="table")
    p.add_argument("--max-rows", type=int, default=50)
    p.set_defaults(func=cmd_sql)

    p = sub.add_parser(
        "report", help="write the static results site (HTML) from the gold tables (no JVM)"
    )
    p.add_argument("--out", type=Path, default=Path("site"), help="output directory")
    p.set_defaults(func=cmd_report)

    p = sub.add_parser(
        "dashboard", help="Streamlit dashboard over the gold tables (needs [dashboard])"
    )
    p.add_argument("--port", type=int, default=8501)
    p.add_argument("--address", default="0.0.0.0")
    p.set_defaults(func=cmd_dashboard)

    p = sub.add_parser("stream-reset", help="delete streaming checkpoints and streaming tables")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(func=cmd_stream_reset)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    configure_logging(settings.log_level, settings.log_format)
    from ipl_lakehouse.quality.checks import DataQualityError

    try:
        return args.func(args, settings)
    except DataQualityError as exc:
        log.error("data quality failure", extra={"failed_checks": [f.name for f in exc.failures]})
        return EXIT_DQ
    except KeyboardInterrupt:
        log.info("interrupted", extra={"command": args.command})
        return 130
    except Exception as exc:  # last-resort boundary: log structured, exit non-zero
        log.error(
            "command failed", extra={"command": args.command, "error": str(exc)}, exc_info=True
        )
        return EXIT_FAILURE


if __name__ == "__main__":
    raise SystemExit(main())
