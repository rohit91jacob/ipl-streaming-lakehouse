"""The streaming job end to end, with a file source standing in for Kafka (same columns)."""

from __future__ import annotations

import copy
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ipl_lakehouse.cricsheet import innings_totals
from ipl_lakehouse.streaming import processor
from ipl_lakehouse.streaming.events import match_to_events, serialize
from ipl_lakehouse.streaming.verify import reconcile
from support import DOUBLE_SUPER_OVER, FINAL_2025, load_match, make_settings

pytestmark = pytest.mark.spark

T0 = datetime(2026, 10, 7, 14, 0, tzinfo=UTC)
KAFKA_JSON_SCHEMA = (
    "key string, value string, topic string, partition int, offset long, timestamp timestamp"
)


class FakeTopic:
    """Appends Kafka-shaped JSON records into a directory read by Spark's file source."""

    def __init__(self, root: Path):
        self.dir = root / "topic"
        self.dir.mkdir(parents=True)
        self.offset = 0
        self.files = 0

    def publish(self, records: list[tuple[str | None, str]]) -> None:
        lines = []
        for key, value in records:
            lines.append(
                json.dumps(
                    {
                        "key": key,
                        "value": value,
                        "topic": "test",
                        "partition": 0,
                        "offset": self.offset,
                        "timestamp": T0.isoformat(),
                    }
                )
            )
            self.offset += 1
        self.files += 1
        tmp = self.dir.parent / f".batch-{self.files}.json"
        tmp.write_text("\n".join(lines), encoding="utf-8")
        os.replace(tmp, self.dir / f"batch-{self.files:04d}.json")


def envelopes(match_id: str, start: int = 0, stop: int | None = None) -> list[tuple[str, str]]:
    events = match_to_events(load_match(match_id), match_id)[start:stop]
    out = []
    for event in events:
        ts = T0 + timedelta(seconds=event.sequence)
        out.append((match_id, serialize(event.envelope(ts, ts)).decode()))
    return out


def run_available_now(spark, settings, topic: FakeTopic) -> None:
    source = spark.readStream.schema(KAFKA_JSON_SCHEMA).json(str(topic.dir))
    processor.run(spark, settings, available_now=True, source=source)


def table(spark, path: Path):
    return spark.read.format("delta").load(str(path))


def scorecard(spark, settings, match_id: str) -> list[dict]:
    rows = (
        table(spark, settings.lake.gold("live_scorecard"))
        .filter(f"match_id = '{match_id}'")
        .collect()
    )
    return sorted((r.asDict() for r in rows), key=lambda r: r["innings_number"])


def expected(match_id: str) -> list[dict]:
    return [t.as_dict() for t in innings_totals(load_match(match_id))]


def test_live_scorecard_converges_through_duplicates_bad_events_and_restarts(spark, tmp_path):
    settings = make_settings(tmp_path)
    topic = FakeTopic(tmp_path)
    full = envelopes(FINAL_2025)

    # 1) First 150 events: innings 1 complete, chase in progress.
    topic.publish(full[:150])
    run_available_now(spark, settings, topic)
    partial = scorecard(spark, settings, FINAL_2025)
    assert (partial[0]["innings_number"], partial[0]["score_text"]) == (1, "190/9 (20.0)")
    assert partial[1]["match_status"] == "live" and partial[1]["target_runs"] == 191
    assert partial[1]["runs_required"] == 191 - partial[1]["runs"]
    assert partial[1]["required_run_rate"] > 0

    # 2) The rest, plus duplicates and every flavour of bad event.
    bad_version = json.loads(full[10][1]) | {"schema_version": 99}
    inconsistent = json.loads(full[20][1])
    inconsistent["payload"]["runs"]["total"] += 1
    topic.publish(
        full[150:]
        + full[100:130]  # 30 duplicates
        + [
            (FINAL_2025, "{not json"),
            (FINAL_2025, json.dumps(bad_version)),
            ("other-key", full[5][1]),
            (FINAL_2025, json.dumps(inconsistent)),
        ]
    )
    run_available_now(spark, settings, topic)
    final = scorecard(spark, settings, FINAL_2025)
    assert reconcile(expected(FINAL_2025), final, FINAL_2025).ok
    assert {r["match_status"] for r in final} == {"completed"}
    assert final[1]["score_text"] == "184/7 (20.0)" and final[1]["runs_required"] == 7

    dlq = {r.error_reason for r in table(spark, settings.lake.stream_dlq).collect()}
    assert dlq == {
        "malformed_json",
        "unsupported_schema_version",
        "key_mismatch",
        "runs_inconsistent",
    }
    assert table(spark, settings.lake.stream_events).count() == 254 + 30  # raw log keeps duplicates
    assert table(spark, settings.lake.silver("stream_deliveries")).count() == 252  # deduplicated

    # 3) A restart with nothing new, then a full replay of the match: nothing changes.
    run_available_now(spark, settings, topic)
    topic.publish(envelopes(FINAL_2025))
    run_available_now(spark, settings, topic)
    assert reconcile(expected(FINAL_2025), scorecard(spark, settings, FINAL_2025), FINAL_2025).ok
    assert table(spark, settings.lake.silver("stream_deliveries")).count() == 252

    # 4) The stateful activity query deduplicated within the watermark and emitted closed windows.
    activity = table(spark, settings.lake.gold("stream_activity")).collect()
    assert activity, "windows older than the watermark are emitted"
    assert all(r.events <= 60 for r in activity), (
        "1-minute windows hold at most 60 one-second events"
    )


def test_super_overs_get_their_own_live_innings(spark, tmp_path):
    settings = make_settings(tmp_path)
    topic = FakeTopic(tmp_path)
    topic.publish(envelopes(DOUBLE_SUPER_OVER))
    run_available_now(spark, settings, topic)
    rows = scorecard(spark, settings, DOUBLE_SUPER_OVER)
    assert [r["is_super_over"] for r in rows] == [False, False, True, True, True, True]
    assert reconcile(expected(DOUBLE_SUPER_OVER), rows, DOUBLE_SUPER_OVER).ok


def test_impossible_over_fails_the_stream_loudly(spark, tmp_path):
    settings = make_settings(tmp_path)
    topic = FakeTopic(tmp_path)
    events = envelopes(FINAL_2025, 0, 12)  # match_started + the first deliveries
    extra = copy.deepcopy(json.loads(events[1][1]))
    # A 7th legal ball in over 0 without an umpire-miscount flag cannot happen.
    extra["event_id"] = f"{FINAL_2025}-1-00-30"
    extra["payload"].update(ball_seq=30, umpire_miscount=False)
    extra["payload"]["extras"] = {"wides": 0, "noballs": 0, "byes": 0, "legbyes": 0, "penalty": 0}
    extra["payload"]["runs"] = {"batter": 1, "extras": 0, "total": 1}
    topic.publish([*events, (FINAL_2025, json.dumps(extra))])
    with pytest.raises(Exception, match="stream_max_six_legal_balls_per_over"):
        run_available_now(spark, settings, topic)
