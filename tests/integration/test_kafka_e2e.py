"""Producer -> real Kafka -> Structured Streaming -> Delta. Needs a broker (CI starts one)."""

from __future__ import annotations

import os
import socket
import uuid

import pytest
from confluent_kafka import Producer

from ipl_lakehouse.cricsheet import innings_totals
from ipl_lakehouse.streaming import processor
from ipl_lakehouse.streaming.kafka import client_config, ensure_topic
from ipl_lakehouse.streaming.producer import ReplayProducer
from ipl_lakehouse.streaming.verify import live_scorecard, reconcile
from support import FINAL_2025, load_match, make_settings

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("IPL_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _broker_up() -> bool:
    host, _, port = BOOTSTRAP.split(",")[0].rpartition(":")
    try:
        with socket.create_connection((host or "localhost", int(port)), timeout=3):
            return True
    except OSError:
        return False


@pytest.fixture
def settings(tmp_path):
    if not _broker_up():
        pytest.skip(f"no Kafka broker at {BOOTSTRAP}")
    return make_settings(
        tmp_path,
        kafka_bootstrap_servers=BOOTSTRAP,
        kafka_topic=f"ipl.it.{uuid.uuid4().hex[:8]}",
        kafka_topic_partitions=3,
    )


def test_replay_through_kafka_is_effectively_once(spark, settings):
    assert ensure_topic(settings) is True
    assert ensure_topic(settings) is False  # idempotent
    match = load_match(FINAL_2025)

    stats = ReplayProducer(settings).replay([(FINAL_2025, match)], speedup=0, duplicate_every=7)
    assert stats.errors == [] and stats.delivered == stats.sent + stats.duplicates

    raw = Producer(client_config(settings))
    raw.produce(settings.kafka_topic, key=FINAL_2025.encode(), value=b"{garbage")
    raw.flush(30)

    processor.run(spark, settings, available_now=True)
    expected = [t.as_dict() for t in innings_totals(match)]
    assert reconcile(expected, live_scorecard(settings, FINAL_2025), FINAL_2025).ok
    dlq = spark.read.format("delta").load(str(settings.lake.stream_dlq)).collect()
    assert [r.error_reason for r in dlq] == ["malformed_json"]

    # Replaying the whole match again (a producer restart) must not change the result.
    ReplayProducer(settings).replay([(FINAL_2025, match)], speedup=0)
    processor.run(spark, settings, available_now=True)
    assert reconcile(expected, live_scorecard(settings, FINAL_2025), FINAL_2025).ok
    deliveries = spark.read.format("delta").load(str(settings.lake.silver("stream_deliveries")))
    assert deliveries.count() == 252
