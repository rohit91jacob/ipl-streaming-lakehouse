from pathlib import Path

import pytest

from ipl_lakehouse.config import ConfigError, Settings


def test_defaults():
    s = Settings.from_env({})
    assert s.data_dir == Path("data")
    assert s.checkpoint_dir == Path("data/_checkpoints")
    assert s.kafka_topic == "ipl.deliveries.v1"
    assert s.kafka_starting_offsets == "earliest"
    assert s.dq_fail_on_error is True
    assert s.lake.silver("deliveries") == Path("data/silver/deliveries")


def test_overrides_and_derived_paths():
    s = Settings.from_env(
        {
            "IPL_DATA_DIR": "/tmp/lake",
            "IPL_KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "IPL_KAFKA_TOPIC_PARTITIONS": "12",
            "IPL_STREAM_DQ_FAIL_ON_ERROR": "false",
            "IPL_REPLAY_SPEEDUP": "0",
        }
    )
    assert s.data_dir == Path("/tmp/lake")
    assert s.checkpoint_dir == Path("/tmp/lake/_checkpoints")
    assert s.kafka_topic_partitions == 12
    assert s.stream_dq_fail_on_error is False
    assert s.replay_speedup == 0
    assert s.lake.stream_events == Path("/tmp/lake/bronze/stream_events")


@pytest.mark.parametrize(
    "env",
    [
        {"IPL_KAFKA_TOPIC_PARTITIONS": "many"},
        {"IPL_KAFKA_TOPIC_PARTITIONS": "0"},
        {"IPL_DQ_FAIL_ON_ERROR": "maybe"},
        {"IPL_LOG_FORMAT": "xml"},
        {"IPL_KAFKA_SECURITY_PROTOCOL": "SASL_SSL"},  # mechanism missing
        {"IPL_KAFKA_STARTING_OFFSETS": "yesterday"},
        {"IPL_REPLAY_SPEEDUP": "-1"},
    ],
)
def test_invalid_values_fail_fast(env):
    with pytest.raises(ConfigError):
        Settings.from_env(env)


def test_password_not_in_repr():
    s = Settings.from_env(
        {
            "IPL_KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "IPL_KAFKA_SASL_MECHANISM": "PLAIN",
            "IPL_KAFKA_SASL_USERNAME": "svc",
            "IPL_KAFKA_SASL_PASSWORD": "s3cr3t",
        }
    )
    assert s.kafka_sasl_password == "s3cr3t"
    assert "s3cr3t" not in repr(s)
