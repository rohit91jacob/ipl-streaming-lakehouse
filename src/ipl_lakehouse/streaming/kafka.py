"""Kafka client configuration (librdkafka for Python clients, Java options for Spark) and topic admin."""

from __future__ import annotations

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from ipl_lakehouse.config import Settings
from ipl_lakehouse.logs import get_logger

log = get_logger(__name__)

_JAAS_MODULES = {
    "PLAIN": "org.apache.kafka.common.security.plain.PlainLoginModule",
    "SCRAM-SHA-256": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "SCRAM-SHA-512": "org.apache.kafka.common.security.scram.ScramLoginModule",
}


def client_config(settings: Settings, **overrides: object) -> dict[str, object]:
    conf: dict[str, object] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "security.protocol": settings.kafka_security_protocol,
        "client.id": "ipl-streaming-lakehouse",
    }
    if settings.kafka_sasl_mechanism:
        conf["sasl.mechanisms"] = settings.kafka_sasl_mechanism
        conf["sasl.username"] = settings.kafka_sasl_username or ""
        conf["sasl.password"] = settings.kafka_sasl_password or ""
    conf.update(overrides)
    return conf


def spark_kafka_options(settings: Settings) -> dict[str, str]:
    opts = {
        "kafka.bootstrap.servers": settings.kafka_bootstrap_servers,
        "kafka.security.protocol": settings.kafka_security_protocol,
    }
    mechanism = settings.kafka_sasl_mechanism
    if mechanism:
        module = _JAAS_MODULES.get(mechanism)
        if module is None:
            raise ValueError(f"unsupported SASL mechanism for Spark: {mechanism}")
        opts["kafka.sasl.mechanism"] = mechanism
        opts["kafka.sasl.jaas.config"] = (
            f'{module} required username="{settings.kafka_sasl_username or ""}" '
            f'password="{settings.kafka_sasl_password or ""}";'
        )
    return opts


def ensure_topic(
    settings: Settings, topic: str | None = None, admin: AdminClient | None = None
) -> bool:
    """Create the topic if missing. Returns True when it was created."""
    topic = topic or settings.kafka_topic
    admin = admin or AdminClient(client_config(settings))
    existing = admin.list_topics(timeout=15).topics
    if topic in existing:
        log.info(
            "topic exists", extra={"topic": topic, "partitions": len(existing[topic].partitions)}
        )
        return False
    new = NewTopic(
        topic,
        num_partitions=settings.kafka_topic_partitions,
        replication_factor=settings.kafka_replication_factor,
        config={
            "cleanup.policy": "delete",
            "retention.ms": str(settings.kafka_topic_retention_ms),
        },
    )
    try:
        admin.create_topics([new], operation_timeout=30)[topic].result(timeout=60)
    except KafkaException as exc:
        if "TOPIC_ALREADY_EXISTS" in str(exc):  # created concurrently
            return False
        raise
    log.info("topic created", extra={"topic": topic, "partitions": settings.kafka_topic_partitions})
    return True
