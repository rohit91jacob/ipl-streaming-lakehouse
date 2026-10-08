"""Environment-driven settings (12-factor). Every knob is an ``IPL_*`` variable; see .env.example."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path


class ConfigError(ValueError):
    """Raised when an environment variable holds an invalid value."""


_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


class _Env:
    def __init__(self, env: Mapping[str, str]):
        self._env = env

    def text(self, name: str, default: str) -> str:
        value = self._env.get(name, "").strip()
        return value or default

    def optional(self, name: str) -> str | None:
        value = self._env.get(name, "").strip()
        return value or None

    def flag(self, name: str, default: bool) -> bool:
        raw = self._env.get(name, "").strip().lower()
        if not raw:
            return default
        if raw in _TRUE:
            return True
        if raw in _FALSE:
            return False
        raise ConfigError(f"{name} must be a boolean (true/false), got {raw!r}")

    def integer(self, name: str, default: int, minimum: int | None = None) -> int:
        raw = self._env.get(name, "").strip()
        try:
            value = int(raw) if raw else default
        except ValueError as exc:
            raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
        if minimum is not None and value < minimum:
            raise ConfigError(f"{name} must be >= {minimum}, got {value}")
        return value

    def number(self, name: str, default: float, minimum: float | None = None) -> float:
        raw = self._env.get(name, "").strip()
        try:
            value = float(raw) if raw else default
        except ValueError as exc:
            raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
        if minimum is not None and value < minimum:
            raise ConfigError(f"{name} must be >= {minimum}, got {value}")
        return value

    def choice(self, name: str, default: str, choices: set[str]) -> str:
        value = self.text(name, default)
        if value not in choices:
            raise ConfigError(f"{name} must be one of {sorted(choices)}, got {value!r}")
        return value


@dataclass(frozen=True)
class Settings:
    data_dir: Path = Path("data")
    checkpoint_dir: Path = Path("data/_checkpoints")

    # Source
    cricsheet_url: str = "https://cricsheet.org/downloads/ipl_json.zip"
    cricsheet_register_url: str = "https://cricsheet.org/register/people.csv"
    http_timeout_s: float = 60.0
    http_retries: int = 5
    user_agent: str = (
        "ipl-streaming-lakehouse/0.1 (+https://github.com/rohit91jacob/ipl-streaming-lakehouse)"
    )

    # Spark
    spark_master: str = "local[*]"
    spark_driver_memory: str = "2g"
    spark_shuffle_partitions: int = 8
    spark_jars_dir: Path | None = None
    spark_ui_enabled: bool = False
    spark_log_level: str = "WARN"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "ipl.deliveries.v1"
    kafka_topic_partitions: int = 6
    kafka_replication_factor: int = 1
    kafka_topic_retention_ms: int = 7 * 24 * 3600 * 1000
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = field(default=None, repr=False)
    kafka_starting_offsets: str = "earliest"
    kafka_max_offsets_per_trigger: int = 10_000
    kafka_fail_on_data_loss: bool = True

    # Streaming
    stream_trigger_seconds: int = 5
    stream_watermark: str = "2 minutes"
    stream_activity_window: str = "1 minute"
    stream_dq_fail_on_error: bool = True

    # Replay producer
    replay_speedup: float = 60.0
    replay_seconds_per_ball: float = 35.0
    replay_innings_break_seconds: float = 1200.0

    # Batch
    dq_fail_on_error: bool = True

    # Observability
    log_level: str = "INFO"
    log_format: str = "json"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        e = _Env(os.environ if env is None else env)
        data_dir = Path(e.text("IPL_DATA_DIR", "data")).expanduser()
        jars_dir = e.optional("IPL_SPARK_JARS_DIR")
        security = e.choice(
            "IPL_KAFKA_SECURITY_PROTOCOL",
            "PLAINTEXT",
            {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"},
        )
        mechanism = e.optional("IPL_KAFKA_SASL_MECHANISM")
        if security.startswith("SASL") and mechanism is None:
            raise ConfigError("IPL_KAFKA_SASL_MECHANISM is required when using a SASL protocol")
        starting = e.text("IPL_KAFKA_STARTING_OFFSETS", "earliest")
        if starting not in {"earliest", "latest"} and not starting.startswith("{"):
            raise ConfigError("IPL_KAFKA_STARTING_OFFSETS must be earliest, latest or a JSON spec")
        return cls(
            data_dir=data_dir,
            checkpoint_dir=Path(
                e.text("IPL_CHECKPOINT_DIR", str(data_dir / "_checkpoints"))
            ).expanduser(),
            cricsheet_url=e.text("IPL_CRICSHEET_URL", cls.cricsheet_url),
            cricsheet_register_url=e.text("IPL_CRICSHEET_REGISTER_URL", cls.cricsheet_register_url),
            http_timeout_s=e.number("IPL_HTTP_TIMEOUT_SECONDS", cls.http_timeout_s, minimum=1),
            http_retries=e.integer("IPL_HTTP_RETRIES", cls.http_retries, minimum=0),
            user_agent=e.text("IPL_HTTP_USER_AGENT", cls.user_agent),
            spark_master=e.text("IPL_SPARK_MASTER", cls.spark_master),
            spark_driver_memory=e.text("IPL_SPARK_DRIVER_MEMORY", cls.spark_driver_memory),
            spark_shuffle_partitions=e.integer(
                "IPL_SPARK_SHUFFLE_PARTITIONS", cls.spark_shuffle_partitions, minimum=1
            ),
            spark_jars_dir=Path(jars_dir) if jars_dir else None,
            spark_ui_enabled=e.flag("IPL_SPARK_UI_ENABLED", cls.spark_ui_enabled),
            spark_log_level=e.choice(
                "IPL_SPARK_LOG_LEVEL", cls.spark_log_level, {"ERROR", "WARN", "INFO", "DEBUG"}
            ),
            kafka_bootstrap_servers=e.text(
                "IPL_KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers
            ),
            kafka_topic=e.text("IPL_KAFKA_TOPIC", cls.kafka_topic),
            kafka_topic_partitions=e.integer(
                "IPL_KAFKA_TOPIC_PARTITIONS", cls.kafka_topic_partitions, minimum=1
            ),
            kafka_replication_factor=e.integer(
                "IPL_KAFKA_REPLICATION_FACTOR", cls.kafka_replication_factor, minimum=1
            ),
            kafka_topic_retention_ms=e.integer(
                "IPL_KAFKA_TOPIC_RETENTION_MS", cls.kafka_topic_retention_ms, minimum=-1
            ),
            kafka_security_protocol=security,
            kafka_sasl_mechanism=mechanism,
            kafka_sasl_username=e.optional("IPL_KAFKA_SASL_USERNAME"),
            kafka_sasl_password=e.optional("IPL_KAFKA_SASL_PASSWORD"),
            kafka_starting_offsets=starting,
            kafka_max_offsets_per_trigger=e.integer(
                "IPL_KAFKA_MAX_OFFSETS_PER_TRIGGER", cls.kafka_max_offsets_per_trigger, minimum=1
            ),
            kafka_fail_on_data_loss=e.flag(
                "IPL_KAFKA_FAIL_ON_DATA_LOSS", cls.kafka_fail_on_data_loss
            ),
            stream_trigger_seconds=e.integer(
                "IPL_STREAM_TRIGGER_SECONDS", cls.stream_trigger_seconds, minimum=1
            ),
            stream_watermark=e.text("IPL_STREAM_WATERMARK", cls.stream_watermark),
            stream_activity_window=e.text("IPL_STREAM_ACTIVITY_WINDOW", cls.stream_activity_window),
            stream_dq_fail_on_error=e.flag(
                "IPL_STREAM_DQ_FAIL_ON_ERROR", cls.stream_dq_fail_on_error
            ),
            replay_speedup=e.number("IPL_REPLAY_SPEEDUP", cls.replay_speedup, minimum=0),
            replay_seconds_per_ball=e.number(
                "IPL_REPLAY_SECONDS_PER_BALL", cls.replay_seconds_per_ball, minimum=0
            ),
            replay_innings_break_seconds=e.number(
                "IPL_REPLAY_INNINGS_BREAK_SECONDS", cls.replay_innings_break_seconds, minimum=0
            ),
            dq_fail_on_error=e.flag("IPL_DQ_FAIL_ON_ERROR", cls.dq_fail_on_error),
            log_level=e.choice(
                "IPL_LOG_LEVEL", cls.log_level, {"DEBUG", "INFO", "WARNING", "ERROR"}
            ),
            log_format=e.choice("IPL_LOG_FORMAT", cls.log_format, {"json", "text"}),
        )

    @property
    def lake(self) -> Lake:
        return Lake(self.data_dir)


@dataclass(frozen=True)
class Lake:
    """Physical layout of the lakehouse. Every table is a Delta table unless noted."""

    root: Path

    # --- landing / bronze (batch) ---------------------------------------------
    @property
    def landing_dir(self) -> Path:
        return self.root / "landing" / "cricsheet"

    @property
    def bronze_matches_dir(self) -> Path:
        """Immutable, content-addressed raw match JSON: match_id=<id>/<sha256>.json (not Delta)."""
        return self.root / "bronze" / "cricsheet" / "matches"

    @property
    def bronze_register_dir(self) -> Path:
        return self.root / "bronze" / "cricsheet" / "register"

    @property
    def bronze_quarantine_dir(self) -> Path:
        return self.root / "bronze" / "cricsheet" / "_quarantine"

    @property
    def ingest_state_file(self) -> Path:
        return self.root / "bronze" / "cricsheet" / "_state.json"

    @property
    def manifest(self) -> Path:
        return self.root / "bronze" / "cricsheet" / "manifest"

    # --- streaming bronze ------------------------------------------------------
    @property
    def stream_events(self) -> Path:
        return self.root / "bronze" / "stream_events"

    # --- silver ------------------------------------------------------------------
    def silver(self, table: str) -> Path:
        return self.root / "silver" / table

    # --- gold --------------------------------------------------------------------
    def gold(self, table: str) -> Path:
        return self.root / "gold" / table

    # --- ops ---------------------------------------------------------------------
    @property
    def dq_results(self) -> Path:
        return self.root / "ops" / "dq_results"

    @property
    def stream_dlq(self) -> Path:
        return self.root / "ops" / "stream_dlq"
