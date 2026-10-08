"""Replay historical Cricsheet matches into Kafka as a live ball-by-ball feed.

* key = ``match_id`` so a match is totally ordered on one partition;
* idempotent producer (``enable.idempotence``, ``acks=all``) so broker retries never duplicate;
* ``--speedup`` compresses the match clock (60 = a 3.5 hour match in ~3.5 minutes; 0 = no
  sleeping); ``--duplicate-every`` and ``--limit`` exist to exercise consumer idempotency and
  restarts.
"""

from __future__ import annotations

import json
import signal
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Producer

from ipl_lakehouse.config import Settings
from ipl_lakehouse.cricsheet import match_id_from_filename
from ipl_lakehouse.ingest import manifest
from ipl_lakehouse.logs import get_logger
from ipl_lakehouse.streaming.events import ReplayEvent, match_to_events, serialize
from ipl_lakehouse.streaming.kafka import client_config

log = get_logger(__name__)


@dataclass
class ReplayStats:
    matches: int = 0
    sent: int = 0
    duplicates: int = 0
    delivered: int = 0
    errors: list[str] = field(default_factory=list)
    stopped_early: bool = False


def load_replay_matches(
    settings: Settings,
    *,
    match_ids: Sequence[str] = (),
    season: int | None = None,
    files: Sequence[Path] = (),
) -> list[tuple[str, dict]]:
    """Matches to replay, oldest first, from explicit files or from the bronze zone."""
    if files:
        out = []
        for path in files:
            match_id = match_id_from_filename(path.name)
            if match_id is None:
                raise ValueError(f"{path} is not named <match_id>.json")
            out.append((match_id, json.loads(path.read_text(encoding="utf-8"))))
        return out
    latest = manifest.latest_versions(settings.lake.manifest)
    if not latest:
        raise ValueError("bronze manifest is empty; run `ipl ingest` (or pass --file)")
    rows = [
        r
        for r in latest.values()
        if (not match_ids or r["match_id"] in set(match_ids))
        and (season is None or r["season"] == season)
    ]
    if not rows:
        raise ValueError(f"no matches found for match_ids={list(match_ids)} season={season}")
    rows.sort(key=lambda r: (r["match_date"] or "", int(r["match_id"])))
    return [
        (
            r["match_id"],
            json.loads((settings.data_dir / r["bronze_path"]).read_text(encoding="utf-8")),
        )
        for r in rows
    ]


class ReplayProducer:
    def __init__(
        self,
        settings: Settings,
        *,
        topic: str | None = None,
        producer: Producer | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.settings = settings
        self.topic = topic or settings.kafka_topic
        self._producer = producer or Producer(
            client_config(
                settings,
                **{
                    "enable.idempotence": True,
                    "acks": "all",
                    "compression.type": "zstd",
                    "linger.ms": 20,
                    "delivery.timeout.ms": 120_000,
                },
            )
        )
        self._sleep, self._monotonic, self._now = sleep, monotonic, now
        self._stop = False
        self.stats = ReplayStats()

    def request_stop(self, *_: object) -> None:
        log.info("stop requested; flushing in-flight events")
        self._stop = True

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.stats.errors.append(str(err))
        else:
            self.stats.delivered += 1

    def send(self, event: ReplayEvent) -> None:
        ts = self._now()  # the replayed "live" clock: when this ball happens
        value = serialize(event.envelope(ts, ts))
        headers = [("schema_version", b"1"), ("event_type", event.event_type.encode())]
        while True:
            try:
                self._producer.produce(
                    self.topic,
                    key=event.match_id.encode(),
                    value=value,
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:  # local queue full: let librdkafka drain, then retry
                self._producer.poll(0.5)
        self._producer.poll(0)

    def replay(
        self,
        matches: Iterable[tuple[str, dict]],
        *,
        speedup: float | None = None,
        limit: int | None = None,
        duplicate_every: int | None = None,
        start_sequence: int = 0,
        flush_timeout: float = 60.0,
    ) -> ReplayStats:
        speedup = self.settings.replay_speedup if speedup is None else speedup
        started = self._monotonic()
        sim_base = 0.0  # simulated seconds of everything replayed before the current match
        for match_id, match in matches:
            events = match_to_events(
                match,
                match_id,
                seconds_per_ball=self.settings.replay_seconds_per_ball,
                innings_break_seconds=self.settings.replay_innings_break_seconds,
            )
            self.stats.matches += 1
            log.info("replaying match", extra={"match_id": match_id, "events": len(events)})
            first_offset = None
            for event in events:
                if event.sequence < start_sequence:
                    continue
                if self._stop or (limit is not None and self.stats.sent >= limit):
                    self.stats.stopped_early = True
                    break
                if first_offset is None:
                    first_offset = event.offset_seconds
                if speedup > 0:
                    due = (sim_base + event.offset_seconds - first_offset) / speedup
                    delay = due - (self._monotonic() - started)
                    if delay > 0:
                        self._sleep(delay)
                self.send(event)
                self.stats.sent += 1
                if duplicate_every and self.stats.sent % duplicate_every == 0:
                    self.send(event)
                    self.stats.duplicates += 1
            if self.stats.stopped_early:
                break
            sim_base += events[-1].offset_seconds - (first_offset or 0.0)
            start_sequence = 0  # only the first match resumes mid-way
        remaining = self._producer.flush(flush_timeout)
        if remaining:
            self.stats.errors.append(f"{remaining} messages still queued after flush")
        log.info(
            "replay finished",
            extra={
                "matches": self.stats.matches,
                "sent": self.stats.sent,
                "duplicates": self.stats.duplicates,
                "delivered": self.stats.delivered,
                "errors": len(self.stats.errors),
                "stopped_early": self.stats.stopped_early,
            },
        )
        return self.stats


def install_signal_handlers(producer: ReplayProducer) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, producer.request_stop)
