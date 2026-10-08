import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ipl_lakehouse.streaming.events import match_to_events
from ipl_lakehouse.streaming.producer import ReplayProducer, load_replay_matches
from support import FINAL_2025, FIXTURES, load_match, make_settings


class FakeProducer:
    def __init__(self, fail_first: int = 0):
        self.messages = []
        self._fail = fail_first
        self.polls = 0

    def produce(self, topic, key, value, headers, on_delivery):
        if self._fail:
            self._fail -= 1
            raise BufferError("queue full")
        self.messages.append((topic, key, json.loads(value), dict(headers)))
        on_delivery(None, None)

    def poll(self, timeout):
        self.polls += 1

    def flush(self, timeout):
        return 0


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.t

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.t += seconds


def _producer(tmp_path: Path, fake: FakeProducer, clock: FakeClock | None = None) -> ReplayProducer:
    clock = clock or FakeClock()
    return ReplayProducer(
        make_settings(tmp_path, replay_seconds_per_ball=30.0, replay_innings_break_seconds=600.0),
        topic="t",
        producer=fake,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        now=lambda: datetime(2026, 10, 7, tzinfo=UTC),
    )


def test_replay_sends_every_event_keyed_by_match(tmp_path):
    fake = FakeProducer()
    stats = _producer(tmp_path, fake).replay([(FINAL_2025, load_match(FINAL_2025))], speedup=0)
    assert stats.sent == len(fake.messages) == 254 and stats.delivered == 254 and not stats.errors
    assert {m[1] for m in fake.messages} == {FINAL_2025.encode()}
    assert fake.messages[0][2]["event_type"] == "match_started"
    assert fake.messages[0][3] == {"schema_version": b"1", "event_type": b"match_started"}


def test_duplicates_limit_and_resume(tmp_path):
    fake = FakeProducer()
    stats = _producer(tmp_path, fake).replay(
        [(FINAL_2025, load_match(FINAL_2025))], speedup=0, limit=100, duplicate_every=10
    )
    assert stats.stopped_early and stats.sent == 100 and stats.duplicates == 10
    ids = [m[2]["event_id"] for m in fake.messages]
    assert len(ids) == 110 and len(set(ids)) == 100

    resumed = FakeProducer()
    _producer(tmp_path, resumed).replay(
        [(FINAL_2025, load_match(FINAL_2025))], speedup=0, start_sequence=100
    )
    assert resumed.messages[0][2]["sequence"] == 100
    every_event = {e.event_id for e in match_to_events(load_match(FINAL_2025), FINAL_2025)}
    assert set(ids) | {m[2]["event_id"] for m in resumed.messages} == every_event


def test_speedup_paces_events_on_the_match_clock(tmp_path):
    clock = FakeClock()
    fake = FakeProducer()
    _producer(tmp_path, fake, clock).replay(
        [(FINAL_2025, load_match(FINAL_2025))], speedup=30, limit=4
    )
    # 30 simulated seconds per ball at 30x => one real second between deliveries.
    assert clock.sleeps == pytest.approx([1.0, 1.0, 1.0])


def test_backpressure_is_retried(tmp_path):
    fake = FakeProducer(fail_first=2)
    stats = _producer(tmp_path, fake).replay(
        [(FINAL_2025, load_match(FINAL_2025))], speedup=0, limit=3
    )
    assert stats.sent == 3 and len(fake.messages) == 3 and fake.polls >= 2


def test_load_replay_matches_from_files(tmp_path):
    matches = load_replay_matches(make_settings(tmp_path), files=[FIXTURES / f"{FINAL_2025}.json"])
    assert [m[0] for m in matches] == [FINAL_2025]
    bad = tmp_path / "final.json"
    bad.write_text("{}")
    with pytest.raises(ValueError, match="match_id"):
        load_replay_matches(make_settings(tmp_path), files=[bad])
    with pytest.raises(ValueError, match="manifest is empty"):
        load_replay_matches(make_settings(tmp_path), season=2025)
