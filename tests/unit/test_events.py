import copy
import json
from datetime import UTC, datetime

import pytest

from ipl_lakehouse.cricsheet import iter_deliveries
from ipl_lakehouse.streaming.events import (
    SCHEMA_VERSION,
    delivery_event_id,
    deserialize,
    match_to_events,
    serialize,
)
from support import DOUBLE_SUPER_OVER, FINAL_2025, load_match

T0 = datetime(2026, 10, 7, 14, 0, 0, 250_000, tzinfo=UTC)


def test_event_stream_shape():
    match = load_match(FINAL_2025)
    events = match_to_events(match, FINAL_2025)
    assert events[0].event_type == "match_started"
    assert events[-1].event_type == "match_completed"
    deliveries = [e for e in events if e.event_type == "delivery"]
    assert len(deliveries) == len(list(iter_deliveries(match))) == 252
    assert [e.sequence for e in events] == list(range(len(events)))
    offsets = [e.offset_seconds for e in events]
    assert offsets == sorted(offsets)


def test_event_ids_are_deterministic_and_unique():
    match = load_match(FINAL_2025)
    first = [e.event_id for e in match_to_events(match, FINAL_2025)]
    again = [e.event_id for e in match_to_events(copy.deepcopy(match), FINAL_2025)]
    assert first == again
    assert len(set(first)) == len(first)
    assert first[1] == delivery_event_id(FINAL_2025, 1, 0, 1) == "1473511-1-00-01"


def test_innings_break_and_target_are_carried():
    events = [e for e in match_to_events(load_match(FINAL_2025), FINAL_2025, innings_break_seconds=600)
              if e.event_type == "delivery"]  # fmt: skip
    first = [e for e in events if e.payload["innings"] == 1]
    second = [e for e in events if e.payload["innings"] == 2]
    assert second[0].offset_seconds - first[-1].offset_seconds >= 600
    assert all(e.payload["target"] is None for e in first)
    assert {json.dumps(e.payload["target"]) for e in second} == {'{"runs": 191, "overs": 20}'}


def test_super_overs_are_flagged():
    events = match_to_events(load_match(DOUBLE_SUPER_OVER), DOUBLE_SUPER_OVER)
    supers = {
        e.payload["innings"]
        for e in events
        if e.event_type == "delivery" and e.payload["is_super_over"]
    }
    assert supers == {3, 4, 5, 6}
    completed = events[-1].payload
    assert completed["winner"] == completed["super_over_winner"] == "Kings XI Punjab"


def test_umpire_miscount_flag_comes_from_source():
    match = load_match(FINAL_2025)
    match["innings"][0]["miscounted_overs"] = {"3": {"balls": 7}}
    flagged = {e.payload["over"] for e in match_to_events(match, FINAL_2025)
               if e.event_type == "delivery" and e.payload["umpire_miscount"]}  # fmt: skip
    assert flagged == {3}


def test_envelope_round_trip():
    event = match_to_events(load_match(FINAL_2025), FINAL_2025)[5]
    envelope = event.envelope(T0, T0)
    assert envelope["schema_version"] == SCHEMA_VERSION
    assert envelope["event_time"] == "2026-10-07T14:00:00.250Z"
    assert deserialize(serialize(envelope)) == envelope


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (b"[1, 2]", "not a JSON object"),
        (b'{"schema_version": 1, "event_id": "x"}', "missing"),
        (
            b'{"schema_version": 99, "event_id": "x", "event_type": "delivery", "match_id": "1"}',
            "unsupported",
        ),
    ],
)
def test_deserialize_rejects_bad_events(raw, message):
    with pytest.raises(ValueError, match=message):
        deserialize(raw)
