"""Versioned event envelope for the live ball-by-ball feed.

Every Kafka message is one JSON envelope keyed by ``match_id`` (so a match's events stay in
order on one partition)::

    {
      "schema_version": 1,
      "event_id": "1473511-1-19-04",          # deterministic: replays reuse the same id
      "event_type": "delivery",               # match_started | delivery | match_completed
      "match_id": "1473511",
      "sequence": 245,                        # position of the event within its match
      "event_time": "2026-10-07T14:03:11.250Z",
      "produced_at": "2026-10-07T14:03:11.251Z",
      "source": "cricsheet-replay",
      "payload": {...}
    }

``event_time`` is when the event happened on the (replayed) live clock; the original match date
is in the payload. Consumers must treat ``event_id`` as the idempotency key.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ipl_lakehouse.cricsheet import iter_deliveries, season_of

SCHEMA_VERSION = 1
SOURCE = "cricsheet-replay"
EVENT_TYPES = ("match_started", "delivery", "match_completed")


def delivery_event_id(match_id: str, innings: int, over_index: int, seq: int) -> str:
    return f"{match_id}-{innings}-{over_index:02d}-{seq:02d}"


def lifecycle_event_id(match_id: str, event_type: str) -> str:
    return f"{match_id}-{event_type}"


def isoformat(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class ReplayEvent:
    event_id: str
    event_type: str
    match_id: str
    sequence: int
    offset_seconds: float  # simulated seconds since the start of the match
    payload: dict[str, Any]

    def envelope(self, event_time: datetime, produced_at: datetime | None = None) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "match_id": self.match_id,
            "sequence": self.sequence,
            "event_time": isoformat(event_time),
            "produced_at": isoformat(produced_at or datetime.now(UTC)),
            "source": SOURCE,
            "payload": self.payload,
        }


def serialize(envelope: dict[str, Any]) -> bytes:
    return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def deserialize(raw: bytes | str) -> dict[str, Any]:
    doc = json.loads(raw)
    if not isinstance(doc, dict):
        raise ValueError("event is not a JSON object")
    missing = [k for k in ("schema_version", "event_id", "event_type", "match_id") if k not in doc]
    if missing:
        raise ValueError(f"event is missing {missing}")
    if doc["schema_version"] != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema_version {doc['schema_version']!r}")
    return doc


def _match_payload(match: dict) -> dict[str, Any]:
    info = match["info"]
    event = info.get("event") or {}
    return {
        "season": season_of(match),
        "match_date": str(info["dates"][0]),
        "event_name": event.get("name"),
        "match_number": event.get("match_number"),
        "stage": event.get("stage") or "League",
        "teams": list(info["teams"]),
        "venue": info.get("venue"),
        "city": info.get("city"),
        "toss": info.get("toss") or {},
        "scheduled_overs": info.get("overs"),
    }


def match_to_events(
    match: dict,
    match_id: str,
    *,
    seconds_per_ball: float = 35.0,
    innings_break_seconds: float = 1200.0,
) -> list[ReplayEvent]:
    """Deterministic event stream for one match (lifecycle + one event per delivery)."""
    events: list[ReplayEvent] = []
    offset = 0.0
    events.append(
        ReplayEvent(
            lifecycle_event_id(match_id, "match_started"),
            "match_started",
            match_id,
            0,
            offset,
            _match_payload(match),
        )
    )
    current_innings = None
    scheduled = match["info"].get("overs")
    for d in iter_deliveries(match):
        if current_innings is not None and d.innings_number != current_innings:
            offset += innings_break_seconds
        current_innings = d.innings_number
        offset += seconds_per_ball
        payload = {
            "season": season_of(match),
            "match_date": str(match["info"]["dates"][0]),
            "innings": d.innings_number,
            "is_super_over": d.is_super_over,
            "batting_team": d.batting_team,
            "bowling_team": d.bowling_team,
            "over": d.over_index,
            "ball_seq": d.seq,
            "ball_label": d.label,
            "batter": d.batter,
            "non_striker": d.non_striker,
            "bowler": d.bowler,
            "runs": {"batter": d.runs_batter, "extras": d.runs_extras, "total": d.runs_total},
            "extras": {
                "wides": d.wides,
                "noballs": d.noballs,
                "byes": d.byes,
                "legbyes": d.legbyes,
                "penalty": d.penalty,
            },
            "non_boundary": d.non_boundary,
            "wickets": [
                {
                    "player_out": w.get("player_out"),
                    "kind": w.get("kind"),
                    "fielders": [f.get("name") for f in w.get("fielders") or [] if f.get("name")],
                }
                for w in d.wickets
            ],
            "target": (
                {"runs": d.target_runs, "overs": d.target_overs}
                if d.target_runs is not None
                else None
            ),
            "scheduled_overs": scheduled,
            "umpire_miscount": d.umpire_miscount,
        }
        events.append(
            ReplayEvent(
                delivery_event_id(match_id, d.innings_number, d.over_index, d.seq),
                "delivery",
                match_id,
                len(events),
                offset,
                payload,
            )
        )
    outcome = match["info"].get("outcome") or {}
    events.append(
        ReplayEvent(
            lifecycle_event_id(match_id, "match_completed"),
            "match_completed",
            match_id,
            len(events),
            offset + seconds_per_ball,
            {
                "winner": outcome.get("winner") or outcome.get("eliminator"),
                "result": outcome.get("result") or ("win" if outcome.get("winner") else None),
                "by_runs": (outcome.get("by") or {}).get("runs"),
                "by_wickets": (outcome.get("by") or {}).get("wickets"),
                "method": outcome.get("method"),
                "super_over_winner": outcome.get("eliminator"),
                "player_of_match": list(match["info"].get("player_of_match") or []),
            },
        )
    )
    return events
