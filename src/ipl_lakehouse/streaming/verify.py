"""Reconcile the streaming ``gold/live_scorecard`` against the batch view of the same match."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ipl_lakehouse.config import Settings
from ipl_lakehouse.cricsheet import innings_totals
from ipl_lakehouse.ingest import manifest
from ipl_lakehouse.query import read_table

COMPARED = ("batting_team", "runs", "wickets", "legal_balls")


@dataclass
class Reconciliation:
    match_id: str
    expected: list[dict]
    actual: list[dict]
    mismatches: list[str]

    @property
    def ok(self) -> bool:
        return not self.mismatches


def expected_from_source(settings: Settings, match_id: str, file: Path | None = None) -> list[dict]:
    """Innings totals computed directly from the Cricsheet file (independent of Spark)."""
    if file is None:
        latest = manifest.latest_versions(settings.lake.manifest).get(match_id)
        if latest is None:
            raise ValueError(f"match {match_id} is not in the bronze manifest; pass --file")
        file = settings.data_dir / latest["bronze_path"]
    match = json.loads(Path(file).read_text(encoding="utf-8"))
    return [t.as_dict() for t in innings_totals(match)]


def expected_from_gold(settings: Settings, match_id: str) -> list[dict]:
    table = read_table(settings.lake.gold("innings_summary"), match_id=match_id)
    return sorted(table.to_pylist(), key=lambda r: r["innings_number"])


def live_scorecard(settings: Settings, match_id: str) -> list[dict]:
    path = settings.lake.gold("live_scorecard")
    if not (path / "_delta_log").is_dir():
        return []
    return sorted(
        read_table(path, match_id=match_id).to_pylist(), key=lambda r: r["innings_number"]
    )


def reconcile(expected: list[dict], actual: list[dict], match_id: str) -> Reconciliation:
    mismatches = []
    exp = {r["innings_number"]: r for r in expected}
    act = {r["innings_number"]: r for r in actual}
    for number in sorted(set(exp) | set(act)):
        if number not in act:
            mismatches.append(f"innings {number}: missing from live_scorecard")
            continue
        if number not in exp:
            mismatches.append(f"innings {number}: unexpected in live_scorecard")
            continue
        for column in COMPARED:
            if exp[number][column] != act[number][column]:
                mismatches.append(
                    f"innings {number} {column}: expected {exp[number][column]!r}, "
                    f"live {act[number][column]!r}"
                )
    return Reconciliation(match_id, expected, actual, mismatches)
