"""Pure-Python view over Cricsheet match JSON (https://cricsheet.org/format/json/).

The batch pipeline parses the files with Spark; this module is the lightweight equivalent used
by the replay producer, ``ipl verify-stream`` and the tests (it needs no JVM).
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from ipl_lakehouse.cricket import (
    NON_DISMISSAL_KINDS,
    balls_to_overs,
    is_legal_delivery,
    overs_to_balls,
)

MATCH_FILE_RE = re.compile(r"^(?:.*/)?(\d+)\.json$")
REQUIRED_INFO_KEYS = ("dates", "teams", "outcome", "season")


def match_id_from_filename(name: str) -> str | None:
    found = MATCH_FILE_RE.match(name)
    return found.group(1) if found else None


def validate_match(doc: Any) -> list[str]:
    """Structural problems that make a file unusable (empty list means OK)."""
    if not isinstance(doc, dict):
        return ["document is not a JSON object"]
    problems = [f"missing top-level key {k!r}" for k in ("meta", "info", "innings") if k not in doc]
    info = doc.get("info")
    if isinstance(info, dict):
        problems += [f"missing info.{k}" for k in REQUIRED_INFO_KEYS if k not in info]
        if isinstance(info.get("teams"), list) and len(info["teams"]) != 2:
            problems.append("info.teams must list exactly two teams")
    elif "info" in doc:
        problems.append("info is not an object")
    if "innings" in doc and not isinstance(doc["innings"], list):
        problems.append("innings is not a list")
    return problems


def season_of(match: dict) -> int:
    """IPL season as the calendar year of the first match day ("2007/08" -> 2008)."""
    return int(str(match["info"]["dates"][0])[:4])


def other_team(teams: list[str], team: str) -> str:
    rest = [t for t in teams if t != team]
    return rest[0] if rest else ""


@dataclass(frozen=True)
class Delivery:
    innings_number: int
    is_super_over: bool
    batting_team: str
    bowling_team: str
    over_index: int  # 0-based, as in Cricsheet ("19.4" is over_index 19)
    seq: int  # 1-based position inside the over, counting wides and no-balls
    label: str  # Cricsheet `actual_delivery`, e.g. "19.4"
    batter: str
    non_striker: str
    bowler: str
    runs_batter: int
    runs_extras: int
    runs_total: int
    non_boundary: bool
    wides: int
    noballs: int
    byes: int
    legbyes: int
    penalty: int
    wickets: tuple[dict, ...] = field(default_factory=tuple)
    target_runs: int | None = None
    target_overs: float | None = None
    umpire_miscount: bool = False

    @property
    def is_legal(self) -> bool:
        return is_legal_delivery(self.wides, self.noballs)

    @property
    def team_wickets(self) -> int:
        return sum(1 for w in self.wickets if w.get("kind") not in NON_DISMISSAL_KINDS)


def iter_deliveries(match: dict) -> Iterator[Delivery]:
    teams = match["info"]["teams"]
    for number, innings in enumerate(match.get("innings", []), start=1):
        target = innings.get("target") or {}
        miscounted = {int(k) for k in (innings.get("miscounted_overs") or {})}
        batting = innings["team"]
        for over in innings.get("overs", []):
            over_index = int(over["over"])
            for seq, d in enumerate(over.get("deliveries", []), start=1):
                runs, extras = d["runs"], d.get("extras") or {}
                yield Delivery(
                    innings_number=number,
                    is_super_over=bool(innings.get("super_over", False)),
                    batting_team=batting,
                    bowling_team=other_team(teams, batting),
                    over_index=over_index,
                    seq=seq,
                    label=str(d.get("actual_delivery", "")),
                    batter=d["batter"],
                    non_striker=d["non_striker"],
                    bowler=d["bowler"],
                    runs_batter=int(runs["batter"]),
                    runs_extras=int(runs["extras"]),
                    runs_total=int(runs["total"]),
                    non_boundary=bool(runs.get("non_boundary", False)),
                    wides=int(extras.get("wides", 0)),
                    noballs=int(extras.get("noballs", 0)),
                    byes=int(extras.get("byes", 0)),
                    legbyes=int(extras.get("legbyes", 0)),
                    penalty=int(extras.get("penalty", 0)),
                    wickets=tuple(d.get("wickets") or ()),
                    target_runs=target.get("runs"),
                    target_overs=target.get("overs"),
                    umpire_miscount=over_index in miscounted,
                )


@dataclass(frozen=True)
class InningsTotal:
    innings_number: int
    batting_team: str
    is_super_over: bool
    runs: int
    wickets: int
    legal_balls: int
    target_runs: int | None
    target_balls: int | None

    @property
    def overs(self) -> str:
        return balls_to_overs(self.legal_balls)

    def as_dict(self) -> dict[str, Any]:
        return {
            "innings_number": self.innings_number,
            "batting_team": self.batting_team,
            "is_super_over": self.is_super_over,
            "runs": self.runs,
            "wickets": self.wickets,
            "legal_balls": self.legal_balls,
            "overs": self.overs,
            "target_runs": self.target_runs,
            "target_balls": self.target_balls,
        }


def innings_totals(match: dict) -> list[InningsTotal]:
    """Reference innings totals computed straight from the source file."""
    totals: list[InningsTotal] = []
    for number, innings in enumerate(match.get("innings", []), start=1):
        runs = wickets = balls = 0
        for over in innings.get("overs", []):
            for d in over.get("deliveries", []):
                extras = d.get("extras") or {}
                runs += int(d["runs"]["total"])
                wickets += sum(
                    1 for w in d.get("wickets") or () if w.get("kind") not in NON_DISMISSAL_KINDS
                )
                balls += is_legal_delivery(extras.get("wides", 0), extras.get("noballs", 0))
        penalty = innings.get("penalty_runs") or {}
        runs += int(penalty.get("pre", 0)) + int(penalty.get("post", 0))
        target = innings.get("target") or {}
        totals.append(
            InningsTotal(
                innings_number=number,
                batting_team=innings["team"],
                is_super_over=bool(innings.get("super_over", False)),
                runs=runs,
                wickets=wickets,
                legal_balls=balls,
                target_runs=target.get("runs"),
                target_balls=overs_to_balls(target["overs"]) if "overs" in target else None,
            )
        )
    return totals
