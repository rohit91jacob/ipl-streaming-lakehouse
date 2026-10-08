"""Curated reference data shipped with the package.

* ``teams.csv`` maps the name a team played under to its franchise (Delhi Daredevils and
  Delhi Capitals are one franchise; Deccan Chargers and Sunrisers Hyderabad are not).
* ``venues.csv`` folds Cricsheet's venue spellings ("M.Chinnaswamy Stadium",
  "M Chinnaswamy Stadium, Bengaluru", ...) and renames (Feroz Shah Kotla -> Arun Jaitley
  Stadium) into one canonical venue and city.
* ``match_adjustments.csv`` fills gaps in the source: Cricsheet only publishes matches with
  ball-by-ball data, so league matches abandoned without a ball being bowled are missing even
  though both teams got a point. Voided fixtures that were replayed are listed too.
"""

from __future__ import annotations

import csv
from functools import cache
from importlib import resources


@cache
def _rows(name: str) -> tuple[dict[str, str], ...]:
    with resources.files(__package__).joinpath(name).open(encoding="utf-8", newline="") as fh:
        return tuple(csv.DictReader(fh))


def teams() -> tuple[dict[str, str], ...]:
    return _rows("teams.csv")


def venues() -> tuple[dict[str, str], ...]:
    return _rows("venues.csv")


def match_adjustments() -> tuple[dict[str, str], ...]:
    return _rows("match_adjustments.csv")


def franchise_of(team: str) -> str:
    for row in teams():
        if row["team"] == team:
            return row["franchise"]
    return team
