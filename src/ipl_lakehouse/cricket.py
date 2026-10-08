"""Pure cricket domain rules shared by the batch marts, the streaming job and the tests.

Spark code re-expresses some of these rules as column expressions; the unit tests pin both
implementations to the same cases so they cannot drift apart.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

BALLS_PER_OVER = 6
T20_OVERS = 20
MAX_WICKETS = 10
SUPER_OVER_MAX_WICKETS = 2

# Dismissals credited to the bowler (run outs, retirements, obstruction etc. are not).
BOWLER_WICKET_KINDS = frozenset(
    {"bowled", "caught", "caught and bowled", "lbw", "stumped", "hit wicket"}
)
# Cricsheet records these in `wickets`, but they do not count against the batting side.
NON_DISMISSAL_KINDS = frozenset({"retired hurt", "retired not out"})

PHASE_POWERPLAY = "powerplay"
PHASE_MIDDLE = "middle"
PHASE_DEATH = "death"
PHASE_SUPER_OVER = "super_over"


def phase_for_over(over_number: int, is_super_over: bool = False) -> str:
    """Phase of a 1-based over number: powerplay 1-6, middle 7-15, death 16-20."""
    if is_super_over:
        return PHASE_SUPER_OVER
    if over_number < 1:
        raise ValueError(f"over_number is 1-based, got {over_number}")
    if over_number <= 6:
        return PHASE_POWERPLAY
    if over_number <= 15:
        return PHASE_MIDDLE
    return PHASE_DEATH


def overs_to_balls(overs: float | int | str) -> int:
    """Convert cricket over notation to balls: ``9.2`` means 9 overs and 2 balls (56 balls)."""
    value = float(overs)
    if value < 0 or math.isnan(value):
        raise ValueError(f"invalid overs value {overs!r}")
    whole = math.floor(value)
    part = round((value - whole) * 10)
    if part >= BALLS_PER_OVER:
        raise ValueError(f"invalid overs value {overs!r}: ball part must be 0-5")
    return whole * BALLS_PER_OVER + part


def balls_to_overs(balls: int) -> str:
    """Inverse of :func:`overs_to_balls`, rendered the way scorecards show it (``"19.4"``)."""
    if balls < 0:
        raise ValueError(f"balls must be >= 0, got {balls}")
    return f"{balls // BALLS_PER_OVER}.{balls % BALLS_PER_OVER}"


def run_rate(runs: int, balls: int) -> float | None:
    return None if balls <= 0 else runs * BALLS_PER_OVER / balls


def net_run_rate(
    runs_for: int, balls_for: int, runs_against: int, balls_against: int
) -> float | None:
    """NRR = runs scored per over minus runs conceded per over (aggregated over a season)."""
    rate_for, rate_against = run_rate(runs_for, balls_for), run_rate(runs_against, balls_against)
    if rate_for is None or rate_against is None:
        return None
    return rate_for - rate_against


@dataclass(frozen=True)
class InningsForNrr:
    runs: int
    balls: int  # balls as called by the umpires (every completed over counts as 6)
    all_out: bool


@dataclass(frozen=True)
class NrrCredit:
    """Runs and balls a team is credited with in one match for the NRR calculation."""

    runs_for: int
    balls_for: int
    runs_against: int
    balls_against: int


def nrr_credits(
    first: InningsForNrr,
    second: InningsForNrr,
    target_runs: int | None,
    target_balls: int | None,
    scheduled_balls: int = T20_OVERS * BALLS_PER_OVER,
) -> tuple[NrrCredit, NrrCredit]:
    """NRR credits for (team batting first, team batting second) in a match with a result.

    IPL follows the ICC playing conditions:

    * a side that is bowled out is charged its full quota of overs, not the overs it faced;
    * when the chasing side's target is revised (rain, DLS), the side batting first is
      credited with ``target - 1`` runs off the overs the chasing side was allotted. Cricsheet
      records the revised target (for a chase cut short it is the DLS par at the stoppage), so
      the same rule also covers matches decided on DLS par;
    * super overs and matches without a result never count (callers exclude them).
    """
    if target_runs is not None and target_balls is not None:
        first_runs, first_balls = target_runs - 1, target_balls
        second_quota = target_balls
    else:
        first_runs = first.runs
        first_balls = scheduled_balls if first.all_out else first.balls
        second_quota = scheduled_balls
    second_runs = second.runs
    second_balls = second_quota if second.all_out else second.balls
    return (
        NrrCredit(first_runs, first_balls, second_runs, second_balls),
        NrrCredit(second_runs, second_balls, first_runs, first_balls),
    )


def is_legal_delivery(wides: int, noballs: int) -> bool:
    return not wides and not noballs
