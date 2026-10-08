from ipl_lakehouse.cricsheet import (
    innings_totals,
    iter_deliveries,
    match_id_from_filename,
    season_of,
    validate_match,
)
from support import (
    ABSENT_HURT_2019,
    ALL_FIXTURES,
    DLS_FINAL_2023,
    DOUBLE_SUPER_OVER,
    FINAL_2025,
    load_match,
)


def test_fixtures_are_valid_matches():
    for match_id in ALL_FIXTURES:
        assert validate_match(load_match(match_id)) == []


def test_validate_reports_structural_problems():
    assert validate_match([]) == ["document is not a JSON object"]
    problems = validate_match({"meta": {}, "info": {"teams": ["A"]}, "innings": {}})
    assert "missing info.dates" in problems
    assert "info.teams must list exactly two teams" in problems
    assert "innings is not a list" in problems


def test_match_id_from_filename():
    assert match_id_from_filename("1473511.json") == "1473511"
    assert match_id_from_filename("ipl_json/1473511.json") == "1473511"
    assert match_id_from_filename("README.txt") is None


def test_season_is_calendar_year_of_first_day():
    assert season_of({"info": {"dates": ["2008-04-18"], "season": "2007/08"}}) == 2008
    assert season_of(load_match(DOUBLE_SUPER_OVER)) == 2020  # Cricsheet labels it "2020/21"


def test_final_2025_totals():
    totals = [t.as_dict() for t in innings_totals(load_match(FINAL_2025))]
    assert [(t["batting_team"], t["runs"], t["wickets"], t["overs"]) for t in totals] == [
        ("Royal Challengers Bengaluru", 190, 9, "20.0"),
        ("Punjab Kings", 184, 7, "20.0"),
    ]
    assert totals[1]["target_runs"] == 191 and totals[1]["target_balls"] == 120


def test_double_super_over_has_six_innings():
    totals = innings_totals(load_match(DOUBLE_SUPER_OVER))
    assert [(t.batting_team, t.runs, t.wickets, t.is_super_over) for t in totals] == [
        ("Mumbai Indians", 176, 6, False),
        ("Kings XI Punjab", 176, 6, False),
        ("Kings XI Punjab", 5, 2, True),
        ("Mumbai Indians", 5, 1, True),
        ("Mumbai Indians", 11, 1, True),
        ("Kings XI Punjab", 15, 0, True),
    ]


def test_dls_target_is_revised():
    second = innings_totals(load_match(DLS_FINAL_2023))[1]
    assert (second.runs, second.wickets, second.target_runs, second.target_balls) == (
        171,
        5,
        171,
        90,
    )


def test_deliveries_have_consistent_runs_and_bowling_team():
    match = load_match(ABSENT_HURT_2019)
    deliveries = list(iter_deliveries(match))
    assert len(deliveries) == 246
    for d in deliveries:
        assert d.runs_total == d.runs_batter + d.runs_extras
        assert d.bowling_team != d.batting_team and d.bowling_team in match["info"]["teams"]
    # MI were 176 "all out" with nine wickets: Bumrah was absent hurt.
    assert innings_totals(match)[1].wickets == 9
