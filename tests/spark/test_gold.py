import pytest

from ipl_lakehouse.cricket import InningsForNrr, nrr_credits
from support import ABSENT_HURT_2019, DLS_FINAL_2023, DOUBLE_SUPER_OVER, FINAL_2025

pytestmark = pytest.mark.spark


def _rows(df, where: str):
    return [r.asDict() for r in df.filter(where).collect()]


def test_scorecards_for_the_2025_final(gold):
    bat = {r["batter"]: r for r in _rows(gold("batting_scorecard"), f"match_id = '{FINAL_2025}'")}
    kohli = bat["V Kohli"]
    assert (kohli["runs"], kohli["balls"], kohli["batting_position"]) == (43, 35, 2)
    assert kohli["is_out"] and kohli["dismissal_kind"] == "caught and bowled"
    assert kohli["dismissal_bowler"] == "Azmatullah Omarzai"
    bowl = {r["bowler"]: r for r in _rows(gold("bowling_scorecard"), f"match_id = '{FINAL_2025}'")}
    krunal = bowl["KH Pandya"]
    assert (krunal["overs"], krunal["runs_conceded"], krunal["wickets"], krunal["economy"]) == (
        "4.0",
        17,
        2,
        4.25,
    )
    assert (bowl["Arshdeep Singh"]["wickets"], bowl["Arshdeep Singh"]["runs_conceded"]) == (3, 40)
    summary = _rows(gold("match_summary"), f"match_id = '{FINAL_2025}'")[0]
    assert summary["result_text"] == "Royal Challengers Bengaluru won by 6 runs"
    assert (summary["first_innings_score"], summary["second_innings_score"]) == (
        "190/9 (20.0)",
        "184/7 (20.0)",
    )


def test_result_texts(gold):
    texts = {r["match_id"]: r["result_text"] for r in gold("match_summary").collect()}
    assert texts[DLS_FINAL_2023] == "Chennai Super Kings won by 5 wickets (DLS)"
    assert texts[DOUBLE_SUPER_OVER] == "Match tied (Kings XI Punjab won the super over)"


def test_points_table_on_fixture_league_matches(gold):
    table = {(r["season"], r["team"]): r for r in gold("points_table").collect()}
    dc, mi = table[(2019, "Delhi Capitals")], table[(2019, "Mumbai Indians")]
    assert (dc["won"], dc["points"], mi["lost"], mi["points"]) == (1, 2, 1, 0)
    # MI were bowled out (absent hurt) in 19.2 overs: charged the full 20 overs.
    assert (mi["runs_for"], mi["balls_for"]) == (176, 120)
    assert dc["net_run_rate"] == pytest.approx((213 - 176) / 20, abs=1e-3)
    rcb = table[(2019, "Royal Challengers Bangalore")]
    assert (rcb["played"], rcb["no_result"], rcb["points"], rcb["balls_for"]) == (1, 1, 1, 0)
    kxip = table[(2020, "Kings XI Punjab")]
    assert (kxip["won"], kxip["tied"], kxip["points"]) == (1, 1, 2)
    assert (2025, "Punjab Kings") not in table, "the voided Dharamsala match must not count"
    # Abandoned-without-a-ball matches come from reference data (Cricsheet has no file).
    assert table[(2025, "Kolkata Knight Riders")]["no_result"] == 1
    assert table[(2024, "Gujarat Titans")]["points"] == 2  # two washouts in 2024


@pytest.mark.parametrize("match_id", [ABSENT_HURT_2019, DOUBLE_SUPER_OVER])
def test_spark_nrr_credits_equal_python_rules(gold, silver, match_id):
    inn = {
        r.innings_number: r for r in silver("innings").filter(f"match_id = '{match_id}'").collect()
    }
    i1, i2 = inn[1], inn[2]
    first, second = nrr_credits(
        InningsForNrr(i1.runs, i1.official_balls, i1.all_out),
        InningsForNrr(i2.runs, i2.official_balls, i2.all_out),
        i2.target_runs,
        i2.target_balls,
    )
    season = i1.season
    table = {r["team"]: r for r in _rows(gold("points_table"), f"season = {season}")}
    assert (table[i1.batting_team]["runs_for"], table[i1.batting_team]["balls_for"]) == (
        first.runs_for,
        first.balls_for,
    )
    assert (table[i2.batting_team]["runs_for"], table[i2.batting_team]["balls_for"]) == (
        second.runs_for,
        second.balls_for,
    )


def test_head_to_head_uses_franchises(gold):
    pairs = {(r["franchise_a"], r["franchise_b"]): r for r in gold("head_to_head").collect()}
    mi_pbks = pairs[("Mumbai Indians", "Punjab Kings")]  # played as Kings XI Punjab in 2020
    assert (mi_pbks["matches"], mi_pbks["super_over_finishes"], mi_pbks["franchise_b_wins"]) == (
        1,
        1,
        1,
    )
    assert ("Delhi Capitals", "Punjab Kings") not in pairs, "voided match is excluded"


def test_season_aggregates_exclude_super_overs(gold, silver):
    bat = {r["player_name"]: r for r in _rows(gold("player_season_batting"), "season = 2020")}
    # KL Rahul made 77 in the main innings of the double super over match (plus super-over runs).
    main_runs = (
        silver("deliveries")
        .filter(f"match_id = '{DOUBLE_SUPER_OVER}' AND NOT is_super_over AND batter = 'KL Rahul'")
        .groupBy()
        .sum("runs_batter")
        .collect()[0][0]
    )
    assert bat["KL Rahul"]["runs"] == main_runs == 77


def test_phase_and_venue_marts(gold):
    phases = _rows(
        gold("team_phase_stats"),
        "season = 2025 AND team = 'Royal Challengers Bengaluru' AND perspective = 'batting'",
    )
    assert {p["phase"] for p in phases} == {"powerplay", "middle", "death"}
    assert sum(p["legal_balls"] for p in phases) == 120  # only the final among the fixtures
    venue = _rows(gold("venue_stats"), "venue = 'Narendra Modi Stadium'")[0]
    assert venue["matches"] == 2 and venue["highest_total"] == 214
