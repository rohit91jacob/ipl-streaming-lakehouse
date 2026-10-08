import json

import pytest

from ipl_lakehouse.batch.silver import build_silver
from ipl_lakehouse.cricsheet import innings_totals, iter_deliveries
from ipl_lakehouse.ingest.cricsheet import run_ingest
from support import (
    ABSENT_HURT_2019,
    ALL_FIXTURES,
    DLS_FINAL_2023,
    DOUBLE_SUPER_OVER,
    FINAL_2025,
    FIXTURES,
    NO_RESULT_2019,
    build_archive,
    load_match,
    make_settings,
)

pytestmark = pytest.mark.spark


def _row(df, **where):
    rows = df.filter(" AND ".join(f"{k} = '{v}'" for k, v in where.items())).collect()
    assert len(rows) == 1, rows
    return rows[0].asDict()


def test_row_counts_match_the_source(silver):
    assert silver("matches").count() == len(ALL_FIXTURES)
    expected = sum(len(list(iter_deliveries(load_match(m)))) for m in ALL_FIXTURES)
    assert silver("deliveries").count() == expected == 1097
    assert silver("innings").count() == sum(len(load_match(m)["innings"]) for m in ALL_FIXTURES)


def test_innings_totals_reconcile_with_pure_python(silver):
    innings = {(r.match_id, r.innings_number): r for r in silver("innings").collect()}
    for match_id in ALL_FIXTURES:
        for t in innings_totals(load_match(match_id)):
            row = innings[(match_id, t.innings_number)]
            assert (row.batting_team, row.runs, row.wickets, row.legal_balls, row.is_super_over) == (
                t.batting_team, t.runs, t.wickets, t.legal_balls, t.is_super_over,
            )  # fmt: skip


def test_match_attributes(silver):
    final = _row(silver("matches"), match_id=FINAL_2025)
    assert final["season"] == 2025 and final["stage"] == "Final" and final["is_playoff"]
    assert final["winner"] == "Royal Challengers Bengaluru" and final["win_by_runs"] == 6
    assert final["loser"] == "Punjab Kings"
    assert (final["venue"], final["city"]) == ("Narendra Modi Stadium", "Ahmedabad")
    assert final["player_of_match"] == ["KH Pandya"]

    dls = _row(silver("matches"), match_id=DLS_FINAL_2023)
    assert dls["result_method"] == "D/L" and dls["win_by_wickets"] == 5

    tie = _row(silver("matches"), match_id=DOUBLE_SUPER_OVER)
    assert (
        tie["result_type"] == "tie"
        and tie["winner"] == tie["super_over_winner"] == "Kings XI Punjab"
    )
    assert tie["season"] == 2020 and tie["season_label"] == "2020/21"
    assert tie["winner_franchise"] == "Punjab Kings"

    nr = _row(silver("matches"), match_id=NO_RESULT_2019)
    assert nr["result_type"] == "no_result" and nr["winner"] is None
    assert nr["venue"] == "M Chinnaswamy Stadium"  # "M.Chinnaswamy Stadium" folded


def test_innings_flags(silver):
    inn = silver("innings")
    assert _row(inn, match_id=DLS_FINAL_2023, innings_number=2)["target_balls"] == 90
    mi = _row(inn, match_id=ABSENT_HURT_2019, innings_number=2)
    assert (mi["wickets"], mi["all_out"], mi["absent_hurt"]) == (9, True, ["JJ Bumrah"])
    assert inn.filter(f"match_id = '{DOUBLE_SUPER_OVER}' AND is_super_over").count() == 4


def test_delivery_derivations(silver):
    d = silver("deliveries")
    assert d.filter("ball_label != source_ball_label").count() == 0
    phases = {
        (r.over_number, r.phase)
        for r in d.filter("NOT is_super_over").select("over_number", "phase").distinct().collect()
    }
    assert all(
        p == ("powerplay" if o <= 6 else "middle" if o <= 15 else "death") for o, p in phases
    )
    assert d.filter("is_super_over AND phase != 'super_over'").count() == 0
    legal = d.filter(f"match_id = '{FINAL_2025}' AND innings_number = 1 AND is_legal").count()
    assert legal == 120
    first = _row(d, match_id=FINAL_2025, innings_number=1, over_index=0, delivery_seq=1)
    assert first["delivery_id"] == "1473511-1-0-1" and first["in_powerplay"]


def test_wickets_and_players(silver):
    w = (
        silver("wickets")
        .filter(f"match_id = '{FINAL_2025}' AND innings_number = 1")
        .orderBy("team_wicket_number")
    )
    assert [r.team_wicket_number for r in w.collect()] == list(range(1, 10))
    run_outs = silver("wickets").filter("kind = 'run out'").collect()
    assert all(r.credited_bowler is None for r in run_outs)
    players = silver("match_players").filter(f"match_id = '{FINAL_2025}'")
    assert players.count() == 24 and players.filter("player_id IS NULL").count() == 0
    impact = {r.player_name for r in players.filter("is_impact_substitute").collect()}
    assert impact == {"P Simran Singh", "Suyash Sharma"}


def test_incremental_rebuild_only_touches_changed_matches(spark, tmp_path):
    settings = make_settings(tmp_path)
    run_ingest(settings, archive_path=build_archive(tmp_path / "a.zip"), include_register=False)
    assert len(build_silver(spark, settings).processed_match_ids) == len(ALL_FIXTURES)
    assert build_silver(spark, settings).processed_match_ids == []

    corrected = json.loads((FIXTURES / f"{NO_RESULT_2019}.json").read_text())
    corrected["info"]["player_of_match"] = ["V Kohli"]
    others = [m for m in ALL_FIXTURES if m != NO_RESULT_2019]
    zip2 = build_archive(
        tmp_path / "b.zip", others, extra={f"{NO_RESULT_2019}.json": json.dumps(corrected).encode()}
    )
    run_ingest(settings, archive_path=zip2, include_register=False)
    assert build_silver(spark, settings).processed_match_ids == [NO_RESULT_2019]

    matches = spark.read.format("delta").load(str(settings.lake.silver("matches")))
    assert matches.count() == len(ALL_FIXTURES)
    assert _row(matches, match_id=NO_RESULT_2019)["player_of_match"] == ["V Kohli"]
    deliveries = spark.read.format("delta").load(str(settings.lake.silver("deliveries")))
    assert deliveries.count() == 1097, "replaceWhere must not duplicate rows"
