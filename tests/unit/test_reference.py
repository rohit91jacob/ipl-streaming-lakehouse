from collections import Counter

from ipl_lakehouse import reference
from support import ALL_FIXTURES, VOID_2025, load_match


def test_reference_files_have_unique_keys():
    for rows, key in (
        (reference.teams(), "team"),
        (reference.venues(), "venue_raw"),
        (reference.match_adjustments(), "match_id"),
    ):
        counts = Counter(r[key] for r in rows)
        assert not [k for k, n in counts.items() if n > 1], f"duplicate {key}"


def test_franchise_lineage():
    assert reference.franchise_of("Delhi Daredevils") == "Delhi Capitals"
    assert reference.franchise_of("Kings XI Punjab") == "Punjab Kings"
    assert reference.franchise_of("Royal Challengers Bangalore") == "Royal Challengers Bengaluru"
    assert reference.franchise_of("Deccan Chargers") == "Deccan Chargers"  # not Sunrisers
    assert reference.franchise_of("Unknown XI") == "Unknown XI"


def test_fixture_teams_and_venues_are_mapped():
    teams = {r["team"] for r in reference.teams()}
    venues = {r["venue_raw"] for r in reference.venues()}
    for match_id in ALL_FIXTURES:
        info = load_match(match_id)["info"]
        assert set(info["teams"]) <= teams
        assert info["venue"] in venues


def test_adjustments_are_well_formed():
    rows = reference.match_adjustments()
    assert {r["adjustment"] for r in rows} == {"abandoned_no_ball", "void"}
    teams = {r["team"] for r in reference.teams()}
    venues = {r["venue_raw"] for r in reference.venues()}
    for r in rows:
        assert r["match_date"].startswith(r["season"])
        assert r["team1"] in teams and r["team2"] in teams
        assert r["venue_raw"] in venues
        assert r["stage"] == "League"
    assert [r["match_id"] for r in rows if r["adjustment"] == "void"] == [VOID_2025]
