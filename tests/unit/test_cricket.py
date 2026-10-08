import pytest

from ipl_lakehouse.cricket import (
    InningsForNrr,
    balls_to_overs,
    net_run_rate,
    nrr_credits,
    overs_to_balls,
    phase_for_over,
    run_rate,
)


@pytest.mark.parametrize(
    ("over", "phase"),
    [
        (1, "powerplay"),
        (6, "powerplay"),
        (7, "middle"),
        (15, "middle"),
        (16, "death"),
        (20, "death"),
    ],
)
def test_phase_boundaries(over, phase):
    assert phase_for_over(over) == phase


def test_super_over_phase_and_invalid_over():
    assert phase_for_over(1, is_super_over=True) == "super_over"
    with pytest.raises(ValueError):
        phase_for_over(0)


@pytest.mark.parametrize(
    ("overs", "balls"), [(20, 120), (9.2, 56), ("19.4", 118), (0, 0), (0.5, 5), (15, 90)]
)
def test_overs_to_balls_uses_cricket_notation(overs, balls):
    assert overs_to_balls(overs) == balls


@pytest.mark.parametrize("bad", [9.6, -1, "nan"])
def test_overs_to_balls_rejects_invalid(bad):
    with pytest.raises(ValueError):
        overs_to_balls(bad)


def test_balls_to_overs_round_trip():
    assert balls_to_overs(118) == "19.4"
    assert balls_to_overs(120) == "20.0"
    assert all(overs_to_balls(balls_to_overs(b)) == b for b in range(200))
    with pytest.raises(ValueError):
        balls_to_overs(-1)


def test_rates():
    assert run_rate(190, 120) == 9.5
    assert run_rate(10, 0) is None
    assert net_run_rate(190, 120, 184, 120) == pytest.approx(0.3)
    assert net_run_rate(1, 0, 1, 6) is None


def test_nrr_full_match():
    first, second = nrr_credits(
        InningsForNrr(190, 120, False),
        InningsForNrr(184, 120, False),
        target_runs=191,
        target_balls=120,
    )
    assert (first.runs_for, first.balls_for, first.runs_against, first.balls_against) == (
        190,
        120,
        184,
        120,
    )
    assert (second.runs_for, second.balls_for) == (184, 120)


def test_nrr_bowled_out_side_is_charged_full_quota():
    # DC 213/6 v MI 176 all out in 19.2 overs (absent hurt): MI is charged 20 overs.
    first, second = nrr_credits(
        InningsForNrr(213, 120, False),
        InningsForNrr(176, 116, True),
        target_runs=214,
        target_balls=120,
    )
    assert (second.runs_for, second.balls_for) == (176, 120)
    assert first.balls_against == 120


def test_nrr_revised_target_credits_target_minus_one():
    # 2023 final: GT 214/4 (20); CSK set 171 in 15 overs (D/L) and got there in 15.
    first, second = nrr_credits(
        InningsForNrr(214, 120, False),
        InningsForNrr(171, 90, False),
        target_runs=171,
        target_balls=90,
    )
    assert (first.runs_for, first.balls_for) == (170, 90)
    assert (second.runs_for, second.balls_for, second.runs_against, second.balls_against) == (
        171,
        90,
        170,
        90,
    )


def test_nrr_chase_terminated_on_dls_par():
    # KKR 149/5; CSK 55/0 after 8 overs when rain ended play, par 52 (target recorded as 53).
    first, second = nrr_credits(
        InningsForNrr(149, 120, False),
        InningsForNrr(55, 48, False),
        target_runs=53,
        target_balls=48,
    )
    assert (first.runs_for, first.balls_for) == (52, 48)
    assert (second.runs_for, second.balls_for) == (55, 48)


def test_nrr_without_target_falls_back_to_quota():
    first, _ = nrr_credits(
        InningsForNrr(120, 108, True),
        InningsForNrr(121, 100, False),
        target_runs=None,
        target_balls=None,
    )
    assert (first.runs_for, first.balls_for) == (120, 120)
