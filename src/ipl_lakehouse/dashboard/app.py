"""Streamlit dashboard over the gold tables (read with delta-rs; no Spark needed).

Run with ``ipl dashboard`` (installs: ``uv sync --extra dashboard``).
"""

from __future__ import annotations

import streamlit as st

from ipl_lakehouse.config import Settings
from ipl_lakehouse.query import discover_tables, run_sql

st.set_page_config(page_title="IPL lakehouse", page_icon=":cricket_bat_and_ball:", layout="wide")
settings = Settings.from_env()


def query(sql: str):
    return run_sql(settings, sql).to_pandas()


def has(*tables: str) -> bool:
    available = discover_tables(settings)
    missing = [t for t in tables if t not in available]
    if missing:
        st.info(
            f"Tables not built yet: {', '.join(missing)}. Run `ipl batch` / `ipl stream` first."
        )
    return not missing


st.title("IPL lakehouse")
live, standings, leaders, rivalry, venues, ops = st.tabs(
    ["Live", "Points table", "Season leaders", "Head to head", "Venues", "Pipeline health"]
)

with live:

    @st.fragment(run_every="5s")
    def live_scores() -> None:
        if not has("live_scorecard"):
            return
        df = query(
            """SELECT match_id, innings_number, batting_team, score_text, run_rate, target_runs,
                      runs_required, balls_remaining, required_run_rate, striker, non_striker,
                      bowler, last_ball, match_status, last_event_time
               FROM live_scorecard ORDER BY last_event_time DESC, innings_number DESC LIMIT 40"""
        )
        st.dataframe(df, hide_index=True, use_container_width=True)

    live_scores()

with standings:
    if has("points_table"):
        seasons = query("SELECT DISTINCT season FROM points_table ORDER BY season DESC")["season"]
        season = st.selectbox("Season", seasons.tolist(), key="pt_season")
        st.dataframe(
            query(
                f"""SELECT position, team, played, won, lost, no_result, points, net_run_rate,
                           runs_for, overs_for, runs_against, overs_against
                    FROM points_table WHERE season = {int(season)} ORDER BY position"""
            ),
            hide_index=True,
            use_container_width=True,
        )

with leaders:
    if has("player_season_batting", "player_season_bowling"):
        seasons = query("SELECT DISTINCT season FROM player_season_batting ORDER BY season DESC")[
            "season"
        ]
        season = st.selectbox("Season", seasons.tolist(), key="ld_season")
        left, right = st.columns(2)
        left.subheader("Most runs")
        left.dataframe(
            query(
                f"""SELECT player_name, matches, innings, runs, highest_score_text AS hs, average,
                           strike_rate, fifties, hundreds, sixes
                    FROM player_season_batting WHERE season = {int(season)}
                    ORDER BY runs DESC LIMIT 15"""
            ),
            hide_index=True,
        )
        right.subheader("Most wickets")
        right.dataframe(
            query(
                f"""SELECT player_name, matches, overs, wickets, best_figures, economy, average
                    FROM player_season_bowling WHERE season = {int(season)}
                    ORDER BY wickets DESC, economy ASC LIMIT 15"""
            ),
            hide_index=True,
        )

with rivalry:
    if has("head_to_head"):
        st.dataframe(
            query(
                """SELECT franchise_a, franchise_b, matches, franchise_a_wins, franchise_b_wins,
                          super_over_finishes, no_results, last_match_date, last_winner
                   FROM head_to_head ORDER BY matches DESC"""
            ),
            hide_index=True,
            use_container_width=True,
        )

with venues:
    if has("venue_stats"):
        st.dataframe(
            query(
                """SELECT venue, city, matches, avg_first_innings_runs, chasing_win_pct,
                          toss_winner_win_pct, highest_total, highest_total_team, first_season,
                          last_season
                   FROM venue_stats ORDER BY matches DESC"""
            ),
            hide_index=True,
            use_container_width=True,
        )

with ops:
    tables = discover_tables(settings)
    if "dq_results" in tables:
        st.subheader("Latest data-quality run")
        st.dataframe(
            query(
                """SELECT layer, name, severity, passed, violation_count, checked_at
                   FROM dq_results
                   WHERE run_id = (SELECT run_id FROM dq_results ORDER BY checked_at DESC LIMIT 1)
                   ORDER BY passed, layer, name"""
            ),
            hide_index=True,
            use_container_width=True,
        )
    if "stream_dlq" in tables:
        st.subheader("Dead-letter queue")
        st.dataframe(
            query("SELECT error_reason, COUNT(*) AS events FROM stream_dlq GROUP BY error_reason"),
            hide_index=True,
        )
    if "stream_activity" in tables:
        st.subheader("Feed activity (events per minute)")
        activity = query(
            "SELECT window_start, SUM(events) AS events FROM stream_activity GROUP BY window_start ORDER BY window_start"
        )
        st.line_chart(activity, x="window_start", y="events")
