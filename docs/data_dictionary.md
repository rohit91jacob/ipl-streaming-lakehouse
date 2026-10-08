# Data dictionary

All tables are Delta Lake tables under `IPL_DATA_DIR`. Every table except the raw match files can
be queried by its folder name with `ipl sql`. Conventions:

* `match_id` is Cricsheet's match id (the same as ESPNcricinfo's).
* `season` is the calendar year of the first match day. Cricsheet labels some seasons `2007/08`
  or `2020/21`; that label is kept in `season_label`.
* `innings_number` is 1-based. Super overs are innings 3+ with `is_super_over = true`.
* `over_index` is 0-based as in Cricsheet, so `"19.4"` is `over_index = 19`. `over_number` is
  1-based.
* *Legal ball* means not a wide and not a no-ball. *Official balls* are the balls as called by the
  umpires: a completed over always counts as 6, even the eight overs in IPL history that
  Cricsheet records as umpire miscounts (5 or 7 balls).

## Bronze (raw, immutable)

| Table | Grain / key | Notes |
|---|---|---|
| `bronze/cricsheet/matches/match_id=<id>/<sha256>.json` (files, not Delta) | One file per *version* of a match | Exact bytes from the archive. Content-addressed and never overwritten. |
| `bronze_manifest` (`bronze/cricsheet/manifest`) | One row per landed version: (`match_id`, `sha256`) | Columns: `size_bytes`, `bronze_path`, `data_version`, `season`, `match_date`, `change_type` (`new`/`changed`), `source_url`, `source_archive_sha256`, `source_last_modified`, `ingest_run_id`, `ingested_at`. The latest row per match is the current version. |
| `bronze/cricsheet/register/<sha256>.csv` (file) | Cricsheet people register | Feeds `silver.people` |
| `stream_events` (`bronze/stream_events`) | One row per Kafka message that passed validation; **duplicates kept** | Partitioned by `event_date`. Envelope columns (`event_id`, `event_type`, `schema_version`, `match_id`, `sequence`, `event_time`, `produced_at`, `source`), typed `payload` struct, `raw_value`, Kafka coordinates (`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`), `ingested_at`, `ingest_batch_id`. |

## Silver (conformed, typed)

All batch silver tables are partitioned by `season` and replaced per `match_id` on incremental
runs.

| Table | Grain / key | Key columns |
|---|---|---|
| `matches` | One row per match: `match_id` | `season`, `season_label`, `match_date`, `match_end_date` (reserve days), `stage` (`League`, `Qualifier 1`, `Eliminator`, `Final`, …), `is_playoff`, `team1`, `team2`, `toss_winner`, `toss_decision`, `result_type` (`win`/`tie`/`no_result`), `winner` (the super-over winner for ties), `loser`, `super_over_winner`, `win_by_runs`, `win_by_wickets`, `result_method` (`D/L`), `player_of_match[]`, `venue` + `city` + `country` (canonical, from `reference/venues.csv`), `venue_raw`, `city_raw`, `team1/team2/winner_franchise` (from `reference/teams.csv`), officials, `scheduled_overs`, `source_sha256`, `source_ingested_at` |
| `innings` | One row per innings: (`match_id`, `innings_number`) | `batting_team`, `bowling_team`, `is_super_over`, `runs`, `wickets` (retired hurt excluded), `legal_balls`, `official_balls`, `overs` (from official balls), `run_rate`, extras breakdown, `fours`, `sixes`, `dot_balls`, `all_out` (10 out, counting absent-hurt players; 2 in a super over), `target_runs`, `target_overs`, `target_balls` (revised DLS targets as recorded), `scheduled_balls`, `absent_hurt[]`, `penalty_runs_pre/post`, `miscounted_overs`, `powerplays` |
| `deliveries` | One row per ball bowled, wides and no-balls included: (`match_id`, `innings_number`, `over_index`, `delivery_seq`); `delivery_id` is the same key as a string | `batter`, `non_striker`, `bowler`, `runs_batter`, `runs_extras`, `runs_total`, `wides`, `noballs`, `byes`, `legbyes`, `penalty`, `is_legal`, `batter_faced` (not a wide), `is_four`/`is_six` (all-run fours excluded), `is_dot`, `bowler_runs` (runs charged to the bowler), `team_wickets`, `bowler_wickets`, `players_out[]`, `wicket_kinds[]`, `phase` (`powerplay` overs 1-6, `middle` 7-15, `death` 16-20, `super_over`), `in_powerplay` (the official powerplay from Cricsheet, which differs from overs 1-6 in shortened games), `legal_ball_in_over`, `ball_label` (derived `over.ball`, checked against `source_ball_label`), `legal_ball_number`, `team_runs_after`, `team_wickets_after`, DRS `review_*`, `umpire_miscount`, `target_runs/target_balls` |
| `wickets` | One row per dismissal event: (`match_id`, `innings_number`, `over_index`, `delivery_seq`, `wicket_seq`) | `player_out`, `player_out_id`, `kind`, `fielders[]`, `fielder_is_substitute`, `is_bowler_wicket`, `credited_bowler`, `counts_as_team_wicket` (false for retired hurt), `team_wicket_number`, `team_runs_at_fall`, `ball_label` |
| `match_players` | One row per player listed for a team: (`match_id`, `team`, `player_name`) | `player_id` (Cricsheet registry id), `lineup_order`, `is_impact_substitute` |
| `substitutions` | One row per match-level replacement | `team`, `player_in`, `player_out`, `reason` (`impact_player`, `concussion_substitute`, …), with the delivery where it happened |
| `people` | One row per person who appears in IPL data: `player_id` | `name`, `unique_name`, `cricinfo_id`, `cricbuzz_id` |
| `stream_deliveries` | One row per distinct delivery `event_id` from the live feed | Flattened delivery fields, `event_time`, `first_seen_at`. Insert-only MERGE, so it is deduplicated. |
| `stream_matches` | One row per live match: `match_id` | Teams, venue and toss (from `match_started`); `winner`, `result`, margins and `completed_at` (from `match_completed`); `last_event_time` |

## Gold (marts; rebuilt atomically on every batch run)

| Table | Grain / key | What it answers |
|---|---|---|
| `innings_summary` | (`match_id`, `innings_number`), super overs included | Team score per innings: `score_text` (`190/9 (20.0)`), runs, wickets, legal and official balls, run rate, extras, boundaries, dots, `all_out`, target. **The batch side of the streaming reconciliation.** |
| `match_summary` | `match_id` | One line per match: teams, toss, both first-innings scores, `result_text` (`Chennai Super Kings won by 5 wickets (DLS)`, `Match tied (Kings XI Punjab won the super over)`, `No result`), player of the match |
| `batting_scorecard` | (`match_id`, `innings_number`, `batter`) | `batting_position`, `runs`, `balls`, `fours`, `sixes`, `dots`, `strike_rate`, `is_out`, `dismissal_kind`/`_bowler`/`_fielders`, fall-of-wicket number/score/ball, `player_id` |
| `bowling_scorecard` | (`match_id`, `innings_number`, `bowler`) | `bowling_order`, `overs`, `maidens` (complete six-ball overs by one bowler conceding 0), `runs_conceded` (byes and leg byes excluded), `wickets` (bowler-credited), `economy`, `dots`, boundaries conceded, `wides`, `noballs`, `player_id` |
| `player_season_batting` | (`season`, `player_id`); super overs excluded | `matches`, `innings`, `runs`, `balls`, `outs`, `not_outs`, `highest_score(_text)` (`*` for not out), `average`, `strike_rate`, `fifties`, `hundreds`, `ducks`, `fours`, `sixes`, `teams[]` |
| `player_season_bowling` | (`season`, `player_id`); super overs excluded | `matches`, `innings`, `overs`, `runs_conceded`, `wickets`, `maidens`, `dots`, `best_figures` (most wickets, then fewest runs), `average`, `economy`, `strike_rate`, `four_wickets`, `five_wickets`, `teams[]` |
| `points_table` | (`season`, `team`); league stage only | `played`, `won`, `lost` (super-over results count as W/L), `tied`, `no_result` (includes matches abandoned without a ball, from `reference/match_adjustments.csv`; voided fixtures are excluded), `points`, `runs_for`, `balls_for`/`overs_for`, `runs_against`, `balls_against`/`overs_against`, `net_run_rate` (3 dp), `position` (points, then NRR) |
| `venue_stats` | `venue` (canonical) | `matches`, seasons active, bat-first vs chasing wins, `chasing_win_pct`, `avg_first/second_innings_runs` (unreduced 20-over games only), `toss_winner_win_pct`, `field_first_pct`, highest total and lowest all-out total with team |
| `team_phase_stats` | (`season`, `team`, `perspective` = batting/bowling, `phase`) | `runs`, `legal_balls`, `wickets`, `run_rate`, `boundary_pct`, `dot_pct`, `runs_per_wicket` |
| `head_to_head` | (`franchise_a`, `franchise_b`), alphabetical, all stages | `matches`, wins each, `super_over_finishes`, `no_results`, first and last meeting, `last_winner`. Franchises merge renamed teams (Delhi Daredevils → Delhi Capitals, Kings XI Punjab → Punjab Kings, …). |
| `live_scorecard` | (`match_id`, `innings_number`), from the stream | `score_text`, `runs`, `wickets`, `legal_balls`, `overs`, `run_rate`, `target_runs`, `runs_required`, `balls_remaining`, `required_run_rate`, `striker`, `non_striker`, `bowler`, `last_ball`, `last_event_id`, `match_status` (`live`/`completed`), `match_winner`, `last_event_time`, `updated_at` |
| `stream_activity` | (`window_start`, `match_id`): 1-minute event-time windows | `events`, `deliveries`, `runs`, `wickets` after deduplication within the watermark |

## Ops

| Table | Grain | Notes |
|---|---|---|
| `dq_results` | One row per check per run | `name`, `table`, `layer` (`silver`/`gold`/`stream`), `severity` (`error`/`warn`), `passed`, `violation_count`, `sample` (JSON, up to 5 rows), `run_id`, `checked_at` |
| `stream_dlq` | One row per rejected Kafka message | `raw_value`, `kafka_key`, Kafka coordinates, `error_reason`, `dlq_at`, `ingest_batch_id` (see [streaming.md](streaming.md#malformed-data-dead-letter-queue)) |

## Reference data (`src/ipl_lakehouse/reference/`)

| File | Purpose |
|---|---|
| `teams.csv` | Team name as played → franchise and short names |
| `venues.csv` | 60 Cricsheet venue spellings → canonical venue, city, country (merging renames such as Feroz Shah Kotla → Arun Jaitley Stadium and Sheikh Zayed → Zayed Cricket Stadium) |
| `match_adjustments.csv` | 12 league matches abandoned without a ball being bowled (1 point each; Cricsheet has no file for them), plus the voided 2025 PBKS v DC match that was replayed in full |
