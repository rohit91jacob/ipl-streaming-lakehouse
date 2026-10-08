# Verification

These checks were run against the **full** Cricsheet IPL archive downloaded on 8 October 2026
(1,243 matches, 2008 – 2026), built from scratch with `uv run ipl batch`.

## Batch build

| Check | Result |
|---|---|
| `ipl batch` from an empty data directory | exit 0 in 8 min 49 s (12-core laptop shared with other workloads, `local[4]`, 2 GB driver). |
| Rows | 1,243 matches · 2,514 innings · 295,732 deliveries · 14,705 wickets, equal to a pure-Python count over the archive |
| Data-quality checks | 14 silver + 6 gold checks: 0 failures and 0 warnings. This includes the independent cross-check that every unrevised chase target equals the first-innings total + 1. |
| Second `ipl batch` | Archive not modified (HTTP 304), 0 matches reprocessed in silver, gold rebuilt |

## Points tables vs. published standings: 165 / 166 team-seasons identical

For every season from 2008 to 2026, `gold.points_table` was compared column by column (played,
won, lost, no result, points, NRR to 3 dp) with the league tables on the Wikipedia season pages
(`https://en.wikipedia.org/wiki/<year>_Indian_Premier_League`).

* **165 of 166** team-seasons are identical.
* The single difference is **2025 Delhi Capitals**: Wikipedia shows NRR −0.011, and the pipeline
  computes **+0.011**. Cricbuzz's official standings
  (`cricbuzz.com/cricket-series/9237/indian-premier-league-2025/points-table`) list DC at
  **+0.011**, so this is a sign typo on Wikipedia.

The match got exactly right because of these rules, each of which had to be implemented:

| Rule | Where it mattered |
|---|---|
| A side bowled out is charged its full quota of overs | Every season. It also counts "all out" sides with an absent-hurt player (e.g. MI v DC 2019, 176 all out for 9). |
| After a revised target, the side batting first is credited with target − 1 off the chasing side's allotted overs; for chases cut short, Cricsheet records the DLS par target | 23 D/L matches, e.g. 2008 KKR v CSK, 2009 KKR v KXIP (9.2-over par), 2016 RCB v KXIP |
| Umpire-miscounted overs (5 or 7 legal balls) count as 6 balls | Without this, 9 team-seasons in 2008, 2009, 2011 and 2018 differed in the 3rd decimal |
| Super overs are excluded from NRR, and the super-over winner gets the 2 points | 16 tied matches, including the 2020 double super over |
| Matches abandoned without a ball give 1 point each, although Cricsheet has no file for them | 12 matches in 2008, 2009, 2011, 2012, 2015, 2017, 2024 and 2025 (`reference/match_adjustments.csv`) |
| A voided fixture (2025 PBKS v DC, stopped at 10.1 overs and replayed in full) awards no points | 2025 |

## Season match counts

Cricsheet's league matches, plus the abandoned-without-a-ball matches, minus the voided fixture,
equal the number of league matches on the published tables in **all 19 seasons**. For example,
2024 has 67 Cricsheet league matches + 3 washouts = 70, and 2025 has 70 + 1 washout − 1 void = 70.

| Season | Cricsheet matches (all stages) | League | Published league matches |
|---|---|---|---|
| 2008 | 58 | 55 | 56 |
| 2009 | 57 | 54 | 56 |
| 2010 | 60 | 56 | 56 |
| 2011 | 73 | 69 | 70 |
| 2012 | 74 | 70 | 72 |
| 2013 | 76 | 72 | 72 |
| 2014 | 60 | 56 | 56 |
| 2015 | 59 | 55 | 56 |
| 2016 – 2021 | 59 – 60 | 55 – 56 | 56 |
| 2022, 2023, 2025, 2026 | 74 | 70 | 70 |
| 2024 | 71 | 67 | 70 |

## Well-known player numbers

| Fact | Pipeline (`player_season_*`) |
|---|---|
| Virat Kohli, IPL 2016: 973 runs, 4 hundreds | 973 runs, 16 innings, 4 hundreds, 7 fifties |
| 2016 Purple Cap: Bhuvneshwar Kumar, 23 wickets | B Kumar 23 |
| 2024 Orange Cap: Kohli 741; Purple Cap: Harshal Patel 24 | V Kohli 741; HV Patel 24 |
| 2025 Orange Cap: Sai Sudharsan 759; Purple Cap: Prasidh Krishna 25 | B Sai Sudharsan 759; M Prasidh Krishna 25 |
| 2025 final: RCB 190/9 beat PBKS 184/7 by 6 runs; Kohli 43 (35); Krunal Pandya 2/17 | Asserted in `tests/spark/test_gold.py` |

## Streaming drill (real Kafka, real Spark)

The drill used a local single-node Kafka 4.3.1 (KRaft; the binary's sha512 was verified),
`ipl stream` in continuous mode with 5 s triggers, and the 2025 final replayed at 60× speed:

1. Mid-chase, the live scorecard read: **PBKS 87/3 (10.1), target 191, need 104, required rate
   10.58**, status `live`.
2. The stream processor was then **killed with SIGKILL** (Python driver and JVM).
3. It was restarted from the same checkpoints. The producer finished the match, then **replayed
   the whole match again with every 5th event duplicated** (558 raw events for 254 distinct ones).
4. Final live scorecard: RCB 190/9 (20.0), PBKS 184/7 (20.0), status `completed`.
   `silver/stream_deliveries` holds exactly **252** rows, one per delivery.
5. `ipl verify-stream --match-id 1473511` returned **OK against both the source file and gold**
   (exit 0).

The same properties are tested automatically: `tests/spark/test_streaming.py` (file source with
duplicates, every DLQ reason, restarts, full replays, a DQ stop) and
`tests/integration/test_kafka_e2e.py` (a real broker, both locally and in CI).
