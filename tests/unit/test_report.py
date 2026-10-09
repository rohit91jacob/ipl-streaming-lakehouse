"""The static results site, built from tiny gold tables written with delta-rs (no JVM)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from ipl_lakehouse.cli import build_parser
from ipl_lakehouse.report import build_site
from support import make_settings

NOW = datetime(2026, 10, 9, 5, 0, tzinfo=UTC)


def _match(match_id, season, day, stage, winner, result, pom):
    return {
        "match_id": match_id,
        "season": season,
        "match_date": day,
        "stage": stage,
        "venue": "Wankhede Stadium",
        "first_batting_team": "Mumbai Indians",
        "first_innings_score": "180/5 (20.0)",
        "second_batting_team": "Chennai Super Kings",
        "second_innings_score": "170/8 (20.0)",
        "result_type": "win",
        "winner": winner,
        "result_text": result,
        "player_of_match": pom,
    }


def _bat(season, pid, name, runs, balls, outs, team="Mumbai Indians"):
    return {
        "season": season,
        "player_id": pid,
        "player_name": name,
        "teams": [team],
        "matches": 14,
        "innings": 14,
        "runs": runs,
        "balls": balls,
        "outs": outs,
        "highest_score_text": "88*",
        "average": runs / outs if outs else None,
        "strike_rate": runs * 100 / balls,
        "fifties": 3,
        "hundreds": 0,
    }


def _bowl(season, pid, name, wickets, runs, balls):
    return {
        "season": season,
        "player_id": pid,
        "player_name": name,
        "teams": ["Chennai Super Kings"],
        "matches": 14,
        "overs": f"{balls // 6}.{balls % 6}",
        "balls": balls,
        "wickets": wickets,
        "runs_conceded": runs,
        "economy": runs * 6 / balls,
        "average": runs / wickets,
        "best_figures": "4/20",
    }


def _points(season, position, team, won, lost, nrr, tied=0, no_result=0):
    return {
        "season": season,
        "position": position,
        "team": team,
        "played": won + lost + no_result,
        "won": won,
        "lost": lost,
        "tied": tied,
        "no_result": no_result,
        "points": 2 * won + no_result,
        "net_run_rate": nrr,
    }


@pytest.fixture
def lake(tmp_path: Path):
    settings = make_settings(tmp_path)
    gold = settings.data_dir / "gold"
    tables = {
        "match_summary": [
            _match(
                "1",
                2024,
                date(2024, 5, 26),
                "Final",
                "Kolkata Knight Riders",
                "Kolkata Knight Riders won by 8 wickets",
                "MA Starc",
            ),
            _match(
                "2",
                2025,
                date(2025, 4, 1),
                "League",
                "Mumbai Indians",
                "Mumbai Indians won by 10 runs",
                "JJ Bumrah",
            ),
            _match(
                "3",
                2025,
                date(2025, 6, 3),
                "Final",
                "Royal Challengers Bengaluru",
                "Royal Challengers Bengaluru won by 6 runs",
                "KH Pandya",
            ),
        ],
        "points_table": [
            _points(2025, 1, "Punjab Kings", 9, 4, 0.372, no_result=1),
            _points(2025, 2, "Royal Challengers Bengaluru", 9, 4, 0.301, no_result=1),
            _points(2025, 3, "Gujarat Titans", 9, 5, 0.254),
            _points(2025, 4, "Mumbai Indians", 8, 6, 1.142),
            _points(2025, 5, "Delhi <Capitals>", 7, 6, 0.011, tied=1, no_result=1),
            _points(2025, 6, "Chennai Super Kings", 4, 10, -0.647),
        ],
        "player_season_batting": [
            _bat(2025, "a", "B Sai Sudharsan", 759, 486, 14),
            _bat(2025, "b", "Tied Slow", 700, 600, 10),
            _bat(2025, "c", "Tied Fast", 700, 400, 10),
            _bat(2024, "a", "B Sai Sudharsan", 527, 374, 11),
            _bat(2024, "d", "Never Out", 50, 30, 0),
        ],
        "player_season_bowling": [
            _bowl(2025, "x", "M Prasidh Krishna", 25, 488, 354),
            _bowl(2025, "y", "Noor Ahmad", 24, 408, 300),
            _bowl(2024, "x", "M Prasidh Krishna", 10, 300, 240),
        ],
    }
    for name, rows in tables.items():
        write_deltalake(str(gold / name), pa.Table.from_pylist(rows))
    return settings


class _Tables(HTMLParser):
    """Collects every table as {caption: [[cell text, ...], ...]}."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables: dict[str, list[list[str]]] = {}
        self._caption: str | None = None
        self._rows: list[list[str]] = []
        self._row: list[str] = []
        self._cell: str | None = None
        self._in_caption = False

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._caption, self._rows = "", []
        elif tag == "caption":
            self._in_caption = True
        elif tag == "tr":
            self._row = []
        elif tag in ("td", "th"):
            self._cell = ""

    def handle_endtag(self, tag):
        if tag == "table":
            self.tables[(self._caption or "").strip()] = self._rows
        elif tag == "caption":
            self._in_caption = False
        elif tag == "tr":
            self._rows.append(self._row)
        elif tag in ("td", "th") and self._cell is not None:
            self._row.append(self._cell.strip())
            self._cell = None

    def handle_data(self, data):
        if self._in_caption:
            self._caption = (self._caption or "") + data
        elif self._cell is not None:
            self._cell += data


def _tables(path: Path) -> dict[str, list[list[str]]]:
    parser = _Tables()
    parser.feed(path.read_text(encoding="utf-8"))
    return parser.tables


def test_site_layout_and_summary(lake, tmp_path):
    out = tmp_path / "site"
    summary = build_site(out, lake, now=NOW)
    assert summary["as_of"] == "2025-06-03"
    assert summary["latest_season"] == 2025
    assert summary["matches"] == 3
    assert summary["generated_at"] == "2026-10-09T05:00:00Z"
    for name in (
        "index.html",
        "style.css",
        ".nojekyll",
        "summary.json",
        "seasons/2025.html",
        "seasons/2024.html",
    ):
        assert (out / name).exists(), name
    assert json.loads((out / "summary.json").read_text()) == summary


def test_points_table_and_qualification_marker(lake, tmp_path):
    build_site(tmp_path / "site", lake, now=NOW)
    rows = _tables(tmp_path / "site" / "seasons" / "2025.html")["IPL 2025 league stage"]
    assert rows[0] == ["#", "Team", "P", "W", "L", "SO", "NR", "Pts", "NRR"]
    assert rows[1] == ["1", "Punjab Kings Qualified", "14", "9", "4", "0", "1", "19", "+0.372"]
    assert rows[5][1] == "Delhi <Capitals>"  # escaped in HTML, text round-trips
    assert rows[5][-1] == "+0.011"
    assert rows[6][-1] == "-0.647"
    assert sum("Qualified" in r[1] for r in rows[1:]) == 4


def test_caps_ordering_and_tie_breaks(lake, tmp_path):
    build_site(tmp_path / "site", lake, now=NOW)
    tables = _tables(tmp_path / "site" / "index.html")
    orange = [r[0] for r in tables["Top 10 run scorers, IPL 2025"][1:]]
    assert orange == ["B Sai Sudharsan", "Tied Fast", "Tied Slow"]  # equal runs: higher SR first
    purple = tables["Top 10 wicket takers, IPL 2025"]
    assert purple[1][:5] == ["M Prasidh Krishna", "Chennai Super Kings", "14", "59.0", "25"]


def test_all_time_leaders_aggregate_seasons(lake, tmp_path):
    build_site(tmp_path / "site", lake, now=NOW)
    batting = _tables(tmp_path / "site" / "index.html")[
        "Most runs, all IPL seasons (super overs excluded)"
    ]
    sudharsan = next(r for r in batting if r[0] == "B Sai Sudharsan")
    assert sudharsan[1:4] == ["2", "28", "1286"]  # seasons, innings, runs
    assert sudharsan[4] == f"{1286 / 25:.2f}"
    never_out = next(r for r in batting if r[0] == "Never Out")
    assert never_out[4] == chr(0x2013)  # en dash: no dismissals, so no average (not a crash)


def test_champions_and_escaping(lake, tmp_path):
    build_site(tmp_path / "site", lake, now=NOW)
    index = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    champions = _tables(tmp_path / "site" / "index.html")["Winner of every IPL final"]
    assert champions[1][:2] == ["2025", "Royal Challengers Bengaluru"]
    assert champions[2][:2] == ["2024", "Kolkata Knight Riders"]
    season = (tmp_path / "site" / "seasons" / "2025.html").read_text(encoding="utf-8")
    assert "Delhi &lt;Capitals&gt;" in season and "<Capitals>" not in season
    assert "cricsheet.org" in index and "ODC-By" in index
    assert "<script" not in index and "http://" not in index.replace("http://www.w3", "")


def test_empty_lake_is_an_error(tmp_path):
    settings = make_settings(tmp_path)
    write_deltalake(
        str(settings.data_dir / "gold" / "match_summary"),
        pa.table(
            {
                "match_id": pa.array([], pa.string()),
                "season": pa.array([], pa.int32()),
                "match_date": pa.array([], pa.date32()),
            }
        ),
    )
    with pytest.raises(ValueError, match="empty"):
        build_site(tmp_path / "site", settings)


def test_report_command_parses():
    args = build_parser().parse_args(["report", "--out", "public"])
    assert args.command == "report"
    assert args.out == Path("public")
